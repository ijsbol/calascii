import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Final, Literal, TypedDict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse


__all__: tuple[str, ...] = ()


CHUNK_SIZE: Final[int] = 50
SAVE_PATH: Final[Path] = Path(__file__).parent / "data.json"
SAVE_INTERVAL: Final[int] = 300  # seconds
BROADCAST_INTERVAL: Final[int] = 1  # seconds


class CalasciiData:
    def __init__(self) -> None:
        self._data: dict[int, dict[int, list[list[str]]]] = {}

    def _ensure_chunk(self, g_x: int, g_y: int) -> list[list[str]]:
        if g_x not in self._data:
            self._data[g_x] = {}
        if g_y not in self._data[g_x]:
            self._data[g_x][g_y] = [
                ["" for _ in range(CHUNK_SIZE)]
                for _ in range(CHUNK_SIZE)
            ]
        return self._data[g_x][g_y]

    def set(self, g_x: int, g_y: int, s_x: int, s_y: int, data: str) -> None:
        self._ensure_chunk(g_x, g_y)[s_x][s_y] = data

    def get(self, g_x: int, g_y: int) -> list[list[str]]:
        return self._ensure_chunk(g_x, g_y)

    def save(self, path: Path) -> None:
        serialized = {
            str(g_x): {str(g_y): chunk for g_y, chunk in cols.items()}
            for g_x, cols in self._data.items()
        }
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(serialized))
        tmp.replace(path)

    def load(self, path: Path) -> None:
        if not path.exists():
            return
        raw = json.loads(path.read_text())
        self._data = {
            int(g_x): {int(g_y): chunk for g_y, chunk in cols.items()}
            for g_x, cols in raw.items()
        }


class CalasciiRouter(FastAPI):
    connected_clients: dict[WebSocket, list[tuple[int, int]]] = {}
    client_ids: dict[WebSocket, str] = {}
    cursors: dict[str, tuple[int, int] | None] = {}
    data: CalasciiData = CalasciiData()
    pending_chunks: set[tuple[int, int]] = set()
    pending_cursor_updates: dict[str, tuple[int, int]] = {}
    pending_cursor_removes: set[str] = set()


class _MessageSet(TypedDict):
    type: Literal["set"]
    g_x: int
    g_y: int
    s_x: int
    s_y: int
    data: str


class _MessageGet(TypedDict):
    type: Literal["get"]
    g_x: int
    g_y: int


class _MessageCursorMove(TypedDict):
    type: Literal["cursor_move"]
    wx: int
    wy: int


class _MessageCursorClear(TypedDict):
    type: Literal["cursor_clear"]


type Message = _MessageSet | _MessageGet | _MessageCursorMove | _MessageCursorClear


class _UpdatePacketGet(TypedDict):
    type: Literal["update_packet_get"]
    g_x: int
    g_y: int
    data: list[list[str]]


async def _save_loop() -> None:
    while True:
        await asyncio.sleep(SAVE_INTERVAL)
        app.data.save(SAVE_PATH)


async def _broadcast_loop() -> None:
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL)

        if app.pending_chunks:
            chunks_snapshot = app.pending_chunks.copy()
            app.pending_chunks.clear()
            for (g_x, g_y) in chunks_snapshot:
                packet = _UpdatePacketGet({
                    "type": "update_packet_get",
                    "g_x": g_x,
                    "g_y": g_y,
                    "data": app.data.get(g_x=g_x, g_y=g_y),
                })
                for client, subscribed in app.connected_clients.items():
                    if (g_x, g_y) in subscribed:
                        await client.send_json(packet)

        if app.pending_cursor_removes:
            removes_snapshot = app.pending_cursor_removes.copy()
            app.pending_cursor_removes.clear()
            for client_id in removes_snapshot:
                for client in app.connected_clients:
                    await client.send_json({"type": "cursor_remove", "id": client_id})

        if app.pending_cursor_updates:
            updates_snapshot = app.pending_cursor_updates.copy()
            app.pending_cursor_updates.clear()
            for client_id, (wx, wy) in updates_snapshot.items():
                for client in app.connected_clients:
                    await client.send_json({
                        "type": "cursor_update",
                        "id": client_id,
                        "wx": wx,
                        "wy": wy,
                    })


@asynccontextmanager
async def lifecycle(app: CalasciiRouter):
    app.data.load(SAVE_PATH)
    save_task = asyncio.create_task(_save_loop())
    broadcast_task = asyncio.create_task(_broadcast_loop())
    yield
    save_task.cancel()
    broadcast_task.cancel()
    app.data.save(SAVE_PATH)


app = CalasciiRouter(
    lifespan=lifecycle,
)


async def process_message(message: Message, websocket: WebSocket) -> None:
    if message["type"] == "set":
        if (
            app.connected_clients.get(websocket) is None
            or (message["g_x"], message["g_y"]) not in app.connected_clients[websocket]
        ):
            return
        if not (0 <= message["s_x"] < CHUNK_SIZE and 0 <= message["s_y"] < CHUNK_SIZE):
            return
        app.data.set(
            g_x=message["g_x"],
            g_y=message["g_y"],
            s_x=message["s_x"],
            s_y=message["s_y"],
            data=message["data"],
        )
        app.pending_chunks.add((message["g_x"], message["g_y"]))

    elif message["type"] == "cursor_move":
        client_id = app.client_ids.get(websocket)
        if client_id is None:
            return
        app.cursors[client_id] = (message["wx"], message["wy"])
        app.pending_cursor_updates[client_id] = (message["wx"], message["wy"])
        app.pending_cursor_removes.discard(client_id)

    elif message["type"] == "cursor_clear":
        client_id = app.client_ids.get(websocket)
        if client_id is None:
            return
        app.cursors[client_id] = None
        app.pending_cursor_removes.add(client_id)
        app.pending_cursor_updates.pop(client_id, None)

    elif message["type"] == "get":
        if app.connected_clients.get(websocket) is None:
            return
        chunk_key = (message["g_x"], message["g_y"])
        if chunk_key not in app.connected_clients[websocket]:
            app.connected_clients[websocket].append(chunk_key)
        await websocket.send_json(_UpdatePacketGet({
            "type": "update_packet_get",
            "g_x": message["g_x"],
            "g_y": message["g_y"],
            "data": app.data.get(
                g_x=message["g_x"],
                g_y=message["g_y"],
            ),
        }))


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = str(uuid.uuid4())
    app.connected_clients[websocket] = []
    app.client_ids[websocket] = client_id
    app.cursors[client_id] = None

    await websocket.send_json({
        "type": "welcome",
        "id": client_id,
        "cursors": [
            {"id": cid, "wx": pos[0], "wy": pos[1]}
            for cid, pos in app.cursors.items()
            if cid != client_id and pos is not None
        ],
    })

    try:
        while True:
            message = await websocket.receive_json()
            await process_message(message, websocket)
    except WebSocketDisconnect:
        del app.connected_clients[websocket]
        del app.client_ids[websocket]
        del app.cursors[client_id]
        app.pending_cursor_removes.add(client_id)
        app.pending_cursor_updates.pop(client_id, None)


@app.get("/")
async def root():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
