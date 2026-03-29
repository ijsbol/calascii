import asyncio
import random
import secrets
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Final, Literal, TypedDict

import auth
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.requests import Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse


__all__: tuple[str, ...] = ()


CHUNK_SIZE: Final[int] = 50
SAVE_INTERVAL: Final[int] = 300  # seconds
BROADCAST_INTERVAL: Final[int] = 1  # seconds


class CalasciiData:
    def __init__(self) -> None:
        self._data: dict[int, dict[int, list[list[tuple[str, str | None]]]]] = {}
        self._dirty_tiles: set[tuple[int, int, int, int]] = set()

    def _ensure_chunk(self, g_x: int, g_y: int) -> list[list[tuple[str, str | None]]]:
        if g_x not in self._data:
            self._data[g_x] = {}
        if g_y not in self._data[g_x]:
            self._data[g_x][g_y] = [
                [("", None) for _ in range(CHUNK_SIZE)]
                for _ in range(CHUNK_SIZE)
            ]
        return self._data[g_x][g_y]

    def set(self, g_x: int, g_y: int, s_x: int, s_y: int, data: str, user_id: str | None) -> None:
        self._ensure_chunk(g_x, g_y)[s_x][s_y] = (data, user_id)
        self._dirty_tiles.add((g_x, g_y, s_x, s_y))

    def get(self, g_x: int, g_y: int) -> list[list[tuple[str, str | None]]]:
        return self._ensure_chunk(g_x, g_y)

    async def load_from_db(self) -> None:
        for tile in await auth.load_all_tiles():
            self._ensure_chunk(tile["g_x"], tile["g_y"])[tile["s_x"]][tile["s_y"]] = (
                tile["char"], tile["user_id"]
            )

    async def flush_dirty(self) -> None:
        dirty = self._dirty_tiles.copy()
        self._dirty_tiles.clear()
        if not dirty:
            return
        to_upsert: list[auth.CanvasTile] = []
        to_delete: list[tuple[int, int, int, int]] = []
        for (g_x, g_y, s_x, s_y) in dirty:
            char, user_id = self._data[g_x][g_y][s_x][s_y]
            if char is not None:
                to_upsert.append(auth.CanvasTile(g_x=g_x, g_y=g_y, s_x=s_x, s_y=s_y, char=char, user_id=user_id))
            else:
                to_delete.append((g_x, g_y, s_x, s_y))
        try:
            await auth.save_tiles(to_upsert)
            await auth.delete_tiles(to_delete)
        except Exception as exc:
            print(f"Warning: failed to flush {len(dirty)} dirty tiles: {exc}")
            self._dirty_tiles.update(dirty)


class CalasciiRouter(FastAPI):
    connected_clients: dict[WebSocket, list[tuple[int, int]]] = {}
    client_ids: dict[WebSocket, str] = {}
    client_users: dict[WebSocket, dict | None] = {}
    id_to_username: dict[str, str] = {}
    id_to_color: dict[str, str] = {}
    sub_to_color: dict[str, str] = {}
    sub_to_username: dict[str, str] = {}
    cursors: dict[str, tuple[int, int] | None] = {}
    data: CalasciiData = CalasciiData()
    pending_chunks: set[tuple[int, int]] = set()
    pending_cursor_updates: dict[str, tuple[int, int]] = {}
    pending_cursor_removes: set[str] = set()
    # Tile allowance tracking (keyed by user UUID / sub)
    user_id_to_sockets: dict[str, set] = {}
    user_id_to_allowance: dict[str, int] = {}
    user_id_to_claimed: dict[str, int] = {}


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
    data: list[list[tuple[str, str | None]]]
    user_colors: dict[str, str]
    user_names: dict[str, str]


async def _save_loop() -> None:
    while True:
        await asyncio.sleep(SAVE_INTERVAL)
        await app.data.flush_dirty()


def _build_user_colors(chunk_data: list[list[tuple[str, str | None]]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for col in chunk_data:
        for (_, uid) in col:
            if uid and uid not in result:
                sub = uid[2:] if uid.startswith("u:") else uid
                color = app.sub_to_color.get(sub)
                if color:
                    result[uid] = color
    return result


def _build_user_names(chunk_data: list[list[tuple[str, str | None]]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for col in chunk_data:
        for (_, uid) in col:
            if uid and uid not in result:
                sub = uid[2:] if uid.startswith("u:") else uid
                name = app.sub_to_username.get(sub)
                if name:
                    result[uid] = name
    return result


async def _fill_user_cache(chunk_data: list[list[tuple[str, str | None]]]) -> None:
    """Fetch username/color from DB for any tile owners not already in the in-memory cache."""
    missing = set()
    for col in chunk_data:
        for (_, uid) in col:
            if uid and uid.startswith("u:"):
                sub = uid[2:]
                if sub not in app.sub_to_color or sub not in app.sub_to_username:
                    missing.add(sub)
    if not missing:
        return
    rows = await auth.get_users_by_ids(list(missing))
    for sub, (username, color) in rows.items():
        app.sub_to_color[sub] = color
        app.sub_to_username[sub] = username


async def _build_chunk_packet(g_x: int, g_y: int, chunk_data: list[list[tuple[str, str | None]]]) -> _UpdatePacketGet:
    await _fill_user_cache(chunk_data)
    return _UpdatePacketGet({
        "type": "update_packet_get",
        "g_x": g_x,
        "g_y": g_y,
        "data": chunk_data,
        "user_colors": _build_user_colors(chunk_data),
        "user_names": _build_user_names(chunk_data),
    })


async def _broadcast_loop() -> None:
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL)

        if app.pending_chunks:
            chunks_snapshot = app.pending_chunks.copy()
            app.pending_chunks.clear()
            for (g_x, g_y) in chunks_snapshot:
                chunk_data = app.data.get(g_x=g_x, g_y=g_y)
                packet = await _build_chunk_packet(g_x, g_y, chunk_data)
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
                        "username": app.id_to_username.get(client_id, client_id[:6]),
                        "color": app.id_to_color.get(client_id, auth.CURSOR_COLORS[0]),
                    })

        if app.user_id_to_sockets:
            connected_ids = list(app.user_id_to_sockets.keys())
            new_allowances = await auth.increment_tile_allowances(connected_ids)
            for user_id, new_allowance in new_allowances.items():
                app.user_id_to_allowance[user_id] = new_allowance
                claimed = app.user_id_to_claimed.get(user_id, 0)
                packet = {"type": "tile_allowance_update", "allowance": new_allowance, "claimed": claimed}
                for sock in list(app.user_id_to_sockets.get(user_id, set())):
                    try:
                        await sock.send_json(packet)
                    except Exception:
                        pass



@asynccontextmanager
async def lifecycle(app: CalasciiRouter):
    await auth.init_db()
    await app.data.load_from_db()
    save_task = asyncio.create_task(_save_loop())
    broadcast_task = asyncio.create_task(_broadcast_loop())
    yield
    save_task.cancel()
    broadcast_task.cancel()
    await app.data.flush_dirty()
    await auth.close_db()


app = CalasciiRouter(
    lifespan=lifecycle,
)


async def process_message(message: Message, websocket: WebSocket) -> None:
    if message["type"] == "set":
        user_payload = app.client_users.get(websocket)
        if not auth.NO_AUTH and user_payload is None:
            return
        if (
            app.connected_clients.get(websocket) is None
            or (message["g_x"], message["g_y"]) not in app.connected_clients[websocket]
        ):
            return
        if not (0 <= message["s_x"] < CHUNK_SIZE and 0 <= message["s_y"] < CHUNK_SIZE):
            return
        effective_user_id = ("u:" + user_payload["sub"]) if user_payload else None
        existing_owner: str | None = None
        if not auth.NO_AUTH and effective_user_id:
            existing = app.data.get(g_x=message["g_x"], g_y=message["g_y"])[message["s_x"]][message["s_y"]]
            existing_owner = existing[1]
            if existing_owner and existing_owner.startswith("u:") and existing_owner != effective_user_id:
                return
            # Enforce tile allowance for new claims
            if message["data"] and existing_owner is None:
                sub = effective_user_id[2:]  # strip "u:" prefix
                claimed = app.user_id_to_claimed.get(sub, 0)
                allowance = app.user_id_to_allowance.get(sub, 100)
                if claimed >= allowance:
                    chunk_data = app.data.get(g_x=message["g_x"], g_y=message["g_y"])
                    await websocket.send_json(await _build_chunk_packet(message["g_x"], message["g_y"], chunk_data))
                    return
        stored_user_id = effective_user_id if message["data"] else None
        app.data.set(
            g_x=message["g_x"],
            g_y=message["g_y"],
            s_x=message["s_x"],
            s_y=message["s_y"],
            data=message["data"],
            user_id=stored_user_id,
        )
        app.pending_chunks.add((message["g_x"], message["g_y"]))
        # Update claimed count and notify the user's sockets
        if not auth.NO_AUTH and effective_user_id:
            sub = effective_user_id[2:]  # strip "u:" prefix
            count_changed = False
            if message["data"] and existing_owner is None:
                app.user_id_to_claimed[sub] = app.user_id_to_claimed.get(sub, 0) + 1
                count_changed = True
            elif not message["data"] and existing_owner == effective_user_id:
                app.user_id_to_claimed[sub] = max(0, app.user_id_to_claimed.get(sub, 0) - 1)
                count_changed = True
            if count_changed:
                allowance = app.user_id_to_allowance.get(sub, 100)
                claimed = app.user_id_to_claimed[sub]
                packet = {"type": "tile_allowance_update", "allowance": allowance, "claimed": claimed}
                for sock in list(app.user_id_to_sockets.get(sub, set())):
                    try:
                        await sock.send_json(packet)
                    except Exception:
                        pass

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
        chunk_data = app.data.get(g_x=message["g_x"], g_y=message["g_y"])
        await websocket.send_json(await _build_chunk_packet(message["g_x"], message["g_y"], chunk_data))


async def _dispatch_account_update(client_id: str, username: str, color: str, user_id: str | None = None) -> None:
    packet = {"type": "account_update", "id": client_id, "username": username, "color": color, "user_id": user_id}
    for client in list(app.connected_clients):
        try:
            await client.send_json(packet)
        except Exception:
            pass


def _find_client_ids_by_user_id(user_id: str) -> list[str]:
    return [
        cid for ws, user in app.client_users.items()
        if user and user.get("sub") == user_id
        and (cid := app.client_ids.get(ws)) is not None
    ]


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    token = websocket.cookies.get("session")
    user_payload = auth.decode_jwt(token) if token else None

    client_id = str(uuid.uuid4())
    username = user_payload["username"] if user_payload else client_id[:6]
    cursor_color = (
        user_payload.get("cursor_color", auth.CURSOR_COLORS[0])
        if user_payload
        else random.choice(auth.CURSOR_COLORS)
    )
    app.connected_clients[websocket] = []
    app.client_ids[websocket] = client_id
    app.client_users[websocket] = user_payload
    app.id_to_username[client_id] = username
    app.id_to_color[client_id] = cursor_color
    app.cursors[client_id] = None
    if user_payload:
        app.sub_to_color[user_payload["sub"]] = cursor_color
        app.sub_to_username[user_payload["sub"]] = username

    # Register socket for tile allowance tracking
    tile_allowance_packet: dict | None = None
    if user_payload and not auth.NO_AUTH:
        sub = user_payload["sub"]
        if sub not in app.user_id_to_sockets:
            app.user_id_to_sockets[sub] = set()
            allowance, claimed = await auth.get_user_tile_info(sub)
            app.user_id_to_allowance[sub] = allowance
            app.user_id_to_claimed[sub] = claimed
        app.user_id_to_sockets[sub].add(websocket)
        tile_allowance_packet = {
            "type": "tile_allowance_update",
            "allowance": app.user_id_to_allowance[sub],
            "claimed": app.user_id_to_claimed[sub],
        }

    await websocket.send_json({
        "type": "welcome",
        "id": client_id,
        "my_user_id": ("u:" + user_payload["sub"]) if user_payload else None,
        "cursors": [
            {
                "id": cid,
                "wx": pos[0],
                "wy": pos[1],
                "username": app.id_to_username.get(cid, cid[:6]),
                "color": app.id_to_color.get(cid, auth.CURSOR_COLORS[0]),
            }
            for cid, pos in app.cursors.items()
            if cid != client_id and pos is not None
        ],
    })
    if tile_allowance_packet:
        await websocket.send_json(tile_allowance_packet)

    try:
        while True:
            message = await websocket.receive_json()
            await process_message(message, websocket)
    except WebSocketDisconnect:
        del app.connected_clients[websocket]
        del app.client_ids[websocket]
        del app.client_users[websocket]
        del app.id_to_username[client_id]
        del app.id_to_color[client_id]
        del app.cursors[client_id]
        app.pending_cursor_removes.add(client_id)
        app.pending_cursor_updates.pop(client_id, None)
        if user_payload:
            sub = user_payload["sub"]
            sockets = app.user_id_to_sockets.get(sub)
            if sockets is not None:
                sockets.discard(websocket)
                if not sockets:
                    del app.user_id_to_sockets[sub]
                    app.user_id_to_allowance.pop(sub, None)
                    app.user_id_to_claimed.pop(sub, None)


@app.get("/auth/discord/login")
async def discord_login():
    state = secrets.token_urlsafe(32)
    url = auth.make_discord_auth_url(state)
    response = RedirectResponse(url)
    response.set_cookie("oauth_state", state, max_age=600, httponly=True, samesite="lax")
    return response


@app.get("/auth/discord/callback")
async def discord_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    stored_state = request.cookies.get("oauth_state")
    if error or not code or not state or not stored_state or stored_state != state:
        response = RedirectResponse("/")
        response.delete_cookie("oauth_state")
        return response

    token_data = await auth.exchange_code(code)
    if not token_data or "access_token" not in token_data:
        response = RedirectResponse("/")
        response.delete_cookie("oauth_state")
        return response

    user_data = await auth.get_discord_user(token_data["access_token"])
    if not user_data:
        response = RedirectResponse("/")
        response.delete_cookie("oauth_state")
        return response

    user_id = user_data["id"]
    cursor_color = auth.CURSOR_COLORS[0]
    try:
        user_id, cursor_color = await auth.upsert_user(
            discord_id=user_data["id"],
            username=user_data["username"],
            global_name=user_data.get("global_name"),
        )
    except RuntimeError:
        pass  # DB not yet ready; proceed without recording the user

    jwt_token = auth.create_jwt(
        user_id=user_id,
        username=user_data["username"],
        cursor_color=cursor_color,
    )

    response = RedirectResponse("/")
    response.delete_cookie("oauth_state")
    response.set_cookie(
        "session",
        jwt_token,
        max_age=auth.JWT_EXPIRY_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.get("/auth/logout")
async def logout():
    response = RedirectResponse("/")
    response.delete_cookie("session")
    return response


@app.get("/auth/me")
async def me(request: Request):
    token = request.cookies.get("session")
    if not token:
        # Give users an automatic anonymous identity if NO_AUTH is enabled
        if auth.NO_AUTH:
            random_id = secrets.token_hex(4)
            random_username = "Anon" + random_id
            random_color = random.choice(auth.CURSOR_COLORS)
            token = auth.create_jwt(user_id=random_id, username=random_username, cursor_color=random_color)
        else:
            return JSONResponse({"authenticated": False})
    payload = auth.decode_jwt(token)
    if not payload:
        return JSONResponse({"authenticated": False})
    response = JSONResponse({
        "authenticated": True,
        "username": payload["username"],
        "cursor_color": payload.get("cursor_color", auth.CURSOR_COLORS[0]),
    })
    response.set_cookie(
        "session",
        token,
        max_age=auth.JWT_EXPIRY_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.post("/auth/username")
async def change_username(request: Request):
    token = request.cookies.get("session")
    if not token:
        return JSONResponse({"error": "not authenticated"}, status_code=401)
    payload = auth.decode_jwt(token)
    if not payload:
        return JSONResponse({"error": "not authenticated"}, status_code=401)

    body = await request.json()
    new_username = body.get("username", "").strip()
    if not new_username or len(new_username) > 32:
        return JSONResponse({"error": "invalid username"}, status_code=400)

    try:
        await auth.update_username(payload["sub"], new_username)
    except RuntimeError:
        pass

    color = payload.get("cursor_color", auth.CURSOR_COLORS[0])
    app.sub_to_username[payload["sub"]] = new_username
    for client_id in _find_client_ids_by_user_id(payload["sub"]):
        app.id_to_username[client_id] = new_username
        asyncio.create_task(_dispatch_account_update(client_id, new_username, color, "u:" + payload["sub"]))

    new_token = auth.create_jwt(user_id=payload["sub"], username=new_username, cursor_color=color)
    response = JSONResponse({"username": new_username, "cursor_color": color})
    response.set_cookie(
        "session",
        new_token,
        max_age=auth.JWT_EXPIRY_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.post("/auth/color")
async def change_color(request: Request):
    token = request.cookies.get("session")
    if not token:
        return JSONResponse({"error": "not authenticated"}, status_code=401)
    payload = auth.decode_jwt(token)
    if not payload:
        return JSONResponse({"error": "not authenticated"}, status_code=401)

    body = await request.json()
    new_color = body.get("color", "")
    if new_color not in auth.CURSOR_COLORS:
        return JSONResponse({"error": "invalid color"}, status_code=400)

    try:
        await auth.update_cursor_color(payload["sub"], new_color)
    except RuntimeError:
        pass

    app.sub_to_color[payload["sub"]] = new_color
    for client_id in _find_client_ids_by_user_id(payload["sub"]):
        app.id_to_color[client_id] = new_color
        asyncio.create_task(_dispatch_account_update(client_id, app.id_to_username.get(client_id, ""), new_color, "u:" + payload["sub"]))

    new_token = auth.create_jwt(
        user_id=payload["sub"],
        username=payload["username"],
        cursor_color=new_color,
    )
    response = JSONResponse({"cursor_color": new_color})
    response.set_cookie(
        "session",
        new_token,
        max_age=auth.JWT_EXPIRY_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.get("/")
async def root():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


if __name__ == "__main__":
    import os
    import uvicorn

    ssl_certfile = os.environ.get("SSL_CERTFILE")
    ssl_keyfile = os.environ.get("SSL_KEYFILE")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=1984,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
    )
