#!/usr/bin/env python3
"""
Migrate canvas data from data.json to the MySQL canvas_tiles table.

Usage:
    python migrate_json_to_mysql.py [path/to/data.json]

The JSON path defaults to data.json in the same directory as this script.
Database connection is read from the .env file (DB_HOST, DB_PORT, DB_USER,
DB_PASSWORD, DB_NAME).
"""
import asyncio
import json
import sys
from pathlib import Path

import auth

CHUNK_SIZE: int = 50
BATCH_SIZE: int = 1000


def _parse_json(path: Path) -> list[auth.CanvasTile]:
    """Read data.json and return a flat list of all non-empty tiles, applying
    the same version-migration rules that main.py previously handled on load."""
    raw: dict = json.loads(path.read_text())
    version: int = raw.get("version", 0)
    raw_chunks: dict = raw.get("chunks", raw) if version >= 1 else raw

    tiles: list[auth.CanvasTile] = []
    for g_x_str, cols in raw_chunks.items():
        g_x = int(g_x_str)
        for g_y_str, chunk in cols.items():
            g_y = int(g_y_str)
            for s_x, col in enumerate(chunk):
                for s_y, cell in enumerate(col):
                    if isinstance(cell, str):
                        char: str = cell
                        user_id: str | None = None
                    elif isinstance(cell, list) and len(cell) == 2:
                        char = cell[0] or ""
                        user_id = cell[1]
                        # v1 stored raw session UUIDs — strip them; only keep "u:…" ids
                        if version < 2 and user_id and not user_id.startswith("u:"):
                            user_id = None
                    else:
                        char, user_id = "", None

                    if char:
                        tiles.append(
                            auth.CanvasTile(
                                g_x=g_x, g_y=g_y,
                                s_x=s_x, s_y=s_y,
                                char=char, user_id=user_id,
                            )
                        )
    return tiles


async def main() -> None:
    json_path = (
        Path(sys.argv[1]) if len(sys.argv) > 1
        else Path(__file__).parent / "data.json"
    )

    if not json_path.exists():
        print(f"Error: {json_path} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {json_path} …")
    tiles = _parse_json(json_path)
    print(f"  {len(tiles):,} non-empty tiles found")

    print("Connecting to database …")
    await auth.init_db()

    print("Inserting tiles (this may take a while for large canvases) …")
    for i in range(0, max(len(tiles), 1), BATCH_SIZE):
        batch = tiles[i : i + BATCH_SIZE]
        await auth.save_tiles(batch)
        done = min(i + BATCH_SIZE, len(tiles))
        print(f"  {done:,} / {len(tiles):,}")

    print("Migration complete.")
    await auth.close_db()


if __name__ == "__main__":
    asyncio.run(main())
