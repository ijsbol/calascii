import os
import random
import time
import uuid
from urllib.parse import urlencode

import aiomysql
import httpx
import jwt
from dotenv import load_dotenv

__all__: tuple[str, ...] = ()


load_dotenv()


JWT_SECRET: str = os.environ["JWT_SECRET"]
JWT_ALGORITHM: str = "HS256"
JWT_EXPIRY_SECONDS: int = 3 * 60 * 60  # 3 hours

DISCORD_CLIENT_ID: str = os.environ["DISCORD_OAUTH_CLIENT_ID"]
DISCORD_CLIENT_SECRET: str = os.environ["DISCORD_OAUTH_CLIENT_SECRET"]
DISCORD_REDIRECT_URI: str = os.environ.get(
    "DISCORD_REDIRECT_URI", "http://localhost:1984/auth/discord/callback"
)

NO_AUTH: bool = os.environ.get("NO_AUTH", "").lower() in ("1", "true", "yes")

CURSOR_COLORS: list[str] = [
    "#ef4444", "#f97316", "#eab308",
    "#22c55e", "#06b6d4", "#a855f7", "#ec4899",
]

DB_HOST: str = os.environ.get("DB_HOST", "localhost")
DB_PORT: int = int(os.environ.get("DB_PORT", "3306"))
DB_USER: str = os.environ["DB_USER"]
DB_PASSWORD: str = os.environ["DB_PASSWORD"]
DB_NAME: str = os.environ["DB_NAME"]

_pool: aiomysql.Pool | None = None


async def init_db() -> None:
    global _pool
    _pool = await aiomysql.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        autocommit=True,
    )
    assert _pool is not None
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id CHAR(36) NOT NULL,
                    discord_id VARCHAR(64) NOT NULL,
                    username VARCHAR(100) NOT NULL,
                    global_name VARCHAR(100),
                    cursor_color VARCHAR(7) NOT NULL DEFAULT '#ef4444',
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE KEY uq_discord_id (discord_id)
                )
            """)
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS _migrations (
                    name VARCHAR(100) PRIMARY KEY,
                    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Migration 001: add cursor_color to pre-existing installs
            await cur.execute(
                "SELECT 1 FROM _migrations WHERE name = '001_add_cursor_color'"
            )
            if not await cur.fetchone():
                await cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                    AND table_name = 'users'
                    AND column_name = 'cursor_color'
                """)
                if not await cur.fetchone():
                    await cur.execute(
                        "ALTER TABLE users ADD COLUMN cursor_color VARCHAR(7)"
                        " NOT NULL DEFAULT '#ef4444'"
                    )
                await cur.execute(
                    "INSERT INTO _migrations (name) VALUES ('001_add_cursor_color')"
                )
            # Migration 002: introduce UUID primary key, demote discord_id to unique key
            await cur.execute(
                "SELECT 1 FROM _migrations WHERE name = '002_uuid_primary_key'"
            )
            if not await cur.fetchone():
                await cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                    AND table_name = 'users'
                    AND column_name = 'id'
                """)
                if not await cur.fetchone():
                    await cur.execute(
                        "ALTER TABLE users ADD COLUMN id CHAR(36) NULL"
                    )
                    await cur.execute("SELECT discord_id FROM users")
                    rows = await cur.fetchall()
                    for (did,) in rows:
                        await cur.execute(
                            "UPDATE users SET id = %s WHERE discord_id = %s",
                            (str(uuid.uuid4()), did),
                        )
                    await cur.execute(
                        "ALTER TABLE users MODIFY COLUMN id CHAR(36) NOT NULL"
                    )
                    await cur.execute("ALTER TABLE users DROP PRIMARY KEY")
                    await cur.execute("ALTER TABLE users ADD PRIMARY KEY (id)")
                    await cur.execute(
                        "ALTER TABLE users ADD UNIQUE KEY uq_discord_id (discord_id)"
                    )
                await cur.execute(
                    "INSERT INTO _migrations (name) VALUES ('002_uuid_primary_key')"
                )


async def close_db() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None


async def upsert_user(
    discord_id: str,
    username: str,
    global_name: str | None,
) -> tuple[str, str]:
    """Insert or update user, returning (user_id, cursor_color)."""
    if _pool is None:
        raise RuntimeError("Database not yet available")
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            color = random.choice(CURSOR_COLORS)
            new_id = str(uuid.uuid4())
            await cur.execute(
                """
                INSERT INTO users (id, discord_id, username, global_name, cursor_color)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    username = VALUES(username),
                    global_name = VALUES(global_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (new_id, discord_id, username, global_name, color),
            )
            await cur.execute(
                "SELECT id, cursor_color FROM users WHERE discord_id = %s", (discord_id,)
            )
            row = await cur.fetchone()
            return (row[0], row[1]) if row else (new_id, color)


async def get_users_by_ids(ids: list[str]) -> dict[str, tuple[str, str]]:
    """Return {id: (username, cursor_color)} for the given user ids."""
    if _pool is None or not ids:
        return {}
    placeholders = ",".join(["%s"] * len(ids))
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT id, username, cursor_color FROM users WHERE id IN ({placeholders})",
                ids,
            )
            return {row[0]: (row[1], row[2]) for row in await cur.fetchall()}


async def update_username(user_id: str, username: str) -> None:
    if _pool is None:
        raise RuntimeError("Database not yet available")
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET username = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (username, user_id),
            )


async def update_cursor_color(user_id: str, color: str) -> None:
    if _pool is None:
        raise RuntimeError("Database not yet available")
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET cursor_color = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                (color, user_id),
            )


def create_jwt(user_id: str, username: str, cursor_color: str) -> str:
    now = int(time.time())
    payload = {
        "sub": user_id,
        "username": username,
        "cursor_color": cursor_color,
        "iat": now,
        "exp": now + JWT_EXPIRY_SECONDS,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_jwt(token: str) -> dict | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None


def make_discord_auth_url(state: str) -> str:
    return "https://discord.com/oauth2/authorize?" + urlencode({
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope": "identify",
        "state": state,
    })


async def exchange_code(code: str) -> dict | None:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://discord.com/api/oauth2/token",
            data={
                "client_id": DISCORD_CLIENT_ID,
                "client_secret": DISCORD_CLIENT_SECRET,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": DISCORD_REDIRECT_URI,
            },
        )
        if resp.status_code != 200:
            return None
        return resp.json()


async def get_discord_user(access_token: str) -> dict | None:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://discord.com/api/users/@me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if resp.status_code != 200:
            return None
        return resp.json()
