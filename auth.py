import os
import time
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
                    discord_id VARCHAR(64) PRIMARY KEY,
                    username VARCHAR(100) NOT NULL,
                    global_name VARCHAR(100),
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)


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
) -> None:
    if _pool is None:
        raise RuntimeError("Database not yet available")
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO users (discord_id, username, global_name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    username = VALUES(username),
                    global_name = VALUES(global_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (discord_id, username, global_name),
            )


def create_jwt(discord_id: str, username: str) -> str:
    now = int(time.time())
    payload = {
        "sub": discord_id,
        "username": username,
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
