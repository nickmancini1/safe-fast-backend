
import os
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException

APP_TITLE = "SAFE-FAST Backend"
API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/0.2.0"

app = FastAPI(title=APP_TITLE, version="0.2.0")


def _env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise HTTPException(status_code=500, detail=f"Missing env var: {name}")
    return value


async def get_access_token() -> str:
    client_id = _env("TT_CLIENT_ID")
    client_secret = _env("TT_CLIENT_SECRET")
    refresh_token = _env("TT_REFRESH_TOKEN")
    redirect_uri = _env("TT_REDIRECT_URI")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(f"{API_BASE}/oauth/token", data=data, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    payload = resp.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise HTTPException(status_code=500, detail={"oauth_response": payload})
    return access_token


async def api_get(path: str) -> Dict[str, Any]:
    token = await get_access_token()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{API_BASE}{path}", headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@app.get("/")
def root():
    return {"status": "ok", "service": "safe-fast-backend"}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/tt/auth-test")
async def tt_auth_test():
    token = await get_access_token()
    return {"ok": True, "access_token_present": bool(token), "prefix": token[:10]}


@app.get("/tt/accounts")
async def tt_accounts():
    data = await api_get("/customers/me/accounts")
    return data
