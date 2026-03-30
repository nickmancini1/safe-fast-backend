import os
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="SAFE-FAST Backend", version="0.3.0")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/0.3"

TT_CLIENT_ID = os.getenv("TT_CLIENT_ID")
TT_CLIENT_SECRET = os.getenv("TT_CLIENT_SECRET")
TT_REDIRECT_URI = os.getenv("TT_REDIRECT_URI")
TT_REFRESH_TOKEN = os.getenv("TT_REFRESH_TOKEN")


def _required_env() -> None:
    missing = [
        name for name, value in {
            "TT_CLIENT_ID": TT_CLIENT_ID,
            "TT_CLIENT_SECRET": TT_CLIENT_SECRET,
            "TT_REDIRECT_URI": TT_REDIRECT_URI,
            "TT_REFRESH_TOKEN": TT_REFRESH_TOKEN,
        }.items() if not value
    ]
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env vars: {', '.join(missing)}")


async def get_access_token() -> str:
    _required_env()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{API_BASE}/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": TT_CLIENT_ID,
                "client_secret": TT_CLIENT_SECRET,
                "redirect_uri": TT_REDIRECT_URI,
                "refresh_token": TT_REFRESH_TOKEN,
            },
            headers={"User-Agent": USER_AGENT},
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=500, detail={"oauth_error": resp.text})
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise HTTPException(status_code=500, detail={"oauth_error": data})
    return token


async def tt_get(path: str, params: Optional[dict] = None) -> dict:
    token = await get_access_token()
    headers = {
        "Authorization": token,
        "User-Agent": USER_AGENT,
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{API_BASE}{path}", headers=headers, params=params)
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
    return {"ok": True, "access_token_present": bool(token), "prefix": token[:8]}


@app.get("/tt/accounts")
async def tt_accounts():
    return await tt_get("/customers/me/accounts")


@app.get("/tt/quote")
async def tt_quote(
    symbol: str = Query(..., description="Ticker or symbol, e.g. SPY"),
    instrument_type: str = Query("Equity", alias="type", description="Tastytrade instrument type"),
):
    params = {
        "symbols": symbol,
        "type": instrument_type,
    }
    data = await tt_get("/market-data", params=params)
    return data
