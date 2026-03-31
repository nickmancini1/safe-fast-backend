import os
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="SAFE-FAST Backend", version="0.7.0")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/0.7.0"

TT_CLIENT_ID = os.getenv("TT_CLIENT_ID", "")
TT_CLIENT_SECRET = os.getenv("TT_CLIENT_SECRET", "")
TT_REDIRECT_URI = os.getenv("TT_REDIRECT_URI", "")
TT_REFRESH_TOKEN = os.getenv("TT_REFRESH_TOKEN", "")

ALLOWED_SYMBOLS = {"SPY", "QQQ", "IWM", "GLD"}


def _headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }


def _clean_symbols(symbols: str) -> List[str]:
    items = [s.strip().upper() for s in symbols.split(",") if s.strip()]

    if not items:
        raise HTTPException(status_code=400, detail="No symbols provided")

    bad = [s for s in items if s not in ALLOWED_SYMBOLS]
    if bad:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Only SAFE-FAST symbols are allowed",
                "allowed": sorted(ALLOWED_SYMBOLS),
                "bad_symbols": bad,
            },
        )

    return items


def _clean_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if value not in ALLOWED_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Only SAFE-FAST symbols are allowed",
                "allowed": sorted(ALLOWED_SYMBOLS),
                "bad_symbol": value,
            },
        )
    return value


async def get_access_token() -> str:
    if not all([TT_CLIENT_ID, TT_CLIENT_SECRET, TT_REDIRECT_URI, TT_REFRESH_TOKEN]):
        raise HTTPException(status_code=500, detail="Missing TT OAuth environment variables")

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{API_BASE}/oauth/token",
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            data={
                "grant_type": "refresh_token",
                "client_id": TT_CLIENT_ID,
                "client_secret": TT_CLIENT_SECRET,
                "redirect_uri": TT_REDIRECT_URI,
                "refresh_token": TT_REFRESH_TOKEN,
            },
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        token = payload.get("access_token")
        if not token:
            raise HTTPException(status_code=500, detail=payload)

        return token


async def _fetch_option_chain(symbol: str, token: str) -> Any:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/option-chains/{symbol}",
            headers=_headers(token),
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        return payload


@app.get("/")
def root() -> Dict[str, Any]:
    return {"status": "ok", "service": "safe-fast-backend"}


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/tt/auth-test")
async def tt_auth_test() -> Dict[str, Any]:
    token = await get_access_token()
    return {
        "ok": True,
        "access_token_present": bool(token),
        "prefix": token[:8],
    }


@app.get("/tt/accounts")
async def tt_accounts() -> Any:
    token = await get_access_token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/customers/me/accounts",
            headers=_headers(token),
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        return payload


@app.get("/tt/quote-token")
async def tt_quote_token() -> Any:
    token = await get_access_token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/api-quote-tokens",
            headers=_headers(token),
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        return payload


@app.get("/tt/quotes")
async def tt_quotes(
    symbols: str = Query("SPY,QQQ,IWM,GLD")
) -> Any:
    clean_symbols = _clean_symbols(symbols)
    token = await get_access_token()

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/market-data",
            headers=_headers(token),
            params={
                "type": "Equity",
                "symbols": ",".join(clean_symbols),
            },
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        return {
            "ok": True,
            "symbols": clean_symbols,
            "payload": payload,
        }


@app.get("/tt/option-chain")
async def tt_option_chain(
    symbol: str = Query("SPY")
) -> Any:
    clean_symbol = _clean_symbol(symbol)
    token = await get_access_token()
    payload = await _fetch_option_chain(clean_symbol, token)

    return {
        "ok": True,
        "symbol": clean_symbol,
        "payload": payload,
    }


@app.get("/tt/option-expirations")
async def tt_option_expirations(
    symbol: str = Query("SPY"),
    min_dte: int = Query(14),
    max_dte: int = Query(30),
) -> Any:
    clean_symbol = _clean_symbol(symbol)

    if min_dte < 0 or max_dte < 0 or min_dte > max_dte:
        raise HTTPException(
            status_code=400,
            detail="Invalid DTE range",
        )

    token = await get_access_token()
    payload = await _fetch_option_chain(clean_symbol, token)

    items = payload.get("data", {}).get("items", [])
    seen = set()
    expirations = []

    for item in items:
        dte = item.get("days-to-expiration")
        expiration_date = item.get("expiration-date")

        if dte is None or expiration_date is None:
            continue

        if min_dte <= int(dte) <= max_dte:
            key = (expiration_date, int(dte))
            if key not in seen:
                seen.add(key)
                expirations.append(
                    {
                        "expiration_date": expiration_date,
                        "days_to_expiration": int(dte),
                    }
                )

    expirations.sort(key=lambda x: (x["days_to_expiration"], x["expiration_date"]))

    return {
        "ok": True,
        "symbol": clean_symbol,
        "min_dte": min_dte,
        "max_dte": max_dte,
        "count": len(expirations),
        "expirations": expirations,
    }
