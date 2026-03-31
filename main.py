import os
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="SAFE-FAST Backend", version="1.2.0")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/1.2.0"

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


def _clean_option_type(option_type: str) -> str:
    value = option_type.strip().upper()
    if value not in {"C", "P"}:
        raise HTTPException(status_code=400, detail="option_type must be C or P")
    return value


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _best_price(contract: Dict[str, Any]) -> Optional[float]:
    for field in ["mid", "mark", "last"]:
        value = _to_float(contract.get(field))
        if value is not None:
            return value
    return None


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


async def _fetch_quotes(symbols: List[str], token: str) -> Any:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/market-data",
            headers=_headers(token),
            params={
                "type": "Equity",
                "symbols": ",".join(symbols),
            },
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        return payload


async def _fetch_option_quotes(option_symbols: List[str], token: str) -> Any:
    if not option_symbols:
        return {"data": {"items": []}}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/market-data/by-type",
            headers=_headers(token),
            params={
                "equity-option": ",".join(option_symbols),
            },
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise HTTPException(status_code=resp.status_code, detail=payload)

        return payload


async def _get_underlying_price(symbol: str, token: str) -> float:
    payload = await _fetch_quotes([symbol], token)
    items = payload.get("data", {}).get("items", [])
    if not items:
        raise HTTPException(status_code=500, detail="No quote data returned")

    item = items[0]

    for field in ["mark", "last", "mid", "close"]:
        value = _to_float(item.get(field))
        if value is not None:
            return value

    raise HTTPException(status_code=500, detail="Could not determine underlying price")


def _build_near_contracts(
    chain_payload: Any,
    expiration_date: str,
    option_type: str,
    underlying_price: float,
) -> List[Dict[str, Any]]:
    items = chain_payload.get("data", {}).get("items", [])
    contracts: List[Dict[str, Any]] = []

    for item in items:
        if item.get("expiration-date") != expiration_date:
            continue
        if item.get("option-type") != option_type:
            continue

        strike_value = _to_float(item.get("strike-price"))
        if strike_value is None:
            continue

        contracts.append(
            {
                "symbol": item.get("symbol"),
                "streamer_symbol": item.get("streamer-symbol"),
                "strike_price": strike_value,
                "distance_from_underlying": round(abs(strike_value - underlying_price), 4),
                "expiration_date": item.get("expiration-date"),
                "days_to_expiration": item.get("days-to-expiration"),
                "option_type": item.get("option-type"),
                "active": item.get("active"),
            }
        )

    contracts.sort(key=lambda x: (x["distance_from_underlying"], x["strike_price"]))
    return contracts


async def _get_quoted_near_contracts(
    symbol: str,
    expiration_date: str,
    option_type: str,
    limit: int,
    token: str,
) -> Dict[str, Any]:
    underlying_price = await _get_underlying_price(symbol, token)
    chain_payload = await _fetch_option_chain(symbol, token)

    near_contracts = _build_near_contracts(
        chain_payload=chain_payload,
        expiration_date=expiration_date,
        option_type=option_type,
        underlying_price=underlying_price,
    )[:limit]

    option_symbols = [c["symbol"] for c in near_contracts if c.get("symbol")]
    quote_payload = await _fetch_option_quotes(option_symbols, token)
    quote_items = quote_payload.get("data", {}).get("items", [])
    quote_map = {item.get("symbol"): item for item in quote_items}

    merged = []
    for contract in near_contracts:
        quote = quote_map.get(contract["symbol"], {})
        merged.append(
            {
                **contract,
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "mid": quote.get("mid"),
                "mark": quote.get("mark"),
                "last": quote.get("last"),
                "bid_size": quote.get("bid-size"),
                "ask_size": quote.get("ask-size"),
                "updated_at": quote.get("updated-at"),
            }
        )

    return {
        "underlying_price": underlying_price,
        "contracts": merged,
    }


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
async def tt_quotes(symbols: str = Query("SPY,QQQ,IWM,GLD")) -> Any:
    clean_symbols = _clean_symbols(symbols)
    token = await get_access_token()
    payload = await _fetch_quotes(clean_symbols, token)

    return {
        "ok": True,
        "symbols": clean_symbols,
        "payload": payload,
    }


@app.get("/tt/option-chain")
async def tt_option_chain(symbol: str = Query("SPY")) -> Any:
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
        raise HTTPException(status_code=400, detail="Invalid DTE range")

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

        dte_int = int(dte)
        if min_dte <= dte_int <= max_dte:
            key = (expiration_date, dte_int)
            if key not in seen:
                seen.add(key)
                expirations.append(
                    {
                        "expiration_date": expiration_date,
                        "days_to_expiration": dte_int,
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


@app.get("/tt/option-contracts")
async def tt_option_contracts(
    symbol: str = Query("SPY"),
    expiration_date: str = Query(...),
    option_type: str = Query("C"),
    limit: int = Query(10),
) -> Any:
    clean_symbol = _clean_symbol(symbol)
    clean_option_type = _clean_option_type(option_type)

    if limit <= 0 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")

    token = await get_access_token()
    payload = await _fetch_option_chain(clean_symbol, token)

    items = payload.get("data", {}).get("items", [])
    contracts = []

    for item in items:
        if item.get("expiration-date") != expiration_date:
            continue
        if item.get("option-type") != clean_option_type:
            continue

        strike_value = _to_float(item.get("strike-price"))

        contracts.append(
            {
                "symbol": item.get("symbol"),
                "streamer_symbol": item.get("streamer-symbol"),
                "strike_price": strike_value,
                "expiration_date": item.get("expiration-date"),
                "days_to_expiration": item.get("days-to-expiration"),
                "option_type": item.get("option-type"),
                "active": item.get("active"),
            }
        )

    contracts.sort(key=lambda x: (x["strike_price"] is None, x["strike_price"]))

    return {
        "ok": True,
        "symbol": clean_symbol,
        "expiration_date": expiration_date,
        "option_type": clean_option_type,
        "count": len(contracts),
        "contracts": contracts[:limit],
    }


@app.get("/tt/option-contracts-near")
async def tt_option_contracts_near(
    symbol: str = Query("SPY"),
    expiration_date: str = Query(...),
    option_type: str = Query("C"),
    limit: int = Query(10),
) -> Any:
    clean_symbol = _clean_symbol(symbol)
    clean_option_type = _clean_option_type(option_type)

    if limit <= 0 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")

    token = await get_access_token()
    data = await _get_quoted_near_contracts(
        symbol=clean_symbol,
        expiration_date=expiration_date,
        option_type=clean_option_type,
        limit=limit,
        token=token,
    )

    return {
        "ok": True,
        "symbol": clean_symbol,
        "underlying_price": data["underlying_price"],
        "expiration_date": expiration_date,
        "option_type": clean_option_type,
        "count": len(data["contracts"]),
        "contracts": [
            {
                "symbol": c["symbol"],
                "streamer_symbol": c["streamer_symbol"],
                "strike_price": c["strike_price"],
                "distance_from_underlying": c["distance_from_underlying"],
                "expiration_date": c["expiration_date"],
                "days_to_expiration": c["days_to_expiration"],
                "option_type": c["option_type"],
                "active": c["active"],
            }
            for c in data["contracts"]
        ],
    }


@app.get("/tt/option-quotes-near")
async def tt_option_quotes_near(
    symbol: str = Query("SPY"),
    expiration_date: str = Query(...),
    option_type: str = Query("C"),
    limit: int = Query(6),
) -> Any:
    clean_symbol = _clean_symbol(symbol)
    clean_option_type = _clean_option_type(option_type)

    if limit <= 0 or limit > 20:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 20")

    token = await get_access_token()
    data = await _get_quoted_near_contracts(
        symbol=clean_symbol,
        expiration_date=expiration_date,
        option_type=clean_option_type,
        limit=limit,
        token=token,
    )

    return {
        "ok": True,
        "symbol": clean_symbol,
        "underlying_price": data["underlying_price"],
        "expiration_date": expiration_date,
        "option_type": clean_option_type,
        "count": len(data["contracts"]),
        "contracts": data["contracts"],
    }


@app.get("/tt/debit-spread-candidates")
async def tt_debit_spread_candidates(
    symbol: str = Query("SPY"),
    expiration_date: str = Query(...),
    option_type: str = Query("C"),
    near_limit: int = Query(16),
    width_min: float = Query(5.0),
    width_max: float = Query(10.0),
    risk_min_dollars: float = Query(250.0),
    risk_max_dollars: float = Query(300.0),
    hard_max_dollars: float = Query(400.0),
    enforce_hard_max: bool = Query(True),
    only_preferred: bool = Query(False),
    limit: int = Query(8),
) -> Any:
    clean_symbol = _clean_symbol(symbol)
    clean_option_type = _clean_option_type(option_type)

    if near_limit <= 1 or near_limit > 40:
        raise HTTPException(status_code=400, detail="near_limit must be between 2 and 40")
    if width_min <= 0 or width_max <= 0 or width_min > width_max:
        raise HTTPException(status_code=400, detail="Invalid width range")
    if risk_min_dollars < 0 or risk_max_dollars < 0 or risk_min_dollars > risk_max_dollars:
        raise HTTPException(status_code=400, detail="Invalid preferred risk range")
    if hard_max_dollars <= 0:
        raise HTTPException(status_code=400, detail="hard_max_dollars must be greater than 0")
    if limit <= 0 or limit > 20:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 20")

    token = await get_access_token()
    data = await _get_quoted_near_contracts(
        symbol=clean_symbol,
        expiration_date=expiration_date,
        option_type=clean_option_type,
        limit=near_limit,
        token=token,
    )

    underlying_price = data["underlying_price"]
    contracts = sorted(
        data["contracts"],
        key=lambda c: (c["strike_price"] is None, c["strike_price"])
    )

    candidates = []
    target_risk_mid = (risk_min_dollars + risk_max_dollars) / 2.0

    for i in range(len(contracts)):
        for j in range(i + 1, len(contracts)):
            left = contracts[i]
            right = contracts[j]

            left_strike = _to_float(left.get("strike_price"))
            right_strike = _to_float(right.get("strike_price"))
            if left_strike is None or right_strike is None:
                continue

            width = round(abs(right_strike - left_strike), 4)
            if width < width_min or width > width_max:
                continue

            if clean_option_type == "C":
                long_leg = left
                short_leg = right
            else:
                long_leg = right
                short_leg = left

            long_price = _best_price(long_leg)
            short_price = _best_price(short_leg)

            if long_price is None or short_price is None:
                continue

            est_debit = round(long_price - short_price, 4)
            if est_debit <= 0:
                continue

            max_loss = est_debit
            max_profit = round(width - est_debit, 4)
            max_loss_dollars_1lot = round(max_loss * 100, 2)
            max_profit_dollars_1lot = round(max_profit * 100, 2)

            feasibility_pass = (1.6 * est_debit) <= width
            within_hard_max = max_loss_dollars_1lot <= hard_max_dollars
            preferred_risk_band_pass = risk_min_dollars <= max_loss_dollars_1lot <= risk_max_dollars

            if enforce_hard_max and not within_hard_max:
                continue
            if only_preferred and not preferred_risk_band_pass:
                continue

            long_strike = _to_float(long_leg.get("strike_price"))
            short_strike = _to_float(short_leg.get("strike_price"))
            if long_strike is None or short_strike is None:
                continue

            candidates.append(
                {
                    "long_symbol": long_leg.get("symbol"),
                    "short_symbol": short_leg.get("symbol"),
                    "long_strike": long_strike,
                    "short_strike": short_strike,
                    "width": width,
                    "long_mid": long_leg.get("mid"),
                    "short_mid": short_leg.get("mid"),
                    "long_mark": long_leg.get("mark"),
                    "short_mark": short_leg.get("mark"),
                    "est_debit": est_debit,
                    "max_loss": max_loss,
                    "max_profit": max_profit,
                    "max_loss_dollars_1lot": max_loss_dollars_1lot,
                    "max_profit_dollars_1lot": max_profit_dollars_1lot,
                    "risk_reward": round(max_profit / max_loss, 4) if max_loss > 0 else None,
                    "feasibility_pass": feasibility_pass,
                    "preferred_risk_band_pass": preferred_risk_band_pass,
                    "within_hard_max": within_hard_max,
                    "fits_risk_budget": preferred_risk_band_pass and within_hard_max,
                    "long_distance_from_underlying": round(abs(long_strike - underlying_price), 4),
                    "short_distance_from_underlying": round(abs(short_strike - underlying_price), 4),
                    "distance_from_target_risk_mid": round(abs(max_loss_dollars_1lot - target_risk_mid), 2),
                }
            )

    candidates.sort(
        key=lambda x: (
            not x["fits_risk_budget"],
            not x["feasibility_pass"],
            x["distance_from_target_risk_mid"],
            x["long_distance_from_underlying"],
            x["width"],
            x["est_debit"],
        )
    )

    return {
        "ok": True,
        "symbol": clean_symbol,
        "underlying_price": underlying_price,
        "expiration_date": expiration_date,
        "option_type": clean_option_type,
        "width_min": width_min,
        "width_max": width_max,
        "risk_min_dollars": risk_min_dollars,
        "risk_max_dollars": risk_max_dollars,
        "hard_max_dollars": hard_max_dollars,
        "enforce_hard_max": enforce_hard_max,
        "only_preferred": only_preferred,
        "near_limit": near_limit,
        "count": len(candidates),
        "pricing_rule": "mid_then_mark_then_last",
        "candidates": candidates[:limit],
    }
