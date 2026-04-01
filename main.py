
import os
import re
from datetime import datetime, time, timedelta
from html import unescape
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from dxlink_candles import get_1h_ema50_snapshot

app = FastAPI(title="SAFE-FAST Backend", version="1.8.0")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/1.8.0"

TT_CLIENT_ID = os.getenv("TT_CLIENT_ID", "")
TT_CLIENT_SECRET = os.getenv("TT_CLIENT_SECRET", "")
TT_REDIRECT_URI = os.getenv("TT_REDIRECT_URI", "")
TT_REFRESH_TOKEN = os.getenv("TT_REFRESH_TOKEN", "")

ALLOWED_SYMBOLS = {"SPY", "QQQ", "IWM", "GLD"}
SYMBOL_ORDER = ["SPY", "QQQ", "IWM", "GLD"]

NY_TZ = ZoneInfo("America/New_York")


class OnDemandRequest(BaseModel):
    option_type: str = "C"
    min_dte: int = 14
    max_dte: int = 30
    near_limit: int = 16
    width_min: float = 5.0
    width_max: float = 10.0
    risk_min_dollars: float = 250.0
    risk_max_dollars: float = 300.0
    hard_max_dollars: float = 400.0
    allow_fallback: bool = True
    include_chart_checks: bool = True
    open_positions: int = 0
    weekly_trade_count: int = 0
    macro_context_requested: bool = True


def _headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }


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
    for field in ("mid", "mark", "last", "bid", "ask"):
        value = _to_float(contract.get(field))
        if value is not None:
            return value
    return None


def _market_context_now() -> Dict[str, Any]:
    now_et = datetime.now(NY_TZ)
    is_weekday = now_et.weekday() < 5
    in_regular_session = time(9, 30) <= now_et.time() < time(16, 0)
    is_open = is_weekday and in_regular_session

    return {
        "is_open": is_open,
        "as_of_et": now_et.isoformat(timespec="seconds"),
        "session": "regular" if is_open else "closed",
    }


def _other_ticker_candidates(
    summary_payload: Dict[str, Any],
    best_ticker: Optional[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for s in summary_payload.get("ticker_summaries", []):
        if s.get("symbol") == best_ticker:
            continue
        out.append(
            {
                "symbol": s.get("symbol"),
                "verdict": s.get("verdict"),
                "reason": s.get("reason"),
                "primary_candidate": s.get("primary_candidate"),
            }
        )

    return out


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
            params={"type": "Equity", "symbols": ",".join(symbols)},
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
            params={"equity-option": ",".join(option_symbols)},
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
    for field in ("mark", "last", "mid", "close"):
        value = _to_float(item.get(field))
        if value is not None:
            return value

    raise HTTPException(status_code=500, detail="Could not determine underlying price")


def _extract_expirations(chain_payload: Any, min_dte: int, max_dte: int) -> List[Dict[str, Any]]:
    items = chain_payload.get("data", {}).get("items", [])
    expirations: List[Dict[str, Any]] = []
    seen = set()

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
    return expirations


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
                "strike_price": strike_value,
                "distance_from_underlying": round(abs(strike_value - underlying_price), 4),
                "expiration_date": item.get("expiration-date"),
                "days_to_expiration": item.get("days-to-expiration"),
                "option_type": item.get("option-type"),
            }
        )

    contracts.sort(key=lambda x: (x["distance_from_underlying"], x["strike_price"]))
    return contracts


def _merge_quotes_into_contracts(
    near_contracts: List[Dict[str, Any]],
    quote_payload: Any,
) -> List[Dict[str, Any]]:
    quote_items = quote_payload.get("data", {}).get("items", [])
    quote_map = {item.get("symbol"): item for item in quote_items}

    merged: List[Dict[str, Any]] = []
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
            }
        )

    return merged


def _generate_debit_spread_candidates(
    contracts: List[Dict[str, Any]],
    underlying_price: float,
    option_type: str,
    width_min: float,
    width_max: float,
    risk_min_dollars: float,
    risk_max_dollars: float,
    hard_max_dollars: float,
    enforce_hard_max: bool,
    only_preferred: bool,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    target_risk_mid = (risk_min_dollars + risk_max_dollars) / 2.0

    ordered = sorted(contracts, key=lambda c: (c["strike_price"] is None, c["strike_price"]))

    for i in range(len(ordered)):
        for j in range(i + 1, len(ordered)):
            left = ordered[i]
            right = ordered[j]

            left_strike = _to_float(left.get("strike_price"))
            right_strike = _to_float(right.get("strike_price"))
            if left_strike is None or right_strike is None:
                continue

            width = round(abs(right_strike - left_strike), 4)
            if width < width_min or width > width_max:
                continue

            if option_type == "C":
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

            max_loss_dollars_1lot = round(est_debit * 100, 2)
            max_profit_dollars_1lot = round((width - est_debit) * 100, 2)
            feasibility_pass = (1.6 * est_debit) <= width
            within_hard_max = max_loss_dollars_1lot <= hard_max_dollars
            preferred_risk_band_pass = risk_min_dollars <= max_loss_dollars_1lot <= risk_max_dollars

            if enforce_hard_max and not within_hard_max:
                continue
            if only_preferred and not preferred_risk_band_pass:
                continue

            long_strike = _to_float(long_leg.get("strike_price")) or 0.0

            candidates.append(
                {
                    "long_symbol": long_leg.get("symbol"),
                    "short_symbol": short_leg.get("symbol"),
                    "long_strike": left_strike if option_type == "C" else right_strike,
                    "short_strike": right_strike if option_type == "C" else left_strike,
                    "width": width,
                    "est_debit": est_debit,
                    "max_loss_dollars_1lot": max_loss_dollars_1lot,
                    "max_profit_dollars_1lot": max_profit_dollars_1lot,
                    "risk_reward": round(max_profit_dollars_1lot / max_loss_dollars_1lot, 4) if max_loss_dollars_1lot > 0 else None,
                    "feasibility_pass": feasibility_pass,
                    "preferred_risk_band_pass": preferred_risk_band_pass,
                    "within_hard_max": within_hard_max,
                    "fits_risk_budget": preferred_risk_band_pass and within_hard_max,
                    "long_distance_from_underlying": round(abs(long_strike - underlying_price), 4),
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
    return candidates


def _select_shortlist(all_candidates: List[Dict[str, Any]], allow_fallback: bool) -> Dict[str, Any]:
    preferred = [c for c in all_candidates if c["feasibility_pass"] and c["fits_risk_budget"]]
    fallback = [c for c in all_candidates if c["feasibility_pass"] and c["within_hard_max"]]

    if preferred:
        selected = preferred
        selection_mode = "preferred"
        reason = "Using candidates that pass feasibility, preferred risk band, and hard max."
    elif allow_fallback and fallback:
        selected = fallback
        selection_mode = "fallback"
        reason = "No preferred candidates found. Using feasible candidates that still stay under hard max."
    else:
        selected = []
        selection_mode = "none"
        reason = "No feasible candidates found for the current filters."

    return {
        "selection_mode": selection_mode,
        "reason": reason,
        "preferred_count": len(preferred),
        "fallback_count": len(fallback),
        "primary_candidate": selected[0] if len(selected) >= 1 else None,
        "backup_candidate": selected[1] if len(selected) >= 2 else None,
    }


def _compact_candidate(candidate: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidate:
        return None
    return {
        "long_symbol": candidate.get("long_symbol"),
        "short_symbol": candidate.get("short_symbol"),
        "long_strike": candidate.get("long_strike"),
        "short_strike": candidate.get("short_strike"),
        "width": candidate.get("width"),
        "est_debit": candidate.get("est_debit"),
        "max_loss_dollars_1lot": candidate.get("max_loss_dollars_1lot"),
        "max_profit_dollars_1lot": candidate.get("max_profit_dollars_1lot"),
        "risk_reward": candidate.get("risk_reward"),
        "feasibility_pass": candidate.get("feasibility_pass"),
        "fits_risk_budget": candidate.get("fits_risk_budget"),
        "distance_from_target_risk_mid": candidate.get("distance_from_target_risk_mid"),
    }


def _compact_ticker_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": summary.get("symbol"),
        "verdict": summary.get("verdict"),
        "selection_mode": summary.get("selection_mode"),
        "expiration_date": summary.get("expiration_date"),
        "days_to_expiration": summary.get("days_to_expiration"),
        "underlying_price": summary.get("underlying_price"),
        "preferred_count": summary.get("preferred_count"),
        "fallback_count": summary.get("fallback_count"),
        "reason": summary.get("reason"),
        "primary_candidate": _compact_candidate(summary.get("primary_candidate")),
        "backup_candidate": _compact_candidate(summary.get("backup_candidate")),
    }


def _rank_ticker_summaries(ticker_summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        ticker_summaries,
        key=lambda x: (
            {"ACTIVE_NOW": 0, "PENDING": 1, "NO_TRADE": 2}.get(x["verdict"], 3),
            x["primary_candidate"]["distance_from_target_risk_mid"] if x.get("primary_candidate") else 999999,
            SYMBOL_ORDER.index(x["symbol"]) if x["symbol"] in SYMBOL_ORDER else 999999,
        ),
    )


async def _build_ticker_summary(
    symbol: str,
    option_type: str,
    min_dte: int,
    max_dte: int,
    near_limit: int,
    width_min: float,
    width_max: float,
    risk_min_dollars: float,
    risk_max_dollars: float,
    hard_max_dollars: float,
    allow_fallback: bool,
    token: str,
) -> Dict[str, Any]:
    chain_payload = await _fetch_option_chain(symbol, token)
    expirations = _extract_expirations(chain_payload, min_dte, max_dte)

    if not expirations:
        return {
            "symbol": symbol,
            "verdict": "NO_TRADE",
            "reason": "No expirations found in requested DTE range.",
            "selection_mode": "none",
            "expiration_date": None,
            "days_to_expiration": None,
            "underlying_price": None,
            "preferred_count": 0,
            "fallback_count": 0,
            "primary_candidate": None,
            "backup_candidate": None,
        }

    chosen_expiration = expirations[0]
    underlying_price = await _get_underlying_price(symbol, token)

    near_contracts = _build_near_contracts(
        chain_payload=chain_payload,
        expiration_date=chosen_expiration["expiration_date"],
        option_type=option_type,
        underlying_price=underlying_price,
    )[:near_limit]

    option_symbols = [c["symbol"] for c in near_contracts if c.get("symbol")]
    quote_payload = await _fetch_option_quotes(option_symbols, token)
    merged_contracts = _merge_quotes_into_contracts(near_contracts, quote_payload)

    all_candidates = _generate_debit_spread_candidates(
        contracts=merged_contracts,
        underlying_price=underlying_price,
        option_type=option_type,
        width_min=width_min,
        width_max=width_max,
        risk_min_dollars=risk_min_dollars,
        risk_max_dollars=risk_max_dollars,
        hard_max_dollars=hard_max_dollars,
        enforce_hard_max=True,
        only_preferred=False,
    )

    shortlist = _select_shortlist(all_candidates, allow_fallback)

    if shortlist["selection_mode"] == "preferred":
        verdict = "ACTIVE_NOW"
    elif shortlist["selection_mode"] == "fallback":
        verdict = "PENDING"
    else:
        verdict = "NO_TRADE"

    return {
        "symbol": symbol,
        "verdict": verdict,
        "reason": shortlist["reason"],
        "selection_mode": shortlist["selection_mode"],
        "expiration_date": chosen_expiration["expiration_date"],
        "days_to_expiration": chosen_expiration["days_to_expiration"],
        "underlying_price": underlying_price,
        "preferred_count": shortlist["preferred_count"],
        "fallback_count": shortlist["fallback_count"],
        "primary_candidate": shortlist["primary_candidate"],
        "backup_candidate": shortlist["backup_candidate"],
    }


async def _build_summary_compact_payload(
    option_type: str,
    min_dte: int,
    max_dte: int,
    near_limit: int,
    width_min: float,
    width_max: float,
    risk_min_dollars: float,
    risk_max_dollars: float,
    hard_max_dollars: float,
    allow_fallback: bool,
    token: str,
) -> Dict[str, Any]:
    clean_option_type = _clean_option_type(option_type)
    ticker_summaries = []

    for symbol in SYMBOL_ORDER:
        summary = await _build_ticker_summary(
            symbol=symbol,
            option_type=clean_option_type,
            min_dte=min_dte,
            max_dte=max_dte,
            near_limit=near_limit,
            width_min=width_min,
            width_max=width_max,
            risk_min_dollars=risk_min_dollars,
            risk_max_dollars=risk_max_dollars,
            hard_max_dollars=hard_max_dollars,
            allow_fallback=allow_fallback,
            token=token,
        )
        ticker_summaries.append(summary)

    ranked = _rank_ticker_summaries(ticker_summaries)
    best_summary = ranked[0] if ranked else None
    best_ticker = best_summary["symbol"] if best_summary and best_summary["primary_candidate"] else None
    verdict = best_summary["verdict"] if best_summary else "NO_TRADE"

    return {
        "ok": True,
        "verdict": verdict,
        "best_ticker": best_ticker,
        "selection_mode": best_summary["selection_mode"] if best_summary else "none",
        "reason": best_summary["reason"] if best_summary else "No summary available.",
        "primary_candidate": _compact_candidate(best_summary["primary_candidate"]) if best_summary else None,
        "backup_candidate": _compact_candidate(best_summary["backup_candidate"]) if best_summary else None,
        "ticker_summaries": [_compact_ticker_summary(s) for s in ticker_summaries],
    }



async def _build_chart_check_payload(symbol: str, token: str) -> Dict[str, Any]:
    snapshot = await get_1h_ema50_snapshot(
        symbol=symbol,
        access_token=token,
        api_base=API_BASE,
        user_agent=USER_AGENT,
        days_back=14,
    )
    return {
        "ok": True,
        "symbol": symbol,
        "latest_close": snapshot["latest_close"],
        "ema50_1h": snapshot["ema50_1h"],
        "price_vs_ema50_1h": snapshot["price_vs_ema50_1h"],
        "latest_candle_time": snapshot["latest_candle_time"],
        "candle_count": snapshot["candle_count"],
        "recent_candles": snapshot.get("recent_candles", []),
        "_all_candles": snapshot.get("all_candles", []),
    }


def _calc_ema(values: List[float], length: int) -> Optional[float]:
    if not values:
        return None
    multiplier = 2 / (length + 1)
    ema = values[0]
    for value in values[1:]:
        ema = ((value - ema) * multiplier) + ema
    return round(ema, 4)


def _candles_by_day_et(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    ordered_days: List[str] = []

    for candle in candles:
        ts = datetime.fromisoformat(candle["time_iso"]).astimezone(NY_TZ)
        day_key = ts.date().isoformat()
        if day_key not in grouped:
            grouped[day_key] = {
                "date": day_key,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
            }
            ordered_days.append(day_key)
        else:
            grouped[day_key]["high"] = max(grouped[day_key]["high"], candle["high"])
            grouped[day_key]["low"] = min(grouped[day_key]["low"], candle["low"])
            grouped[day_key]["close"] = candle["close"]

    return [grouped[day] for day in ordered_days]


def _condense_levels(levels: List[float], tolerance: float, descending: bool = False) -> List[float]:
    ordered = sorted(levels, reverse=descending)
    out: List[float] = []
    for level in ordered:
        if not out or abs(level - out[-1]) > tolerance:
            out.append(level)
    return out


def _find_wall_levels(
    candles: List[Dict[str, Any]],
    latest_close: float,
    option_type: str,
) -> Dict[str, Any]:
    if not candles:
        return {
            "first_wall": None,
            "next_pocket": None,
            "room_distance": None,
            "room_ratio": None,
            "room_pass": None,
        }

    window = candles[-35:] if len(candles) >= 35 else candles
    tolerance = max(latest_close * 0.0015, 0.10)

    if option_type == "C":
        candidate_levels = [round(c["high"], 2) for c in window if c["high"] > latest_close]
        levels = _condense_levels(candidate_levels, tolerance, descending=False)
        first_wall = levels[0] if levels else None
        next_pocket = levels[1] if len(levels) > 1 else None
    else:
        candidate_levels = [round(c["low"], 2) for c in window if c["low"] < latest_close]
        levels = _condense_levels(candidate_levels, tolerance, descending=True)
        first_wall = levels[0] if levels else None
        next_pocket = levels[1] if len(levels) > 1 else None

    return {
        "first_wall": first_wall,
        "next_pocket": next_pocket,
        "room_distance": round(abs(first_wall - latest_close), 4) if first_wall is not None else None,
    }


def _twentyfour_hour_context(candles: List[Dict[str, Any]], option_type: str) -> Dict[str, Any]:
    daily_bars = _candles_by_day_et(candles)
    closes = [bar["close"] for bar in daily_bars if bar.get("close") is not None]

    if len(closes) < 4:
        return {
            "label": "unconfirmed",
            "supportive": None,
            "source": "1h_aggregated_daily_proxy",
        }

    ema3 = _calc_ema(closes[-6:], 3)
    ema5 = _calc_ema(closes[-6:], 5)
    slope_up = ema3 is not None and ema5 is not None and ema3 > ema5 and closes[-1] > closes[-3]
    slope_down = ema3 is not None and ema5 is not None and ema3 < ema5 and closes[-1] < closes[-3]

    if slope_up:
        label = "bullish"
    elif slope_down:
        label = "bearish"
    else:
        label = "mixed"

    supportive = None
    if option_type == "C":
        supportive = True if label == "bullish" else False if label == "bearish" else None
    else:
        supportive = True if label == "bearish" else False if label == "bullish" else None

    return {
        "label": label,
        "supportive": supportive,
        "source": "1h_aggregated_daily_proxy",
    }


def _is_chop(candles: List[Dict[str, Any]]) -> bool:
    if len(candles) < 4:
        return False
    recent = candles[-4:]
    overlap_hits = 0
    for i in range(1, len(recent)):
        current = recent[i]
        prev = recent[i - 1]
        overlap = max(0.0, min(current["high"], prev["high"]) - max(current["low"], prev["low"]))
        current_range = max(current["high"] - current["low"], 0.0001)
        if (overlap / current_range) > 0.5:
            overlap_hits += 1
    return overlap_hits >= 3


def _extension_state(
    symbol: str,
    latest_close: float,
    ema50_1h: float,
    first_wall: Optional[float],
) -> Dict[str, Any]:
    pct_from_ema = abs(latest_close - ema50_1h) / ema50_1h if ema50_1h else None
    threshold = 0.008 if symbol == "GLD" else 0.006
    room_distance = abs(first_wall - latest_close) if first_wall is not None else None
    move_ratio = (abs(latest_close - ema50_1h) / room_distance) if room_distance not in (None, 0) else None
    is_extended = bool(
        (pct_from_ema is not None and pct_from_ema > threshold) or
        (move_ratio is not None and move_ratio > 0.5)
    )
    return {
        "state": "extended" if is_extended else "acceptable",
        "pct_from_ema": round(pct_from_ema * 100, 3) if pct_from_ema is not None else None,
        "move_to_wall_ratio": round(move_ratio, 3) if move_ratio is not None else None,
        "threshold_pct": round(threshold * 100, 3),
        "late_move": is_extended,
    }


def _wall_thesis(
    option_type: str,
    primary_candidate: Optional[Dict[str, Any]],
    first_wall: Optional[float],
    next_pocket: Optional[float],
    invalidation_distance: Optional[float],
) -> Dict[str, Any]:
    if not primary_candidate or first_wall is None:
        return {
            "wall_thesis": "unconfirmed",
            "wall_pass": None,
        }

    short_strike = _to_float(primary_candidate.get("short_strike"))
    if short_strike is None:
        return {
            "wall_thesis": "unconfirmed",
            "wall_pass": None,
        }

    next_pocket_room = None
    if next_pocket is not None and invalidation_distance not in (None, 0):
        next_pocket_room = abs(next_pocket - first_wall) / invalidation_distance

    if option_type == "C":
        if short_strike > first_wall:
            return {"wall_thesis": "TO_THE_WALL", "wall_pass": True, "next_pocket_room_ratio": next_pocket_room}
        if next_pocket is not None and (next_pocket_room or 0) >= 1.5:
            return {"wall_thesis": "THROUGH_THE_WALL", "wall_pass": True, "next_pocket_room_ratio": next_pocket_room}
    else:
        if short_strike < first_wall:
            return {"wall_thesis": "TO_THE_WALL", "wall_pass": True, "next_pocket_room_ratio": next_pocket_room}
        if next_pocket is not None and (next_pocket_room or 0) >= 1.5:
            return {"wall_thesis": "THROUGH_THE_WALL", "wall_pass": True, "next_pocket_room_ratio": next_pocket_room}

    return {
        "wall_thesis": "WALL_MISMATCH",
        "wall_pass": False,
        "next_pocket_room_ratio": next_pocket_room,
    }


def _setup_classifier(
    option_type: str,
    chart_check: Dict[str, Any],
    trend_ctx: Dict[str, Any],
    room_ratio: Optional[float],
    room_pass: Optional[bool],
    wall_pass: Optional[bool],
    extension_state: Dict[str, Any],
    candles: List[Dict[str, Any]],
) -> Dict[str, Any]:
    latest_close = chart_check.get("latest_close")
    ema50_1h = chart_check.get("ema50_1h")

    if latest_close is None or ema50_1h is None:
        return {"setup_type": "UNCONFIRMED", "trend_label": "unconfirmed", "allowed_setup": None}

    near_ema = abs(latest_close - ema50_1h) / ema50_1h <= 0.0025
    chop = _is_chop(candles)
    recent_closes = [c["close"] for c in candles[-3:]] if len(candles) >= 3 else []
    tight_break = False
    if recent_closes and latest_close:
        tight_break = (max(recent_closes) - min(recent_closes)) / latest_close <= 0.003

    if room_pass is False or wall_pass is False or extension_state.get("state") == "extended":
        return {"setup_type": "NOT_ALLOWED", "trend_label": "unconfirmed", "allowed_setup": False}

    trend_supportive = trend_ctx.get("supportive")
    if trend_supportive is True:
        if near_ema and (room_ratio or 0) >= 2.5 and not chop:
            return {"setup_type": "Ideal", "trend_label": "Trend-aligned", "allowed_setup": True}
        if tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": "Trend-aligned", "allowed_setup": True}
        if near_ema:
            return {"setup_type": "Continuation", "trend_label": "Trend-aligned", "allowed_setup": True}
        return {"setup_type": "PENDING_CONTINUATION", "trend_label": "Trend-aligned", "allowed_setup": False}

    if trend_supportive is False:
        if tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": "Countertrend", "allowed_setup": True}
        return {"setup_type": "NOT_ALLOWED", "trend_label": "Countertrend", "allowed_setup": False}

    return {"setup_type": "UNCONFIRMED", "trend_label": "unconfirmed", "allowed_setup": None}


def _build_structure_context(
    symbol: str,
    option_type: str,
    chart_check: Optional[Dict[str, Any]],
    primary_candidate: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not chart_check or not chart_check.get("ok"):
        return {
            "ok": False,
            "why": "chart check unavailable",
        }

    candles = chart_check.get("_all_candles", [])
    if not candles:
        return {
            "ok": False,
            "why": "full candle history unavailable",
        }

    latest_close = chart_check.get("latest_close")
    ema50_1h = chart_check.get("ema50_1h")
    if latest_close is None or ema50_1h is None:
        return {
            "ok": False,
            "why": "missing latest close or ema",
        }

    trend_ctx = _twentyfour_hour_context(candles, option_type)
    wall_levels = _find_wall_levels(candles, latest_close, option_type)
    invalidation_distance = abs(latest_close - ema50_1h) if ema50_1h is not None else None
    room_ratio = None
    if wall_levels["room_distance"] is not None and invalidation_distance not in (None, 0):
        room_ratio = wall_levels["room_distance"] / invalidation_distance

    room_pass = (room_ratio is not None and room_ratio >= 2.0)
    extension_ctx = _extension_state(symbol, latest_close, ema50_1h, wall_levels.get("first_wall"))
    wall_ctx = _wall_thesis(
        option_type=option_type,
        primary_candidate=primary_candidate,
        first_wall=wall_levels.get("first_wall"),
        next_pocket=wall_levels.get("next_pocket"),
        invalidation_distance=invalidation_distance,
    )
    setup_ctx = _setup_classifier(
        option_type=option_type,
        chart_check=chart_check,
        trend_ctx=trend_ctx,
        room_ratio=room_ratio,
        room_pass=room_pass,
        wall_pass=wall_ctx.get("wall_pass"),
        extension_state=extension_ctx,
        candles=candles,
    )

    return {
        "ok": True,
        "twentyfour_hour_trend": trend_ctx.get("label"),
        "twentyfour_hour_supportive": trend_ctx.get("supportive"),
        "twentyfour_hour_source": trend_ctx.get("source"),
        "first_wall": wall_levels.get("first_wall"),
        "next_pocket": wall_levels.get("next_pocket"),
        "room_to_first_wall": wall_levels.get("room_distance"),
        "room_ratio": round(room_ratio, 3) if room_ratio is not None else None,
        "room_pass": room_pass,
        "wall_thesis": wall_ctx.get("wall_thesis"),
        "wall_pass": wall_ctx.get("wall_pass"),
        "next_pocket_room_ratio": wall_ctx.get("next_pocket_room_ratio"),
        "extension_state": extension_ctx.get("state"),
        "pct_from_ema": extension_ctx.get("pct_from_ema"),
        "late_move": extension_ctx.get("late_move"),
        "iv_state": "unconfirmed",
        "setup_type": setup_ctx.get("setup_type"),
        "trend_label": setup_ctx.get("trend_label"),
        "allowed_setup": setup_ctx.get("allowed_setup"),
        "chop_risk": _is_chop(candles),
    }


def _status_field(value: Any, confirmed: bool) -> Dict[str, Any]:

    return {"status": "confirmed" if confirmed else "unconfirmed", "value": value}


def _chart_alignment_ok(option_type: str, chart_check: Optional[Dict[str, Any]]) -> Optional[bool]:
    if not chart_check or not chart_check.get("ok"):
        return None
    side = chart_check.get("price_vs_ema50_1h")
    return side == "above" if option_type == "C" else side == "below"



def _final_verdict(
    request: OnDemandRequest,
    engine_status: str,
    chart_alignment: Optional[bool],
    market_context: Dict[str, Any],
    macro_context: Dict[str, Any],
    structure_context: Dict[str, Any],
) -> str:
    if request.open_positions > 0:
        return "NO_TRADE"
    if request.weekly_trade_count >= 4:
        return "NO_TRADE"
    if not market_context["is_open"]:
        return "NO_TRADE"
    if macro_context.get("ok") and (
        macro_context.get("has_major_event_today") or macro_context.get("has_major_event_tomorrow")
    ):
        return "NO_TRADE"
    if engine_status == "NO_TRADE":
        return "NO_TRADE"
    if chart_alignment is False:
        return "NO_TRADE"
    if structure_context.get("ok"):
        if structure_context.get("room_pass") is False:
            return "NO_TRADE"
        if structure_context.get("wall_pass") is False:
            return "NO_TRADE"
        if structure_context.get("extension_state") == "extended":
            return "NO_TRADE"
        if structure_context.get("allowed_setup") is False:
            return "NO_TRADE"
    return "PENDING"




def _build_chart_confirmation_block(
    request: OnDemandRequest,
    chart_check: Optional[Dict[str, Any]],
    chart_check_error: Optional[str],
    structure_context: Dict[str, Any],
) -> Dict[str, Any]:
    one_hour_confirmed = bool(chart_check and chart_check.get("ok"))
    structure_confirmed = bool(structure_context.get("ok"))
    message = (
        "Candidate engine result only - chart confirmation still required. Chart check failed in this run."
        if chart_check_error
        else "Candidate engine result only - chart confirmation still required."
    )
    return {
        "confirmed": False,
        "message": message,
        "fields": {
            "one_hour_50_ema": _status_field(chart_check.get("ema50_1h") if chart_check else None, one_hour_confirmed),
            "one_hour_price_vs_50_ema": _status_field(chart_check.get("price_vs_ema50_1h") if chart_check else None, one_hour_confirmed),
            "latest_close": _status_field(chart_check.get("latest_close") if chart_check else None, one_hour_confirmed),
            "twentyfour_hour_trend": _status_field(structure_context.get("twentyfour_hour_trend"), structure_confirmed),
            "room_to_first_wall": _status_field(structure_context.get("room_to_first_wall"), structure_confirmed),
            "first_wall": _status_field(structure_context.get("first_wall"), structure_confirmed),
            "next_pocket": _status_field(structure_context.get("next_pocket"), structure_confirmed),
            "room_ratio": _status_field(structure_context.get("room_ratio"), structure_confirmed),
            "wall_thesis": _status_field(structure_context.get("wall_thesis"), structure_confirmed),
            "extension_state": _status_field(structure_context.get("extension_state"), structure_confirmed),
            "iv_state": _status_field(structure_context.get("iv_state"), False),
            "setup_type": _status_field(structure_context.get("setup_type"), structure_confirmed),
            "trend_label": _status_field(structure_context.get("trend_label"), structure_confirmed),
            "open_positions_state": _status_field(request.open_positions, True),
            "weekly_trade_count_state": _status_field(request.weekly_trade_count, True),
        },
    }



def _build_user_facing_block(
    request: OnDemandRequest,
    engine_status: str,
    final_verdict: str,
    best_ticker: Optional[str],
    chart_check: Optional[Dict[str, Any]],
    chart_check_error: Optional[str],
    engine_reason: str,
    market_context: Dict[str, Any],
    macro_context: Dict[str, Any],
    structure_context: Dict[str, Any],
) -> Dict[str, Any]:
    ticker = best_ticker or "UNKNOWN"
    ema_text = str(chart_check.get("ema50_1h")) if chart_check and chart_check.get("ok") else "unconfirmed"

    if request.open_positions > 0:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No new entry allowed while open_positions > 0.",
            "setup_state": "NO TRADE",
            "why": "You already have 1 open position. SAFE-FAST allows max 1 open trade total.",
        }

    if request.weekly_trade_count >= 4:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No new entry allowed after max weekly trade count is reached.",
            "setup_state": "NO TRADE",
            "why": "Weekly trade count is already at or above the SAFE-FAST max.",
        }

    if not market_context["is_open"]:
        return {
            "good_idea_now": "WAIT",
            "ticker": ticker,
            "action": "wait for next regular session",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "WAIT_MARKET_CLOSED",
            "why": f"Candidate exists, but the regular session is closed as of {market_context['as_of_et']}. Re-check next session before entry.",
        }

    if macro_context.get("ok") and (
        macro_context.get("has_major_event_today") or macro_context.get("has_major_event_tomorrow")
    ):
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "NO TRADE",
            "why": macro_context.get("note") or "Major event risk is inside the expected hold window.",
        }

    if engine_status == "NO_TRADE" or not best_ticker:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No valid candidate engine setup is available.",
            "setup_state": "NO TRADE",
            "why": engine_reason,
        }

    if structure_context.get("ok"):
        if structure_context.get("room_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": "Room to first wall is too tight for SAFE-FAST.",
            }
        if structure_context.get("wall_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": "Wall thesis and strike placement do not match.",
            }
        if structure_context.get("extension_state") == "extended":
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": "Move is extended vs the 1H 50 EMA or too late relative to the first wall.",
            }
        if structure_context.get("allowed_setup") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": f"Setup type is {structure_context.get('setup_type')}, which is not tradable now.",
            }

    if final_verdict == "NO_TRADE":
        why = "Best ticker failed the 1H EMA alignment check."
        if chart_check_error:
            why = "Chart check failed in this run."
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No valid new entry from the current combined read.",
            "setup_state": "NO TRADE",
            "why": why,
        }

    return {
        "good_idea_now": "WAIT",
        "ticker": ticker,
        "action": "wait for full chart confirmation",
        "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
        "setup_state": "PENDING",
        "why": "Candidate engine is valid, but trigger/entry-zone timing still needs confirmation.",
    }




async def _build_on_demand_payload(request: OnDemandRequest) -> Dict[str, Any]:
    clean_option_type = _clean_option_type(request.option_type)
    market_context = _market_context_now()
    macro_context = await _build_macro_context(request.macro_context_requested)

    if request.open_positions < 0 or request.open_positions > 1:
        raise HTTPException(status_code=400, detail="open_positions must be 0 or 1")
    if request.weekly_trade_count < 0:
        raise HTTPException(status_code=400, detail="weekly_trade_count must be >= 0")

    token = await get_access_token()
    summary_payload = await _build_summary_compact_payload(
        option_type=clean_option_type,
        min_dte=request.min_dte,
        max_dte=request.max_dte,
        near_limit=request.near_limit,
        width_min=request.width_min,
        width_max=request.width_max,
        risk_min_dollars=request.risk_min_dollars,
        risk_max_dollars=request.risk_max_dollars,
        hard_max_dollars=request.hard_max_dollars,
        allow_fallback=request.allow_fallback,
        token=token,
    )

    best_ticker = summary_payload.get("best_ticker")
    engine_status = summary_payload.get("verdict", "NO_TRADE")
    primary_candidate = summary_payload.get("primary_candidate")
    chart_check: Optional[Dict[str, Any]] = None
    chart_check_error: Optional[str] = None

    if request.include_chart_checks and best_ticker:
        try:
            chart_check = await _build_chart_check_payload(best_ticker, token)
        except Exception as e:
            chart_check_error = str(e)

    structure_context = _build_structure_context(
        symbol=best_ticker or "UNKNOWN",
        option_type=clean_option_type,
        chart_check=chart_check,
        primary_candidate=primary_candidate,
    ) if best_ticker else {"ok": False, "why": "no best ticker"}

    chart_alignment = _chart_alignment_ok(clean_option_type, chart_check)
    final_verdict = _final_verdict(
        request=request,
        engine_status=engine_status,
        chart_alignment=chart_alignment,
        market_context=market_context,
        macro_context=macro_context,
        structure_context=structure_context,
    )

    if request.include_chart_checks:
        chart_check_block: Dict[str, Any] = chart_check if chart_check else {
            "ok": False,
            "symbol": best_ticker,
            "error": chart_check_error or "Chart check unavailable in this run.",
        }
    else:
        chart_check_block = {
            "ok": False,
            "symbol": best_ticker,
            "status": "skipped",
            "message": "Chart checks were not requested.",
        }

    # remove internal candle payload from external response
    if chart_check_block.get("_all_candles") is not None:
        chart_check_block = {k: v for k, v in chart_check_block.items() if k != "_all_candles"}

    return {
        "ok": True,
        "mode": "on_demand",
        "source_of_truth": "candidate_engine",
        "engine_status": engine_status,
        "final_verdict": final_verdict,
        "best_ticker": best_ticker,
        "market_context": market_context,
        "macro_context": macro_context,
        "structure_context": structure_context,
        "other_ticker_candidates": _other_ticker_candidates(summary_payload, best_ticker),
        "request": request.model_dump(),
        "candidate_engine": summary_payload,
        "chart_check": chart_check_block,
        "chart_confirmation": _build_chart_confirmation_block(
            request=request,
            chart_check=chart_check,
            chart_check_error=chart_check_error,
            structure_context=structure_context,
        ),
        "user_facing": _build_user_facing_block(
            request=request,
            engine_status=engine_status,
            final_verdict=final_verdict,
            best_ticker=best_ticker,
            chart_check=chart_check,
            chart_check_error=chart_check_error,
            engine_reason=summary_payload.get("reason", "No summary available."),
            market_context=market_context,
            macro_context=macro_context,
            structure_context=structure_context,
        ),
    }


@app.get("/")

def root() -> Dict[str, Any]:
    return {"status": "ok", "service": "safe-fast-backend"}


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/tt/safe-fast-summary-compact")
async def tt_safe_fast_summary_compact(
    option_type: str = Query("C"),
    min_dte: int = Query(14),
    max_dte: int = Query(30),
    near_limit: int = Query(16),
    width_min: float = Query(5.0),
    width_max: float = Query(10.0),
    risk_min_dollars: float = Query(250.0),
    risk_max_dollars: float = Query(300.0),
    hard_max_dollars: float = Query(400.0),
    allow_fallback: bool = Query(True),
) -> Any:
    token = await get_access_token()
    return await _build_summary_compact_payload(
        option_type=option_type,
        min_dte=min_dte,
        max_dte=max_dte,
        near_limit=near_limit,
        width_min=width_min,
        width_max=width_max,
        risk_min_dollars=risk_min_dollars,
        risk_max_dollars=risk_max_dollars,
        hard_max_dollars=hard_max_dollars,
        allow_fallback=allow_fallback,
        token=token,
    )


@app.get("/tt/safe-fast-chart-check")
async def tt_safe_fast_chart_check(symbol: str = Query("SPY")) -> Any:
    clean_symbol = _clean_symbol(symbol)
    token = await get_access_token()
    try:
        return await _build_chart_check_payload(clean_symbol, token)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.post("/safe-fast/on-demand")
async def safe_fast_on_demand(request: OnDemandRequest) -> Any:
    return await _build_on_demand_payload(request)
