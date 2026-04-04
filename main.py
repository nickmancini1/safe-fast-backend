import asyncio

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

app = FastAPI(title="SAFE-FAST Backend", version="1.9.36")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/1.9.36"

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

def _round_or_none(value: Optional[float], places: int = 4) -> Optional[float]:
    if value is None:
        return None
    return round(value, places)


def _calc_pct_of_mid(bid: Optional[float], ask: Optional[float], mid: Optional[float]) -> Optional[float]:
    if bid is None or ask is None or mid in (None, 0):
        return None
    return round(((ask - bid) / mid) * 100, 3)


def _classify_liquidity(
    mid_debit: Optional[float],
    natural_debit: Optional[float],
    entry_slippage_vs_mid: Optional[float],
    spread_market_width: Optional[float],
    long_leg_width: Optional[float],
    short_leg_width: Optional[float],
    long_leg_width_pct_of_mid: Optional[float],
    short_leg_width_pct_of_mid: Optional[float],
) -> Dict[str, Any]:
    required_values = [
        mid_debit,
        entry_slippage_vs_mid,
        spread_market_width,
        long_leg_width,
        short_leg_width,
    ]
    if any(value is None for value in required_values):
        return {
            "label": "unconfirmed",
            "liquidity_pass": None,
            "why": "Quotes did not provide enough bid/ask detail to confirm liquidity.",
        }

    spread_width_limit = 0.15 if mid_debit < 1.50 else 0.20
    spread_width_pct_of_debit = None
    if mid_debit not in (None, 0):
        spread_width_pct_of_debit = round((spread_market_width / mid_debit) * 100, 3)

    failed_reasons: List[str] = []
    if long_leg_width > 0.15:
        failed_reasons.append("long leg bid/ask exceeds $0.15")
    if short_leg_width > 0.15:
        failed_reasons.append("short leg bid/ask exceeds $0.15")
    if spread_market_width > spread_width_limit:
        failed_reasons.append(f"net spread width exceeds ${spread_width_limit:.2f}")
    if spread_width_pct_of_debit is not None and spread_width_pct_of_debit > 10:
        failed_reasons.append("net spread width exceeds 10% of debit")
    if entry_slippage_vs_mid > 0.05:
        failed_reasons.append("fill would require more than mid + $0.05")

    if failed_reasons:
        return {
            "label": "wide",
            "liquidity_pass": False,
            "why": " / ".join(failed_reasons),
            "spread_width_limit": spread_width_limit,
            "spread_width_pct_of_debit": spread_width_pct_of_debit,
        }

    if (
        long_leg_width <= 0.10
        and short_leg_width <= 0.10
        and spread_market_width <= 0.15
        and entry_slippage_vs_mid <= 0.03
    ):
        return {
            "label": "tight",
            "liquidity_pass": True,
            "why": "Leg widths, net spread width, and entry slippage all pass tight SAFE-FAST liquidity thresholds.",
            "spread_width_limit": spread_width_limit,
            "spread_width_pct_of_debit": spread_width_pct_of_debit,
        }

    return {
        "label": "acceptable",
        "liquidity_pass": True,
        "why": "Liquidity passes SAFE-FAST hard thresholds, but not at the tighter end.",
        "spread_width_limit": spread_width_limit,
        "spread_width_pct_of_debit": spread_width_pct_of_debit,
    }


def _build_liquidity_block(candidate: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidate:
        return {
            "ok": False,
            "status": "unconfirmed",
            "why": "No candidate available.",
        }

    label_ctx = _classify_liquidity(
        candidate.get("est_debit"),
        candidate.get("natural_debit"),
        candidate.get("entry_slippage_vs_mid"),
        candidate.get("spread_market_width"),
        candidate.get("long_leg_width"),
        candidate.get("short_leg_width"),
        candidate.get("long_leg_width_pct_of_mid"),
        candidate.get("short_leg_width_pct_of_mid"),
    )

    return {
        "ok": True,
        "status": label_ctx["label"],
        "liquidity_pass": label_ctx["liquidity_pass"],
        "why": label_ctx["why"],
        "mid_debit": candidate.get("est_debit"),
        "natural_debit": candidate.get("natural_debit"),
        "entry_slippage_vs_mid": candidate.get("entry_slippage_vs_mid"),
        "spread_market_width": candidate.get("spread_market_width"),
        "long_leg_width": candidate.get("long_leg_width"),
        "short_leg_width": candidate.get("short_leg_width"),
        "long_leg_width_pct_of_mid": candidate.get("long_leg_width_pct_of_mid"),
        "short_leg_width_pct_of_mid": candidate.get("short_leg_width_pct_of_mid"),
        "spread_width_limit": label_ctx.get("spread_width_limit"),
        "spread_width_pct_of_debit": label_ctx.get("spread_width_pct_of_debit"),
        "long_iv": candidate.get("long_iv"),
        "short_iv": candidate.get("short_iv"),
        "long_bid_iv": candidate.get("long_bid_iv"),
        "long_ask_iv": candidate.get("long_ask_iv"),
        "short_bid_iv": candidate.get("short_bid_iv"),
        "short_ask_iv": candidate.get("short_ask_iv"),
        "iv_source": candidate.get("iv_source"),
    }




def _extract_option_iv_fields(quote: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort IV extraction from option quote payloads.
    This is intentionally tolerant because quote providers can expose IV fields
    under different names or nested structures.
    """
    if not quote:
        return {
            "iv_mid": None,
            "iv_bid": None,
            "iv_ask": None,
            "iv_source": None,
        }

    def pick(obj: Dict[str, Any], keys: List[str]) -> Optional[float]:
        for key in keys:
            if key in obj:
                value = _to_float(obj.get(key))
                if value is not None:
                    return value
        return None

    # Flat key variants
    iv_mid = pick(
        quote,
        [
            "implied-volatility",
            "implied_volatility",
            "iv",
            "iv_mid",
            "mark-implied-volatility",
            "mark_implied_volatility",
            "mid-implied-volatility",
            "mid_implied_volatility",
        ],
    )
    iv_bid = pick(
        quote,
        [
            "bid-implied-volatility",
            "bid_implied_volatility",
            "iv_bid",
        ],
    )
    iv_ask = pick(
        quote,
        [
            "ask-implied-volatility",
            "ask_implied_volatility",
            "iv_ask",
        ],
    )

    # Nested/extra containers occasionally appear
    if iv_mid is None and isinstance(quote.get("greeks"), dict):
        iv_mid = pick(quote["greeks"], ["implied-volatility", "implied_volatility", "iv"])
    if iv_bid is None and isinstance(quote.get("greeks"), dict):
        iv_bid = pick(quote["greeks"], ["bid-implied-volatility", "bid_implied_volatility", "iv_bid"])
    if iv_ask is None and isinstance(quote.get("greeks"), dict):
        iv_ask = pick(quote["greeks"], ["ask-implied-volatility", "ask_implied_volatility", "iv_ask"])

    source = None
    if any(v is not None for v in [iv_mid, iv_bid, iv_ask]):
        source = "option_quote_fields"

    return {
        "iv_mid": _round_or_none(iv_mid, 6),
        "iv_bid": _round_or_none(iv_bid, 6),
        "iv_ask": _round_or_none(iv_ask, 6),
        "iv_source": source,
    }


def _build_iv_context(primary_candidate: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not primary_candidate:
        return {
            "ok": False,
            "status": "unconfirmed",
            "why": "No primary candidate is available for IV evaluation.",
        }

    long_iv = _to_float(primary_candidate.get("long_iv"))
    short_iv = _to_float(primary_candidate.get("short_iv"))
    long_bid_iv = _to_float(primary_candidate.get("long_bid_iv"))
    long_ask_iv = _to_float(primary_candidate.get("long_ask_iv"))
    short_bid_iv = _to_float(primary_candidate.get("short_bid_iv"))
    short_ask_iv = _to_float(primary_candidate.get("short_ask_iv"))

    available_mid_ivs = [value for value in [long_iv, short_iv] if value is not None]
    spread_mid_iv = round(sum(available_mid_ivs) / len(available_mid_ivs), 6) if available_mid_ivs else None

    if spread_mid_iv is None and all(v is not None for v in [long_bid_iv, long_ask_iv, short_bid_iv, short_ask_iv]):
        spread_mid_iv = round((long_bid_iv + long_ask_iv + short_bid_iv + short_ask_iv) / 4.0, 6)

    if spread_mid_iv is None:
        return {
            "ok": False,
            "status": "unconfirmed",
            "why": "Option quote payload did not include usable IV fields.",
            "source": None,
            "spread_mid_iv": None,
            "long_leg_iv": _round_or_none(long_iv, 6),
            "short_leg_iv": _round_or_none(short_iv, 6),
        }

    return {
        "ok": True,
        "status": "confirmed",
        "source": primary_candidate.get("iv_source") or "option_quote_fields",
        "spread_mid_iv": _round_or_none(spread_mid_iv, 6),
        "long_leg_iv": _round_or_none(long_iv, 6),
        "short_leg_iv": _round_or_none(short_iv, 6),
        "long_leg_bid_iv": _round_or_none(long_bid_iv, 6),
        "long_leg_ask_iv": _round_or_none(long_ask_iv, 6),
        "short_leg_bid_iv": _round_or_none(short_bid_iv, 6),
        "short_leg_ask_iv": _round_or_none(short_ask_iv, 6),
        "why": "IV fields were sourced from option quote data for the selected spread.",
    }


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


def _time_day_gate(market_context: Dict[str, Any]) -> Dict[str, Any]:
    now_et = datetime.fromisoformat(market_context["as_of_et"])
    weekday = now_et.weekday()

    if not market_context.get("is_open"):
        return {
            "fresh_entry_allowed": False,
            "reason": "market_closed",
            "cutoff_et": None,
        }

    # Monday-Thursday: no fresh setups after 2:00 p.m. ET
    if weekday <= 3:
        cutoff = time(14, 0)
        allowed = now_et.time() < cutoff
        return {
            "fresh_entry_allowed": allowed,
            "reason": "within_time_window" if allowed else "past_monday_thursday_cutoff",
            "cutoff_et": "14:00:00",
        }

    # Friday: no fresh setups after 12:00 p.m. ET
    if weekday == 4:
        cutoff = time(12, 0)
        allowed = now_et.time() < cutoff
        return {
            "fresh_entry_allowed": allowed,
            "reason": "within_time_window" if allowed else "past_friday_cutoff",
            "cutoff_et": "12:00:00",
        }

    return {
        "fresh_entry_allowed": False,
        "reason": "weekend",
        "cutoff_et": None,
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


_MACRO_MONTHS = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}


async def _fetch_text(url: str) -> str:
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})
    resp.raise_for_status()
    return unescape(resp.text)


def _next_trading_days(start_dt: datetime, count: int = 3) -> List[datetime.date]:
    out: List[datetime.date] = []
    cur = start_dt.date()
    while len(out) < count:
        if cur.weekday() < 5:
            out.append(cur)
        cur = cur + timedelta(days=1)
    return out


def _parse_month_day_year(raw: str, fallback_year: int) -> Optional[datetime.date]:
    cleaned = raw.strip().replace(",", "").replace(".", "")
    parts = cleaned.split()
    if len(parts) < 2:
        return None

    month = _MACRO_MONTHS.get(parts[0].lower())
    if not month:
        return None

    day_token = parts[1]
    if "-" in day_token:
        day_token = day_token.split("-")[0]
    if "Ã¢ÂÂ" in day_token:
        day_token = day_token.split("Ã¢ÂÂ")[0]
    day_token = re.sub(r"[^0-9]", "", day_token)
    if not day_token:
        return None

    year = fallback_year
    if len(parts) >= 3 and parts[2].isdigit():
        year = int(parts[2])

    try:
        return datetime(year, month, int(day_token), tzinfo=NY_TZ).date()
    except Exception:
        return None


def _extract_dates_by_patterns(text: str, fallback_year: int) -> List[datetime.date]:
    pattern = re.compile(
        r"(January|February|March|April|May|June|July|August|September|October|November|December|"
        r"Jan\.?|Feb\.?|Mar\.?|Apr\.?|May|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?)"
        r"\s+\d{1,2}(?:\s*[-Ã¢ÂÂ]\s*\d{1,2})?(?:,\s*\d{4}|\s+\d{4})?",
        re.IGNORECASE,
    )
    out: List[datetime.date] = []
    seen = set()
    for match in pattern.findall(text):
        # findall with groups only returns group text; use finditer instead
        pass
    for match in pattern.finditer(text):
        parsed = _parse_month_day_year(match.group(0), fallback_year)
        if parsed and parsed not in seen:
            seen.add(parsed)
            out.append(parsed)
    return out


async def _fetch_fomc_events(now_et: datetime) -> List[Dict[str, Any]]:
    text = await _fetch_text("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm")
    dates = _extract_dates_by_patterns(text, now_et.year)
    events = []
    for d in dates:
        if d >= now_et.date() - timedelta(days=1):
            events.append({
                "date": d.isoformat(),
                "event": "FOMC",
                "major": True,
                "source": "federalreserve.gov",
            })
    return events


async def _fetch_bls_events(now_et: datetime) -> List[Dict[str, Any]]:
    urls = [
        ("CPI", "https://www.bls.gov/schedule/news_release/cpi.htm"),
        ("Employment Situation", "https://www.bls.gov/schedule/news_release/empsit.htm"),
    ]
    events: List[Dict[str, Any]] = []
    for label, url in urls:
        text = await _fetch_text(url)
        dates = _extract_dates_by_patterns(text, now_et.year)
        for d in dates:
            if d >= now_et.date() - timedelta(days=1):
                events.append({
                    "date": d.isoformat(),
                    "event": label,
                    "major": True,
                    "source": "bls.gov",
                })
    return events


async def _build_macro_context(requested: bool) -> Dict[str, Any]:
    if not requested:
        return {
            "ok": False,
            "requested": False,
            "why": "macro not requested",
            "has_major_event_today": False,
            "has_major_event_tomorrow": False,
            "events": [],
            "risk_level": "skipped",
            "note": "Macro context not requested.",
        }

    now_et = datetime.now(NY_TZ)
    today = now_et.date()
    tomorrow = today + timedelta(days=1)
    hold_window = {d.isoformat() for d in _next_trading_days(now_et, 3)}

    events: List[Dict[str, Any]] = []
    warnings: List[str] = []

    try:
        events.extend(await _fetch_fomc_events(now_et))
    except Exception as e:
        warnings.append(f"FOMC source unavailable: {e}")

    try:
        events.extend(await _fetch_bls_events(now_et))
    except Exception:
        warnings.append("BLS schedule unavailable; macro check used available sources.")

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for ev in sorted(events, key=lambda x: (x["date"], x["event"])):
        key = (ev["date"], ev["event"])
        if key not in seen:
            seen.add(key)
            deduped.append(ev)

    has_today = any(ev["date"] == today.isoformat() and ev["major"] for ev in deduped)
    has_tomorrow = any(ev["date"] == tomorrow.isoformat() and ev["major"] for ev in deduped)
    visible_events = [ev for ev in deduped if ev["date"] in hold_window]
    in_hold_window = [ev for ev in visible_events if ev["major"]]

    if in_hold_window:
        risk_level = "high"
        note = "Major macro event is inside the next 3 trading days."
    elif visible_events or deduped:
        risk_level = "normal"
        note = "No major macro event found inside the next 3 trading days."
    else:
        risk_level = "unconfirmed"
        note = "Macro sources returned no usable schedule data."

    if warnings:
        note = f"{note} {' | '.join(warnings)}"

    return {
        "ok": True,
        "requested": True,
        "has_major_event_today": has_today,
        "has_major_event_tomorrow": has_tomorrow,
        "events": visible_events,
        "risk_level": risk_level,
        "note": note,
        "as_of_et": now_et.isoformat(timespec="seconds"),
    }


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


async def _fetch_index_quotes(symbols: List[str], token: str) -> Any:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{API_BASE}/market-data",
            headers=_headers(token),
            params={"index": ",".join(symbols)},
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
        iv_fields = _extract_option_iv_fields(quote)
        merged.append(
            {
                **contract,
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "mid": quote.get("mid"),
                "mark": quote.get("mark"),
                "last": quote.get("last"),
                "iv_mid": iv_fields.get("iv_mid"),
                "iv_bid": iv_fields.get("iv_bid"),
                "iv_ask": iv_fields.get("iv_ask"),
                "iv_source": iv_fields.get("iv_source"),
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

            long_bid = _to_float(long_leg.get("bid"))
            long_ask = _to_float(long_leg.get("ask"))
            long_mid = _to_float(long_leg.get("mid")) or _to_float(long_leg.get("mark"))
            short_bid = _to_float(short_leg.get("bid"))
            short_ask = _to_float(short_leg.get("ask"))
            short_mid = _to_float(short_leg.get("mid")) or _to_float(short_leg.get("mark"))

            natural_debit = None
            if long_ask is not None and short_bid is not None:
                natural_debit = round(long_ask - short_bid, 4)

            bid_debit = None
            if long_bid is not None and short_ask is not None:
                bid_debit = round(long_bid - short_ask, 4)

            spread_market_width = None
            if natural_debit is not None and bid_debit is not None:
                spread_market_width = round(natural_debit - bid_debit, 4)

            entry_slippage_vs_mid = None
            if natural_debit is not None:
                entry_slippage_vs_mid = round(max(natural_debit - est_debit, 0.0), 4)

            long_leg_width = None
            if long_bid is not None and long_ask is not None:
                long_leg_width = round(long_ask - long_bid, 4)

            short_leg_width = None
            if short_bid is not None and short_ask is not None:
                short_leg_width = round(short_ask - short_bid, 4)

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
                    "long_bid": _round_or_none(long_bid),
                    "long_ask": _round_or_none(long_ask),
                    "long_mid": _round_or_none(long_mid),
                    "short_bid": _round_or_none(short_bid),
                    "short_ask": _round_or_none(short_ask),
                    "short_mid": _round_or_none(short_mid),
                    "natural_debit": _round_or_none(natural_debit),
                    "bid_debit": _round_or_none(bid_debit),
                    "spread_market_width": _round_or_none(spread_market_width),
                    "entry_slippage_vs_mid": _round_or_none(entry_slippage_vs_mid),
                    "long_leg_width": _round_or_none(long_leg_width),
                    "short_leg_width": _round_or_none(short_leg_width),
                    "long_leg_width_pct_of_mid": _calc_pct_of_mid(long_bid, long_ask, long_mid),
                    "short_leg_width_pct_of_mid": _calc_pct_of_mid(short_bid, short_ask, short_mid),
                    "long_iv": long_leg.get("iv_mid"),
                    "short_iv": short_leg.get("iv_mid"),
                    "long_bid_iv": long_leg.get("iv_bid"),
                    "long_ask_iv": long_leg.get("iv_ask"),
                    "short_bid_iv": short_leg.get("iv_bid"),
                    "short_ask_iv": short_leg.get("iv_ask"),
                    "iv_source": long_leg.get("iv_source") or short_leg.get("iv_source"),
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
        "natural_debit": candidate.get("natural_debit"),
        "bid_debit": candidate.get("bid_debit"),
        "spread_market_width": candidate.get("spread_market_width"),
        "entry_slippage_vs_mid": candidate.get("entry_slippage_vs_mid"),
        "long_leg_width": candidate.get("long_leg_width"),
        "short_leg_width": candidate.get("short_leg_width"),
        "long_leg_width_pct_of_mid": candidate.get("long_leg_width_pct_of_mid"),
        "short_leg_width_pct_of_mid": candidate.get("short_leg_width_pct_of_mid"),
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
    best_ticker = best_summary["symbol"] if best_summary else None
    verdict = best_summary["verdict"] if best_summary else "NO_TRADE"

    return {
        "ok": True,
        "verdict": verdict,
        "best_ticker": best_ticker,
        "candidate_sort_reason": _candidate_sort_reason_from_best(best_summary),
        "selection_mode": best_summary["selection_mode"] if best_summary else "none",
        "reason": best_summary["reason"] if best_summary else "No summary available.",
        "primary_candidate": _compact_candidate(best_summary["primary_candidate"]) if best_summary else None,
        "backup_candidate": _compact_candidate(best_summary["backup_candidate"]) if best_summary else None,
        "ticker_summaries": [_compact_ticker_summary(s) for s in ticker_summaries],
    }





def _candidate_sort_reason_from_best(best_summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    primary = (best_summary or {}).get("primary_candidate") or {}
    return {
        "best_ticker": (best_summary or {}).get("symbol"),
        "selection_mode": (best_summary or {}).get("selection_mode"),
        "reason": (best_summary or {}).get("reason"),
        "distance_from_target_risk_mid": primary.get("distance_from_target_risk_mid"),
        "feasibility_pass": primary.get("feasibility_pass"),
        "fits_risk_budget": primary.get("fits_risk_budget"),
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


def _calc_atr(candles: List[Dict[str, Any]], length: int = 14) -> Optional[float]:
    if not candles or len(candles) < 2:
        return None

    true_ranges: List[float] = []
    prev_close: Optional[float] = None

    for candle in candles:
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        close = _to_float(candle.get("close"))
        if high is None or low is None or close is None:
            continue

        if prev_close is None:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
        true_ranges.append(tr)
        prev_close = close

    if len(true_ranges) < length:
        return None

    atr = sum(true_ranges[-length:]) / length
    return round(atr, 4)


def _build_vwap_context_from_candles(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candles:
        return {
            "status": "unconfirmed",
            "session_vwap": None,
            "anchored_vwap": None,
            "source": None,
            "volume_field_used": None,
            "candle_count_with_volume": 0,
            "why": "No candle data is available for VWAP.",
        }

    volume_fields = ["volume", "dayVolume", "totalVolume", "vol"]
    chosen_field: Optional[str] = None
    weighted_sum = 0.0
    volume_sum = 0.0
    used = 0

    for candle in candles:
        volume_value = None
        field_used = None
        for field in volume_fields:
            maybe = _to_float(candle.get(field))
            if maybe is not None and maybe > 0:
                volume_value = maybe
                field_used = field
                break

        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        close = _to_float(candle.get("close"))
        if volume_value is None or high is None or low is None or close is None:
            continue

        typical_price = (high + low + close) / 3.0
        weighted_sum += typical_price * volume_value
        volume_sum += volume_value
        used += 1
        if chosen_field is None:
            chosen_field = field_used

    if volume_sum <= 0 or used == 0:
        return {
            "status": "unconfirmed",
            "session_vwap": None,
            "anchored_vwap": None,
            "source": None,
            "volume_field_used": None,
            "candle_count_with_volume": 0,
            "why": "Current chart feed snapshot does not include usable volume, so VWAP is not computed yet.",
        }

    return {
        "status": "confirmed",
        "session_vwap": round(weighted_sum / volume_sum, 4),
        "anchored_vwap": None,
        "source": "candle_volume_snapshot",
        "volume_field_used": chosen_field,
        "candle_count_with_volume": used,
        "why": None,
    }


def _build_volume_diagnostics_context(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    volume_fields = ["volume", "dayVolume", "totalVolume", "vol"]

    if not candles:
        return {
            "ok": False,
            "status": "no_candles",
            "fields_checked": volume_fields,
            "candle_count": 0,
            "volume_field_counts": {field: 0 for field in volume_fields},
            "usable_volume_fields": [],
            "sample_preview": [],
            "why": "No candle data is available for volume diagnostics.",
        }

    counts = {field: 0 for field in volume_fields}
    preview = []

    for candle in candles[:3]:
        preview.append(
            {
                field: candle.get(field)
                for field in volume_fields
            }
        )

    for candle in candles:
        for field in volume_fields:
            value = _to_float(candle.get(field))
            if value is not None and value > 0:
                counts[field] += 1

    usable_fields = [field for field, count in counts.items() if count > 0]

    return {
        "ok": True,
        "status": "usable_volume_found" if usable_fields else "no_usable_volume",
        "fields_checked": volume_fields,
        "candle_count": len(candles),
        "volume_field_counts": counts,
        "usable_volume_fields": usable_fields,
        "sample_preview": preview,
        "why": None if usable_fields else "No checked candle volume field contained usable positive values.",
    }


async def _build_vix_context(token: str) -> Dict[str, Any]:
    attempts = [
        ["VIX"],
        ["^VIX"],
        ["VIX.X"],
        ["$VIX.X"],
        ["VIX", "^VIX"],
        ["VIX.X", "$VIX.X"],
    ]

    errors: List[str] = []

    for symbols in attempts:
        try:
            payload = await _fetch_index_quotes(symbols, token)
            items = payload.get("data", {}).get("items", [])
            if not items:
                errors.append(f"{','.join(symbols)}: no items")
                continue

            for idx, item in enumerate(items):
                value = _extract_market_data_value(item)
                if value is None:
                    continue

                symbol = (
                    item.get("symbol")
                    or item.get("streamer-symbol")
                    or item.get("index-symbol")
                    or symbols[min(idx, len(symbols) - 1)]
                )

                if value < 16:
                    regime = "calm"
                elif value < 22:
                    regime = "normal"
                elif value < 30:
                    regime = "elevated"
                else:
                    regime = "high_stress"

                return {
                    "status": "confirmed",
                    "value": round(value, 4),
                    "regime": regime,
                    "symbol": symbol,
                    "why": None,
                }

            errors.append(f"{','.join(symbols)}: items returned but no usable mark/last/mid/close value")
        except Exception as exc:
            errors.append(f"{','.join(symbols)}: {str(exc)}")

    why = "VIX fetch did not return a usable value."
    if errors:
        why = why + " Attempts: " + " | ".join(errors[:6])

    return {
        "status": "unconfirmed",
        "value": None,
        "regime": None,
        "symbol": None,
        "why": why,
    }



def _extract_market_data_value(item: Dict[str, Any]) -> Optional[float]:
    for field in ("mark", "last", "mid", "close", "price", "value"):
        value = _to_float(item.get(field))
        if value is not None:
            return value
    return None


def _breadth_state_from_value(value: float) -> str:
    if value >= 1000:
        return "strongly_positive"
    if value > 0:
        return "positive"
    if value <= -1000:
        return "strongly_negative"
    if value < 0:
        return "negative"
    return "neutral"


def _tick_state_from_value(value: float) -> str:
    if value >= 1000:
        return "strongly_positive"
    if value >= 500:
        return "positive"
    if value <= -1000:
        return "strongly_negative"
    if value <= -500:
        return "negative"
    return "neutral"


async def _build_tick_context(token: str) -> Dict[str, Any]:
    attempts = [
        ["$TICK"],
        ["TICK"],
        ["$TICKI"],
        ["TICKI"],
        ["$TICKQ"],
        ["TICKQ"],
        ["$TICK-NYSE"],
        ["TICK-NYSE"],
        ["$TICK-NASDAQ"],
        ["TICK-NASDAQ"],
        ["$TICK-NQ"],
        ["TICK-NQ"],
        ["$TICK", "$TICKI", "$TICKQ"],
        ["TICK", "TICKI", "TICKQ"],
    ]

    errors: List[str] = []

    for symbols in attempts:
        try:
            payload = await _fetch_index_quotes(symbols, token)
            items = payload.get("data", {}).get("items", [])
            if not items:
                errors.append(f"{','.join(symbols)}: no items")
                continue

            selected_item = None
            selected_symbol = None

            for requested in symbols:
                preferred = requested.replace("$", "").upper()
                for item in items:
                    raw_symbol = (
                        item.get("symbol")
                        or item.get("streamer-symbol")
                        or item.get("instrument-symbol")
                        or item.get("eventSymbol")
                        or ""
                    )
                    normalized = str(raw_symbol).replace("$", "").upper()
                    if normalized == preferred:
                        value = _extract_market_data_value(item)
                        if value is not None:
                            selected_item = item
                            selected_symbol = requested
                            break
                if selected_item is not None:
                    break

            if selected_item is None:
                for item in items:
                    value = _extract_market_data_value(item)
                    if value is not None:
                        selected_item = item
                        raw_symbol = (
                            item.get("symbol")
                            or item.get("streamer-symbol")
                            or item.get("instrument-symbol")
                            or item.get("eventSymbol")
                            or symbols[0]
                        )
                        selected_symbol = str(raw_symbol)
                        break

            if selected_item is None:
                errors.append(f"{','.join(symbols)}: items returned but no usable mark/last/mid/close value")
                continue

            value = _extract_market_data_value(selected_item)
            if value is None:
                errors.append(f"{','.join(symbols)}: no usable mark/last/mid/close value")
                continue

            value = round(value, 4)
            return {
                "status": "confirmed",
                "value": value,
                "tick_state": _tick_state_from_value(value),
                "source_symbol": selected_symbol,
                "why": None,
            }
        except Exception as exc:
            errors.append(f"{','.join(symbols)}: {str(exc)}")

    why = "TICK fetch did not return a usable value."
    if errors:
        why = why + " Attempts: " + " | ".join(errors[:8])

    return {
        "status": "unconfirmed",
        "value": None,
        "tick_state": None,
        "source_symbol": None,
        "why": why,
    }


async def _build_advance_decline_context(token: str) -> Dict[str, Any]:
    attempts = [
        {"mode": "difference", "symbols": ["$ADUSD"]},
        {"mode": "difference", "symbols": ["ADUSD"]},
        {"mode": "difference", "symbols": ["$ADUSDC"]},
        {"mode": "difference", "symbols": ["ADUSDC"]},
        {"mode": "components", "symbols": ["$ADVUS", "$DECLUS"]},
        {"mode": "components", "symbols": ["ADVUS", "DECLUS"]},
        {"mode": "components", "symbols": ["$ADVUSC", "$DECLUSC"]},
        {"mode": "components", "symbols": ["ADVUSC", "DECLUSC"]},
    ]

    errors: List[str] = []

    for attempt in attempts:
        try:
            payload = await _fetch_index_quotes(attempt["symbols"], token)
            items = payload.get("data", {}).get("items", [])
            if not items:
                errors.append(f"{','.join(attempt['symbols'])}: no items")
                continue

            if attempt["mode"] == "difference":
                preferred_symbol = attempt["symbols"][0].replace("$", "").upper()
                selected_item = None
                for item in items:
                    raw_symbol = (
                        item.get("symbol")
                        or item.get("streamer-symbol")
                        or item.get("instrument-symbol")
                        or item.get("eventSymbol")
                        or ""
                    )
                    normalized = str(raw_symbol).replace("$", "").upper()
                    if normalized == preferred_symbol:
                        selected_item = item
                        break

                if selected_item is None:
                    selected_item = items[0]

                value = _extract_market_data_value(selected_item)
                if value is None:
                    errors.append(f"{attempt['symbols'][0]}: no usable mark/last/mid/close value")
                    continue
                value = round(value, 4)
                return {
                    "status": "confirmed",
                    "value": value,
                    "breadth_state": _breadth_state_from_value(value),
                    "source_symbol": attempt["symbols"][0],
                    "why": None,
                }

            symbol_map: Dict[str, Dict[str, Any]] = {}
            for item in items:
                raw_symbol = (
                    item.get("symbol")
                    or item.get("streamer-symbol")
                    or item.get("instrument-symbol")
                    or item.get("eventSymbol")
                    or ""
                )
                normalized = str(raw_symbol).replace("$", "").upper()
                symbol_map[normalized] = item

            adv_key = attempt["symbols"][0].replace("$", "").upper()
            dec_key = attempt["symbols"][1].replace("$", "").upper()

            adv_item = symbol_map.get(adv_key)
            dec_item = symbol_map.get(dec_key)
            if not adv_item or not dec_item:
                errors.append(f"{','.join(attempt['symbols'])}: {adv_key}/{dec_key} items not both returned")
                continue

            adv_value = _extract_market_data_value(adv_item)
            dec_value = _extract_market_data_value(dec_item)
            if adv_value is None or dec_value is None:
                errors.append(f"{','.join(attempt['symbols'])}: {adv_key}/{dec_key} values unusable")
                continue

            value = round(adv_value - dec_value, 4)
            return {
                "status": "confirmed",
                "value": value,
                "breadth_state": _breadth_state_from_value(value),
                "source_symbol": f"{adv_key}-{dec_key}",
                "advancing_issues": round(adv_value, 4),
                "declining_issues": round(dec_value, 4),
                "why": None,
            }
        except Exception as exc:
            errors.append(f"{','.join(attempt['symbols'])}: {str(exc)}")

    why = "Advance/Decline breadth fetch did not return a usable value."
    if errors:
        why = why + " Attempts: " + " | ".join(errors[:8])

    return {
        "status": "unconfirmed",
        "value": None,
        "breadth_state": None,
        "source_symbol": None,
        "why": why,
    }


def _build_indicator_context(
    best_ticker: Optional[str],
    chart_check: Optional[Dict[str, Any]],
    structure_context: Dict[str, Any],
    vix_context: Optional[Dict[str, Any]] = None,
    advance_decline_context: Optional[Dict[str, Any]] = None,
    tick_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    candles = chart_check.get("_all_candles", []) if chart_check else []
    latest_close = _to_float(chart_check.get("latest_close")) if chart_check else None

    closes = [
        _to_float(c.get("close"))
        for c in candles
        if _to_float(c.get("close")) is not None
    ]

    ema20 = _calc_ema(closes, 20) if len(closes) >= 20 else None
    atr14 = _calc_atr(candles, 14)

    keltner_upper = None
    keltner_lower = None
    keltner_position = None
    if ema20 is not None and atr14 is not None:
        keltner_upper = round(ema20 + (2 * atr14), 4)
        keltner_lower = round(ema20 - (2 * atr14), 4)
        if latest_close is not None:
            if latest_close > keltner_upper:
                keltner_position = "above_upper"
            elif latest_close < keltner_lower:
                keltner_position = "below_lower"
            else:
                keltner_position = "inside_channel"

    atr_pct = None
    if atr14 is not None and latest_close not in (None, 0):
        atr_pct = round((atr14 / latest_close) * 100, 3)

    vwap_context = _build_vwap_context_from_candles(candles)

    extension_note = None
    if structure_context.get("extension_state") == "extended":
        extension_note = "Core structure already flags extension; indicators are supporting context only."

    return {
        "ok": True,
        "design_goal": "background_only_supporting_indicators",
        "note": "Indicators are supporting context only and do not override core SAFE-FAST structure rules.",
        "ticker": best_ticker,
        "atr": {
            "status": "confirmed" if atr14 is not None else "unconfirmed",
            "length": 14,
            "atr_14_1h": atr14,
            "atr_percent_of_price": atr_pct,
        },
        "keltner": {
            "status": "confirmed" if ema20 is not None and atr14 is not None else "unconfirmed",
            "basis_ema_length": 20,
            "atr_length": 14,
            "multiplier": 2.0,
            "middle_ema_20_1h": ema20,
            "upper_band_1h": keltner_upper,
            "lower_band_1h": keltner_lower,
            "channel_position": keltner_position,
        },
        "vwap": vwap_context,
        "tick": tick_context or {
            "status": "unconfirmed",
            "value": None,
            "tick_state": None,
            "source_symbol": None,
            "why": "TICK feed is not wired into the backend yet.",
        },
        "vix": vix_context or {
            "status": "unconfirmed",
            "value": None,
            "regime": None,
            "why": "VIX feed is not wired into the backend yet.",
        },
        "advance_decline": advance_decline_context or {
            "status": "unconfirmed",
            "value": None,
            "breadth_state": None,
            "source_symbol": None,
            "why": "Advance/Decline breadth feed is not wired into the backend yet.",
        },
        "extension_support_note": extension_note,
    }


def _build_indicator_filter_context(
    indicator_context: Dict[str, Any],
    structure_context: Dict[str, Any],
) -> Dict[str, Any]:
    atr_pct = _to_float(indicator_context.get("atr", {}).get("atr_percent_of_price"))
    keltner = indicator_context.get("keltner", {})
    vwap = indicator_context.get("vwap", {})
    channel_position = keltner.get("channel_position")
    extension_state = structure_context.get("extension_state")
    pct_from_ema = _to_float(structure_context.get("pct_from_ema"))
    vwap_status = vwap.get("status")

    atr_regime = "unconfirmed"
    if atr_pct is not None:
        if atr_pct >= 0.8:
            atr_regime = "expanded"
        elif atr_pct >= 0.5:
            atr_regime = "active"
        else:
            atr_regime = "calm"

    keltner_filter_state = "unconfirmed"
    if channel_position in {"above_upper", "below_lower"}:
        keltner_filter_state = "outside_channel"
    elif channel_position == "inside_channel":
        keltner_filter_state = "inside_channel"

    caution_flags: List[str] = []
    if extension_state == "extended":
        caution_flags.append("structure_extended")
    if atr_regime in {"active", "expanded"}:
        caution_flags.append(f"atr_{atr_regime}")
    if keltner_filter_state == "outside_channel":
        caution_flags.append("outside_keltner_channel")
    if pct_from_ema is not None and pct_from_ema >= 1.0:
        caution_flags.append("pct_from_ema_elevated")

    caution = len(caution_flags) > 0
    summary = "caution" if caution else "neutral"

    return {
        "ok": True,
        "design_goal": "background_only_filtering",
        "note": "Confirmed indicators may warn or filter in the background, but do not change the simple output format.",
        "confirmed_inputs": [
            name
            for name, status in [
                ("atr", indicator_context.get("atr", {}).get("status")),
                ("keltner", indicator_context.get("keltner", {}).get("status")),
                ("vwap", vwap_status),
            ]
            if status == "confirmed"
        ],
        "atr_regime": atr_regime,
        "keltner_filter_state": keltner_filter_state,
        "vwap_filter_state": "confirmed_available" if vwap_status == "confirmed" else "unconfirmed",
        "extension_state": extension_state,
        "pct_from_ema": pct_from_ema,
        "caution": caution,
        "caution_flags": caution_flags,
        "summary": summary,
        "blocks_trade_directly": False,
    }


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
            "room_basis": "unconfirmed",
            "effective_wall": None,
        }

    short_strike = _to_float(primary_candidate.get("short_strike"))
    if short_strike is None:
        return {
            "wall_thesis": "unconfirmed",
            "wall_pass": None,
            "room_basis": "unconfirmed",
            "effective_wall": None,
        }

    next_pocket_room = None
    if next_pocket is not None and invalidation_distance not in (None, 0):
        next_pocket_room = abs(next_pocket - first_wall) / invalidation_distance

    through_wall_candidate = False
    if option_type == "C":
        through_wall_candidate = next_pocket is not None and short_strike > next_pocket
    else:
        through_wall_candidate = next_pocket is not None and short_strike < next_pocket

    if through_wall_candidate and next_pocket is not None:
        return {
            "wall_thesis": "THROUGH_THE_WALL",
            "wall_pass": True,
            "next_pocket_room_ratio": next_pocket_room,
            "room_basis": "next_pocket",
            "effective_wall": next_pocket,
        }

    if option_type == "C":
        if short_strike > first_wall:
            return {
                "wall_thesis": "TO_THE_WALL",
                "wall_pass": True,
                "next_pocket_room_ratio": next_pocket_room,
                "room_basis": "first_wall",
                "effective_wall": first_wall,
            }
    else:
        if short_strike < first_wall:
            return {
                "wall_thesis": "TO_THE_WALL",
                "wall_pass": True,
                "next_pocket_room_ratio": next_pocket_room,
                "room_basis": "first_wall",
                "effective_wall": first_wall,
            }

    return {
        "wall_thesis": "WALL_MISMATCH",
        "wall_pass": False,
        "next_pocket_room_ratio": next_pocket_room,
        "room_basis": "first_wall",
        "effective_wall": first_wall,
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

    trend_supportive = trend_ctx.get("supportive")
    trend_label = "Trend-aligned" if trend_supportive is True else "Countertrend" if trend_supportive is False else "unconfirmed"

    if latest_close is None or ema50_1h is None:
        return {"setup_type": "UNCONFIRMED", "trend_label": trend_label, "allowed_setup": None}

    close_distance_pct = abs(latest_close - ema50_1h) / ema50_1h if ema50_1h else None
    near_ema = bool(close_distance_pct is not None and close_distance_pct <= 0.0035)

    chop = _is_chop(candles)

    recent_closes = [c["close"] for c in candles[-3:]] if len(candles) >= 3 else []
    recent_highs = [c["high"] for c in candles[-6:-1]] if len(candles) >= 6 else []
    recent_lows = [c["low"] for c in candles[-6:-1]] if len(candles) >= 6 else []
    tight_break = False
    break_signal = False
    ema_reclaim = False

    if recent_closes and latest_close:
        tight_break = (max(recent_closes) - min(recent_closes)) / latest_close <= 0.003

    if option_type == "C":
        if recent_highs:
            break_signal = latest_close > max(recent_highs)
        if len(recent_closes) >= 2:
            ema_reclaim = (
                latest_close > ema50_1h and
                min(recent_closes[-2:]) >= ema50_1h * 0.998
            )
    else:
        if recent_lows:
            break_signal = latest_close < min(recent_lows)
        if len(recent_closes) >= 2:
            ema_reclaim = (
                latest_close < ema50_1h and
                max(recent_closes[-2:]) <= ema50_1h * 1.002
            )

    ideal_candidate = bool(
        trend_supportive is True and
        near_ema and
        ema_reclaim and
        not chop and
        (room_ratio or 0) >= 2.5
    )
    clean_fast_break_candidate = bool(
        trend_supportive is True and
        break_signal and
        tight_break and
        not chop
    )
    continuation_candidate = bool(
        trend_supportive is True and
        near_ema and
        not chop
    )

    structural_block = bool(
        room_pass is False or
        wall_pass is False or
        extension_state.get("state") == "extended"
    )

    if structural_block:
        if ideal_candidate:
            return {"setup_type": "Ideal", "trend_label": trend_label, "allowed_setup": False}
        if clean_fast_break_candidate:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": False}
        if continuation_candidate:
            return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": False}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False}

    if trend_supportive is True:
        if ideal_candidate:
            return {"setup_type": "Ideal", "trend_label": trend_label, "allowed_setup": True}
        if clean_fast_break_candidate:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True}
        if continuation_candidate:
            return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": True}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False}

    if trend_supportive is False:
        if break_signal and tight_break and not chop and not structural_block:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False}

    return {"setup_type": "UNCONFIRMED", "trend_label": trend_label, "allowed_setup": None}


def _build_structure_context(
    symbol: str,
    option_type: str,
    chart_check: Optional[Dict[str, Any]],
    primary_candidate: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not primary_candidate:
        return {
            "ok": False,
            "why": "no candidate available",
        }

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
    extension_ctx = _extension_state(symbol, latest_close, ema50_1h, wall_levels.get("first_wall"))
    wall_ctx = _wall_thesis(
        option_type=option_type,
        primary_candidate=primary_candidate,
        first_wall=wall_levels.get("first_wall"),
        next_pocket=wall_levels.get("next_pocket"),
        invalidation_distance=invalidation_distance,
    )

    effective_wall = wall_ctx.get("effective_wall")
    effective_room_distance = None
    if effective_wall is not None:
        effective_room_distance = round(abs(effective_wall - latest_close), 4)

    room_ratio = None
    if effective_room_distance is not None and invalidation_distance not in (None, 0):
        room_ratio = effective_room_distance / invalidation_distance

    room_pass = (room_ratio is not None and room_ratio >= 2.0)
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

    room_required_for_pass = round((invalidation_distance * 2.0), 4) if invalidation_distance not in (None, 0) else None
    room_shortfall = None
    if effective_room_distance is not None and room_required_for_pass is not None:
        room_shortfall = round(effective_room_distance - room_required_for_pass, 4)

    if room_pass is True:
        room_note = "Room passes the SAFE-FAST 2x invalidation rule."
    elif room_pass is False:
        room_note = (
            f"Room fails the SAFE-FAST 2x invalidation rule on {wall_ctx.get('room_basis')}: needs {room_required_for_pass}, "
            f"has {effective_room_distance}."
        )
    else:
        room_note = "Room could not be confirmed."

    return {
        "ok": True,
        "twentyfour_hour_trend": trend_ctx.get("label"),
        "twentyfour_hour_supportive": trend_ctx.get("supportive"),
        "twentyfour_hour_source": trend_ctx.get("source"),
        "first_wall": wall_levels.get("first_wall"),
        "next_pocket": wall_levels.get("next_pocket"),
        "effective_wall": wall_ctx.get("effective_wall"),
        "room_basis": wall_ctx.get("room_basis"),
        "invalidation_distance": round(invalidation_distance, 4) if invalidation_distance is not None else None,
        "room_to_first_wall": wall_levels.get("room_distance"),
        "effective_room_distance": effective_room_distance,
        "room_required_for_pass": room_required_for_pass,
        "room_shortfall": room_shortfall,
        "room_note": room_note,
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
    time_day_gate: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    screenshot_traps_context: Dict[str, Any],
) -> str:
    if request.open_positions > 0:
        return "NO_TRADE"
    if request.weekly_trade_count >= 4:
        return "NO_TRADE"
    if not market_context["is_open"]:
        return "NO_TRADE"
    if not time_day_gate.get("fresh_entry_allowed"):
        return "NO_TRADE"
    if macro_context.get("ok") and (
        macro_context.get("has_major_event_today") or macro_context.get("has_major_event_tomorrow")
    ):
        return "NO_TRADE"
    if liquidity_context.get("liquidity_pass") is False:
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
    if _trap_blocks_trade(screenshot_traps_context):
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
    confirmed = bool(one_hour_confirmed and structure_confirmed and not chart_check_error)

    if structure_context.get("why") == "no candidate available":
        message = "No candidate available in this run, so chart confirmation was not attempted."
    elif chart_check_error:
        message = "Chart check failed in this run."
    elif confirmed:
        message = "Chart confirmation fields are present in this run."
    else:
        message = "Candidate engine result only - chart confirmation still required."

    return {
        "confirmed": confirmed,
        "message": message,
        "fields": {
            "one_hour_50_ema": _status_field(chart_check.get("ema50_1h") if chart_check else None, one_hour_confirmed),
            "one_hour_price_vs_50_ema": _status_field(chart_check.get("price_vs_ema50_1h") if chart_check else None, one_hour_confirmed),
            "latest_close": _status_field(chart_check.get("latest_close") if chart_check else None, one_hour_confirmed),
            "twentyfour_hour_trend": _status_field(structure_context.get("twentyfour_hour_trend"), structure_confirmed),
            "room_to_first_wall": _status_field(structure_context.get("room_to_first_wall"), structure_confirmed),
            "room_required_for_pass": _status_field(structure_context.get("room_required_for_pass"), structure_confirmed),
            "room_shortfall": _status_field(structure_context.get("room_shortfall"), structure_confirmed),
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



def _room_failure_user_text(structure_context: Dict[str, Any]) -> str:
    room_basis = structure_context.get("room_basis")
    if room_basis == "next_pocket":
        return "Room to next pocket is too tight for SAFE-FAST."
    if room_basis == "first_wall":
        return "Room to first wall is too tight for SAFE-FAST."
    return "Room is too tight for SAFE-FAST."


def _room_failure_failed_reason_text(structure_context: Dict[str, Any]) -> str:
    room_basis = structure_context.get("room_basis")
    if room_basis == "next_pocket":
        return "room to the next pocket fails"
    if room_basis == "first_wall":
        return "room to the first wall fails"
    return "room fails the SAFE-FAST rule"


def _priority_blocker_user_text(
    checklist: Optional[Dict[str, Any]],
    structure_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    screenshot_traps_context: Dict[str, Any],
    chart_check_error: Optional[str] = None,
    engine_reason: Optional[str] = None,
) -> Optional[str]:
    if liquidity_context.get("why") == "No candidate available.":
        return engine_reason or "No feasible candidates found for the current filters."

    blocker_order = []
    if checklist:
        blocker_order = checklist.get("decision_blockers_priority") or checklist.get("all_failed_items") or checklist.get("failed_items") or []

    for blocker in blocker_order:
        if blocker in {"hidden_left_level", "noisy_chop", "volume_climax"}:
            return _trap_failure_user_text(screenshot_traps_context)
        if blocker == "allowed_setup_type":
            setup_type = structure_context.get("setup_type")
            if setup_type is None:
                return engine_reason or "No feasible candidates found for the current filters."
            return f"Setup type is {setup_type}, which is not tradable now."
        if blocker == "twentyfour_hour_supportive":
            return "24H context is not supportive for this setup."
        if blocker == "one_hour_clean_around_ema":
            return "1H structure around the 50 EMA is not clean enough."
        if blocker == "clear_room":
            return _room_failure_user_text(structure_context)
        if blocker == "early_enough":
            if structure_context.get("extension_state") == "extended":
                return "Move is extended vs the 1H 50 EMA or too late relative to the wall."
            return "Entry timing is too late for SAFE-FAST."
        if blocker == "clear_trigger":
            return trigger_state.get("why") or "No valid live trigger is present."
        if blocker == "liquidity_ok":
            return liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry."
        if blocker == "invalidation_clear":
            return "Invalidation is not clear enough for SAFE-FAST."
        if blocker == "fits_risk":
            return "Risk does not fit the SAFE-FAST budget."
        if blocker == "open_trade_already":
            return "You already have 1 open position. SAFE-FAST allows max 1 open trade total."

    if chart_check_error:
        return "Chart check failed in this run."
    return None


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
    time_day_gate: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    screenshot_traps_context: Dict[str, Any],
    checklist: Optional[Dict[str, Any]] = None,
    trigger_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ticker = best_ticker or "UNKNOWN"
    ema_text = str(chart_check.get("ema50_1h")) if chart_check and chart_check.get("ok") else "unconfirmed"
    no_candidate_mode = liquidity_context.get("why") == "No candidate available."
    base_invalidation_text = (
        "No valid new entry from the current combined read."
        if no_candidate_mode and not (chart_check and chart_check.get("ok"))
        else base_invalidation_text
    )
    trigger_state = trigger_state or {}
    primary_blocker_text = _priority_blocker_user_text(
        checklist=checklist,
        structure_context=structure_context,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
        screenshot_traps_context=screenshot_traps_context,
        chart_check_error=chart_check_error,
        engine_reason=engine_reason,
    )

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
        blocking_reasons: List[str] = []

        if liquidity_context.get("liquidity_pass") is False:
            blocking_reasons.append(
                liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry."
            )

        if chart_check_error:
            blocking_reasons.append("Chart check failed in this run.")

        if structure_context.get("ok"):
            if structure_context.get("room_pass") is False:
                blocking_reasons.append(_room_failure_user_text(structure_context))
            if structure_context.get("extension_state") == "extended":
                blocking_reasons.append("Move is too extended from the 1H 50 EMA.")
            if structure_context.get("allowed_setup") is False:
                blocking_reasons.append(f"Setup type is {structure_context.get('setup_type')}, which is not tradable now.")
            if structure_context.get("wall_pass") is False:
                blocking_reasons.append("Wall thesis and strike placement do not match.")
        elif chart_check_error:
            blocking_reasons.append("Candidate engine result only - chart confirmation still required.")

        if _trap_blocks_trade(screenshot_traps_context):
            blocking_reasons.append(_trap_failure_user_text(screenshot_traps_context))

        if primary_blocker_text or blocking_reasons:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": primary_blocker_text or blocking_reasons[0],
            }

        return {
            "good_idea_now": "WAIT",
            "ticker": ticker,
            "action": "wait for next regular session",
            "invalidation": base_invalidation_text,
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
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": macro_context.get("note") or "Major event risk is inside the expected hold window.",
        }

    if not time_day_gate.get("fresh_entry_allowed"):
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": f"Time/day filter fails: {time_day_gate.get('reason')}.",
        }

    if liquidity_context.get("liquidity_pass") is False:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry.",
        }

    if not best_ticker:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No valid candidate engine setup is available.",
            "setup_state": "NO TRADE",
            "why": engine_reason,
        }

    if primary_blocker_text:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": primary_blocker_text,
        }

    if structure_context.get("ok"):
        if structure_context.get("room_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": _room_failure_user_text(structure_context),
            }
        if structure_context.get("wall_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": "Wall thesis and strike placement do not match.",
            }
        if structure_context.get("extension_state") == "extended":
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": "Move is extended vs the 1H 50 EMA or too late relative to the first wall.",
            }
        if structure_context.get("allowed_setup") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": f"Setup type is {structure_context.get('setup_type')}, which is not tradable now.",
            }

    if final_verdict == "NO_TRADE":
        why = "Best ticker failed the 1H EMA alignment check."
        if chart_check_error:
            why = "Chart check failed in this run."
        invalidation_text = (
            base_invalidation_text
            if best_ticker and chart_check and chart_check.get("ok")
            else "No valid new entry from the current combined read."
        )
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": invalidation_text,
            "setup_state": "NO TRADE",
            "why": why,
        }

    return {
        "good_idea_now": "WAIT",
        "ticker": ticker,
        "action": "wait for full chart confirmation",
        "invalidation": base_invalidation_text,
        "setup_state": "PENDING",
        "why": "Candidate engine is valid, but trigger/entry-zone timing still needs confirmation.",
    }





def _build_trigger_state(
    option_type: str,
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    structure_context: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    trigger_style = "close_above_recent_high" if option_type == "C" else "close_below_recent_low"
    market_open = bool(market_context.get("is_open"))
    fresh_entry_allowed = bool(time_day_gate.get("fresh_entry_allowed"))

    if structure_context.get("why") == "no candidate available":
        return {
            "ok": False,
            "entry_state": "NO_CANDIDATE",
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": None,
            "price_vs_ema50_1h": None,
            "price_vs_trigger": None,
            "distance_to_trigger": None,
            "crossed_trigger": False,
            "structure_ready": False,
            "market_open": market_open,
            "fresh_entry_allowed": fresh_entry_allowed,
            "entry_blockers": ["no_candidate"],
            "why": "no_candidate",
        }

    if not chart_check or not chart_check.get("ok"):
        return {
            "ok": False,
            "entry_state": "UNCONFIRMED_CHART",
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": None,
            "price_vs_ema50_1h": None,
            "price_vs_trigger": None,
            "distance_to_trigger": None,
            "crossed_trigger": False,
            "structure_ready": False,
            "market_open": market_open,
            "fresh_entry_allowed": fresh_entry_allowed,
            "entry_blockers": ["chart_unavailable"],
            "why": "chart_unavailable",
        }

    recent = chart_check.get("recent_candles") or []
    current_close = chart_check.get("latest_close")
    price_side = chart_check.get("price_vs_ema50_1h")

    if len(recent) < 2 or current_close is None:
        return {
            "ok": False,
            "entry_state": "UNCONFIRMED_CHART",
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": _round_or_none(current_close, 4),
            "price_vs_ema50_1h": price_side,
            "price_vs_trigger": None,
            "distance_to_trigger": None,
            "crossed_trigger": False,
            "structure_ready": False,
            "market_open": market_open,
            "fresh_entry_allowed": fresh_entry_allowed,
            "entry_blockers": ["insufficient_recent_candles"],
            "why": "insufficient_recent_candles",
        }

    prior = recent[:-1] if len(recent) >= 2 else recent
    window = prior[-3:] if len(prior) >= 3 else prior

    trigger_level: Optional[float]
    crossed = False
    if option_type == "C":
        trigger_level = max((c.get("high") for c in window if c.get("high") is not None), default=None)
        crossed = bool(trigger_level is not None and current_close > trigger_level)
        side_ok = price_side == "above"
    else:
        trigger_level = min((c.get("low") for c in window if c.get("low") is not None), default=None)
        crossed = bool(trigger_level is not None and current_close < trigger_level)
        side_ok = price_side == "below"

    structure_ok = bool(
        structure_context.get("allowed_setup") is True
        and structure_context.get("room_pass") is True
        and structure_context.get("wall_pass") is True
        and structure_context.get("extension_state") != "extended"
        and structure_context.get("chop_risk") is False
    )

    entry_blockers: List[str] = []
    if not market_open:
        entry_blockers.append("market_closed")
    if market_open and not fresh_entry_allowed:
        entry_blockers.append(time_day_gate.get("reason", "time_day_gate_blocked"))
    if not structure_ok:
        entry_blockers.append("structure_not_ready")
    if not side_ok:
        entry_blockers.append("wrong_side_of_ema")
    if not crossed:
        entry_blockers.append("close_trigger_not_hit")

    signal_present = bool(crossed and side_ok)
    trigger_present = bool(signal_present and structure_ok and market_open and fresh_entry_allowed)

    if trigger_present:
        entry_state = "ACTIVE_NOW"
        why = "trigger_present"
    elif not market_open:
        entry_state = "BLOCKED_MARKET_CLOSED"
        why = "market_closed"
    elif not fresh_entry_allowed:
        entry_state = "BLOCKED_TIME_WINDOW"
        why = time_day_gate.get("reason", "time_day_gate_blocked")
    elif signal_present and not structure_ok:
        entry_state = "SIGNAL_PRESENT_BUT_BLOCKED"
        why = "structure_not_ready"
    elif not structure_ok:
        entry_state = "NO_TRADE_STRUCTURE"
        why = "structure_not_ready"
    else:
        entry_state = "PENDING_TRIGGER"
        why = "close_trigger_not_hit" if not crossed else "wrong_side_of_ema"

    price_vs_trigger = None
    distance_to_trigger = None
    if trigger_level is not None:
        price_vs_trigger = round(current_close - trigger_level, 4)
        distance_to_trigger = round(abs(current_close - trigger_level), 4)

    return {
        "ok": True,
        "entry_state": entry_state,
        "signal_present": signal_present,
        "trigger_present": trigger_present,
        "trigger_style": trigger_style,
        "trigger_level": _round_or_none(trigger_level, 4),
        "current_close": _round_or_none(current_close, 4),
        "price_vs_ema50_1h": price_side,
        "price_vs_trigger": _round_or_none(price_vs_trigger, 4),
        "distance_to_trigger": _round_or_none(distance_to_trigger, 4),
        "crossed_trigger": crossed,
        "structure_ready": structure_ok,
        "market_open": market_open,
        "fresh_entry_allowed": fresh_entry_allowed,
        "entry_blockers": entry_blockers,
        "why": why,
    }

def _build_targets_block(primary_candidate: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not primary_candidate:
        return {
            "ok": False,
            "debit": None,
            "max_loss_dollars_1lot": None,
            "target_40_pct_value": None,
            "target_50_pct_value": None,
            "target_60_pct_value": None,
            "target_70_pct_value": None,
        }

    debit = _to_float(primary_candidate.get("est_debit"))
    max_loss = _to_float(primary_candidate.get("max_loss_dollars_1lot"))
    if debit is None:
        return {
            "ok": False,
            "debit": None,
            "max_loss_dollars_1lot": max_loss,
            "target_40_pct_value": None,
            "target_50_pct_value": None,
            "target_60_pct_value": None,
            "target_70_pct_value": None,
        }

    return {
        "ok": True,
        "debit": debit,
        "max_loss_dollars_1lot": max_loss,
        "target_40_pct_value": round(debit * 1.40, 4),
        "target_50_pct_value": round(debit * 1.50, 4),
        "target_60_pct_value": round(debit * 1.60, 4),
        "target_70_pct_value": round(debit * 1.70, 4),
    }


def _build_checklist_block(
    request: OnDemandRequest,
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    structure_context: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
    primary_candidate: Optional[Dict[str, Any]],
    liquidity_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    screenshot_traps_context: Dict[str, Any],
) -> Dict[str, Any]:
    ema_value = chart_check.get("ema50_1h") if chart_check else None
    price_side = chart_check.get("price_vs_ema50_1h") if chart_check else None

    items = [
        {"item": "allowed_setup_type", "yes": bool(structure_context.get("allowed_setup") is True)},
        {"item": "twentyfour_hour_supportive", "yes": bool(structure_context.get("twentyfour_hour_supportive") is True)},
        {"item": "one_hour_clean_around_ema", "yes": bool(price_side in {"above", "below"} and structure_context.get("chop_risk") is False)},
        {"item": "clear_room", "yes": bool(structure_context.get("room_pass") is True)},
        {"item": "early_enough", "yes": bool(time_day_gate.get("fresh_entry_allowed"))},
        {"item": "clear_trigger", "yes": bool(trigger_state.get("signal_present") is True)},
        {"item": "liquidity_ok", "yes": bool(liquidity_context.get("liquidity_pass") is True)},
        {"item": "invalidation_clear", "yes": bool(ema_value is not None)},
        {"item": "fits_risk", "yes": bool(primary_candidate and primary_candidate.get("fits_risk_budget") is True)},
        {"item": "open_trade_already", "yes": bool(request.open_positions > 0)},
    ]

    failed_items = [row["item"] for row in items if not row["yes"] and row["item"] != "open_trade_already"]

    hidden_left_level_clear = screenshot_traps_context.get("hidden_left_level_pass") is not False
    noisy_chop = screenshot_traps_context.get("noisy_chop") or {}
    noisy_chop_clear = not (
        noisy_chop.get("status") == "possible" and noisy_chop.get("backend_chop_risk") is True
    )
    volume_climax = screenshot_traps_context.get("volume_climax") or {}
    volume_climax_clear = volume_climax.get("status") != "possible"

    pre_check_items = [
        {"item": "hidden_left_level", "yes": hidden_left_level_clear},
        {"item": "noisy_chop", "yes": noisy_chop_clear},
        {"item": "volume_climax", "yes": volume_climax_clear},
    ]

    pre_check_failed_items = [row["item"] for row in pre_check_items if not row["yes"]]
    if screenshot_traps_context.get("trap_summary") == "blocked" and not pre_check_failed_items:
        pre_check_items.append({"item": "screenshot_traps", "yes": False})
        pre_check_failed_items.append("screenshot_traps")

    all_failed_items = pre_check_failed_items + failed_items

    priority_order = [
        "hidden_left_level",
        "noisy_chop",
        "volume_climax",
        "allowed_setup_type",
        "twentyfour_hour_supportive",
        "one_hour_clean_around_ema",
        "clear_room",
        "early_enough",
        "clear_trigger",
        "liquidity_ok",
        "invalidation_clear",
        "fits_risk",
        "open_trade_already",
    ]
    priority_rank = {name: idx for idx, name in enumerate(priority_order)}
    decision_blockers_priority = sorted(
        all_failed_items,
        key=lambda item: (priority_rank.get(item, 999), item),
    )

    return {
        "ok": True,
        "items": items,
        "failed_items": failed_items,
        "pre_check_items": pre_check_items,
        "pre_check_ok": len(pre_check_failed_items) == 0,
        "pre_check_failed_items": pre_check_failed_items,
        "all_failed_items": all_failed_items,
        "decision_blockers_priority": decision_blockers_priority,
    }



def _failed_reason_messages(
    checklist: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    market_context: Dict[str, Any],
    structure_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    screenshot_traps_context: Dict[str, Any],
    engine_reason: Optional[str] = None,
) -> List[str]:
    reasons: List[str] = []

    if liquidity_context.get("why") == "No candidate available.":
        no_candidate_reason = engine_reason or "No feasible candidates found for the current filters."
        reasons.append(no_candidate_reason)

        gate_reason = time_day_gate.get("reason")
        if not market_context.get("is_open"):
            reasons.append("market is closed")
        elif time_day_gate.get("fresh_entry_allowed") is False:
            if gate_reason == "past_monday_thursday_cutoff":
                reasons.append("fresh entry is outside the SAFE-FAST time/day window")
            elif gate_reason not in {None, "market_closed"}:
                reasons.append(f"fresh entry blocked: {gate_reason}")

        out: List[str] = []
        seen = set()
        for reason in reasons:
            normalized = str(reason).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                out.append(normalized)
        return out

    blocker_order = (
        checklist.get("decision_blockers_priority")
        or checklist.get("all_failed_items")
        or checklist.get("failed_items")
        or []
    )

    trap_reason_map = {
        "hidden_left_level": "hidden left-side level sits inside the room",
        "noisy_chop": "noisy chop proxy is possible",
        "volume_climax": "exhaustion proxy is possible",
    }

    standard_reason_map = {
        "allowed_setup_type": "setup type is not allowed",
        "twentyfour_hour_supportive": "24H context is not supportive",
        "one_hour_clean_around_ema": "1H structure around the 50 EMA is not clean",
        "clear_room": _room_failure_failed_reason_text(structure_context),
        "clear_trigger": "no valid live trigger is present",
        "invalidation_clear": "invalidation is not clear",
        "fits_risk": "risk does not fit the SAFE-FAST budget",
        "open_trade_already": "an open trade already exists",
    }

    gate_reason = time_day_gate.get("reason")

    for blocker in blocker_order:
        if blocker in trap_reason_map:
            reasons.append(trap_reason_map[blocker])
            continue

        if blocker == "early_enough":
            if not market_context.get("is_open"):
                reasons.append("market is closed")
            elif gate_reason == "past_monday_thursday_cutoff":
                reasons.append("fresh entry is outside the SAFE-FAST time/day window")
            elif gate_reason not in {None, "market_closed"}:
                reasons.append(f"fresh entry blocked: {gate_reason}")
            continue

        if blocker == "liquidity_ok":
            reasons.append(
                liquidity_context.get("why")
                or "Bid/ask widths or entry slippage are too wide for a clean SAFE-FAST debit spread entry."
            )
            continue

        msg = standard_reason_map.get(blocker)
        if msg:
            reasons.append(msg)

    if structure_context.get("extension_state") == "extended":
        reasons.append("move is extended versus the 1H 50 EMA")

    if not market_context.get("is_open") and "market is closed" not in reasons:
        reasons.append("market is closed")
    elif time_day_gate.get("fresh_entry_allowed") is False:
        if gate_reason == "past_monday_thursday_cutoff" and "fresh entry is outside the SAFE-FAST time/day window" not in reasons:
            reasons.append("fresh entry is outside the SAFE-FAST time/day window")
        elif gate_reason not in {None, "market_closed"}:
            contextual_gate_reason = f"fresh entry blocked: {gate_reason}"
            if contextual_gate_reason not in reasons:
                reasons.append(contextual_gate_reason)

    trap_reasons = _trap_failed_reason_messages(screenshot_traps_context)
    for trap_reason in trap_reasons:
        if trap_reason not in reasons:
            reasons.append(trap_reason)

    if liquidity_context.get("liquidity_pass") is False:
        liquidity_reason = (
            liquidity_context.get("why")
            or "Bid/ask widths or entry slippage are too wide for a clean SAFE-FAST debit spread entry."
        )
        if liquidity_reason not in reasons:
            reasons.append(liquidity_reason)

    # de-duplicate while preserving order
    out: List[str] = []
    seen = set()
    for reason in reasons:
        normalized = str(reason).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out




def _screened_sort_key(item: Dict[str, Any]) -> Any:
    structure = item.get("structure_context", {})
    primary = item.get("primary_candidate") or {}
    liquidity = item.get("liquidity_context") or {}
    trigger_state = item.get("trigger_state") or {}
    checklist = item.get("checklist") or {}
    final_verdict = item.get("final_verdict", "NO_TRADE")

    verdict_rank = {"PENDING": 0, "NO_TRADE": 1}.get(final_verdict, 2)
    chart_rank = 0 if structure.get("ok") is True else 1
    setup_rank = 0 if structure.get("allowed_setup") is True else 1 if structure.get("allowed_setup") is False else 2
    room_rank = 0 if structure.get("room_pass") is True else 1 if structure.get("room_pass") is False else 2
    wall_rank = 0 if structure.get("wall_pass") is True else 1 if structure.get("wall_pass") is False else 2
    ext_rank = 0 if structure.get("extension_state") == "acceptable" else 1 if structure.get("extension_state") == "extended" else 2
    trend_rank = 0 if structure.get("trend_label") == "Trend-aligned" else 1 if structure.get("trend_label") == "Countertrend" else 2
    liquidity_rank = 0 if liquidity.get("liquidity_pass") is True else 1 if liquidity.get("liquidity_pass") is False else 2
    trigger_rank = 0 if trigger_state.get("trigger_present") is True else 1 if trigger_state.get("ok") is True else 2
    failed_count = len(checklist.get("all_failed_items", checklist.get("failed_items", [])))
    room_ratio = -(structure.get("room_ratio") or -999999)
    risk_mid = primary.get("distance_from_target_risk_mid", 999999)
    ticker_rank = SYMBOL_ORDER.index(item["symbol"]) if item.get("symbol") in SYMBOL_ORDER else 999999

    return (
        verdict_rank,
        chart_rank,
        setup_rank,
        room_rank,
        wall_rank,
        ext_rank,
        liquidity_rank,
        trigger_rank,
        trend_rank,
        failed_count,
        room_ratio,
        risk_mid,
        ticker_rank,
    )


def _screened_other_candidates(screened: List[Dict[str, Any]], best_ticker: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in screened:
        if item.get("symbol") == best_ticker:
            continue
        out.append(
            {
                "symbol": item.get("symbol"),
                "engine_verdict": item.get("engine_verdict"),
                "final_verdict": item.get("final_verdict"),
                "reason": item.get("reason"),
                "primary_candidate": item.get("primary_candidate"),
                "structure_context": item.get("structure_context"),
                "liquidity_context": item.get("liquidity_context"),
                "trigger_state": item.get("trigger_state"),
                "checklist_failed_items": item.get("checklist", {}).get("all_failed_items", item.get("checklist", {}).get("failed_items", [])),
            }
        )
    return out


def _build_screened_best_context(
    selected: Optional[Dict[str, Any]],
    engine_best_ticker: Optional[str],
    screened_candidates: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not selected:
        return {"ok": False, "why": "no screened candidates"}

    engine_pick = next(
        (item for item in screened_candidates if item.get("symbol") == engine_best_ticker),
        None,
    )

    return {
        "ok": True,
        "screened_best_ticker": selected.get("symbol"),
        "engine_best_ticker": engine_best_ticker,
        "changed_from_engine_best": selected.get("symbol") != engine_best_ticker,
        "screened_final_verdict": selected.get("final_verdict"),
        "screened_reason": selected.get("reason"),
        "screened_checklist_failed_items": (selected.get("checklist") or {}).get("all_failed_items", (selected.get("checklist") or {}).get("failed_items", [])),
        "engine_best_final_verdict_after_screen": engine_pick.get("final_verdict") if engine_pick else None,
        "engine_best_reason_after_screen": engine_pick.get("reason") if engine_pick else None,
    }


async def _screen_ticker_candidate(
    summary: Dict[str, Any],
    option_type: str,
    token: str,
    request: OnDemandRequest,
    market_context: Dict[str, Any],
    macro_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    include_chart_checks: bool,
) -> Dict[str, Any]:
    symbol = summary.get("symbol")
    primary_candidate = summary.get("primary_candidate")
    chart_check: Optional[Dict[str, Any]] = None
    chart_check_error: Optional[str] = None

    if include_chart_checks and symbol and primary_candidate:
        try:
            chart_check = await _build_chart_check_payload(symbol, token)
        except Exception as e:
            chart_check_error = str(e)

    structure_context = _build_structure_context(
        symbol=symbol or "UNKNOWN",
        option_type=option_type,
        chart_check=chart_check,
        primary_candidate=primary_candidate,
    ) if symbol else {"ok": False, "why": "no symbol"}

    screenshot_traps_context = _build_screenshot_traps_context(
        structure_context=structure_context,
        chart_check=chart_check,
        option_type=option_type,
    )

    liquidity_context = _build_liquidity_block(primary_candidate)
    trigger_state = _build_trigger_state(
        option_type=option_type,
        market_context=market_context,
        time_day_gate=time_day_gate,
        structure_context=structure_context,
        chart_check=chart_check,
    )

    chart_alignment = _chart_alignment_ok(option_type, chart_check)
    final_verdict = _final_verdict(
        request=request,
        engine_status=summary.get("verdict", "NO_TRADE"),
        chart_alignment=chart_alignment,
        market_context=market_context,
        macro_context=macro_context,
        structure_context=structure_context,
        time_day_gate=time_day_gate,
        liquidity_context=liquidity_context,
        screenshot_traps_context=screenshot_traps_context,
    )

    checklist = _build_checklist_block(
        request=request,
        market_context=market_context,
        time_day_gate=time_day_gate,
        structure_context=structure_context,
        chart_check=chart_check,
        primary_candidate=primary_candidate,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
        screenshot_traps_context=screenshot_traps_context,
    )

    reason = summary.get("reason", "No summary available.")
    failed_items = checklist.get("all_failed_items", checklist.get("failed_items", []))
    priority_reason = _priority_blocker_user_text(
        checklist=checklist,
        structure_context=structure_context,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
        screenshot_traps_context=screenshot_traps_context,
        chart_check_error=chart_check_error,
        engine_reason=summary.get("reason"),
    )
    if priority_reason:
        reason = priority_reason
    elif "liquidity_ok" in failed_items:
        reason = liquidity_context.get("why") or "Options liquidity is too wide for a clean debit spread entry."
    elif "clear_trigger" in failed_items:
        reason = trigger_state.get("why") or "No valid live trigger is present."
    elif structure_context.get("ok"):
        if structure_context.get("room_pass") is False:
            reason = _room_failure_user_text(structure_context)
        elif structure_context.get("wall_pass") is False:
            reason = "Wall thesis and strike placement do not match."
        elif structure_context.get("extension_state") == "extended":
            reason = "Move is too extended from the 1H 50 EMA."
        elif structure_context.get("allowed_setup") is False:
            reason = f"Setup type not allowed: {structure_context.get('setup_type')}"
        elif _trap_blocks_trade(screenshot_traps_context):
            reason = _trap_failure_user_text(screenshot_traps_context)
        elif chart_alignment is False:
            reason = "Price is on the wrong side of the 1H 50 EMA."

    return {
        "symbol": symbol,
        "engine_verdict": summary.get("verdict"),
        "final_verdict": final_verdict,
        "reason": reason,
        "primary_candidate": primary_candidate,
        "backup_candidate": summary.get("backup_candidate"),
        "summary": summary,
        "chart_check": chart_check,
        "chart_check_error": chart_check_error,
        "structure_context": structure_context,
        "liquidity_context": liquidity_context,
        "trigger_state": trigger_state,
        "screenshot_traps_context": screenshot_traps_context,
        "checklist": checklist,
        "decision_blockers_priority": checklist.get("decision_blockers_priority", []),
    }


async def _build_on_demand_payload(request: OnDemandRequest) -> Dict[str, Any]:
    clean_option_type = _clean_option_type(request.option_type)
    market_context = _market_context_now()
    time_day_gate = _time_day_gate(market_context)
    macro_context = await _build_macro_context(request.macro_context_requested)

    if request.open_positions < 0 or request.open_positions > 1:
        raise HTTPException(status_code=400, detail="open_positions must be 0 or 1")
    if request.weekly_trade_count < 0:
        raise HTTPException(status_code=400, detail="weekly_trade_count must be >= 0")

    token = await get_access_token()
    vix_context = await _build_vix_context(token)
    advance_decline_context = await _build_advance_decline_context(token)
    tick_context = await _build_tick_context(token)
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

    screened_candidates = list(
        await asyncio.gather(
            *[
                _screen_ticker_candidate(
                    summary=summary,
                    option_type=clean_option_type,
                    token=token,
                    request=request,
                    market_context=market_context,
                    macro_context=macro_context,
                    time_day_gate=time_day_gate,
                    include_chart_checks=request.include_chart_checks,
                )
                for summary in summary_payload.get("ticker_summaries", [])
            ]
        )
    )

    screened_candidates = sorted(screened_candidates, key=_screened_sort_key)
    selected = screened_candidates[0] if screened_candidates else None

    best_ticker = selected.get("symbol") if selected else summary_payload.get("best_ticker")
    candidate_engine_status = selected.get("engine_verdict", "NO_TRADE") if selected else summary_payload.get("verdict", "NO_TRADE")
    final_verdict = selected.get("final_verdict", "NO_TRADE") if selected else "NO_TRADE"
    engine_status = final_verdict
    primary_candidate = selected.get("primary_candidate") if selected else summary_payload.get("primary_candidate")
    chart_check = selected.get("chart_check") if selected else None
    chart_check_error = selected.get("chart_check_error") if selected else None
    structure_context = selected.get("structure_context") if selected else {"ok": False, "why": "no screened candidates"}
    liquidity_context = selected.get("liquidity_context") if selected else _build_liquidity_block(primary_candidate)
    trigger_state = selected.get("trigger_state") if selected else _build_trigger_state(
        option_type=clean_option_type,
        market_context=market_context,
        time_day_gate=time_day_gate,
        structure_context=structure_context,
        chart_check=chart_check,
    )
    screenshot_traps_context = selected.get("screenshot_traps_context") if selected else _build_screenshot_traps_context(
        structure_context=structure_context,
        chart_check=chart_check,
        option_type=clean_option_type,
    )
    checklist_block = selected.get("checklist") if selected else _build_checklist_block(
        request=request,
        market_context=market_context,
        time_day_gate=time_day_gate,
        structure_context=structure_context,
        chart_check=chart_check,
        primary_candidate=primary_candidate,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
        screenshot_traps_context=screenshot_traps_context,
    )
    selected_reason = selected.get("reason", summary_payload.get("reason", "No summary available.")) if selected else summary_payload.get("reason", "No summary available.")

    if request.include_chart_checks:
        if chart_check:
            chart_check_block: Dict[str, Any] = chart_check
        elif not primary_candidate:
            chart_check_block = {
                "ok": False,
                "symbol": best_ticker,
                "status": "skipped_no_candidate",
                "message": "No candidate available in this run, so chart check was not attempted.",
            }
        else:
            chart_check_block = {
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

    if chart_check_block.get("_all_candles") is not None:
        chart_check_block = {k: v for k, v in chart_check_block.items() if k != "_all_candles"}

    user_facing_block = _build_user_facing_block(
        request=request,
        engine_status=engine_status,
        final_verdict=final_verdict,
        best_ticker=best_ticker,
        chart_check=chart_check,
        chart_check_error=chart_check_error,
        engine_reason=selected_reason,
        market_context=market_context,
        macro_context=macro_context,
        structure_context=structure_context,
        time_day_gate=time_day_gate,
        liquidity_context=liquidity_context,
        screenshot_traps_context=screenshot_traps_context,
        checklist=checklist_block,
        trigger_state=trigger_state,
    )

    return {
        "ok": True,
        "mode": "on_demand",
        "build_tag": "ao_patch_no_candidate_context_action_2026_04_04",
        "source_of_truth": "candidate_engine",
        "read_this_first": "simple_output",
        "engine_status": engine_status,
        "candidate_engine_status": candidate_engine_status,
        "final_verdict": final_verdict,
        "best_ticker": best_ticker,
        "engine_best_ticker": summary_payload.get("best_ticker"),
        "simple_output": _build_simple_output_block(
            user_facing=user_facing_block,
            trigger_state=trigger_state,
        ),
        "user_facing": user_facing_block,
        "screened_best_context": _build_screened_best_context(
            selected=selected,
            engine_best_ticker=summary_payload.get("best_ticker"),
            screened_candidates=screened_candidates,
        ),
        "no_candidate_context": _build_no_candidate_context(
            summary_payload=summary_payload,
            chart_check_block=chart_check_block,
            trigger_state=trigger_state,
            structure_context=structure_context,
            liquidity_context=liquidity_context,
            iv_context=_build_iv_context(primary_candidate),
            failed_reasons=_failed_reason_messages(
                checklist=checklist_block,
                time_day_gate=time_day_gate,
                market_context=market_context,
                structure_context=structure_context,
                liquidity_context=liquidity_context,
                trigger_state=trigger_state,
                screenshot_traps_context=screenshot_traps_context,
            ),
            market_context=market_context,
            time_day_gate=time_day_gate,
            user_facing=user_facing_block,
        ),
        "market_context": market_context,
        "macro_context": macro_context,
        "indicator_context": _build_indicator_context(
            best_ticker=best_ticker,
            chart_check=chart_check,
            structure_context=structure_context,
            vix_context=vix_context,
            advance_decline_context=advance_decline_context,
            tick_context=tick_context,
        ),
        "volume_diagnostics_context": _build_volume_diagnostics_context(
            chart_check.get("_all_candles", []) if chart_check else []
        ),
        "indicator_filter_context": _build_indicator_filter_context(
            indicator_context=_build_indicator_context(
                best_ticker=best_ticker,
                chart_check=chart_check,
                structure_context=structure_context,
                vix_context=vix_context,
                advance_decline_context=advance_decline_context,
                tick_context=tick_context,
            ),
            structure_context=structure_context,
        ),
        "structure_context": structure_context,
        "screenshot_traps_context": screenshot_traps_context,
        "active_trade_flow": _build_active_trade_flow_block(
            request=request,
            option_type=clean_option_type,
            chart_check=chart_check,
            market_context=market_context,
        ),
        "close_trade_flow": _build_close_trade_flow_block(
            request=request,
            market_context=market_context,
        ),
        "journal_context": _build_journal_context_block(
            request=request,
            market_context=market_context,
            best_ticker=best_ticker,
            final_verdict=final_verdict,
            checklist=checklist_block,
            failed_reasons=_failed_reason_messages(
                checklist=checklist_block,
                time_day_gate=time_day_gate,
                market_context=market_context,
                structure_context=structure_context,
                liquidity_context=liquidity_context,
                trigger_state=trigger_state,
                screenshot_traps_context=screenshot_traps_context,
                engine_reason=selected_reason,
            ),
        ),
        "time_day_gate": time_day_gate,
        "iv_context": _build_iv_context(primary_candidate),
        "liquidity_context": liquidity_context,
        "trigger_state": trigger_state,
        "targets": _build_targets_block(primary_candidate),
        "invalidation_level_1h_ema50": chart_check.get("ema50_1h") if chart_check else None,
        "checklist": checklist_block,
        "failed_reasons": _failed_reason_messages(
            checklist=checklist_block,
            time_day_gate=time_day_gate,
            market_context=market_context,
            structure_context=structure_context,
            liquidity_context=liquidity_context,
            trigger_state=trigger_state,
            screenshot_traps_context=screenshot_traps_context,
            engine_reason=selected_reason,
        ),
        "other_ticker_candidates": _screened_other_candidates(screened_candidates, best_ticker),
        "request": request.model_dump(),
        "candidate_engine": summary_payload,
        "chart_check": chart_check_block,
        "chart_confirmation": _build_chart_confirmation_block(
            request=request,
            chart_check=chart_check,
            chart_check_error=chart_check_error,
            structure_context=structure_context,
        ),
        "two_path": _build_two_path_block(
            market_context=market_context,
            time_day_gate=time_day_gate,
            structure_context=structure_context,
            checklist=checklist_block,
            chart_check=chart_check,
        ),
    }





def _nearest_hidden_left_level(
    candles: List[Dict[str, Any]],
    latest_close: Optional[float],
    option_type: str,
) -> Optional[float]:
    if latest_close is None or not candles or len(candles) < 8:
        return None

    left_side = candles[:-5]
    candidates: List[float] = []

    if option_type == "C":
        for candle in left_side:
            high = _to_float(candle.get("high"))
            if high is not None and high > latest_close:
                candidates.append(high)
        return round(min(candidates), 4) if candidates else None

    for candle in left_side:
        low = _to_float(candle.get("low"))
        if low is not None and low < latest_close:
            candidates.append(low)
    return round(max(candidates), 4) if candidates else None


def _build_noisy_chop_proxy(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candles or len(candles) < 6:
        return {
            "status": "unconfirmed",
            "backend_chop_risk": None,
            "overlap_ratio": None,
            "why": "Not enough candles to build a chop proxy.",
        }

    recent = candles[-6:]
    overlap_hits = 0
    comparisons = 0

    for prev, curr in zip(recent[:-1], recent[1:]):
        prev_high = _to_float(prev.get("high"))
        prev_low = _to_float(prev.get("low"))
        curr_high = _to_float(curr.get("high"))
        curr_low = _to_float(curr.get("low"))
        if None in (prev_high, prev_low, curr_high, curr_low):
            continue
        comparisons += 1
        overlap_low = max(prev_low, curr_low)
        overlap_high = min(prev_high, curr_high)
        if overlap_high > overlap_low:
            overlap_hits += 1

    overlap_ratio = round(overlap_hits / comparisons, 3) if comparisons else None
    if overlap_ratio is None:
        status = "unconfirmed"
    elif overlap_ratio >= 0.67:
        status = "possible"
    else:
        status = "not_flagged"

    return {
        "status": status,
        "backend_chop_risk": status == "possible",
        "overlap_ratio": overlap_ratio,
        "why": None if overlap_ratio is not None else "Chop proxy could not be computed.",
    }


def _build_volume_climax_proxy(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candles:
        return {
            "status": "unconfirmed",
            "proxy": "range_expansion_only",
            "range_vs_median": None,
            "why": "No candles are available for exhaustion proxy.",
        }

    ranges: List[float] = []
    for candle in candles[-10:]:
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        if high is not None and low is not None:
            ranges.append(high - low)

    if len(ranges) < 4:
        return {
            "status": "unconfirmed",
            "proxy": "range_expansion_only",
            "range_vs_median": None,
            "why": "Not enough candle ranges are available for exhaustion proxy.",
        }

    latest_range = ranges[-1]
    historical = sorted(ranges[:-1])
    median_range = historical[len(historical) // 2] if historical else None
    if not median_range or median_range <= 0:
        return {
            "status": "unconfirmed",
            "proxy": "range_expansion_only",
            "range_vs_median": None,
            "why": "Median range could not be computed for exhaustion proxy.",
        }

    range_vs_median = round(latest_range / median_range, 3)
    status = "possible" if range_vs_median >= 1.8 else "not_flagged"
    return {
        "status": status,
        "proxy": "range_expansion_only",
        "range_vs_median": range_vs_median,
        "why": "Volume fields are unavailable, so exhaustion uses a candle-range proxy only.",
    }


def _build_screenshot_traps_context(
    structure_context: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
    option_type: str,
) -> Dict[str, Any]:
    recent_candles = chart_check.get("recent_candles") if chart_check else None
    all_candles = chart_check.get("_all_candles") if chart_check else None
    latest_close = chart_check.get("latest_close") if chart_check else None
    ema50 = chart_check.get("ema50_1h") if chart_check else None

    pct_from_ema = structure_context.get("pct_from_ema")
    extension_state = structure_context.get("extension_state")
    chop_risk = structure_context.get("chop_risk")
    effective_wall = structure_context.get("effective_wall")

    overextended_hint = None
    if isinstance(pct_from_ema, (int, float)):
        overextended_hint = pct_from_ema >= 1.0

    candle_source = all_candles if all_candles else recent_candles if recent_candles else []
    hidden_left_level = _nearest_hidden_left_level(candle_source, latest_close, option_type)

    hidden_left_level_pass = None
    if hidden_left_level is not None and effective_wall is not None:
        if option_type == "C":
            hidden_left_level_pass = hidden_left_level >= effective_wall
        else:
            hidden_left_level_pass = hidden_left_level <= effective_wall

    noisy_chop = _build_noisy_chop_proxy(candle_source)
    if noisy_chop.get("status") == "not_flagged" and chop_risk:
        noisy_chop["status"] = "possible"
        noisy_chop["backend_chop_risk"] = True

    volume_climax = _build_volume_climax_proxy(candle_source)

    trap_flags: List[str] = []
    if hidden_left_level_pass is False:
        trap_flags.append("hidden_left_level_inside_room")
    if overextended_hint:
        trap_flags.append("overextended_vs_ema")
    if noisy_chop.get("status") == "possible":
        trap_flags.append("noisy_chop_possible")
    if volume_climax.get("status") == "possible":
        trap_flags.append("exhaustion_proxy_possible")

    if hidden_left_level_pass is False:
        trap_summary = "blocked"
    elif trap_flags:
        trap_summary = "caution"
    else:
        trap_summary = "clear"

    return {
        "ok": True,
        "screenshot_review_available": False,
        "source": "backend_proxy_only",
        "trap_summary": trap_summary,
        "trap_flags": trap_flags,
        "hidden_left_level": hidden_left_level,
        "hidden_left_level_pass": hidden_left_level_pass,
        "effective_wall": effective_wall,
        "overextension_vs_ema": {
            "state": extension_state,
            "pct_from_ema": pct_from_ema,
            "overextended_hint": overextended_hint,
        },
        "volume_climax": volume_climax,
        "noisy_chop": noisy_chop,
        "latest_close": latest_close,
        "ema50_1h": ema50,
        "recent_candles_available": bool(recent_candles),
        "all_candles_available": bool(all_candles),
        "note": "Backend proxy now exposes hidden-left-level, chop, and exhaustion proxies, but full screenshot trap review still requires uploaded chart screenshots.",
    }

def _trap_blocks_trade(screenshot_traps_context: Dict[str, Any]) -> bool:
    if not screenshot_traps_context:
        return False
    if screenshot_traps_context.get("trap_summary") == "blocked":
        return True

    noisy_chop = screenshot_traps_context.get("noisy_chop") or {}
    if noisy_chop.get("status") == "possible" and noisy_chop.get("backend_chop_risk") is True:
        return True

    return False


def _trap_failure_user_text(screenshot_traps_context: Dict[str, Any]) -> str:
    if not screenshot_traps_context:
        return "Screenshot trap proxy blocks this setup."

    if screenshot_traps_context.get("hidden_left_level_pass") is False:
        return "Hidden left-side level sits inside the room."
    noisy_chop = screenshot_traps_context.get("noisy_chop") or {}
    if noisy_chop.get("status") == "possible" and noisy_chop.get("backend_chop_risk") is True:
        return "Noisy chop proxy is too high for a clean SAFE-FAST entry."
    volume_climax = screenshot_traps_context.get("volume_climax") or {}
    if volume_climax.get("status") == "possible":
        return "Exhaustion proxy is elevated for a clean SAFE-FAST entry."
    return "Screenshot trap proxy blocks this setup."


def _trap_failed_reason_messages(screenshot_traps_context: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    if not screenshot_traps_context:
        return reasons

    if screenshot_traps_context.get("hidden_left_level_pass") is False:
        reasons.append("hidden left-side level sits inside the room")

    noisy_chop = screenshot_traps_context.get("noisy_chop") or {}
    if noisy_chop.get("status") == "possible" and noisy_chop.get("backend_chop_risk") is True:
        reasons.append("noisy chop proxy is possible")

    volume_climax = screenshot_traps_context.get("volume_climax") or {}
    if volume_climax.get("status") == "possible":
        reasons.append("exhaustion proxy is possible")

    return reasons


def _build_active_trade_flow_block(
    request: OnDemandRequest,
    option_type: str,
    chart_check: Optional[Dict[str, Any]],
    market_context: Dict[str, Any],
) -> Dict[str, Any]:
    if request.open_positions <= 0:
        return {
            "ok": True,
            "active_trade_mode": False,
            "status": "no_open_trade",
            "message": "No open trade is currently reported, so active-trade enforcement is not engaged.",
        }

    latest_close = chart_check.get("latest_close") if chart_check else None
    ema50 = chart_check.get("ema50_1h") if chart_check else None

    invalidated_now = None
    if latest_close is not None and ema50 is not None:
        if option_type == "C":
            invalidated_now = latest_close < ema50
        elif option_type == "P":
            invalidated_now = latest_close > ema50

    if invalidated_now is True:
        action = "exit_now"
        status = "invalidated"
        message = "1H close is beyond the 50 EMA against the thesis. Exit now."
    elif invalidated_now is False:
        action = "hold_only_if_targets_and_structure_support"
        status = "still_valid"
        message = "No 1H EMA invalidation is detected from the latest snapshot."
    else:
        action = "unconfirmed"
        status = "unconfirmed"
        message = "Latest chart snapshot did not provide enough data to confirm invalidation."

    return {
        "ok": True,
        "active_trade_mode": True,
        "market_open": market_context.get("is_open"),
        "status": status,
        "latest_close": latest_close,
        "ema50_1h": ema50,
        "invalidated_now": invalidated_now,
        "action": action,
        "message": message,
        "required_for_full_check": [
            "entry price",
            "original 1H EMA at entry if you want strict historical comparison",
            "current 1H chart if screenshot review is desired",
        ],
    }


def _build_close_trade_flow_block(
    request: OnDemandRequest,
    market_context: Dict[str, Any],
) -> Dict[str, Any]:
    can_close = request.open_positions > 0
    return {
        "ok": True,
        "can_log_close_now": can_close,
        "status": "ready_for_close_flow" if can_close else "no_open_trade_to_close",
        "market_open": market_context.get("is_open"),
        "required_inputs": [
            "ticker",
            "result",
            "new_position_count",
            "weekly_trade_count",
        ],
        "next_state_if_closed": {
            "open_positions_after_close": 0 if can_close else request.open_positions,
            "weekly_trade_count_remains_user_supplied": True,
        },
        "message": "Close-trade flow can be completed once the result and updated counts are supplied." if can_close else "No open trade is reported, so close-trade flow is informational only.",
    }





def _build_journal_context_block(
    request: OnDemandRequest,
    market_context: Dict[str, Any],
    best_ticker: Optional[str],
    final_verdict: str,
    checklist: Dict[str, Any],
    failed_reasons: List[str],
) -> Dict[str, Any]:
    return {
        "ok": True,
        "journal_ready": True,
        "market_open": market_context.get("is_open"),
        "ticker": best_ticker,
        "final_verdict": final_verdict,
        "open_positions": request.open_positions,
        "weekly_trade_count": request.weekly_trade_count,
        "failed_items": checklist.get("all_failed_items", checklist.get("failed_items", [])),
        "failed_reasons": failed_reasons,
        "required_fields_for_manual_log": [
            "ticker",
            "verdict",
            "entry or no-entry decision",
            "reason",
            "open_positions",
            "weekly_trade_count",
        ],
        "message": "Journal/log context is ready for manual logging or a later automated trade log flow.",
    }



def _build_simple_output_block(
    user_facing: Dict[str, Any],
    trigger_state: Dict[str, Any],
) -> Dict[str, Any]:
    signal_present = bool(
        trigger_state.get("signal_present") is True
        or trigger_state.get("trigger_present") is True
    )

    return {
        "design_goal": "complex_inputs_simple_outputs",
        "good_idea_now": user_facing.get("good_idea_now"),
        "ticker": user_facing.get("ticker"),
        "action": user_facing.get("action"),
        "invalidation": user_facing.get("invalidation"),
        "setup_state": user_facing.get("setup_state"),
        "why": user_facing.get("why"),
        "signal_present": signal_present,
    }



def _build_no_candidate_context(
    summary_payload: Dict[str, Any],
    chart_check_block: Dict[str, Any],
    trigger_state: Dict[str, Any],
    structure_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    iv_context: Dict[str, Any],
    failed_reasons: List[str],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    user_facing: Dict[str, Any],
) -> Dict[str, Any]:
    active = bool(
        summary_payload.get("selection_mode") == "none"
        and summary_payload.get("primary_candidate") is None
    )

    return {
        "active": active,
        "reason": summary_payload.get("reason") if active else None,
        "selection_mode": summary_payload.get("selection_mode"),
        "best_ticker": summary_payload.get("best_ticker"),
        "chart_check_status": chart_check_block.get("status") if active else None,
        "trigger_state": trigger_state.get("entry_state") if active else None,
        "structure_status": structure_context.get("why") if active else None,
        "liquidity_status": liquidity_context.get("status") if active else None,
        "liquidity_reason": liquidity_context.get("why") if active else None,
        "iv_status": iv_context.get("status") if active else None,
        "iv_reason": iv_context.get("why") if active else None,
        "market_session": market_context.get("session") if active else None,
        "time_day_gate_reason": time_day_gate.get("reason") if active else None,
        "action": user_facing.get("action") if active else None,
        "invalidation": user_facing.get("invalidation") if active else None,
        "failed_reasons": failed_reasons if active else [],
    }

def _build_two_path_block(
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    structure_context: Dict[str, Any],
    checklist: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    ema = chart_check.get("ema50_1h") if chart_check else None

    if market_context.get("is_open") is False:
        room_note = structure_context.get("room_note")
        ideal_path = "Wait for next regular session. Re-check before entry."
        if room_note:
            ideal_path = f"{ideal_path} {room_note}"
        return {
            "ideal_path": ideal_path,
            "acceptable_path": "No entry while market is closed.",
            "invalidation_1h_ema50": ema,
        }

    failed_items = set(checklist.get("all_failed_items", checklist.get("failed_items", [])))
    if failed_items:
        ideal_parts: List[str] = []
        if "allowed_setup_type" in failed_items:
            ideal_parts.append("allowed setup type")
        if "twentyfour_hour_supportive" in failed_items:
            ideal_parts.append("24H support")
        if "clear_room" in failed_items:
            ideal_parts.append("room pass")
        if "early_enough" in failed_items:
            ideal_parts.append("time/extension pass")
        if "clear_trigger" in failed_items:
            ideal_parts.append("live trigger")
        ideal_text = "Need " + ", ".join(ideal_parts) + " before entry." if ideal_parts else "Need full gate pass before entry."
        return {
            "ideal_path": ideal_text,
            "acceptable_path": "Stand down until all failed gates pass.",
            "invalidation_1h_ema50": ema,
        }

    return {
        "ideal_path": "Setup passes. Enter only if current bar behavior still confirms the trigger.",
        "acceptable_path": "Take only the mapped entry with the 1H EMA invalidation active.",
        "invalidation_1h_ema50": ema,
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
