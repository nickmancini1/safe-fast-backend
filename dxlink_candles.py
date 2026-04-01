import asyncio
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
import websockets


def _iso_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _compute_ema(values: List[float], length: int) -> Optional[float]:
    if not values:
        return None

    multiplier = 2 / (length + 1)
    ema = values[0]

    for value in values[1:]:
        ema = ((value - ema) * multiplier) + ema

    return round(ema, 4)


async def _send_json(ws: websockets.WebSocketClientProtocol, payload: Dict[str, Any]) -> None:
    await ws.send(json.dumps(payload))


async def _recv_json(ws: websockets.WebSocketClientProtocol, timeout: float = 10.0) -> Dict[str, Any]:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


async def _wait_for_message(
    ws: websockets.WebSocketClientProtocol,
    wanted_type: str,
    channel: Optional[int] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        msg = await _recv_json(ws, timeout=remaining)

        if msg.get("type") == "ERROR":
            raise RuntimeError(f"DXLink error: {msg}")

        if msg.get("type") == wanted_type:
            if channel is None or msg.get("channel") == channel:
                return msg

    raise RuntimeError(f"Timed out waiting for {wanted_type} on channel {channel}")


def _parse_candle_feed_data(message: Dict[str, Any]) -> List[Dict[str, Any]]:
    if message.get("type") != "FEED_DATA":
        return []

    data = message.get("data")
    if not isinstance(data, list) or len(data) != 2:
        return []

    event_type, values = data
    if event_type != "Candle" or not isinstance(values, list):
        return []

    candles: List[Dict[str, Any]] = []
    fields_per_candle = 6  # eventSymbol, time, open, high, low, close

    for i in range(0, len(values), fields_per_candle):
        chunk = values[i:i + fields_per_candle]
        if len(chunk) < fields_per_candle:
            break

        event_symbol, ts, open_, high, low, close = chunk

        ts_int = int(ts)
        open_f = _to_float(open_)
        high_f = _to_float(high)
        low_f = _to_float(low)
        close_f = _to_float(close)

        if None in (open_f, high_f, low_f, close_f):
            continue

        candles.append(
            {
                "event_symbol": str(event_symbol),
                "time": ts_int,
                "time_iso": _iso_from_ms(ts_int),
                "open": open_f,
                "high": high_f,
                "low": low_f,
                "close": close_f,
            }
        )

    return candles


async def _fetch_dxlink_token(
    api_base: str,
    access_token: str,
    user_agent: str,
) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{api_base}/api-quote-tokens",
            headers={
                "Authorization": f"Bearer {access_token}",
                "User-Agent": user_agent,
                "Accept": "application/json",
            },
        )

        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}

        if resp.status_code >= 400:
            raise RuntimeError(f"Quote token request failed: {payload}")

        data = payload.get("data", {})
        token = data.get("token")
        dxlink_url = data.get("dxlink-url")

        if not token or not dxlink_url:
            raise RuntimeError(f"Missing DXLink token fields: {payload}")

        return {
            "token": token,
            "dxlink_url": dxlink_url,
            "payload": payload,
        }


async def get_1h_ema50_snapshot(
    symbol: str,
    access_token: str,
    api_base: str,
    user_agent: str,
    days_back: int = 14,
) -> Dict[str, Any]:
    token_info = await _fetch_dxlink_token(
        api_base=api_base,
        access_token=access_token,
        user_agent=user_agent,
    )

    candle_symbol = f"{symbol}{{=1h,tho=true}}"
    from_time_ms = int((time.time() - (days_back * 24 * 60 * 60)) * 1000)

    seen_by_time: Dict[int, Dict[str, Any]] = {}
    last_raw_message: Optional[Dict[str, Any]] = None

    async with websockets.connect(
        token_info["dxlink_url"],
        open_timeout=20,
        close_timeout=5,
        max_size=10_000_000,
    ) as ws:
        await _send_json(
            ws,
            {
                "type": "SETUP",
                "channel": 0,
                "keepaliveTimeout": 60,
                "acceptKeepaliveTimeout": 60,
                "version": "safe-fast-python-dxlink/0.2",
            },
        )

        for _ in range(2):
            try:
                await _recv_json(ws, timeout=0.5)
            except Exception:
                break

        await _send_json(
            ws,
            {
                "type": "AUTH",
                "channel": 0,
                "token": token_info["token"],
            },
        )

        auth_state = await _wait_for_message(ws, wanted_type="AUTH_STATE", channel=0, timeout=10.0)
        if auth_state.get("state") != "AUTHORIZED":
            raise RuntimeError(f"DXLink auth failed: {auth_state}")

        await _send_json(
            ws,
            {
                "type": "CHANNEL_REQUEST",
                "channel": 1,
                "service": "FEED",
                "parameters": {"contract": "AUTO"},
            },
        )

        await _wait_for_message(ws, wanted_type="CHANNEL_OPENED", channel=1, timeout=10.0)

        await _send_json(
            ws,
            {
                "type": "FEED_SETUP",
                "channel": 1,
                "acceptAggregationPeriod": 1,
                "acceptDataFormat": "COMPACT",
                "acceptEventFields": {
                    "Candle": [
                        "eventSymbol",
                        "time",
                        "open",
                        "high",
                        "low",
                        "close",
                    ]
                },
            },
        )

        try:
            await _wait_for_message(ws, wanted_type="FEED_CONFIG", channel=1, timeout=5.0)
        except Exception:
            pass

        await _send_json(
            ws,
            {
                "type": "FEED_SUBSCRIPTION",
                "channel": 1,
                "add": [
                    {
                        "type": "Candle",
                        "symbol": candle_symbol,
                        "fromTime": from_time_ms,
                    }
                ],
            },
        )

        deadline = time.monotonic() + 8.0
        last_new_data_at: Optional[float] = None

        while time.monotonic() < deadline:
            try:
                msg = await _recv_json(ws, timeout=1.0)
            except asyncio.TimeoutError:
                if len(seen_by_time) >= 50 and last_new_data_at is not None:
                    if (time.monotonic() - last_new_data_at) >= 1.0:
                        break
                continue

            last_raw_message = msg

            if msg.get("type") == "ERROR":
                raise RuntimeError(f"DXLink feed error: {msg}")

            if msg.get("type") != "FEED_DATA":
                continue

            candles = _parse_candle_feed_data(msg)
            if not candles:
                continue

            for candle in candles:
                seen_by_time[candle["time"]] = candle

            last_new_data_at = time.monotonic()

    candle_list = sorted(seen_by_time.values(), key=lambda x: x["time"])

    if not candle_list:
        raise RuntimeError(
            f"No candle data returned for {symbol}. Last message: {last_raw_message}"
        )

    closes = [c["close"] for c in candle_list]
    ema50 = _compute_ema(closes, 50)
    latest = candle_list[-1]
    latest_close = latest["close"]

    if ema50 is None:
        raise RuntimeError("Could not compute EMA50")

    return {
        "ok": True,
        "source": "dxlink",
        "symbol": symbol,
        "candle_symbol": candle_symbol,
        "history_days_requested": days_back,
        "candle_count": len(candle_list),
        "ema_length": 50,
        "ema50_1h": ema50,
        "latest_close": latest_close,
        "price_vs_ema50_1h": "above" if latest_close > ema50 else "below" if latest_close < ema50 else "at",
        "latest_candle_time": latest["time_iso"],
        "recent_candles": candle_list[-10:],
        "all_candles": candle_list,
    }
