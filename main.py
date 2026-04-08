
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

app = FastAPI(title="SAFE-FAST Backend", version="1.8.4")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/1.8.4"

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


def _build_price_zone(
    low: Optional[float],
    high: Optional[float],
    label: str,
    source: str,
) -> Optional[Dict[str, Any]]:
    if low is None or high is None:
        return None
    zone_low = min(low, high)
    zone_high = max(low, high)
    return {
        "label": label,
        "low": round(zone_low, 4),
        "high": round(zone_high, 4),
        "source": source,
    }


def _derive_entry_zones(
    option_type: str,
    chart_check: Optional[Dict[str, Any]],
    structure_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
) -> Dict[str, Any]:
    if not chart_check or not chart_check.get("ok"):
        return {
            "primary_entry_zone": None,
            "backup_entry_zone": None,
        }

    ema50_1h = _to_float(chart_check.get("ema50_1h"))
    latest_close = _to_float(chart_check.get("latest_close"))
    first_wall = _to_float(structure_context.get("first_wall"))
    room_to_first_wall = _to_float(structure_context.get("room_to_first_wall"))
    atr_14_1h = _to_float(structure_context.get("atr_14_1h"))
    trigger_level = _to_float(trigger_state.get("trigger_level"))

    zone_half_width = None
    if atr_14_1h is not None and atr_14_1h > 0:
        zone_half_width = max(atr_14_1h * 0.15, 0.10)
    elif latest_close is not None and latest_close > 0:
        zone_half_width = max(latest_close * 0.0015, 0.10)
    else:
        zone_half_width = 0.10

    primary_entry_zone = None
    if ema50_1h is not None:
        primary_entry_zone = _build_price_zone(
            ema50_1h - zone_half_width,
            ema50_1h + zone_half_width,
            "ema_retest_zone",
            "ema50_1h_anchor",
        )

    backup_entry_zone = None
    if trigger_level is not None:
        backup_entry_zone = _build_price_zone(
            trigger_level - zone_half_width,
            trigger_level + zone_half_width,
            "trigger_retest_zone",
            "trigger_level_anchor",
        )
    elif first_wall is not None:
        wall_buffer = max(zone_half_width, (room_to_first_wall or 0.0) * 0.5)
        if option_type == "C":
            backup_entry_zone = _build_price_zone(
                first_wall - wall_buffer,
                first_wall,
                "first_wall_retest_zone",
                "first_wall_anchor",
            )
        else:
            backup_entry_zone = _build_price_zone(
                first_wall,
                first_wall + wall_buffer,
                "first_wall_retest_zone",
                "first_wall_anchor",
            )

    return {
        "primary_entry_zone": primary_entry_zone,
        "backup_entry_zone": backup_entry_zone,
    }


def _relation_to_ema(candle: Optional[Dict[str, Any]], ema50_1h: Optional[float]) -> Optional[str]:
    if not candle or ema50_1h is None:
        return None

    close_value = _to_float(candle.get("close"))
    high_value = _to_float(candle.get("high"))
    low_value = _to_float(candle.get("low"))

    if close_value is None:
        return None
    if close_value > ema50_1h:
        return "above"
    if close_value < ema50_1h:
        return "below"
    if high_value is not None and low_value is not None and low_value <= ema50_1h <= high_value:
        return "inside"
    return "at"

def _build_trigger_detail_context(
    option_type: str,
    chart_check: Optional[Dict[str, Any]],
    trigger_state: Dict[str, Any],
) -> Dict[str, Any]:
    if not chart_check or not chart_check.get("ok"):
        return {
            "trigger_candle": None,
            "current_bar_behavior": {
                "status": "unconfirmed",
                "why": "chart_unavailable",
            },
        }

    recent = chart_check.get("recent_candles") or []
    if not recent:
        return {
            "trigger_candle": None,
            "current_bar_behavior": {
                "status": "unconfirmed",
                "why": "no_recent_candles",
            },
        }

    current_candle = recent[-1]
    prior_candle = recent[-2] if len(recent) >= 2 else None
    ema50_1h = _to_float(chart_check.get("ema50_1h"))
    trigger_level = _to_float(trigger_state.get("trigger_level"))
    trigger_present = bool(trigger_state.get("trigger_present"))
    structure_ready = trigger_state.get("structure_ready")
    price_side = chart_check.get("price_vs_ema50_1h")
    current_close = _to_float(current_candle.get("close"))
    current_high = _to_float(current_candle.get("high"))
    current_low = _to_float(current_candle.get("low"))

    if option_type == "C":
        if trigger_level is not None and current_close is not None and current_close > trigger_level and structure_ready:
            behavior_label = "breaking_above_trigger"
        elif trigger_level is not None and current_high is not None and current_high >= trigger_level:
            behavior_label = "testing_trigger_but_not_confirmed"
        elif price_side == "above" and ema50_1h is not None and current_low is not None and current_high is not None and current_low <= ema50_1h <= current_high:
            behavior_label = "ema_retest_holding_above"
        elif price_side == "above":
            behavior_label = "above_ema_but_below_trigger"
        else:
            behavior_label = "below_ema_or_not_ready"
    else:
        if trigger_level is not None and current_close is not None and current_close < trigger_level and structure_ready:
            behavior_label = "breaking_below_trigger"
        elif trigger_level is not None and current_low is not None and current_low <= trigger_level:
            behavior_label = "testing_trigger_but_not_confirmed"
        elif price_side == "below" and ema50_1h is not None and current_low is not None and current_high is not None and current_low <= ema50_1h <= current_high:
            behavior_label = "ema_retest_holding_below"
        elif price_side == "below":
            behavior_label = "below_ema_but_above_trigger"
        else:
            behavior_label = "above_ema_or_not_ready"

    trigger_candle_source = "current_bar" if trigger_present else "most_recent_completed_candle"
    trigger_candle_ref = current_candle if trigger_present or prior_candle is None else prior_candle
    trigger_candle_close = _to_float(trigger_candle_ref.get("close")) if trigger_candle_ref else None

    qualifies_as_trigger_candle = False
    if trigger_candle_ref and trigger_level is not None and trigger_candle_close is not None:
        if option_type == "C":
            qualifies_as_trigger_candle = trigger_candle_close > trigger_level
        else:
            qualifies_as_trigger_candle = trigger_candle_close < trigger_level

    trigger_candle = None
    if trigger_candle_ref:
        trigger_candle = {
            "source": trigger_candle_source,
            "time_iso": trigger_candle_ref.get("time_iso"),
            "open": _round_or_none(_to_float(trigger_candle_ref.get("open")), 4),
            "high": _round_or_none(_to_float(trigger_candle_ref.get("high")), 4),
            "low": _round_or_none(_to_float(trigger_candle_ref.get("low")), 4),
            "close": _round_or_none(_to_float(trigger_candle_ref.get("close")), 4),
            "relation_to_trigger_level": (
                "above" if trigger_level is not None and trigger_candle_close is not None and trigger_candle_close > trigger_level
                else "below" if trigger_level is not None and trigger_candle_close is not None and trigger_candle_close < trigger_level
                else "at" if trigger_level is not None and trigger_candle_close is not None
                else None
            ),
            "relation_to_ema50_1h": _relation_to_ema(trigger_candle_ref, ema50_1h),
            "qualifies_as_trigger_candle": qualifies_as_trigger_candle,
        }

    current_bar_behavior = {
        "status": "confirmed",
        "label": behavior_label,
        "time_iso": current_candle.get("time_iso"),
        "open": _round_or_none(_to_float(current_candle.get("open")), 4),
        "high": _round_or_none(current_high, 4),
        "low": _round_or_none(current_low, 4),
        "close": _round_or_none(current_close, 4),
        "price_vs_ema50_1h": price_side,
        "trigger_level": _round_or_none(trigger_level, 4),
        "trigger_present": trigger_present,
        "structure_ready": structure_ready,
        "why": trigger_state.get("why"),
    }

    return {
        "trigger_candle": trigger_candle,
        "current_bar_behavior": current_bar_behavior,
    }



def _summarize_trigger_scan_candle(
    candle: Optional[Dict[str, Any]],
    ema50_1h: Optional[float],
) -> Optional[Dict[str, Any]]:
    if not candle:
        return None
    return {
        "time_iso": candle.get("time_iso"),
        "open": _round_or_none(_to_float(candle.get("open")), 4),
        "high": _round_or_none(_to_float(candle.get("high")), 4),
        "low": _round_or_none(_to_float(candle.get("low")), 4),
        "close": _round_or_none(_to_float(candle.get("close")), 4),
        "relation_to_ema50_1h": _relation_to_ema(candle, ema50_1h),
    }


def _evaluate_trigger_scan_candle(
    option_type: str,
    candle: Optional[Dict[str, Any]],
    reference_candles: List[Dict[str, Any]],
    ema50_1h: Optional[float],
    structure_ready: Optional[bool],
    market_open: bool,
    fresh_entry_allowed: bool,
    gate_reason: Optional[str],
) -> Dict[str, Any]:
    if not candle:
        return {
            "status": "unconfirmed",
            "why": "candle_unavailable",
        }

    if len(reference_candles) < 3:
        return {
            "time_iso": candle.get("time_iso"),
            "open": _round_or_none(_to_float(candle.get("open")), 4),
            "high": _round_or_none(_to_float(candle.get("high")), 4),
            "low": _round_or_none(_to_float(candle.get("low")), 4),
            "close": _round_or_none(_to_float(candle.get("close")), 4),
            "reference_window_size": len(reference_candles),
            "reference_trigger_level": None,
            "relation_to_trigger_level": None,
            "relation_to_ema50_1h": _relation_to_ema(candle, ema50_1h),
            "raw_cross_pass": False,
            "ema_side_pass": False,
            "raw_chart_trigger_pass": False,
            "structure_ready": structure_ready,
            "gated_trigger_pass": False,
            "status": "unconfirmed",
            "why": "insufficient_reference_candles",
        }

    close_value = _to_float(candle.get("close"))
    if option_type == "C":
        trigger_level = max((_to_float(ref.get("high")) for ref in reference_candles if _to_float(ref.get("high")) is not None), default=None)
        raw_cross_pass = bool(trigger_level is not None and close_value is not None and close_value > trigger_level)
        relation_to_trigger = (
            "above" if trigger_level is not None and close_value is not None and close_value > trigger_level
            else "below" if trigger_level is not None and close_value is not None and close_value < trigger_level
            else "at" if trigger_level is not None and close_value is not None
            else None
        )
        ema_side_pass = bool(close_value is not None and ema50_1h is not None and close_value > ema50_1h)
    else:
        trigger_level = min((_to_float(ref.get("low")) for ref in reference_candles if _to_float(ref.get("low")) is not None), default=None)
        raw_cross_pass = bool(trigger_level is not None and close_value is not None and close_value < trigger_level)
        relation_to_trigger = (
            "below" if trigger_level is not None and close_value is not None and close_value < trigger_level
            else "above" if trigger_level is not None and close_value is not None and close_value > trigger_level
            else "at" if trigger_level is not None and close_value is not None
            else None
        )
        ema_side_pass = bool(close_value is not None and ema50_1h is not None and close_value < ema50_1h)

    raw_chart_trigger_pass = bool(raw_cross_pass and ema_side_pass)
    gated_trigger_pass = bool(
        raw_chart_trigger_pass
        and structure_ready is True
        and market_open
        and fresh_entry_allowed
    )

    if gated_trigger_pass:
        status = "pass"
        why = "Trigger conditions pass on this candle."
    elif not market_open:
        status = "fail"
        why = "market_closed"
    elif not fresh_entry_allowed:
        status = "fail"
        why = gate_reason or "time_day_gate_blocked"
    elif structure_ready is False:
        status = "fail"
        why = "structure_not_ready"
    elif not ema_side_pass:
        status = "fail"
        why = "wrong_side_of_ema"
    elif not raw_cross_pass:
        status = "fail"
        why = "close_trigger_not_hit"
    else:
        status = "unconfirmed"
        why = "trigger_unconfirmed"

    return {
        "time_iso": candle.get("time_iso"),
        "open": _round_or_none(_to_float(candle.get("open")), 4),
        "high": _round_or_none(_to_float(candle.get("high")), 4),
        "low": _round_or_none(_to_float(candle.get("low")), 4),
        "close": _round_or_none(close_value, 4),
        "reference_window_size": len(reference_candles),
        "reference_trigger_level": _round_or_none(trigger_level, 4),
        "relation_to_trigger_level": relation_to_trigger,
        "relation_to_ema50_1h": _relation_to_ema(candle, ema50_1h),
        "raw_cross_pass": raw_cross_pass,
        "ema_side_pass": ema_side_pass,
        "raw_chart_trigger_pass": raw_chart_trigger_pass,
        "structure_ready": structure_ready,
        "gated_trigger_pass": gated_trigger_pass,
        "status": status,
        "why": why,
    }


def _build_trigger_scan_context(
    option_type: str,
    chart_check: Optional[Dict[str, Any]],
    trigger_state: Dict[str, Any],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
) -> Dict[str, Any]:
    if not chart_check or not chart_check.get("ok"):
        return {
            "scan_basis": "current_bar_plus_last_3_completed_1h_candles",
            "required_completed_candle_count": 3,
            "trigger_style": trigger_state.get("trigger_style"),
            "market_open": market_context.get("is_open"),
            "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
            "current_bar": {
                "status": "unconfirmed",
                "why": "chart_unavailable",
            },
            "most_recent_completed_candle": {
                "status": "unconfirmed",
                "why": "chart_unavailable",
            },
            "current_bar_reference_candles": [],
            "completed_candle_reference_candles": [],
            "trigger_scan_status": "unconfirmed",
            "why_trigger_scan_passes_or_fails": "Trigger scan is unconfirmed because chart data is unavailable.",
        }

    recent = chart_check.get("recent_candles") or []
    ema50_1h = _to_float(chart_check.get("ema50_1h"))
    market_open = bool(market_context.get("is_open"))
    fresh_entry_allowed = bool(time_day_gate.get("fresh_entry_allowed"))
    gate_reason = trigger_state.get("why")
    structure_ready = trigger_state.get("structure_ready")

    current_bar = recent[-1] if recent else None
    most_recent_completed = recent[-2] if len(recent) >= 2 else None
    current_bar_refs = recent[-4:-1] if len(recent) >= 4 else recent[:-1]
    completed_refs = recent[-5:-2] if len(recent) >= 5 else recent[:-2]

    current_bar_eval = _evaluate_trigger_scan_candle(
        option_type=option_type,
        candle=current_bar,
        reference_candles=current_bar_refs,
        ema50_1h=ema50_1h,
        structure_ready=structure_ready,
        market_open=market_open,
        fresh_entry_allowed=fresh_entry_allowed,
        gate_reason=gate_reason,
    )
    completed_eval = _evaluate_trigger_scan_candle(
        option_type=option_type,
        candle=most_recent_completed,
        reference_candles=completed_refs,
        ema50_1h=ema50_1h,
        structure_ready=structure_ready,
        market_open=market_open,
        fresh_entry_allowed=fresh_entry_allowed,
        gate_reason=gate_reason,
    )

    if current_bar_eval.get("gated_trigger_pass"):
        trigger_scan_status = "pass_current_bar"
        why = "SAFE-FAST trigger conditions pass on the current 1H bar."
    elif completed_eval.get("gated_trigger_pass"):
        trigger_scan_status = "pass_most_recent_completed_candle"
        why = "SAFE-FAST trigger conditions passed on the most recent completed 1H candle."
    elif not market_open:
        trigger_scan_status = "fail"
        why = "Market is closed, so trigger scan cannot produce a live entry."
    elif not fresh_entry_allowed:
        trigger_scan_status = "fail"
        why = gate_reason or "Fresh entry is outside the SAFE-FAST time/day window."
    elif structure_ready is False:
        trigger_scan_status = "fail"
        why = "Structure is not ready for a SAFE-FAST trigger."
    elif current_bar_eval.get("raw_chart_trigger_pass") or completed_eval.get("raw_chart_trigger_pass"):
        trigger_scan_status = "fail"
        why = "A raw chart trigger appeared, but SAFE-FAST gating still blocks it."
    elif current_bar_eval.get("status") == "unconfirmed" and completed_eval.get("status") == "unconfirmed":
        trigger_scan_status = "unconfirmed"
        why = "Trigger scan is still unconfirmed from the available candles."
    else:
        trigger_scan_status = "fail"
        why = "No SAFE-FAST trigger condition is currently satisfied."

    return {
        "scan_basis": "current_bar_plus_last_3_completed_1h_candles",
        "required_completed_candle_count": 3,
        "trigger_style": trigger_state.get("trigger_style"),
        "market_open": market_open,
        "fresh_entry_allowed": fresh_entry_allowed,
        "structure_ready": structure_ready,
        "current_bar": current_bar_eval,
        "most_recent_completed_candle": completed_eval,
        "current_bar_reference_candles": [
            _summarize_trigger_scan_candle(candle, ema50_1h) for candle in current_bar_refs
        ],
        "completed_candle_reference_candles": [
            _summarize_trigger_scan_candle(candle, ema50_1h) for candle in completed_refs
        ],
        "trigger_scan_status": trigger_scan_status,
        "why_trigger_scan_passes_or_fails": why,
    }


def _build_setup_route_context(
    option_type: str,
    structure_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    intended_setup_type = structure_context.get("setup_type")
    allowed_setup = structure_context.get("allowed_setup")
    chop_risk = bool(structure_context.get("chop_risk"))
    extension_state = structure_context.get("extension_state")
    room_pass = structure_context.get("room_pass")
    wall_pass = structure_context.get("wall_pass")
    trigger_present = bool(trigger_state.get("trigger_present"))
    structure_ready = bool(trigger_state.get("structure_ready"))
    price_side = chart_check.get("price_vs_ema50_1h") if chart_check else None
    allowed_setup_types = {"Ideal", "Clean Fast Break", "Continuation"}
    route_type_allowed = intended_setup_type in allowed_setup_types

    if intended_setup_type == "Clean Fast Break":
        retest_quality = "breakout_path" if not chop_risk else "messy_breakout"
    elif intended_setup_type in {"Ideal", "Continuation"}:
        if chop_risk:
            retest_quality = "messy_retest"
        elif extension_state == "extended":
            retest_quality = "late_retest"
        elif price_side in {"above", "below"}:
            retest_quality = "clean_retest"
        else:
            retest_quality = "unconfirmed_retest"
    else:
        if chop_risk:
            retest_quality = "messy_retest"
        elif extension_state == "extended":
            retest_quality = "late_retest"
        else:
            retest_quality = "not_applicable"

    fast_entry_allowed = bool(
        intended_setup_type == "Clean Fast Break"
        and allowed_setup is True
        and room_pass is True
        and wall_pass is not False
        and extension_state != "extended"
        and structure_ready
        and trigger_present
    )

    if intended_setup_type == "Clean Fast Break":
        next_bar_confirmation_required = not fast_entry_allowed
    elif intended_setup_type in {"Ideal", "Continuation"}:
        next_bar_confirmation_required = True
    else:
        next_bar_confirmation_required = None

    if (
        route_type_allowed
        and allowed_setup is True
        and room_pass is True
        and wall_pass is not False
        and extension_state != "extended"
    ):
        if fast_entry_allowed:
            setup_route_status = "pass_fast_entry"
            why = "Clean Fast Break conditions are aligned and fast-entry is allowed."
        elif next_bar_confirmation_required:
            setup_route_status = "pending_confirmation"
            why = "Setup route is valid, but next-bar or close confirmation is still required."
        else:
            setup_route_status = "pass"
            why = "Setup route passes the current SAFE-FAST route checks."
    elif route_type_allowed is False:
        setup_route_status = "fail"
        why = "Setup type is not one of the allowed SAFE-FAST routes."
    elif room_pass is False:
        setup_route_status = "fail"
        why = "Room to the first wall is too tight for this setup route."
    elif wall_pass is False:
        setup_route_status = "fail"
        why = "Wall thesis does not support this setup route."
    elif extension_state == "extended":
        setup_route_status = "fail"
        why = "Setup route is too extended or too late versus the 1H 50 EMA."
    elif allowed_setup is False:
        setup_route_status = "fail"
        why = "This is an allowed SAFE-FAST route class, but the current structure does not qualify it as a valid setup."
    else:
        setup_route_status = "unconfirmed"
        why = "Setup route is still unconfirmed from the available chart context."

    return {
        "intended_setup_type": intended_setup_type,
        "retest_quality": retest_quality,
        "fast_entry_allowed": fast_entry_allowed,
        "next_bar_confirmation_required": next_bar_confirmation_required,
        "setup_route_status": setup_route_status,
        "why_setup_route_passes_or_fails": why,
    }


def _build_room_wall_context(structure_context: Dict[str, Any]) -> Dict[str, Any]:
    room_pass = structure_context.get("room_pass")
    wall_pass = structure_context.get("wall_pass")
    wall_thesis = structure_context.get("wall_thesis")
    extension_blocks_now = structure_context.get("extension_blocks_now")

    if room_pass is False:
        room_wall_status = "fail"
        why = "Room to the first wall is too tight for SAFE-FAST."
    elif wall_pass is False:
        room_wall_status = "fail"
        why = "Wall thesis does not support the current path."
    elif room_pass is True and wall_pass is True:
        room_wall_status = "pass"
        if wall_thesis == "TO_THE_WALL":
            why = "Room and wall context are aligned only up to the first wall."
        else:
            why = "Room and wall context are aligned for the current path."
    else:
        room_wall_status = "unconfirmed"
        why = "Room/wall context is still unconfirmed from the available chart inputs."

    return {
        "first_wall": structure_context.get("first_wall"),
        "next_pocket": structure_context.get("next_pocket"),
        "room_to_first_wall": structure_context.get("room_to_first_wall"),
        "room_ratio": structure_context.get("room_ratio"),
        "next_pocket_room_ratio": structure_context.get("next_pocket_room_ratio"),
        "room_pass": room_pass,
        "wall_thesis": wall_thesis,
        "wall_pass": wall_pass,
        "extension_blocks_now": extension_blocks_now,
        "room_wall_status": room_wall_status,
        "why_room_or_wall_passes_or_fails": why,
    }


def _build_extension_quality_context(structure_context: Dict[str, Any]) -> Dict[str, Any]:
    extension_state = structure_context.get("extension_state")
    late_move = structure_context.get("late_move")
    extension_material = structure_context.get("extension_material")
    extension_soft_flag = structure_context.get("extension_soft_flag")
    extension_blocks_now = structure_context.get("extension_blocks_now")
    degraded_entry_quality = structure_context.get("degraded_entry_quality")
    early_trigger_window_passed = structure_context.get("early_trigger_window_passed")
    extension_confirmer_flags = structure_context.get("extension_confirmer_flags")
    extension_confirmer_count = structure_context.get("extension_confirmer_count")

    if extension_blocks_now is True:
        extension_quality_status = "fail"
        why = "Move is materially extended for SAFE-FAST right now."
    elif extension_state == "extended" or late_move is True or extension_material is True:
        if extension_soft_flag is True:
            extension_quality_status = "caution"
            why = "Extension is elevated, but treated as a soft caution rather than a hard blocker."
        else:
            extension_quality_status = "caution"
            why = "Extension is elevated and needs confirmation from cleaner structure."
    elif extension_state is None and late_move is None:
        extension_quality_status = "unconfirmed"
        why = "Extension quality is still unconfirmed from the available chart inputs."
    else:
        extension_quality_status = "pass"
        why = "Extension quality is not currently blocking the setup."

    return {
        "extension_state": extension_state,
        "late_move": late_move,
        "pct_from_ema": structure_context.get("pct_from_ema"),
        "atr_multiple_from_ema": structure_context.get("atr_multiple_from_ema"),
        "degraded_entry_quality": degraded_entry_quality,
        "early_trigger_window_passed": early_trigger_window_passed,
        "extension_confirmer_flags": extension_confirmer_flags,
        "extension_confirmer_count": extension_confirmer_count,
        "extension_material": extension_material,
        "extension_soft_flag": extension_soft_flag,
        "extension_blocks_now": extension_blocks_now,
        "extension_quality_status": extension_quality_status,
        "why_extension_passes_or_fails": why,
    }



def _build_execution_quality_context(
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    macro_context: Dict[str, Any],
    iv_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
) -> Dict[str, Any]:
    market_open = bool(market_context.get("is_open"))
    fresh_entry_allowed = bool(time_day_gate.get("fresh_entry_allowed"))
    liquidity_pass = liquidity_context.get("liquidity_pass")
    liquidity_status = liquidity_context.get("status")
    iv_status = iv_context.get("status")
    has_major_event_today = bool(macro_context.get("has_major_event_today"))
    has_major_event_tomorrow = bool(macro_context.get("has_major_event_tomorrow"))
    macro_risk_level = macro_context.get("risk_level")

    if not market_open or not fresh_entry_allowed:
        execution_quality_status = "fail"
        why = "Fresh entries are not allowed right now."
    elif liquidity_pass is False:
        execution_quality_status = "fail"
        why = liquidity_context.get("why") or "Liquidity is too weak for a clean SAFE-FAST entry."
    elif has_major_event_today:
        execution_quality_status = "caution"
        why = "A major macro event is in play today, so execution quality needs extra caution."
    elif has_major_event_tomorrow:
        execution_quality_status = "caution"
        why = "A major macro event is in play tomorrow, so execution quality needs extra caution."
    elif iv_status == "unconfirmed":
        execution_quality_status = "caution"
        why = "IV is still unconfirmed in this build, even though time window and liquidity are acceptable."
    elif liquidity_pass is True:
        execution_quality_status = "pass"
        why = "Time window and liquidity are acceptable for execution."
    else:
        execution_quality_status = "unconfirmed"
        why = "Execution quality is still unconfirmed from the available inputs."

    return {
        "market_open": market_open,
        "fresh_entry_allowed": fresh_entry_allowed,
        "macro_risk_level": macro_risk_level,
        "has_major_event_today": has_major_event_today,
        "has_major_event_tomorrow": has_major_event_tomorrow,
        "iv_status": iv_status,
        "liquidity_status": liquidity_status,
        "liquidity_pass": liquidity_pass,
        "execution_quality_status": execution_quality_status,
        "why_execution_quality_passes_or_fails": why,
    }





def _build_event_gate_context(
    macro_context: Dict[str, Any],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
) -> Dict[str, Any]:
    market_open = bool(market_context.get("is_open"))
    fresh_entry_allowed = bool(time_day_gate.get("fresh_entry_allowed"))
    has_major_event_today = bool(macro_context.get("has_major_event_today"))
    has_major_event_tomorrow = bool(macro_context.get("has_major_event_tomorrow"))
    events = macro_context.get("events") or []
    risk_level = macro_context.get("risk_level")
    note = macro_context.get("note")

    if not market_open:
        event_gate_status = "unconfirmed"
        why = "Market is closed, so the live event gate is not actionable right now."
    elif has_major_event_today:
        event_gate_status = "fail"
        why = "A major macro event is in play today, so the SAFE-FAST event gate fails unless explicitly approved."
    elif has_major_event_tomorrow:
        event_gate_status = "caution"
        why = "A major macro event is tomorrow, so overnight hold risk needs extra caution."
    elif not fresh_entry_allowed:
        event_gate_status = "caution"
        why = "No major macro event blocks the map, but the fresh-entry window is already closed."
    else:
        event_gate_status = "pass"
        why = "No major macro event is blocking the SAFE-FAST event gate right now."

    return {
        "market_open": market_open,
        "fresh_entry_allowed": fresh_entry_allowed,
        "has_major_event_today": has_major_event_today,
        "has_major_event_tomorrow": has_major_event_tomorrow,
        "events": events,
        "risk_level": risk_level,
        "event_gate_status": event_gate_status,
        "why_event_gate_passes_or_fails": why,
        "note": note,
    }


def _build_wall_thesis_fit_context(
    option_type: str,
    structure_context: Dict[str, Any],
    primary_candidate: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    wall_thesis = structure_context.get("wall_thesis")
    first_wall = _to_float(structure_context.get("first_wall"))
    next_pocket = _to_float(structure_context.get("next_pocket"))
    long_strike = _to_float(primary_candidate.get("long_strike")) if primary_candidate else None
    short_strike = _to_float(primary_candidate.get("short_strike")) if primary_candidate else None

    tolerance = None
    if first_wall is not None:
        tolerance = max(abs(first_wall) * 0.0015, 0.10)

    short_strike_vs_first_wall = None
    if short_strike is not None and first_wall is not None and tolerance is not None:
        if abs(short_strike - first_wall) <= tolerance:
            short_strike_vs_first_wall = "on_wall_zone"
        elif option_type == "C":
            short_strike_vs_first_wall = "beyond_first_wall" if short_strike > first_wall else "inside_first_wall"
        else:
            short_strike_vs_first_wall = "beyond_first_wall" if short_strike < first_wall else "inside_first_wall"

    short_strike_on_magnet_level = None
    if short_strike is not None:
        magnet_hits = []
        for level in (first_wall, next_pocket):
            if level is None:
                continue
            local_tol = max(abs(level) * 0.0015, 0.10)
            if abs(short_strike - level) <= local_tol:
                magnet_hits.append(level)
        short_strike_on_magnet_level = bool(magnet_hits) if magnet_hits else False

    requires_breakout = wall_thesis == "THROUGH_THE_WALL"

    if primary_candidate is None:
        status = "unconfirmed"
        why = "Wall-thesis fit is unconfirmed because no primary candidate is available."
    elif wall_thesis not in {"TO_THE_WALL", "THROUGH_THE_WALL"}:
        status = "unconfirmed"
        why = "Wall thesis is still unconfirmed from the available structure inputs."
    elif short_strike is None or first_wall is None:
        status = "unconfirmed"
        why = "Wall-thesis fit is unconfirmed because strike or wall data is missing."
    elif wall_thesis == "TO_THE_WALL":
        if short_strike_on_magnet_level:
            status = "fail"
            why = "TO_THE_WALL fails because the short strike sits on a magnet level."
        elif short_strike_vs_first_wall != "beyond_first_wall":
            status = "fail"
            why = "TO_THE_WALL fails because the short strike is not beyond the first wall."
        else:
            status = "pass"
            why = "TO_THE_WALL fits because the short strike sits beyond the first wall without sitting on it."
    else:
        if next_pocket is None:
            status = "fail"
            why = "THROUGH_THE_WALL fails because no clear next pocket is mapped beyond the first wall."
        elif short_strike_vs_first_wall not in {"on_wall_zone", "beyond_first_wall"}:
            status = "fail"
            why = "THROUGH_THE_WALL fails because the short strike is not positioned for a breakout path."
        else:
            status = "pass"
            why = "THROUGH_THE_WALL fits because breakout continuation into the next pocket is mapped."

    return {
        "wall_thesis": wall_thesis,
        "long_strike": long_strike,
        "short_strike": short_strike,
        "first_wall": structure_context.get("first_wall"),
        "next_pocket": structure_context.get("next_pocket"),
        "short_strike_vs_first_wall": short_strike_vs_first_wall,
        "requires_breakout": requires_breakout,
        "short_strike_on_magnet_level": short_strike_on_magnet_level,
        "wall_thesis_fit_status": status,
        "why_wall_thesis_fit_passes_or_fails": why,
    }



def _build_adx_filter_context(structure_context: Dict[str, Any]) -> Dict[str, Any]:
    adx_value = _to_float(structure_context.get("adx_value_1h"))
    adx_trend = structure_context.get("adx_trend")
    chop_risk_from_adx = structure_context.get("chop_risk_from_adx")

    adx_override_blocked_by: List[str] = [
        "price",
        "room",
        "late_move",
        "wall_placement",
        "risk",
        "trigger_rules",
    ]

    if adx_value is None:
        status = "unconfirmed"
        why = "ADX is not available from the current candle set, so the secondary ADX filter remains unconfirmed."
    elif chop_risk_from_adx is True:
        status = "caution"
        why = "ADX is secondary only. Current ADX implies chop risk, but it does not override primary SAFE-FAST blockers."
    else:
        status = "pass"
        why = "ADX does not currently add extra chop risk, but it remains secondary to primary SAFE-FAST rules."

    return {
        "adx_value_1h": adx_value,
        "adx_trend": adx_trend,
        "chop_risk_from_adx": chop_risk_from_adx,
        "adx_override_blocked_by": adx_override_blocked_by,
        "adx_filter_status": status,
        "why_adx_passes_or_fails": why,
    }


def _build_options_structure_context(
    request: OnDemandRequest,
    selected_summary: Optional[Dict[str, Any]],
    primary_candidate: Optional[Dict[str, Any]],
    liquidity_context: Dict[str, Any],
) -> Dict[str, Any]:
    expiration_date = selected_summary.get("expiration_date") if selected_summary else None
    days_to_expiration = selected_summary.get("days_to_expiration") if selected_summary else None
    width = primary_candidate.get("width") if primary_candidate else None
    est_debit = primary_candidate.get("est_debit") if primary_candidate else None
    max_loss = primary_candidate.get("max_loss_dollars_1lot") if primary_candidate else None
    max_profit = primary_candidate.get("max_profit_dollars_1lot") if primary_candidate else None
    risk_reward = primary_candidate.get("risk_reward") if primary_candidate else None
    feasibility_pass = primary_candidate.get("feasibility_pass") if primary_candidate else None
    fits_risk_budget = primary_candidate.get("fits_risk_budget") if primary_candidate else None
    liquidity_pass = liquidity_context.get("liquidity_pass")
    liquidity_status = liquidity_context.get("status")

    dte_rule_pass = None
    if isinstance(days_to_expiration, (int, float)):
        dte_rule_pass = request.min_dte <= float(days_to_expiration) <= request.max_dte

    width_rule_pass = None
    if isinstance(width, (int, float)):
        width_rule_pass = request.width_min <= float(width) <= request.width_max

    debit_feasibility_rule_pass = None
    if isinstance(est_debit, (int, float)) and isinstance(width, (int, float)):
        debit_feasibility_rule_pass = (1.60 * float(est_debit)) <= float(width)

    preferred_risk_band_pass = None
    if isinstance(max_loss, (int, float)):
        preferred_risk_band_pass = request.risk_min_dollars <= float(max_loss) <= request.risk_max_dollars

    hard_risk_cap_pass = None
    if isinstance(max_loss, (int, float)):
        hard_risk_cap_pass = float(max_loss) <= request.hard_max_dollars

    if primary_candidate is None:
        options_structure_status = "unconfirmed"
        why = "Options structure is unconfirmed because no primary candidate is available."
    elif feasibility_pass is False:
        options_structure_status = "fail"
        why = "Candidate fails the defined-risk debit spread feasibility rule."
    elif debit_feasibility_rule_pass is False:
        options_structure_status = "fail"
        why = "Candidate fails the 1.60 x debit <= width feasibility rule."
    elif fits_risk_budget is False or hard_risk_cap_pass is False:
        options_structure_status = "fail"
        why = "Candidate does not fit the SAFE-FAST risk budget."
    elif dte_rule_pass is False:
        options_structure_status = "fail"
        why = "Candidate is outside the SAFE-FAST DTE window."
    elif width_rule_pass is False:
        options_structure_status = "fail"
        why = "Candidate is outside the SAFE-FAST width range."
    elif liquidity_pass is False:
        options_structure_status = "fail"
        why = liquidity_context.get("why") or "Options structure is not liquid enough for a clean entry."
    elif (
        feasibility_pass is True
        and debit_feasibility_rule_pass is True
        and fits_risk_budget is True
        and dte_rule_pass is True
        and width_rule_pass is True
        and liquidity_pass is True
    ):
        options_structure_status = "pass"
        why = "Options structure fits DTE, width, risk, feasibility, and liquidity rules."
    else:
        options_structure_status = "caution"
        why = "Options structure is mostly aligned, but one or more checks remain unconfirmed."

    return {
        "expiration_date": expiration_date,
        "days_to_expiration": days_to_expiration,
        "width": width,
        "est_debit": est_debit,
        "max_loss_dollars_1lot": max_loss,
        "max_profit_dollars_1lot": max_profit,
        "risk_reward": risk_reward,
        "feasibility_pass": feasibility_pass,
        "fits_risk_budget": fits_risk_budget,
        "preferred_risk_band_pass": preferred_risk_band_pass,
        "hard_risk_cap_pass": hard_risk_cap_pass,
        "dte_rule_pass": dte_rule_pass,
        "width_rule_pass": width_rule_pass,
        "debit_feasibility_rule_pass": debit_feasibility_rule_pass,
        "liquidity_status": liquidity_status,
        "liquidity_pass": liquidity_pass,
        "options_structure_status": options_structure_status,
        "why_options_structure_passes_or_fails": why,
    }



def _build_live_map_block(
    ticker: Optional[str],
    option_type: str,
    primary_entry_zone: Optional[Dict[str, Any]],
    backup_entry_zone: Optional[Dict[str, Any]],
    trigger_state: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
    structure_context: Dict[str, Any],
    invalidation_level_1h_ema50: Optional[float],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    macro_context: Dict[str, Any],
    iv_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    selected_summary: Optional[Dict[str, Any]],
    primary_candidate: Optional[Dict[str, Any]],
    request: OnDemandRequest,
) -> Dict[str, Any]:
    trigger_detail = _build_trigger_detail_context(
        option_type=option_type,
        chart_check=chart_check,
        trigger_state=trigger_state,
    )
    setup_route = _build_setup_route_context(
        option_type=option_type,
        structure_context=structure_context,
        trigger_state=trigger_state,
        chart_check=chart_check,
    )
    room_wall = _build_room_wall_context(structure_context)
    extension_quality = _build_extension_quality_context(structure_context)
    execution_quality = _build_execution_quality_context(
        market_context=market_context,
        time_day_gate=time_day_gate,
        macro_context=macro_context,
        iv_context=iv_context,
        liquidity_context=liquidity_context,
    )
    event_gate = _build_event_gate_context(
        macro_context=macro_context,
        market_context=market_context,
        time_day_gate=time_day_gate,
    )
    options_structure = _build_options_structure_context(
        request=request,
        selected_summary=selected_summary,
        primary_candidate=primary_candidate,
        liquidity_context=liquidity_context,
    )
    wall_thesis_fit = _build_wall_thesis_fit_context(
        option_type=option_type,
        structure_context=structure_context,
        primary_candidate=primary_candidate,
    )
    adx_filter = _build_adx_filter_context(structure_context)
    trigger_scan = _build_trigger_scan_context(
        option_type=option_type,
        chart_check=chart_check,
        trigger_state=trigger_state,
        market_context=market_context,
        time_day_gate=time_day_gate,
    )
    return {
        "ticker": ticker,
        "primary_entry_zone": primary_entry_zone,
        "backup_entry_zone": backup_entry_zone,
        "trigger_style": trigger_state.get("trigger_style"),
        "trigger_level": trigger_state.get("trigger_level"),
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_candle": trigger_detail.get("trigger_candle"),
        "current_bar_behavior": trigger_detail.get("current_bar_behavior"),
        "setup_route": setup_route,
        "room_wall": room_wall,
        "extension_quality": extension_quality,
        "execution_quality": execution_quality,
        "event_gate": event_gate,
        "options_structure": options_structure,
        "wall_thesis_fit": wall_thesis_fit,
        "adx_filter": adx_filter,
        "trigger_scan": trigger_scan,
        "invalidation_1h_ema50": invalidation_level_1h_ema50,
        "market_open": market_context.get("is_open"),
        "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
    }



def _calc_pct_of_mid(bid: Optional[float], ask: Optional[float], mid: Optional[float]) -> Optional[float]:
    if bid is None or ask is None or mid in (None, 0):
        return None
    return round(((ask - bid) / mid) * 100, 3)


def _classify_liquidity(
    entry_slippage_vs_mid: Optional[float],
    long_leg_width_pct_of_mid: Optional[float],
    short_leg_width_pct_of_mid: Optional[float],
) -> Dict[str, Any]:
    if (
        entry_slippage_vs_mid is None
        or long_leg_width_pct_of_mid is None
        or short_leg_width_pct_of_mid is None
    ):
        return {
            "label": "unconfirmed",
            "liquidity_pass": None,
            "why": "Quotes did not provide enough bid/ask detail to confirm liquidity.",
        }

    if (
        entry_slippage_vs_mid <= 0.15
        and long_leg_width_pct_of_mid <= 12
        and short_leg_width_pct_of_mid <= 12
    ):
        return {
            "label": "tight",
            "liquidity_pass": True,
            "why": "Bid/ask widths and entry slippage are tight enough for a defined-risk debit spread.",
        }

    if (
        entry_slippage_vs_mid <= 0.30
        and long_leg_width_pct_of_mid <= 20
        and short_leg_width_pct_of_mid <= 20
    ):
        return {
            "label": "acceptable",
            "liquidity_pass": True,
            "why": "Bid/ask widths are workable, but not especially tight.",
        }

    return {
        "label": "wide",
        "liquidity_pass": False,
        "why": "Bid/ask widths or entry slippage are too wide for a clean SAFE-FAST debit spread entry.",
    }


def _build_liquidity_block(candidate: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidate:
        return {
            "ok": False,
            "status": "unconfirmed",
            "why": "No candidate available.",
        }

    label_ctx = _classify_liquidity(
        candidate.get("entry_slippage_vs_mid"),
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
    }


def _build_iv_context() -> Dict[str, Any]:
    return {
        "ok": False,
        "status": "unconfirmed",
        "why": "IV source is not wired into this build yet.",
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

    if weekday <= 3:
        cutoff = time(14, 0)
        allowed = now_et.time() < cutoff
        return {
            "fresh_entry_allowed": allowed,
            "reason": "within_time_window" if allowed else "past_monday_thursday_cutoff",
            "cutoff_et": "14:00:00",
        }

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
    if "ÃƒÂ¢Ã‚â‚¬Ã‚â€œ" in day_token:
        day_token = day_token.split("ÃƒÂ¢Ã‚â‚¬Ã‚â€œ")[0]
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
        r"\s+\d{1,2}(?:\s*[-ÃƒÂ¢Ã‚â‚¬Ã‚â€œ]\s*\d{1,2})?(?:,\s*\d{4}|\s+\d{4})?",
        re.IGNORECASE,
    )
    out: List[datetime.date] = []
    seen = set()
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


def _candidate_liquidity_pass(candidate: Dict[str, Any]) -> bool:
    liquidity_ctx = _classify_liquidity(
        candidate.get("entry_slippage_vs_mid"),
        candidate.get("long_leg_width_pct_of_mid"),
        candidate.get("short_leg_width_pct_of_mid"),
    )
    return liquidity_ctx.get("liquidity_pass") is True



def _select_shortlist(all_candidates: List[Dict[str, Any]], allow_fallback: bool) -> Dict[str, Any]:
    preferred = [c for c in all_candidates if c["feasibility_pass"] and c["fits_risk_budget"]]
    fallback = [c for c in all_candidates if c["feasibility_pass"] and c["within_hard_max"]]

    preferred_liquid = [c for c in preferred if _candidate_liquidity_pass(c)]
    fallback_liquid = [c for c in fallback if _candidate_liquidity_pass(c)]

    if preferred_liquid:
        selected = preferred_liquid
        selection_mode = "preferred"
        reason = "Using candidates that pass feasibility, preferred risk band, hard max, and liquidity."
    elif allow_fallback and fallback_liquid:
        selected = fallback_liquid
        selection_mode = "fallback"
        reason = "No preferred liquid candidates found. Using feasible liquid candidates that still stay under hard max."
    else:
        selected = []
        selection_mode = "none"
        reason = "No feasible liquid candidates found for the current filters."

    return {
        "selection_mode": selection_mode,
        "reason": reason,
        "preferred_count": len(preferred_liquid),
        "fallback_count": len(fallback_liquid),
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


def _apply_engine_liquidity_gate(ticker_summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
    liquidity_ready: List[Dict[str, Any]] = []
    liquidity_failed_symbols: List[str] = []
    liquidity_unconfirmed_symbols: List[str] = []

    for summary in ticker_summaries:
        primary_candidate = summary.get("primary_candidate")
        liquidity_block = _build_liquidity_block(primary_candidate)

        summary["engine_liquidity_context"] = liquidity_block
        summary["engine_liquidity_pass"] = liquidity_block.get("liquidity_pass")

        if primary_candidate is None:
            continue

        if liquidity_block.get("liquidity_pass") is True:
            liquidity_ready.append(summary)
        elif liquidity_block.get("liquidity_pass") is False:
            liquidity_failed_symbols.append(summary.get("symbol"))
        else:
            liquidity_unconfirmed_symbols.append(summary.get("symbol"))

    if liquidity_ready:
        return {
            "ranked_summaries": _rank_ticker_summaries(liquidity_ready),
            "liquidity_gate_applied": True,
            "liquidity_gate_reason": "Liquidity-failed candidates were removed before engine best-ticker selection.",
            "liquidity_failed_symbols": liquidity_failed_symbols,
            "liquidity_unconfirmed_symbols": liquidity_unconfirmed_symbols,
        }

    return {
        "ranked_summaries": _rank_ticker_summaries(ticker_summaries),
        "liquidity_gate_applied": False,
        "liquidity_gate_reason": "No liquidity-passing candidates were available, so the engine fell back to the original ranked list.",
        "liquidity_failed_symbols": liquidity_failed_symbols,
        "liquidity_unconfirmed_symbols": liquidity_unconfirmed_symbols,
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

    engine_gate = _apply_engine_liquidity_gate(ticker_summaries)
    ranked = engine_gate["ranked_summaries"]
    best_summary = ranked[0] if ranked else None
    best_ticker = best_summary["symbol"] if best_summary and best_summary.get("primary_candidate") else None
    verdict = best_summary["verdict"] if best_summary else "NO_TRADE"

    return {
        "ok": True,
        "verdict": verdict,
        "best_ticker": best_ticker,
        "candidate_sort_reason": {
            **_candidate_sort_reason_from_best(best_summary),
            "liquidity_gate_applied": engine_gate["liquidity_gate_applied"],
            "liquidity_gate_reason": engine_gate["liquidity_gate_reason"],
            "liquidity_failed_symbols": engine_gate["liquidity_failed_symbols"],
            "liquidity_unconfirmed_symbols": engine_gate["liquidity_unconfirmed_symbols"],
        },
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


def _normalize_engine_verdict_for_session(
    verdict: Optional[str],
    has_primary_candidate: bool,
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
) -> str:
    current = verdict or "NO_TRADE"

    if not has_primary_candidate:
        return "NO_TRADE"

    if current != "ACTIVE_NOW":
        return current

    if not market_context.get("is_open"):
        return "PENDING"

    if not time_day_gate.get("fresh_entry_allowed"):
        return "PENDING"

    return current


def _normalize_engine_summary_for_session(
    summary_payload: Dict[str, Any],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
) -> Dict[str, Any]:
    normalized = {**summary_payload}
    original_summaries = summary_payload.get("ticker_summaries", [])
    normalized_summaries: List[Dict[str, Any]] = []

    for summary in original_summaries:
        updated = {**summary}
        updated["verdict"] = _normalize_engine_verdict_for_session(
            verdict=summary.get("verdict"),
            has_primary_candidate=bool(summary.get("primary_candidate")),
            market_context=market_context,
            time_day_gate=time_day_gate,
        )
        normalized_summaries.append(updated)

    normalized["ticker_summaries"] = normalized_summaries

    best_ticker = normalized.get("best_ticker")
    best_summary = next(
        (summary for summary in normalized_summaries if summary.get("symbol") == best_ticker),
        None,
    )

    if best_summary:
        normalized["verdict"] = best_summary.get("verdict", "NO_TRADE")
        normalized["reason"] = best_summary.get("reason", normalized.get("reason"))
        normalized["selection_mode"] = best_summary.get("selection_mode", normalized.get("selection_mode", "none"))
        normalized["primary_candidate"] = best_summary.get("primary_candidate")
        normalized["backup_candidate"] = best_summary.get("backup_candidate")
    else:
        normalized["verdict"] = _normalize_engine_verdict_for_session(
            verdict=summary_payload.get("verdict"),
            has_primary_candidate=bool(summary_payload.get("primary_candidate")),
            market_context=market_context,
            time_day_gate=time_day_gate,
        )

    return normalized


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
    if not candles or length <= 0:
        return None

    valid: List[Dict[str, float]] = []
    for candle in candles:
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        close = _to_float(candle.get("close"))
        if high is None or low is None or close is None:
            continue
        valid.append({"high": high, "low": low, "close": close})

    if len(valid) < 2:
        return None

    true_ranges: List[float] = []
    prev_close = valid[0]["close"]

    for candle in valid[1:]:
        high = candle["high"]
        low = candle["low"]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append(tr)
        prev_close = candle["close"]

    if not true_ranges:
        return None

    if len(true_ranges) < length:
        return round(sum(true_ranges) / len(true_ranges), 4)

    atr = sum(true_ranges[:length]) / length
    for tr in true_ranges[length:]:
        atr = ((atr * (length - 1)) + tr) / length

    return round(atr, 4)

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

def _calc_adx(candles: List[Dict[str, Any]], length: int = 14) -> Dict[str, Any]:
    """
    Wilder-style 1H ADX. Secondary only.
    Returns a compact block suitable for doctrine display without overriding
    primary SAFE-FAST blockers like room, trigger, wall placement, risk, or extension.
    """
    if not candles or length <= 0:
        return {
            "adx_value_1h": None,
            "plus_di_1h": None,
            "minus_di_1h": None,
            "adx_trend": "unconfirmed",
            "chop_risk_from_adx": None,
        }

    valid: List[Dict[str, float]] = []
    for candle in candles:
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        close = _to_float(candle.get("close"))
        if high is None or low is None or close is None:
            continue
        valid.append({"high": high, "low": low, "close": close})

    if len(valid) < (length * 2 + 2):
        return {
            "adx_value_1h": None,
            "plus_di_1h": None,
            "minus_di_1h": None,
            "adx_trend": "unconfirmed",
            "chop_risk_from_adx": None,
        }

    trs: List[float] = []
    plus_dm_values: List[float] = []
    minus_dm_values: List[float] = []

    for i in range(1, len(valid)):
        current = valid[i]
        prev = valid[i - 1]

        up_move = current["high"] - prev["high"]
        down_move = prev["low"] - current["low"]

        plus_dm = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm = down_move if (down_move > up_move and down_move > 0) else 0.0

        tr = max(
            current["high"] - current["low"],
            abs(current["high"] - prev["close"]),
            abs(current["low"] - prev["close"]),
        )

        trs.append(tr)
        plus_dm_values.append(plus_dm)
        minus_dm_values.append(minus_dm)

    if len(trs) < (length + 1):
        return {
            "adx_value_1h": None,
            "plus_di_1h": None,
            "minus_di_1h": None,
            "adx_trend": "unconfirmed",
            "chop_risk_from_adx": None,
        }

    tr14 = sum(trs[:length])
    plus_dm14 = sum(plus_dm_values[:length])
    minus_dm14 = sum(minus_dm_values[:length])

    dx_values: List[float] = []
    plus_di = minus_di = None

    for i in range(length, len(trs)):
        if i > length:
            tr14 = tr14 - (tr14 / length) + trs[i]
            plus_dm14 = plus_dm14 - (plus_dm14 / length) + plus_dm_values[i]
            minus_dm14 = minus_dm14 - (minus_dm14 / length) + minus_dm_values[i]

        if tr14 <= 0:
            plus_di = 0.0
            minus_di = 0.0
            dx = 0.0
        else:
            plus_di = 100.0 * (plus_dm14 / tr14)
            minus_di = 100.0 * (minus_dm14 / tr14)
            denom = plus_di + minus_di
            dx = 0.0 if denom <= 0 else 100.0 * abs(plus_di - minus_di) / denom

        dx_values.append(dx)

    if not dx_values:
        return {
            "adx_value_1h": None,
            "plus_di_1h": round(plus_di, 3) if plus_di is not None else None,
            "minus_di_1h": round(minus_di, 3) if minus_di is not None else None,
            "adx_trend": "unconfirmed",
            "chop_risk_from_adx": None,
        }

    if len(dx_values) < length:
        adx_series = [sum(dx_values) / len(dx_values)]
    else:
        first_adx = sum(dx_values[:length]) / length
        adx_series = [first_adx]
        for dx in dx_values[length:]:
            adx_series.append(((adx_series[-1] * (length - 1)) + dx) / length)

    adx_value = adx_series[-1] if adx_series else None
    adx_prev = adx_series[-2] if len(adx_series) >= 2 else None

    if adx_value is None:
        adx_trend = "unconfirmed"
    elif adx_prev is None:
        adx_trend = "flat"
    else:
        delta = adx_value - adx_prev
        if delta > 0.25:
            adx_trend = "rising"
        elif delta < -0.25:
            adx_trend = "falling"
        else:
            adx_trend = "flat"

    chop_risk_from_adx = None
    if adx_value is not None:
        chop_risk_from_adx = bool(adx_value < 18 or adx_trend in {"flat", "falling"})

    return {
        "adx_value_1h": round(adx_value, 3) if adx_value is not None else None,
        "plus_di_1h": round(plus_di, 3) if plus_di is not None else None,
        "minus_di_1h": round(minus_di, 3) if minus_di is not None else None,
        "adx_trend": adx_trend,
        "chop_risk_from_adx": chop_risk_from_adx,
    }



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
    pct_from_ema_ratio = abs(latest_close - ema50_1h) / ema50_1h if ema50_1h else None
    threshold = 0.008 if symbol == "GLD" else 0.006
    room_distance = abs(first_wall - latest_close) if first_wall is not None else None
    move_ratio = (abs(latest_close - ema50_1h) / room_distance) if room_distance not in (None, 0) else None
    pct_from_ema = round(pct_from_ema_ratio * 100, 3) if pct_from_ema_ratio is not None else None
    universal_caution_pct = 0.40
    extension_caution_0_40_pct = bool(pct_from_ema is not None and pct_from_ema >= universal_caution_pct)

    return {
        "state": "caution" if extension_caution_0_40_pct else "acceptable",
        "pct_from_ema": pct_from_ema,
        "move_to_wall_ratio": round(move_ratio, 3) if move_ratio is not None else None,
        "threshold_pct": round(threshold * 100, 3),
        "late_move": False,
        "universal_extension_caution_pct": universal_caution_pct,
        "extension_caution_0_40_pct": extension_caution_0_40_pct,
        "extension_caution_note": "0.40% from the 1H EMA is a caution only, not a hard blocker.",
        "baseline_extension_threshold_pct": round(threshold * 100, 3),
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

    trend_supportive = trend_ctx.get("supportive")
    trend_label = "Trend-aligned" if trend_supportive is True else "Countertrend" if trend_supportive is False else "unconfirmed"

    if latest_close is None or ema50_1h is None:
        return {"setup_type": "UNCONFIRMED", "trend_label": trend_label, "allowed_setup": None}

    near_ema = abs(latest_close - ema50_1h) / ema50_1h <= 0.0025
    chop = _is_chop(candles)
    recent_closes = [c["close"] for c in candles[-3:]] if len(candles) >= 3 else []
    tight_break = False
    if recent_closes and latest_close:
        tight_break = (max(recent_closes) - min(recent_closes)) / latest_close <= 0.003

    if room_pass is False or wall_pass is False or extension_state.get("state") == "extended":
        if trend_supportive is True and near_ema:
            return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": False}
        if trend_supportive is True and tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": False}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False}

    if trend_supportive is True:
        if near_ema and (room_ratio or 0) >= 2.5 and not chop:
            return {"setup_type": "Ideal", "trend_label": trend_label, "allowed_setup": True}
        if tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True}
        if near_ema:
            return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": True}
        return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": False}

    if trend_supportive is False:
        if tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False}

    return {"setup_type": "UNCONFIRMED", "trend_label": trend_label, "allowed_setup": None}



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
    base_extension_ctx = _extension_state(symbol, latest_close, ema50_1h, wall_levels.get("first_wall"))
    wall_ctx = _wall_thesis(
        option_type=option_type,
        primary_candidate=primary_candidate,
        first_wall=wall_levels.get("first_wall"),
        next_pocket=wall_levels.get("next_pocket"),
        invalidation_distance=invalidation_distance,
    )

    atr14 = _calc_atr(candles, 14)
    adx_ctx = _calc_adx(candles, 14)
    atr_multiple_from_ema = None
    if atr14 not in (None, 0) and invalidation_distance is not None:
        atr_multiple_from_ema = round(invalidation_distance / atr14, 3)

    recent = candles[-4:] if len(candles) >= 4 else candles
    parabolic_exhaustion = False
    if len(recent) >= 3:
        closes = [_to_float(c.get("close")) for c in recent[-3:]]
        highs = [_to_float(c.get("high")) for c in recent[-3:]]
        lows = [_to_float(c.get("low")) for c in recent[-3:]]
        if all(v is not None for v in closes + highs + lows):
            ranges = [max(h - l, 0.0001) for h, l in zip(highs, lows)]
            directional = closes[0] < closes[1] < closes[2] if option_type == "C" else closes[0] > closes[1] > closes[2]
            range_expanding = ranges[-1] > ranges[-2] > 0
            close_near_extreme = ((highs[-1] - closes[-1]) / ranges[-1] <= 0.15) if option_type == "C" else ((closes[-1] - lows[-1]) / ranges[-1] <= 0.15)
            parabolic_exhaustion = bool(directional and range_expanding and close_near_extreme)

    volume_climax_exhaustion = False
    volume_values = []
    for candle in candles[-8:]:
        vol = _to_float(candle.get("volume"))
        if vol is None:
            vol = _to_float(candle.get("vol"))
        if vol is not None and vol > 0:
            volume_values.append(vol)
    if len(volume_values) >= 6:
        baseline_vol = sum(volume_values[:-1]) / max(len(volume_values[:-1]), 1)
        volume_climax_exhaustion = bool(baseline_vol > 0 and volume_values[-1] >= baseline_vol * 1.8 and parabolic_exhaustion)

    degraded_entry_quality = bool(base_extension_ctx.get("move_to_wall_ratio") is not None and base_extension_ctx.get("move_to_wall_ratio") > 0.5)
    early_trigger_window_passed = bool(base_extension_ctx.get("pct_from_ema") is not None and base_extension_ctx.get("pct_from_ema") > base_extension_ctx.get("baseline_extension_threshold_pct", 999))
    cramped_room = room_pass is False

    extension_confirmer_flags = []
    if cramped_room:
        extension_confirmer_flags.append("cramped_room")
    if parabolic_exhaustion:
        extension_confirmer_flags.append("parabolic_exhaustion")
    if volume_climax_exhaustion:
        extension_confirmer_flags.append("volume_climax_exhaustion")
    if degraded_entry_quality:
        extension_confirmer_flags.append("degraded_entry_quality")
    if early_trigger_window_passed:
        extension_confirmer_flags.append("early_trigger_window_passed")

    extension_confirmer_count = len(extension_confirmer_flags)
    extension_material = bool(
        (atr_multiple_from_ema is not None and atr_multiple_from_ema >= 1.0)
        or early_trigger_window_passed
        or (base_extension_ctx.get("move_to_wall_ratio") is not None and base_extension_ctx.get("move_to_wall_ratio") > 0.5)
    )
    extension_blocks_now = bool(extension_material and extension_confirmer_count >= 1)

    extension_ctx = {
        **base_extension_ctx,
        "state": "extended" if extension_blocks_now else ("caution" if base_extension_ctx.get("extension_caution_0_40_pct") else "acceptable"),
        "late_move": extension_blocks_now,
        "atr_14_1h": atr14,
        "atr_multiple_from_ema": atr_multiple_from_ema,
        "parabolic_exhaustion": parabolic_exhaustion,
        "volume_climax_exhaustion": volume_climax_exhaustion,
        "degraded_entry_quality": degraded_entry_quality,
        "early_trigger_window_passed": early_trigger_window_passed,
        "extension_confirmer_flags": extension_confirmer_flags,
        "extension_confirmer_count": extension_confirmer_count,
        "extension_material": extension_material,
        "extension_soft_flag": bool(base_extension_ctx.get("extension_caution_0_40_pct")),
        "extension_blocks_now": extension_blocks_now,
    }

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
        "universal_extension_caution_pct": extension_ctx.get("universal_extension_caution_pct"),
        "extension_caution_0_40_pct": extension_ctx.get("extension_caution_0_40_pct"),
        "extension_caution_note": extension_ctx.get("extension_caution_note"),
        "atr_14_1h": extension_ctx.get("atr_14_1h"),
        "atr_multiple_from_ema": extension_ctx.get("atr_multiple_from_ema"),
        "parabolic_exhaustion": extension_ctx.get("parabolic_exhaustion"),
        "volume_climax_exhaustion": extension_ctx.get("volume_climax_exhaustion"),
        "degraded_entry_quality": extension_ctx.get("degraded_entry_quality"),
        "early_trigger_window_passed": extension_ctx.get("early_trigger_window_passed"),
        "extension_confirmer_flags": extension_ctx.get("extension_confirmer_flags"),
        "extension_confirmer_count": extension_ctx.get("extension_confirmer_count"),
        "extension_material": extension_ctx.get("extension_material"),
        "extension_soft_flag": extension_ctx.get("extension_soft_flag"),
        "extension_blocks_now": extension_ctx.get("extension_blocks_now"),
        "iv_state": "unconfirmed",
        "setup_type": setup_ctx.get("setup_type"),
        "trend_label": setup_ctx.get("trend_label"),
        "allowed_setup": setup_ctx.get("allowed_setup"),
        "chop_risk": _is_chop(candles),
        "adx_value_1h": adx_ctx.get("adx_value_1h"),
        "plus_di_1h": adx_ctx.get("plus_di_1h"),
        "minus_di_1h": adx_ctx.get("minus_di_1h"),
        "adx_trend": adx_ctx.get("adx_trend"),
        "chop_risk_from_adx": adx_ctx.get("chop_risk_from_adx"),
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

    if confirmed:
        message = "Chart confirmation available from this run."
    elif chart_check_error:
        message = "Candidate engine result only - chart confirmation still required. Chart check failed in this run."
    elif one_hour_confirmed and not structure_confirmed:
        message = "Candidate engine result only - structure confirmation still required."
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
    time_day_gate: Dict[str, Any],
    liquidity_context: Dict[str, Any],
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
        blocking_reasons: List[str] = []

        if structure_context.get("ok"):
            if structure_context.get("room_pass") is False:
                blocking_reasons.append("Room to first wall is too tight for SAFE-FAST.")
            if structure_context.get("extension_state") == "extended":
                blocking_reasons.append("Move is too extended from the 1H 50 EMA.")
            if structure_context.get("allowed_setup") is False:
                blocking_reasons.append(f"Setup type is {structure_context.get('setup_type')}, which is not tradable now.")
            if structure_context.get("wall_pass") is False:
                blocking_reasons.append("Wall thesis and strike placement do not match.")

        if blocking_reasons:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": blocking_reasons[0],
            }

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

    if not time_day_gate.get("fresh_entry_allowed"):
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "NO TRADE",
            "why": f"Time/day filter fails: {time_day_gate.get('reason')}.",
        }

    if liquidity_context.get("liquidity_pass") is False:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "NO TRADE",
            "why": liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry.",
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


def _build_trigger_state(
    option_type: str,
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    structure_context: Dict[str, Any],
    chart_check: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    trigger_style = "close_above_recent_high" if option_type == "C" else "close_below_recent_low"

    if not market_context.get("is_open"):
        return {
            "ok": True,
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": chart_check.get("latest_close") if chart_check else None,
            "why": "market_closed",
        }

    if not time_day_gate.get("fresh_entry_allowed"):
        return {
            "ok": True,
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": chart_check.get("latest_close") if chart_check else None,
            "why": time_day_gate.get("reason", "time_day_gate_blocked"),
        }

    if not chart_check or not chart_check.get("ok"):
        return {
            "ok": False,
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": None,
            "why": "chart_unavailable",
        }

    recent = chart_check.get("recent_candles") or []
    current_close = chart_check.get("latest_close")
    price_side = chart_check.get("price_vs_ema50_1h")

    if len(recent) < 2 or current_close is None:
        return {
            "ok": False,
            "trigger_present": False,
            "trigger_style": trigger_style,
            "trigger_level": None,
            "current_close": current_close,
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

    trigger_present = bool(crossed and side_ok and structure_ok)

    why = "trigger_present"
    if not structure_ok:
        why = "structure_not_ready"
    elif not side_ok:
        why = "wrong_side_of_ema"
    elif not crossed:
        why = "close_trigger_not_hit"

    return {
        "ok": True,
        "trigger_present": trigger_present,
        "trigger_style": trigger_style,
        "trigger_level": _round_or_none(trigger_level, 4),
        "current_close": _round_or_none(current_close, 4),
        "price_vs_ema50_1h": price_side,
        "structure_ready": structure_ok,
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
) -> Dict[str, Any]:
    ema_value = chart_check.get("ema50_1h") if chart_check else None
    price_side = chart_check.get("price_vs_ema50_1h") if chart_check else None

    items = [
        {"item": "allowed_setup_type", "yes": bool(structure_context.get("allowed_setup") is True)},
        {"item": "twentyfour_hour_supportive", "yes": bool(structure_context.get("twentyfour_hour_supportive") is True)},
        {"item": "one_hour_clean_around_ema", "yes": bool(price_side in {"above", "below"} and structure_context.get("chop_risk") is False)},
        {"item": "clear_room", "yes": bool(structure_context.get("room_pass") is True)},
        {"item": "early_enough", "yes": bool(time_day_gate.get("fresh_entry_allowed"))},
        {"item": "clear_trigger", "yes": bool(trigger_state.get("trigger_present") is True)},
        {"item": "liquidity_ok", "yes": bool(liquidity_context.get("liquidity_pass") is True)},
        {"item": "invalidation_clear", "yes": bool(ema_value is not None)},
        {"item": "fits_risk", "yes": bool(primary_candidate and primary_candidate.get("fits_risk_budget") is True)},
        {"item": "open_trade_already", "yes": bool(request.open_positions > 0)},
    ]

    failed_items = [row["item"] for row in items if not row["yes"] and row["item"] != "open_trade_already"]
    priority_order = [
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
    decision_blockers_priority = sorted(failed_items, key=lambda item: (priority_rank.get(item, 999), item))

    return {
        "ok": True,
        "items": items,
        "failed_items": failed_items,
        "decision_blockers_priority": decision_blockers_priority,
    }


def _failed_reason_messages(

    checklist: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    market_context: Dict[str, Any],
    structure_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
) -> List[str]:
    reasons: List[str] = []

    mapping = {
        "allowed_setup_type": "setup type is not allowed",
        "twentyfour_hour_supportive": "24H context is not supportive",
        "one_hour_clean_around_ema": "1H structure around the 50 EMA is not clean",
        "clear_room": "room to the first wall fails",
        "early_enough": "entry is outside the time/day window",
        "clear_trigger": "no valid live trigger is present",
        "liquidity_ok": "options liquidity is too wide for a clean debit spread entry",
        "invalidation_clear": "invalidation is not clear",
        "fits_risk": "risk does not fit the SAFE-FAST budget",
        "open_trade_already": "an open trade already exists",
    }

    for item in checklist.get("failed_items", []):
        msg = mapping.get(item)
        if msg:
            reasons.append(msg)

    if not market_context.get("is_open"):
        reasons.insert(0, "market is closed")
    elif time_day_gate.get("fresh_entry_allowed") is False and time_day_gate.get("reason") not in {"market_closed", None}:
        reasons.insert(0, "fresh entry is outside the SAFE-FAST time/day window")

    if structure_context.get("extension_state") == "extended":
        reasons.append("move is extended versus the 1H 50 EMA")
    if liquidity_context.get("liquidity_pass") is False and liquidity_context.get("why"):
        reasons.append(liquidity_context.get("why"))

    out: List[str] = []
    seen = set()
    for reason in reasons:
        if reason not in seen:
            seen.add(reason)
            out.append(reason)
    return out


def _screened_sort_key(item: Dict[str, Any]) -> Any:
    structure = item.get("structure_context", {})
    primary = item.get("primary_candidate") or {}
    liquidity = item.get("liquidity_context") or {}
    trigger_state = item.get("trigger_state") or {}
    checklist = item.get("checklist") or {}
    final_verdict = item.get("final_verdict", "NO_TRADE")

    verdict_rank = {"PENDING": 0, "NO_TRADE": 1}.get(final_verdict, 2)
    setup_rank = 0 if structure.get("allowed_setup") is True else 1 if structure.get("allowed_setup") is None else 2
    room_rank = 0 if structure.get("room_pass") is True else 1
    wall_rank = 0 if structure.get("wall_pass") is True else 1
    ext_rank = 0 if structure.get("extension_state") == "acceptable" else 1
    trend_rank = 0 if structure.get("trend_label") == "Trend-aligned" else 1 if structure.get("trend_label") == "Countertrend" else 2
    liquidity_rank = 0 if liquidity.get("liquidity_pass") is True else 1
    trigger_rank = 0 if trigger_state.get("trigger_present") is True else 1
    failed_count = len(checklist.get("failed_items", []))
    room_ratio = -(structure.get("room_ratio") or -999999)
    risk_mid = primary.get("distance_from_target_risk_mid", 999999)
    ticker_rank = SYMBOL_ORDER.index(item["symbol"]) if item.get("symbol") in SYMBOL_ORDER else 999999

    return (
        verdict_rank,
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
                "checklist_failed_items": item.get("checklist", {}).get("failed_items", []),
            }
        )
    return out


def _select_screened_best_candidate(screened_candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not screened_candidates:
        return None

    with_primary = [item for item in screened_candidates if item.get("primary_candidate")]
    if with_primary:
        return with_primary[0]

    return screened_candidates[0]


def _build_simple_output_block(
    user_facing: Dict[str, Any],
    trigger_state: Dict[str, Any],
) -> Dict[str, Any]:
    signal_present = bool(trigger_state.get("trigger_present") is True)
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

    selected_checklist = selected.get("checklist") or {}
    engine_pick_reason = engine_pick.get("reason") if engine_pick else None
    engine_pick_verdict = engine_pick.get("final_verdict") if engine_pick else None

    return {
        "ok": True,
        "screened_best_ticker": selected.get("symbol"),
        "engine_best_ticker": engine_best_ticker,
        "changed_from_engine_best": selected.get("symbol") != engine_best_ticker,
        "screened_final_verdict": selected.get("final_verdict"),
        "screened_reason": selected.get("reason"),
        "screened_checklist_failed_items": selected_checklist.get("failed_items", []),
        "engine_best_final_verdict_after_screen": engine_pick_verdict,
        "engine_best_reason_after_screen": engine_pick_reason,
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
    )

    reason = summary.get("reason", "No summary available.")
    failed_items = checklist.get("failed_items", [])
    if "liquidity_ok" in failed_items:
        reason = liquidity_context.get("why") or "Options liquidity is too wide for a clean debit spread entry."
    elif "clear_trigger" in failed_items:
        reason = trigger_state.get("why") or "No valid live trigger is present."
    elif structure_context.get("ok"):
        if structure_context.get("room_pass") is False:
            reason = "Room to first wall is too tight for SAFE-FAST."
        elif structure_context.get("wall_pass") is False:
            reason = "Wall thesis and strike placement do not match."
        elif structure_context.get("extension_state") == "extended":
            reason = "Move is too extended from the 1H 50 EMA."
        elif structure_context.get("allowed_setup") is False:
            reason = f"Setup type not allowed: {structure_context.get('setup_type')}"
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
        "checklist": checklist,
    }


def _build_candidate_context(
    best_ticker: Optional[str],
    option_type: str,
    selected_summary: Optional[Dict[str, Any]],
    primary_candidate: Optional[Dict[str, Any]],
    backup_candidate: Optional[Dict[str, Any]],
    chart_check: Optional[Dict[str, Any]],
    structure_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    checklist: Dict[str, Any],
    user_facing: Dict[str, Any],
    targets: Dict[str, Any],
    invalidation_level_1h_ema50: Optional[float],
    two_path: Dict[str, Any],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    macro_context: Dict[str, Any],
    iv_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    request: OnDemandRequest,
) -> Dict[str, Any]:
    active = bool(best_ticker and primary_candidate)

    options_block = None
    levels_block = None
    targets_block = None
    primary_entry_zone = None
    backup_entry_zone = None
    trigger_candle = None
    current_bar_behavior = None
    setup_route = None
    room_wall = None
    extension_quality = None
    execution_quality = None
    event_gate = None
    options_structure = None
    wall_thesis_fit = None
    adx_filter = None
    trigger_scan = None

    if active:
        entry_zones = _derive_entry_zones(
            option_type=option_type,
            chart_check=chart_check,
            structure_context=structure_context,
            trigger_state=trigger_state,
        )
        trigger_detail = _build_trigger_detail_context(
            option_type=option_type,
            chart_check=chart_check,
            trigger_state=trigger_state,
        )
        setup_route = _build_setup_route_context(
            option_type=option_type,
            structure_context=structure_context,
            trigger_state=trigger_state,
            chart_check=chart_check,
        )
        room_wall = _build_room_wall_context(structure_context)
        extension_quality = _build_extension_quality_context(structure_context)
        execution_quality = _build_execution_quality_context(
            market_context=market_context,
            time_day_gate=time_day_gate,
            macro_context=macro_context,
            iv_context=iv_context,
            liquidity_context=liquidity_context,
        )
        event_gate = _build_event_gate_context(
            macro_context=macro_context,
            market_context=market_context,
            time_day_gate=time_day_gate,
        )
        options_structure = _build_options_structure_context(
            request=request,
            selected_summary=selected_summary,
            primary_candidate=primary_candidate,
            liquidity_context=liquidity_context,
        )
        wall_thesis_fit = _build_wall_thesis_fit_context(
            option_type=option_type,
            structure_context=structure_context,
            primary_candidate=primary_candidate,
        )
        adx_filter = _build_adx_filter_context(structure_context)
        trigger_scan = _build_trigger_scan_context(
            option_type=option_type,
            chart_check=chart_check,
            trigger_state=trigger_state,
            market_context=market_context,
            time_day_gate=time_day_gate,
        )
        primary_entry_zone = entry_zones.get("primary_entry_zone")
        backup_entry_zone = entry_zones.get("backup_entry_zone")
        trigger_candle = trigger_detail.get("trigger_candle")
        current_bar_behavior = trigger_detail.get("current_bar_behavior")
        options_block = {
            "expiration_date": selected_summary.get("expiration_date") if selected_summary else None,
            "days_to_expiration": selected_summary.get("days_to_expiration") if selected_summary else None,
            "underlying_price": selected_summary.get("underlying_price") if selected_summary else None,
            "long_strike": primary_candidate.get("long_strike"),
            "short_strike": primary_candidate.get("short_strike"),
            "width": primary_candidate.get("width"),
            "est_debit": primary_candidate.get("est_debit"),
            "max_loss_dollars_1lot": primary_candidate.get("max_loss_dollars_1lot"),
            "max_profit_dollars_1lot": primary_candidate.get("max_profit_dollars_1lot"),
        }
        levels_block = {
            "latest_close": chart_check.get("latest_close") if chart_check else None,
            "ema50_1h": chart_check.get("ema50_1h") if chart_check else None,
            "price_vs_ema50_1h": chart_check.get("price_vs_ema50_1h") if chart_check else None,
            "first_wall": structure_context.get("first_wall"),
            "next_pocket": structure_context.get("next_pocket"),
            "room_to_first_wall": structure_context.get("room_to_first_wall"),
            "room_ratio": structure_context.get("room_ratio"),
            "wall_thesis": structure_context.get("wall_thesis"),
            "invalidation_1h_ema50": invalidation_level_1h_ema50,
        }
        targets_block = {
            "target_40_pct_value": targets.get("target_40_pct_value"),
            "target_50_pct_value": targets.get("target_50_pct_value"),
            "target_60_pct_value": targets.get("target_60_pct_value"),
            "target_70_pct_value": targets.get("target_70_pct_value"),
        }

    availability_reason = (
        (selected_summary or {}).get("reason")
        or structure_context.get("why")
        or trigger_state.get("why")
        or ("Candidate present." if active else "No feasible candidates found for the current filters.")
    )

    return {
        "active": active,
        "ticker": best_ticker,
        "availability_reason": availability_reason,
        "good_idea_now": user_facing.get("good_idea_now") if active else "NO",
        "action": user_facing.get("action") if active else "stand down",
        "setup_state": user_facing.get("setup_state") if active else "NO TRADE",
        "setup_type": structure_context.get("setup_type") if active else None,
        "trend_label": structure_context.get("trend_label") if active else None,
        "trigger_state": trigger_state.get("why") if active else None,
        "trigger_style": trigger_state.get("trigger_style") if active else None,
        "trigger_level": trigger_state.get("trigger_level") if active else None,
        "trigger_candle": trigger_candle if active else None,
        "current_bar_behavior": current_bar_behavior if active else None,
        "setup_route": setup_route if active else None,
        "room_wall": room_wall if active else None,
        "extension_quality": extension_quality if active else None,
        "execution_quality": execution_quality if active else None,
        "event_gate": event_gate if active else None,
        "options_structure": options_structure if active else None,
        "wall_thesis_fit": wall_thesis_fit if active else None,
        "adx_filter": adx_filter if active else None,
        "trigger_scan": trigger_scan if active else None,
        "primary_entry_zone": primary_entry_zone if active else None,
        "backup_entry_zone": backup_entry_zone if active else None,
        "options": options_block,
        "levels": levels_block,
        "targets": targets_block,
        "primary_candidate": primary_candidate if active else None,
        "backup_candidate": backup_candidate if active else None,
        "invalidation": invalidation_level_1h_ema50 if active else None,
        "checklist_failed_items": checklist.get("failed_items", []) if active else [],
        "decision_blockers_priority": checklist.get("decision_blockers_priority", []) if active else [],
        "execution": {
            "ideal_path": two_path.get("ideal_path"),
            "acceptable_path": two_path.get("acceptable_path"),
            "invalidation_1h_ema50": two_path.get("invalidation_1h_ema50"),
            "market_open": market_context.get("is_open"),
            "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
            "macro_risk_level": macro_context.get("risk_level"),
            "major_event_today": macro_context.get("has_major_event_today"),
            "major_event_tomorrow": macro_context.get("has_major_event_tomorrow"),
        },
        "note": (
            "Candidate context restored in a compact form. Use it as the structured handoff block for the current best ticker."
            if active else None
        ),
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
        return {
            "ideal_path": "Wait for next regular session. Re-check before entry.",
            "acceptable_path": "No entry while market is closed.",
            "invalidation_1h_ema50": ema,
        }

    if not time_day_gate.get("fresh_entry_allowed"):
        return {
            "ideal_path": "Wait for a valid SAFE-FAST entry window before considering a new trade.",
            "acceptable_path": "Stand down until the time/day gate reopens.",
            "invalidation_1h_ema50": ema,
        }

    failed_items = set(checklist.get("failed_items", []))
    if failed_items:
        labels = []
        label_map = {
            "allowed_setup_type": "allowed setup type",
            "twentyfour_hour_supportive": "24H support",
            "one_hour_clean_around_ema": "clean 1H structure",
            "clear_room": "room pass",
            "early_enough": "time window pass",
            "clear_trigger": "live trigger",
            "liquidity_ok": "liquidity pass",
            "invalidation_clear": "clear invalidation",
            "fits_risk": "risk fit",
        }
        order = [
            "allowed_setup_type",
            "twentyfour_hour_supportive",
            "one_hour_clean_around_ema",
            "clear_room",
            "early_enough",
            "clear_trigger",
            "liquidity_ok",
            "invalidation_clear",
            "fits_risk",
        ]
        for key in order:
            if key in failed_items:
                labels.append(label_map[key])

        return {
            "ideal_path": "Need " + ", ".join(labels) + " before entry." if labels else "Need full gate pass before entry.",
            "acceptable_path": "Stand down until all failed gates pass.",
            "invalidation_1h_ema50": ema,
        }

    caution_text = ""
    if structure_context.get("extension_caution_0_40_pct"):
        caution_text = " 0.40%+ from the 1H EMA is present as a caution, not a blocker."

    return {
        "ideal_path": "Setup passes. Enter only if current bar behavior still confirms the trigger." + caution_text,
        "acceptable_path": "Take only the mapped entry with the 1H EMA invalidation active.",
        "invalidation_1h_ema50": ema,
    }


def _build_python_validation(
    request: OnDemandRequest,
    best_ticker: Optional[str],
    primary_candidate: Optional[Dict[str, Any]],
    targets: Dict[str, Any],
    invalidation_level_1h_ema50: Optional[float],
) -> Dict[str, Any]:
    max_loss = _to_float((primary_candidate or {}).get("max_loss_dollars_1lot"))
    return {
        "ok": True,
        "ticker": best_ticker,
        "ticker_allowed": best_ticker in ALLOWED_SYMBOLS if best_ticker else False,
        "risk_preferred_band_ok": bool(max_loss is not None and request.risk_min_dollars <= max_loss <= request.risk_max_dollars),
        "risk_hard_max_ok": bool(max_loss is not None and max_loss <= request.hard_max_dollars),
        "open_positions_ok_for_new_trade": request.open_positions == 0,
        "max_one_open_position_rule": request.open_positions <= 1,
        "max_loss_dollars_1lot": max_loss,
        "targets_confirmed": bool(targets.get("ok")),
        "target_40_pct_value": targets.get("target_40_pct_value"),
        "target_50_pct_value": targets.get("target_50_pct_value"),
        "target_60_pct_value": targets.get("target_60_pct_value"),
        "target_70_pct_value": targets.get("target_70_pct_value"),
        "exit_price_1h_ema50": invalidation_level_1h_ema50,
    }


def _build_ten_second_checklist(
    request: OnDemandRequest,
    checklist_block: Dict[str, Any],
    structure_context: Dict[str, Any],
    iv_context: Dict[str, Any],
) -> Dict[str, Any]:
    item_map = {row.get("item"): bool(row.get("yes")) for row in checklist_block.get("items", [])}
    questions = [
        ("allowed_setup_type", "Is this one of the 3 allowed setup types?", item_map.get("allowed_setup_type")),
        ("twentyfour_hour_supportive", "Is 24H trend/context supportive?", item_map.get("twentyfour_hour_supportive")),
        ("one_hour_clean_around_ema", "Is 1H structure clean around 50 EMA?", item_map.get("one_hour_clean_around_ema")),
        ("clear_room", "Do we have clear room to next level?", item_map.get("clear_room")),
        ("early_enough", "Are we early enough, not overextended?", item_map.get("early_enough") and structure_context.get("extension_blocks_now") is not True),
        ("iv_acceptable", "Is IV acceptable for a debit spread?", None if iv_context.get("status") == "unconfirmed" else bool(iv_context.get("ok"))),
        ("clear_trigger", "Is there a clear entry trigger?", item_map.get("clear_trigger")),
        ("invalidation_clear", "Is invalidation clear: 1H close beyond 50 EMA?", item_map.get("invalidation_clear")),
        ("fits_risk", "Does this fit risk budget?", item_map.get("fits_risk")),
        ("open_trade_already", "Do we already have an open trade?", request.open_positions > 0),
    ]
    return {
        "ok": True,
        "answers": [
            {
                "item": item,
                "question": question,
                "answer": "YES" if value is True else "NO" if value is False else "UNCONFIRMED",
            }
            for item, question, value in questions
        ],
        "failed_items": checklist_block.get("failed_items", []),
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
    summary_payload = _normalize_engine_summary_for_session(
        summary_payload=summary_payload,
        market_context=market_context,
        time_day_gate=time_day_gate,
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
    selected = _select_screened_best_candidate(screened_candidates)

    best_ticker = selected.get("symbol") if selected and selected.get("primary_candidate") else summary_payload.get("best_ticker")
    engine_status = summary_payload.get("verdict", "NO_TRADE")
    final_verdict = selected.get("final_verdict", "NO_TRADE") if selected else "NO_TRADE"
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
    checklist_block = selected.get("checklist") if selected else _build_checklist_block(
        request=request,
        market_context=market_context,
        time_day_gate=time_day_gate,
        structure_context=structure_context,
        chart_check=chart_check,
        primary_candidate=primary_candidate,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
    )
    selected_reason = selected.get("reason", summary_payload.get("reason", "No summary available.")) if selected else summary_payload.get("reason", "No summary available.")

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
    )
    two_path_block = _build_two_path_block(
        market_context=market_context,
        time_day_gate=time_day_gate,
        structure_context=structure_context,
        checklist=checklist_block,
        chart_check=chart_check,
    )
    targets_block = _build_targets_block(primary_candidate)
    iv_context = _build_iv_context()
    python_validation_block = _build_python_validation(
        request=request,
        best_ticker=best_ticker,
        primary_candidate=primary_candidate,
        targets=targets_block,
        invalidation_level_1h_ema50=chart_check.get("ema50_1h") if chart_check else None,
    )
    ten_second_checklist_block = _build_ten_second_checklist(
        request=request,
        checklist_block=checklist_block,
        structure_context=structure_context,
        iv_context=iv_context,
    )

    raw_engine_winner_ticker = summary_payload.get("best_ticker")
    raw_engine_winner_status = summary_payload.get("verdict")
    screened_live_winner_ticker = best_ticker
    screened_live_winner_final_verdict = final_verdict
    changed_after_screening = raw_engine_winner_ticker != screened_live_winner_ticker
    why_changed_after_screening = (
        selected_reason if changed_after_screening else None
    )

    return {
        "ok": True,
        "mode": "on_demand",
        "build_tag": "schema_patch_soft_extension_and_audit_blocks_2026_04_06",
        "source_of_truth": "candidate_engine",
        "read_this_first": "simple_output",
        "engine_status": engine_status,
        "candidate_engine_status": engine_status,
        "final_verdict": final_verdict,
        "best_ticker": best_ticker,
        "engine_best_ticker": summary_payload.get("best_ticker"),
        "winner_context": {
            "raw_engine_winner_ticker": raw_engine_winner_ticker,
            "raw_engine_winner_status": raw_engine_winner_status,
            "screened_live_winner_ticker": screened_live_winner_ticker,
            "screened_live_winner_final_verdict": screened_live_winner_final_verdict,
            "changed_after_screening": changed_after_screening,
            "why_changed_after_screening": why_changed_after_screening,
        },
        "live_map": _build_live_map_block(
            ticker=best_ticker,
            option_type=clean_option_type,
            primary_entry_zone=_derive_entry_zones(
                option_type=clean_option_type,
                chart_check=chart_check,
                structure_context=structure_context,
                trigger_state=trigger_state,
            ).get("primary_entry_zone") if best_ticker and primary_candidate else None,
            backup_entry_zone=_derive_entry_zones(
                option_type=clean_option_type,
                chart_check=chart_check,
                structure_context=structure_context,
                trigger_state=trigger_state,
            ).get("backup_entry_zone") if best_ticker and primary_candidate else None,
            trigger_state=trigger_state,
            chart_check=chart_check,
            structure_context=structure_context,
            invalidation_level_1h_ema50=chart_check.get("ema50_1h") if chart_check else None,
            market_context=market_context,
            time_day_gate=time_day_gate,
            macro_context=macro_context,
            iv_context=iv_context,
            liquidity_context=liquidity_context,
            selected_summary=selected.get("summary") if selected else None,
            primary_candidate=primary_candidate,
            request=request,
        ),
        "simple_output": _build_simple_output_block(
            user_facing=user_facing_block,
            trigger_state=trigger_state,
        ),
        "screened_best_context": _build_screened_best_context(
            selected=selected,
            engine_best_ticker=summary_payload.get("best_ticker"),
            screened_candidates=screened_candidates,
        ),
        "market_context": market_context,
        "macro_context": macro_context,
        "structure_context": structure_context,
        "adx_context": _build_adx_filter_context(structure_context),
        "time_day_gate": time_day_gate,
        "iv_context": iv_context,
        "python_validation": python_validation_block,
        "ten_second_checklist": ten_second_checklist_block,
        "liquidity_context": liquidity_context,
        "trigger_state": trigger_state,
        "targets": targets_block,
        "invalidation_level_1h_ema50": chart_check.get("ema50_1h") if chart_check else None,
        "checklist": checklist_block,
        "failed_reasons": _failed_reason_messages(
            checklist=checklist_block,
            time_day_gate=time_day_gate,
            market_context=market_context,
            structure_context=structure_context,
            liquidity_context=liquidity_context,
            trigger_state=trigger_state,
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
        "user_facing": user_facing_block,
        "candidate_context": _build_candidate_context(
            best_ticker=best_ticker,
            option_type=clean_option_type,
            selected_summary=selected.get("summary") if selected else None,
            primary_candidate=primary_candidate,
            backup_candidate=selected.get("backup_candidate") if selected else summary_payload.get("backup_candidate"),
            chart_check=chart_check,
            structure_context=structure_context,
            trigger_state=trigger_state,
            checklist=checklist_block,
            user_facing=user_facing_block,
            targets=targets_block,
            invalidation_level_1h_ema50=chart_check.get("ema50_1h") if chart_check else None,
            two_path=two_path_block,
            market_context=market_context,
            time_day_gate=time_day_gate,
            macro_context=macro_context,
            iv_context=iv_context,
            liquidity_context=liquidity_context,
            request=request,
        ),
        "two_path": two_path_block,
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
