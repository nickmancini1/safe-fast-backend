# fresh full main.py build with entry_context bridge 2026-04-09T16:05:00Z

import asyncio

import copy
import hashlib
import json
import math

import os
import re
from datetime import datetime, time, timedelta
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from dxlink_candles import get_1h_ema50_snapshot

app = FastAPI(title="SAFE-FAST Backend", version="1.8.5")

API_BASE = "https://api.tastyworks.com"
USER_AGENT = "safe-fast-backend/1.8.5"

TT_CLIENT_ID = os.getenv("TT_CLIENT_ID", "")
TT_CLIENT_SECRET = os.getenv("TT_CLIENT_SECRET", "")
TT_REDIRECT_URI = os.getenv("TT_REDIRECT_URI", "")
TT_REFRESH_TOKEN = os.getenv("TT_REFRESH_TOKEN", "")

ALLOWED_SYMBOLS = {"SPY", "QQQ", "IWM", "GLD"}
SYMBOL_ORDER = ["SPY", "QQQ", "IWM", "GLD"]

NY_TZ = ZoneInfo("America/New_York")
ALLOWED_SETUP_TYPES = {"Ideal", "Clean Fast Break", "Continuation"}


def _is_allowed_setup_type_name(setup_type: Optional[str]) -> bool:
    return isinstance(setup_type, str) and setup_type in ALLOWED_SETUP_TYPES


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


def _decorate_why(why_text: Optional[str], market_closed_context: bool = False) -> str:
    text_value = str(why_text or "unconfirmed").strip()
    if market_closed_context:
        if text_value == "market_closed":
            return "Market is closed right now, so no live entry can be taken."
        closed_suffix = "Market is closed right now, so no live entry can be taken."
        if text_value.endswith(closed_suffix):
            return text_value
        return f"{text_value} {closed_suffix}"
    return text_value


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
    setup_eligible_now = structure_context.get("setup_eligible_now")
    chop_risk = bool(structure_context.get("chop_risk"))
    extension_state = structure_context.get("extension_state")
    room_pass = structure_context.get("room_pass")
    wall_pass = structure_context.get("wall_pass")
    trigger_present = bool(trigger_state.get("trigger_present"))
    structure_ready = bool(trigger_state.get("structure_ready"))
    price_side = chart_check.get("price_vs_ema50_1h") if chart_check else None
    allowed_setup_types = ALLOWED_SETUP_TYPES
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
        and setup_eligible_now is True
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
        and setup_eligible_now is True
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
    trap_check_context = _build_trap_check_context(structure_context)
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
        "trap_check_context": trap_check_context,
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
    if not market_context.get("is_open"):
        return {
            "fresh_entry_allowed": False,
            "reason": "market_closed",
            "cutoff_et": None,
        }

    return {
        "fresh_entry_allowed": True,
        "reason": "within_time_window",
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
    Wilder-style 1H ADX with a softer minimum-history requirement.

    Goal:
    - keep SAFE-FAST response shape unchanged
    - avoid null ADX when there is enough recent 1H history to derive a usable read
    - preserve ADX as a secondary filter only
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

    minimum_bars = max(length + 1, 6)
    if len(valid) < minimum_bars:
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

    if len(trs) < 2:
        return {
            "adx_value_1h": None,
            "plus_di_1h": None,
            "minus_di_1h": None,
            "adx_trend": "unconfirmed",
            "chop_risk_from_adx": None,
        }

    period = min(length, len(trs))
    tr_n = sum(trs[:period])
    plus_dm_n = sum(plus_dm_values[:period])
    minus_dm_n = sum(minus_dm_values[:period])

    def _di_and_dx(smoothed_tr: float, smoothed_plus_dm: float, smoothed_minus_dm: float) -> Dict[str, float]:
        if smoothed_tr <= 0:
            plus_di_local = 0.0
            minus_di_local = 0.0
            dx_local = 0.0
        else:
            plus_di_local = 100.0 * (smoothed_plus_dm / smoothed_tr)
            minus_di_local = 100.0 * (smoothed_minus_dm / smoothed_tr)
            denom = plus_di_local + minus_di_local
            dx_local = 0.0 if denom <= 0 else 100.0 * abs(plus_di_local - minus_di_local) / denom
        return {
            "plus_di": plus_di_local,
            "minus_di": minus_di_local,
            "dx": dx_local,
        }

    first_values = _di_and_dx(tr_n, plus_dm_n, minus_dm_n)
    dx_values: List[float] = [first_values["dx"]]
    plus_di = first_values["plus_di"]
    minus_di = first_values["minus_di"]

    for i in range(period, len(trs)):
        tr_n = tr_n - (tr_n / period) + trs[i]
        plus_dm_n = plus_dm_n - (plus_dm_n / period) + plus_dm_values[i]
        minus_dm_n = minus_dm_n - (minus_dm_n / period) + minus_dm_values[i]

        values = _di_and_dx(tr_n, plus_dm_n, minus_dm_n)
        plus_di = values["plus_di"]
        minus_di = values["minus_di"]
        dx_values.append(values["dx"])

    if not dx_values:
        return {
            "adx_value_1h": None,
            "plus_di_1h": round(plus_di, 3) if plus_di is not None else None,
            "minus_di_1h": round(minus_di, 3) if minus_di is not None else None,
            "adx_trend": "unconfirmed",
            "chop_risk_from_adx": None,
        }

    adx_seed_period = min(period, len(dx_values))
    first_adx = sum(dx_values[:adx_seed_period]) / adx_seed_period
    adx_series: List[float] = [first_adx]
    for dx in dx_values[adx_seed_period:]:
        adx_series.append(((adx_series[-1] * (period - 1)) + dx) / period)

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




def _recent_trading_day_candles(
    candles: List[Dict[str, Any]],
    max_days: int = 5,
) -> List[Dict[str, Any]]:
    if not candles or max_days <= 0:
        return []

    day_keys: List[str] = []
    for candle in reversed(candles):
        time_iso = candle.get("time_iso")
        if not time_iso:
            continue
        try:
            day_key = datetime.fromisoformat(time_iso).astimezone(NY_TZ).date().isoformat()
        except Exception:
            continue
        if day_key not in day_keys:
            day_keys.append(day_key)
        if len(day_keys) >= max_days:
            break

    if not day_keys:
        return candles[-35:] if len(candles) >= 35 else candles

    allowed_days = set(day_keys)
    out: List[Dict[str, Any]] = []
    for candle in candles:
        time_iso = candle.get("time_iso")
        if not time_iso:
            continue
        try:
            day_key = datetime.fromisoformat(time_iso).astimezone(NY_TZ).date().isoformat()
        except Exception:
            continue
        if day_key in allowed_days:
            out.append(candle)
    return out


def _find_hidden_left_wick_cluster(
    candles: List[Dict[str, Any]],
    latest_close: Optional[float],
    option_type: str,
) -> Dict[str, Any]:
    if not candles or latest_close is None:
        return {
            "lookback_days": 5,
            "cluster_found": None,
            "side": "resistance" if option_type == "C" else "support",
            "zone": None,
            "nearest_level": None,
            "distance_from_price": None,
            "wick_count": 0,
            "candidate_levels": [],
            "why": "insufficient_candle_data",
        }

    recent = _recent_trading_day_candles(candles, max_days=5)
    if not recent:
        return {
            "lookback_days": 5,
            "cluster_found": None,
            "side": "resistance" if option_type == "C" else "support",
            "zone": None,
            "nearest_level": None,
            "distance_from_price": None,
            "wick_count": 0,
            "candidate_levels": [],
            "why": "recent_trading_window_unavailable",
        }

    band = max(latest_close * 0.0025, 0.10)
    side = "resistance" if option_type == "C" else "support"
    candidate_levels: List[float] = []

    for candle in recent:
        open_value = _to_float(candle.get("open"))
        high_value = _to_float(candle.get("high"))
        low_value = _to_float(candle.get("low"))
        close_value = _to_float(candle.get("close"))
        if None in {open_value, high_value, low_value, close_value}:
            continue

        candle_range = max(high_value - low_value, 0.0001)
        body = abs(close_value - open_value)
        upper_wick = high_value - max(open_value, close_value)
        lower_wick = min(open_value, close_value) - low_value

        if option_type == "C":
            qualifies = (
                high_value > latest_close
                and upper_wick >= max(body, candle_range * 0.30)
            )
            if qualifies:
                candidate_levels.append(round(high_value, 4))
        else:
            qualifies = (
                low_value < latest_close
                and lower_wick >= max(body, candle_range * 0.30)
            )
            if qualifies:
                candidate_levels.append(round(low_value, 4))

    if len(candidate_levels) < 2:
        return {
            "lookback_days": 5,
            "cluster_found": False,
            "side": side,
            "zone": None,
            "nearest_level": None,
            "distance_from_price": None,
            "wick_count": len(candidate_levels),
            "candidate_levels": sorted(candidate_levels),
            "why": "no_hidden_cluster_detected",
        }

    ordered = sorted(candidate_levels) if option_type == "C" else sorted(candidate_levels, reverse=True)
    current_cluster: List[float] = []
    winning_cluster: Optional[List[float]] = None

    for level in ordered:
        if not current_cluster:
            current_cluster = [level]
            continue
        if abs(level - current_cluster[-1]) <= band:
            current_cluster.append(level)
            continue
        if len(current_cluster) >= 2:
            winning_cluster = current_cluster
            break
        current_cluster = [level]

    if winning_cluster is None and len(current_cluster) >= 2:
        winning_cluster = current_cluster

    if not winning_cluster:
        return {
            "lookback_days": 5,
            "cluster_found": False,
            "side": side,
            "zone": None,
            "nearest_level": None,
            "distance_from_price": None,
            "wick_count": len(candidate_levels),
            "candidate_levels": ordered,
            "why": "no_hidden_cluster_detected",
        }

    zone_low = round(min(winning_cluster), 4)
    zone_high = round(max(winning_cluster), 4)
    nearest_level = zone_low if option_type == "C" else zone_high
    distance_from_price = round(abs(nearest_level - latest_close), 4)

    return {
        "lookback_days": 5,
        "cluster_found": True,
        "side": side,
        "zone": {
            "low": zone_low,
            "high": zone_high,
            "band_width": round(zone_high - zone_low, 4),
        },
        "nearest_level": nearest_level,
        "distance_from_price": distance_from_price,
        "wick_count": len(winning_cluster),
        "candidate_levels": ordered,
        "why": "hidden_left_wick_cluster_detected",
    }


def _compute_noisy_chop_detail(
    candles: List[Dict[str, Any]],
    ema50_1h: Optional[float],
) -> Dict[str, Any]:
    if not candles:
        return {
            "noisy_chop": None,
            "overlap_rule_triggered": None,
            "overlap_hits_last4": 0,
            "ema_whipsaw_chop": None,
            "ema_cross_back_count": 0,
            "why": "no_candles_available",
        }

    recent = candles[-4:] if len(candles) >= 4 else candles
    overlap_hits = 0
    for index in range(1, len(recent)):
        current = recent[index]
        previous = recent[index - 1]
        current_high = _to_float(current.get("high"))
        current_low = _to_float(current.get("low"))
        previous_high = _to_float(previous.get("high"))
        previous_low = _to_float(previous.get("low"))
        if None in {current_high, current_low, previous_high, previous_low}:
            continue
        overlap = max(0.0, min(current_high, previous_high) - max(current_low, previous_low))
        current_range = max(current_high - current_low, 0.0001)
        if (overlap / current_range) > 0.5:
            overlap_hits += 1

    overlap_rule_triggered = overlap_hits >= 3

    ema_cross_back_count = 0
    ema_whipsaw_chop = None
    if ema50_1h is not None:
        sides: List[int] = []
        for candle in recent:
            close_value = _to_float(candle.get("close"))
            if close_value is None:
                continue
            if close_value > ema50_1h:
                sides.append(1)
            elif close_value < ema50_1h:
                sides.append(-1)
            else:
                sides.append(0)

        ema_cross_back_count = 0
        for previous_side, current_side in zip(sides, sides[1:]):
            if previous_side == 0 or current_side == 0:
                continue
            if previous_side != current_side:
                ema_cross_back_count += 1

        ema_whipsaw_chop = ema_cross_back_count >= 2

    noisy_chop = bool(overlap_rule_triggered or ema_whipsaw_chop is True)

    return {
        "noisy_chop": noisy_chop,
        "overlap_rule_triggered": overlap_rule_triggered,
        "overlap_hits_last4": overlap_hits,
        "ema_whipsaw_chop": ema_whipsaw_chop,
        "ema_cross_back_count": ema_cross_back_count,
        "why": (
            "overlap_and_ema_whipsaw"
            if overlap_rule_triggered and ema_whipsaw_chop
            else "overlap_rule"
            if overlap_rule_triggered
            else "ema_whipsaw"
            if ema_whipsaw_chop
            else "no_explicit_noisy_chop_detected"
        ),
    }


def _build_trap_check_context(structure_context: Dict[str, Any]) -> Dict[str, Any]:
    if not structure_context.get("ok"):
        return {
            "trap_check_status": "unconfirmed",
            "primary_trap": None,
            "blockers": [],
            "cautions": [],
            "checks": {
                "hidden_left_structure": {"status": "unconfirmed", "why": "structure_context_unavailable"},
                "overextension_vs_ema": {"status": "unconfirmed", "why": "structure_context_unavailable"},
                "volume_climax_exhaustion": {"status": "unconfirmed", "why": "structure_context_unavailable"},
                "noisy_chop": {"status": "unconfirmed", "why": "structure_context_unavailable"},
                "parabolic_exhaustion": {"status": "unconfirmed", "why": "structure_context_unavailable"},
            },
            "why_trap_check_passes_or_fails": "Trap check is unconfirmed because structure context is unavailable.",
        }

    hidden_left = structure_context.get("hidden_left_wick_cluster") or {}
    hidden_left_ratio = _to_float(structure_context.get("hidden_left_distance_to_invalidation_ratio"))
    hidden_left_confirms_room_trap = bool(structure_context.get("hidden_left_cluster_confirms_room_trap") is True)

    if hidden_left.get("cluster_found") is True:
        if hidden_left_confirms_room_trap or (hidden_left_ratio is not None and hidden_left_ratio < 2.0):
            hidden_left_status = "fail"
            hidden_left_why = "Hidden left-side wick cluster sits too close relative to invalidation distance."
        else:
            hidden_left_status = "caution"
            hidden_left_why = "Hidden left-side wick cluster exists, but does not yet confirm a hard room trap."
    elif hidden_left.get("cluster_found") is False:
        hidden_left_status = "pass"
        hidden_left_why = "No hidden left-side wick cluster was detected in the recent 5-day window."
    else:
        hidden_left_status = "unconfirmed"
        hidden_left_why = hidden_left.get("why") or "Hidden left-side structure is unconfirmed."

    extension_blocks_now = bool(structure_context.get("extension_blocks_now") is True)
    extension_soft_flag = bool(structure_context.get("extension_soft_flag") is True)
    extension_state = structure_context.get("extension_state")

    if extension_blocks_now or extension_state == "extended":
        overextension_status = "fail"
        overextension_why = "Extension is currently blocking the setup."
    elif extension_soft_flag or extension_state == "caution":
        overextension_status = "caution"
        overextension_why = "Extension is elevated, but only as a caution right now."
    elif extension_state in {"acceptable", "pass"}:
        overextension_status = "pass"
        overextension_why = "Extension is not currently a trap."
    else:
        overextension_status = "unconfirmed"
        overextension_why = "Extension status is unconfirmed."

    if structure_context.get("volume_climax_exhaustion") is True:
        volume_status = "fail"
        volume_why = "Volume climax / exhaustion is present."
    elif structure_context.get("volume_climax_exhaustion") is False:
        volume_status = "pass"
        volume_why = "No volume climax / exhaustion was detected."
    else:
        volume_status = "unconfirmed"
        volume_why = "Volume climax / exhaustion is unconfirmed."

    if structure_context.get("noisy_chop_explicit") is True:
        noisy_chop_status = "fail"
        noisy_chop_why = "Explicit noisy chop is present."
    elif structure_context.get("noisy_chop_explicit") is False:
        noisy_chop_status = "pass"
        noisy_chop_why = "No explicit noisy chop was detected."
    else:
        noisy_chop_status = "unconfirmed"
        noisy_chop_why = "Noisy chop is unconfirmed."

    if structure_context.get("parabolic_exhaustion") is True:
        parabolic_status = "fail"
        parabolic_why = "Parabolic / exhausted move behavior is present."
    elif structure_context.get("parabolic_exhaustion") is False:
        parabolic_status = "pass"
        parabolic_why = "No parabolic / exhausted move behavior was detected."
    else:
        parabolic_status = "unconfirmed"
        parabolic_why = "Parabolic / exhausted move behavior is unconfirmed."

    checks = {
        "hidden_left_structure": {
            "status": hidden_left_status,
            "why": hidden_left_why,
            "side": hidden_left.get("side"),
            "zone": hidden_left.get("zone"),
            "nearest_level": hidden_left.get("nearest_level"),
            "wick_count": hidden_left.get("wick_count"),
            "distance_from_price": hidden_left.get("distance_from_price"),
            "distance_to_invalidation_ratio": structure_context.get("hidden_left_distance_to_invalidation_ratio"),
            "confirms_room_trap": structure_context.get("hidden_left_cluster_confirms_room_trap"),
        },
        "overextension_vs_ema": {
            "status": overextension_status,
            "why": overextension_why,
            "extension_state": structure_context.get("extension_state"),
            "pct_from_ema": structure_context.get("pct_from_ema"),
            "atr_multiple_from_ema": structure_context.get("atr_multiple_from_ema"),
            "extension_soft_flag": structure_context.get("extension_soft_flag"),
            "extension_blocks_now": structure_context.get("extension_blocks_now"),
        },
        "volume_climax_exhaustion": {
            "status": volume_status,
            "why": volume_why,
            "volume_climax_exhaustion": structure_context.get("volume_climax_exhaustion"),
        },
        "noisy_chop": {
            "status": noisy_chop_status,
            "why": noisy_chop_why,
            "noisy_chop_explicit": structure_context.get("noisy_chop_explicit"),
            "overlap_hits_last4": structure_context.get("overlap_chop_hits_last4"),
            "ema_whipsaw_chop": structure_context.get("ema_whipsaw_chop"),
            "candle_overlap_chop_risk": structure_context.get("candle_overlap_chop_risk"),
            "chop_risk": structure_context.get("chop_risk"),
        },
        "parabolic_exhaustion": {
            "status": parabolic_status,
            "why": parabolic_why,
            "parabolic_exhaustion": structure_context.get("parabolic_exhaustion"),
        },
    }

    blockers = [name for name, block in checks.items() if block.get("status") == "fail"]
    cautions = [name for name, block in checks.items() if block.get("status") == "caution"]

    if blockers:
        trap_check_status = "fail"
        primary_trap = blockers[0]
        why = "One or more explicit trap checks are failing."
    elif cautions:
        trap_check_status = "caution"
        primary_trap = cautions[0]
        why = "Trap check is not failing, but one or more caution traps are active."
    elif all(block.get("status") == "pass" for block in checks.values()):
        trap_check_status = "pass"
        primary_trap = None
        why = "Explicit trap checks currently pass."
    else:
        trap_check_status = "unconfirmed"
        primary_trap = None
        why = "Trap check remains partly unconfirmed from the available chart inputs."

    return {
        "trap_check_status": trap_check_status,
        "primary_trap": primary_trap,
        "blockers": blockers,
        "cautions": cautions,
        "checks": checks,
        "why_trap_check_passes_or_fails": why,
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
        return {"setup_type": "UNCONFIRMED", "trend_label": trend_label, "allowed_setup": None, "setup_type_allowed": None, "setup_eligible_now": None}

    near_ema = abs(latest_close - ema50_1h) / ema50_1h <= 0.0025
    chop = _is_chop(candles)
    recent_closes = [c["close"] for c in candles[-3:]] if len(candles) >= 3 else []
    tight_break = False
    if recent_closes and latest_close:
        tight_break = (max(recent_closes) - min(recent_closes)) / latest_close <= 0.003

    if room_pass is False or wall_pass is False or extension_state.get("state") == "extended":
        if trend_supportive is True and near_ema:
            return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": False}
        if trend_supportive is True and tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": False}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False, "setup_type_allowed": False, "setup_eligible_now": False}

    if trend_supportive is True:
        if near_ema and (room_ratio or 0) >= 2.5 and not chop:
            return {"setup_type": "Ideal", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": True}
        if tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": True}
        if near_ema:
            return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": True}
        return {"setup_type": "Continuation", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": False}

    if trend_supportive is False:
        if tight_break and not chop:
            return {"setup_type": "Clean Fast Break", "trend_label": trend_label, "allowed_setup": True, "setup_type_allowed": True, "setup_eligible_now": True}
        return {"setup_type": "NOT_ALLOWED", "trend_label": trend_label, "allowed_setup": False, "setup_type_allowed": False, "setup_eligible_now": False}

    return {"setup_type": "UNCONFIRMED", "trend_label": trend_label, "allowed_setup": None, "setup_type_allowed": None, "setup_eligible_now": None}



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

    candles = chart_check.get("_all_candles") or chart_check.get("recent_candles") or []
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

    hidden_left_wick_cluster = _find_hidden_left_wick_cluster(
        candles=candles,
        latest_close=latest_close,
        option_type=option_type,
    )
    hidden_left_distance_to_invalidation_ratio = None
    if (
        hidden_left_wick_cluster.get("distance_from_price") not in (None, 0)
        and invalidation_distance not in (None, 0)
    ):
        hidden_left_distance_to_invalidation_ratio = round(
            hidden_left_wick_cluster.get("distance_from_price") / invalidation_distance,
            3,
        )
    hidden_left_cluster_confirms_room_trap = bool(
        hidden_left_wick_cluster.get("cluster_found") is True
        and hidden_left_distance_to_invalidation_ratio is not None
        and hidden_left_distance_to_invalidation_ratio < 2.0
    )

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
    noisy_chop_detail = _compute_noisy_chop_detail(candles, ema50_1h)
    candle_overlap_chop_risk = bool(noisy_chop_detail.get("overlap_rule_triggered") is True)
    adx_chop_risk = adx_ctx.get("chop_risk_from_adx")
    effective_chop_risk = bool(candle_overlap_chop_risk) if adx_chop_risk is None else bool(candle_overlap_chop_risk and adx_chop_risk)
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
        "hidden_left_wick_cluster": hidden_left_wick_cluster,
        "hidden_left_cluster_found": hidden_left_wick_cluster.get("cluster_found"),
        "hidden_left_level_zone": hidden_left_wick_cluster.get("zone"),
        "hidden_left_distance_from_price": hidden_left_wick_cluster.get("distance_from_price"),
        "hidden_left_distance_to_invalidation_ratio": hidden_left_distance_to_invalidation_ratio,
        "hidden_left_cluster_confirms_room_trap": hidden_left_cluster_confirms_room_trap,
        "noisy_chop_detail": noisy_chop_detail,
        "noisy_chop_explicit": noisy_chop_detail.get("noisy_chop"),
        "ema_whipsaw_chop": noisy_chop_detail.get("ema_whipsaw_chop"),
        "overlap_chop_hits_last4": noisy_chop_detail.get("overlap_hits_last4"),
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
        "setup_type_allowed": setup_ctx.get("setup_type_allowed", setup_ctx.get("allowed_setup")),
        "setup_eligible_now": setup_ctx.get("setup_eligible_now", setup_ctx.get("allowed_setup")),
        "chop_risk": effective_chop_risk,
        "candle_overlap_chop_risk": candle_overlap_chop_risk,
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
        if structure_context.get("setup_eligible_now") is False:
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


def _compact_chart_check_summary(chart_check: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not chart_check:
        return {
            "ok": False,
            "why": "chart_check_unavailable",
        }

    if not chart_check.get("ok"):
        summary = {
            "ok": False,
            "symbol": chart_check.get("symbol"),
        }
        for key in ("why", "error", "message", "status"):
            if chart_check.get(key) is not None:
                summary[key] = chart_check.get(key)
        if len(summary) == 2:
            summary["why"] = "chart_check_unavailable"
        return summary

    return {
        "ok": True,
        "symbol": chart_check.get("symbol"),
        "latest_close": chart_check.get("latest_close"),
        "ema50_1h": chart_check.get("ema50_1h"),
        "price_vs_ema50_1h": chart_check.get("price_vs_ema50_1h"),
        "latest_candle_time": chart_check.get("latest_candle_time"),
        "candle_count": chart_check.get("candle_count"),
    }


def _build_chart_confirmation_entry(
    request: OnDemandRequest,
    symbol: Optional[str],
    chart_check: Optional[Dict[str, Any]],
    chart_check_error: Optional[str],
    structure_context: Dict[str, Any],
) -> Dict[str, Any]:
    chart_confirmation = _build_chart_confirmation_block(
        request=request,
        chart_check=chart_check,
        chart_check_error=chart_check_error,
        structure_context=structure_context,
    )
    return {
        "ticker": symbol,
        "confirmed": chart_confirmation.get("confirmed"),
        "message": chart_confirmation.get("message"),
        "fields": chart_confirmation.get("fields"),
        "chart_check": _compact_chart_check_summary(chart_check),
        "trap_check_context": _build_trap_check_context(structure_context),
    }


def _build_universe_chart_confirmation_block(
    request: OnDemandRequest,
    screened_candidates: List[Dict[str, Any]],
    include_chart_checks: bool,
) -> Dict[str, Any]:
    if not include_chart_checks:
        return {
            "ok": True,
            "requested": False,
            "all_tickers_confirmed": False,
            "confirmed_tickers": [],
            "unconfirmed_tickers": list(SYMBOL_ORDER),
            "tickers": [],
            "message": "Universe chart confirmation was not requested in this run.",
        }

    entries = [
        _build_chart_confirmation_entry(
            request=request,
            symbol=item.get("symbol"),
            chart_check=item.get("chart_check"),
            chart_check_error=item.get("chart_check_error"),
            structure_context=item.get("structure_context") or {"ok": False, "why": "structure_context_unavailable"},
        )
        for item in sorted(
            screened_candidates,
            key=lambda item: SYMBOL_ORDER.index(item.get("symbol")) if item.get("symbol") in SYMBOL_ORDER else 999999,
        )
    ]

    confirmed_tickers = [entry.get("ticker") for entry in entries if entry.get("confirmed")]
    unconfirmed_tickers = [entry.get("ticker") for entry in entries if not entry.get("confirmed")]

    return {
        "ok": True,
        "requested": True,
        "all_tickers_confirmed": len(entries) > 0 and not unconfirmed_tickers,
        "confirmed_tickers": confirmed_tickers,
        "unconfirmed_tickers": unconfirmed_tickers,
        "tickers": entries,
        "message": "Universe chart confirmation block for SPY, QQQ, IWM, and GLD.",
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
    market_closed_context = bool(
        (market_context.get("is_open") is False)
        or (str(time_day_gate.get("reason") or "").strip().lower() == "market_closed")
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

    if macro_context.get("ok") and (
        macro_context.get("has_major_event_today") or macro_context.get("has_major_event_tomorrow")
    ):
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "NO TRADE",
            "why": _decorate_why(
                macro_context.get("note") or "Major event risk is inside the expected hold window.",
                market_closed_context=market_closed_context,
            ),
        }

    if not time_day_gate.get("fresh_entry_allowed") and str(time_day_gate.get("reason") or "").strip().lower() != "market_closed":
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "NO TRADE",
            "why": _decorate_why(
                f"Time/day filter fails: {time_day_gate.get('reason')}.",
                market_closed_context=market_closed_context,
            ),
        }

    if liquidity_context.get("liquidity_pass") is False:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
            "setup_state": "NO TRADE",
            "why": _decorate_why(
                liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry.",
                market_closed_context=market_closed_context,
            ),
        }

    if engine_status == "NO_TRADE" or not best_ticker:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No valid candidate engine setup is available.",
            "setup_state": "NO TRADE",
            "why": _decorate_why(engine_reason, market_closed_context=market_closed_context),
        }

    if structure_context.get("ok"):
        if structure_context.get("setup_type_allowed") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": _decorate_why(
                    f"Setup type is {structure_context.get('setup_type')}, which is not one of the allowed SAFE-FAST setup types.",
                    market_closed_context=market_closed_context,
                ),
            }
        if structure_context.get("room_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": _decorate_why(
                    "Room to first wall is too tight for SAFE-FAST.",
                    market_closed_context=market_closed_context,
                ),
            }
        if structure_context.get("wall_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": _decorate_why(
                    "Wall thesis and strike placement do not match.",
                    market_closed_context=market_closed_context,
                ),
            }
        if structure_context.get("extension_state") == "extended":
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
                "setup_state": "NO TRADE",
                "why": _decorate_why(
                    "Move is extended vs the 1H 50 EMA or too late relative to the first wall.",
                    market_closed_context=market_closed_context,
                ),
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
            "why": _decorate_why(why, market_closed_context=market_closed_context),
        }

    return {
        "good_idea_now": "WAIT",
        "ticker": ticker,
        "action": "wait for full chart confirmation",
        "invalidation": f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}.",
        "setup_state": "PENDING",
        "why": _decorate_why(
            "Candidate engine is valid, but trigger/entry-zone timing still needs confirmation.",
            market_closed_context=market_closed_context,
        ),
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
        structure_context.get("setup_eligible_now") is True
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
        {"item": "allowed_setup_type", "yes": _is_allowed_setup_type_name(structure_context.get("setup_type"))},
        {"item": "twentyfour_hour_supportive", "yes": bool(structure_context.get("twentyfour_hour_supportive") is True)},
        {"item": "one_hour_clean_around_ema", "yes": bool(price_side in {"above", "below"} and structure_context.get("chop_risk") is False)},
        {"item": "clear_room", "yes": bool(structure_context.get("room_pass") is True)},
        {"item": "early_enough", "yes": bool(structure_context.get("extension_blocks_now") is not True)},
        {"item": "clear_trigger", "yes": bool(trigger_state.get("trigger_present") is True)},
        {"item": "liquidity_ok", "yes": bool(liquidity_context.get("liquidity_pass") is True)},
        {"item": "invalidation_clear", "yes": bool(ema_value is not None)},
        {"item": "fits_risk", "yes": bool(primary_candidate and primary_candidate.get("fits_risk_budget") is True)},
        {"item": "open_trade_already", "yes": bool(request.open_positions > 0)},
    ]

    failed_items = [row["item"] for row in items if not row["yes"] and row["item"] != "open_trade_already"]
    global_gate_failures: List[str] = []
    if market_context.get("is_open") is False:
        global_gate_failures.append("time_day_gate")

    effective_failed_items = list(failed_items)
    for item in global_gate_failures:
        if item not in effective_failed_items:
            effective_failed_items.insert(0, item)

    priority_order = [
        "time_day_gate",
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
    effective_decision_blockers_priority = sorted(
        effective_failed_items,
        key=lambda item: (priority_rank.get(item, 999), item),
    )

    return {
        "ok": True,
        "items": items,
        "failed_items": failed_items,
        "decision_blockers_priority": decision_blockers_priority,
        "effective_failed_items": effective_failed_items,
        "effective_decision_blockers_priority": effective_decision_blockers_priority,
        "global_gate_failures": global_gate_failures,
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
        "early_enough": "entry is too late or overextended for SAFE-FAST",
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


def _screened_other_candidates(
    screened: List[Dict[str, Any]],
    best_ticker: Optional[str],
    request: OnDemandRequest,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in screened:
        if item.get("symbol") == best_ticker:
            continue
        structure_context = item.get("structure_context") or {"ok": False, "why": "structure_context_unavailable"}
        out.append(
            {
                "symbol": item.get("symbol"),
                "engine_verdict": item.get("engine_verdict"),
                "final_verdict": item.get("final_verdict"),
                "reason": item.get("reason"),
                "primary_candidate": item.get("primary_candidate"),
                "chart_check": _compact_chart_check_summary(item.get("chart_check")),
                "chart_confirmation": _build_chart_confirmation_block(
                    request=request,
                    chart_check=item.get("chart_check"),
                    chart_check_error=item.get("chart_check_error"),
                    structure_context=structure_context,
                ),
                "structure_context": structure_context,
                "trap_check_context": _build_trap_check_context(structure_context),
                "liquidity_context": item.get("liquidity_context"),
                "trigger_state": item.get("trigger_state"),
                "checklist_failed_items": item.get("checklist", {}).get("failed_items", []),
            }
        )
    return out


_COMPACT_TICKER_UNIVERSE_ORDER: Dict[str, int] = {
    "SPY": 0,
    "QQQ": 1,
    "IWM": 2,
    "GLD": 3,
}


def _compact_ticker_summary_entry(
    item: Dict[str, Any],
    *,
    time_day_gate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    structure_context = item.get("structure_context") or {}
    chart_check = item.get("chart_check") or {}
    trigger_state = item.get("trigger_state") or {}
    checklist = item.get("checklist") or {}
    screened_reason = item.get("reason")

    effective_blockers = _effective_blockers(
        checklist,
        screened_reason=screened_reason,
    )
    effective_primary_blocker = _effective_primary_blocker(
        checklist,
        screened_reason=screened_reason,
    )

    return {
        "ticker": item.get("symbol"),
        "engine_verdict": item.get("engine_verdict"),
        "final_verdict": item.get("final_verdict"),
        "primary_blocker": effective_primary_blocker,
        "blockers": effective_blockers[:4],
        "reason": _decorate_why(
            screened_reason,
            market_closed_context=((time_day_gate or {}).get("reason") == "market_closed"),
        ),
        "setup_type": structure_context.get("setup_type"),
        "trend_label": structure_context.get("trend_label"),
        "room_to_first_wall": structure_context.get("room_to_first_wall"),
        "first_wall": structure_context.get("first_wall"),
        "room_pass": structure_context.get("room_pass"),
        "extension_state": structure_context.get("extension_state"),
        "extension_blocks_now": structure_context.get("extension_blocks_now"),
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_reason": trigger_state.get("why"),
        "ema50_1h": chart_check.get("ema50_1h"),
        "latest_close": chart_check.get("latest_close"),
        "price_vs_ema50_1h": chart_check.get("price_vs_ema50_1h"),
    }


def _build_compact_ticker_summaries(
    screened_candidates: List[Dict[str, Any]],
    *,
    time_day_gate: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    ordered_candidates = sorted(
        screened_candidates,
        key=lambda item: (
            _COMPACT_TICKER_UNIVERSE_ORDER.get(str(item.get("symbol")), 99),
            str(item.get("symbol") or ""),
        ),
    )
    return [
        _compact_ticker_summary_entry(item, time_day_gate=time_day_gate)
        for item in ordered_candidates
    ]


def _should_freeze_winner_to_raw_engine(
    *,
    summary_payload: Dict[str, Any],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
) -> bool:
    raw_best_ticker = summary_payload.get("best_ticker")
    if not raw_best_ticker:
        return False

    global_gate_reason = str(time_day_gate.get("reason") or "").strip().lower()
    if global_gate_reason in {
        "market_closed",
        "past_monday_thursday_cutoff",
        "outside_time_window",
        "outside_day_window",
    }:
        return True

    if market_context.get("is_open") is False:
        return True

    fresh_entry_allowed = time_day_gate.get("fresh_entry_allowed")
    if fresh_entry_allowed is False:
        return True

    return False


def _select_screened_best_candidate(
    screened_candidates: List[Dict[str, Any]],
    *,
    raw_engine_best_ticker: Optional[str] = None,
    freeze_to_raw_engine: bool = False,
) -> Optional[Dict[str, Any]]:
    if not screened_candidates:
        return None

    if freeze_to_raw_engine and raw_engine_best_ticker:
        for item in screened_candidates:
            if item.get("symbol") == raw_engine_best_ticker:
                return item

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




def _build_engine_context_block(
    summary_payload: Dict[str, Any],
    selected: Optional[Dict[str, Any]],
    engine_status: str,
    final_verdict: str,
    best_ticker: Optional[str],
) -> Dict[str, Any]:
    raw_best_ticker = summary_payload.get("best_ticker")
    raw_status = summary_payload.get("verdict")
    raw_reason = summary_payload.get("reason")
    normalized_reason = selected.get("reason", raw_reason) if selected else raw_reason

    return {
        "ok": True,
        "raw_best_ticker": raw_best_ticker,
        "raw_status": raw_status,
        "raw_reason": raw_reason,
        "normalized_best_ticker": best_ticker,
        "normalized_status": engine_status,
        "normalized_final_verdict": final_verdict,
        "normalized_reason": normalized_reason,
        "changed_from_raw_engine": (
            raw_best_ticker != best_ticker
            or raw_status != engine_status
        ),
    }


def _build_candidate_engine_normalized_block(
    summary_payload: Dict[str, Any],
    selected: Optional[Dict[str, Any]],
    engine_status: str,
    final_verdict: str,
    best_ticker: Optional[str],
) -> Dict[str, Any]:
    normalized_reason = selected.get("reason", summary_payload.get("reason")) if selected else summary_payload.get("reason")
    selected_summary = selected.get("summary") if selected else None

    return {
        "ok": summary_payload.get("ok", True),
        "raw_best_ticker": summary_payload.get("best_ticker"),
        "raw_verdict": summary_payload.get("verdict"),
        "raw_reason": summary_payload.get("reason"),
        "normalized_best_ticker": best_ticker,
        "normalized_verdict": engine_status,
        "normalized_final_verdict": final_verdict,
        "normalized_reason": normalized_reason,
        "selection_mode": (
            selected_summary.get("selection_mode")
            if selected_summary else summary_payload.get("selection_mode")
        ),
    }




def _resolve_global_gate_primary_blocker(
    screened_reason: Optional[str] = None,
    time_gate_reason: Optional[str] = None,
) -> Optional[str]:
    gate_reason = time_gate_reason or screened_reason
    if gate_reason in {"past_monday_thursday_cutoff", "outside_time_window", "outside_day_window"}:
        return "time_day_gate"
    return None


def _effective_blockers(
    checklist_block: Dict[str, Any],
    screened_reason: Optional[str] = None,
    time_gate_reason: Optional[str] = None,
) -> List[str]:
    blockers = list(checklist_block.get("decision_blockers_priority") or checklist_block.get("failed_items") or [])
    gate_blocker = _resolve_global_gate_primary_blocker(
        screened_reason=screened_reason,
        time_gate_reason=time_gate_reason,
    )
    if gate_blocker:
        blockers = [gate_blocker] + [item for item in blockers if item != gate_blocker]
    return blockers


def _effective_primary_blocker(
    checklist_block: Dict[str, Any],
    screened_reason: Optional[str] = None,
    time_gate_reason: Optional[str] = None,
) -> Optional[str]:
    blockers = _effective_blockers(
        checklist_block,
        screened_reason=screened_reason,
        time_gate_reason=time_gate_reason,
    )
    return blockers[0] if blockers else None


def _build_decision_context_block(
    summary_payload: Dict[str, Any],
    selected: Optional[Dict[str, Any]],
    engine_status: str,
    final_verdict: str,
    best_ticker: Optional[str],
    checklist_block: Dict[str, Any],
    failed_reasons: List[str],
    user_facing: Dict[str, Any],
) -> Dict[str, Any]:
    raw_reason = summary_payload.get("reason")
    normalized_reason = selected.get("reason", raw_reason) if selected else raw_reason

    effective_blockers = _effective_blockers(
        checklist_block,
        screened_reason=normalized_reason,
    )
    effective_primary_blocker = _effective_primary_blocker(
        checklist_block,
        screened_reason=normalized_reason,
    )

    return {
        "ok": True,
        "ticker": best_ticker,
        "action": user_facing.get("action"),
        "setup_state": user_facing.get("setup_state"),
        "good_idea_now": user_facing.get("good_idea_now"),
        "raw_engine": {
            "ticker": summary_payload.get("best_ticker"),
            "status": summary_payload.get("verdict"),
            "reason": raw_reason,
        },
        "normalized_engine": {
            "ticker": best_ticker,
            "status": engine_status,
            "final_verdict": final_verdict,
            "reason": normalized_reason,
        },
        "screened": {
            "ticker": best_ticker,
            "final_verdict": final_verdict,
            "reason": normalized_reason,
        },
        "primary_blocker": effective_primary_blocker,
        "blockers": effective_blockers,
        "failed_reasons": failed_reasons,
        "changed_from_raw_engine": (
            summary_payload.get("best_ticker") != best_ticker
            or summary_payload.get("verdict") != engine_status
        ),
    }



def _build_blocker_context_block(
    checklist_block: Dict[str, Any],
    failed_reasons: List[str],
    trigger_state: Dict[str, Any],
    structure_context: Dict[str, Any],
    engine_status: str,
    final_verdict: str,
    user_facing: Dict[str, Any],
) -> Dict[str, Any]:
    blocker_items = _effective_blockers(
        checklist_block,
        screened_reason=trigger_state.get("why"),
    )
    primary_blocker = _effective_primary_blocker(
        checklist_block,
        screened_reason=trigger_state.get("why"),
    )

    return {
        "ok": True,
        "primary_blocker": primary_blocker,
        "blockers": blocker_items,
        "failed_reasons": failed_reasons,
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_reason": trigger_state.get("why"),
        "structure_ready": trigger_state.get("structure_ready"),
        "setup_type": structure_context.get("setup_type"),
        "allowed_setup": structure_context.get("allowed_setup"),
        "setup_eligible_now": structure_context.get("setup_eligible_now"),
        "room_pass": structure_context.get("room_pass"),
        "extension_blocks_now": structure_context.get("extension_blocks_now"),
        "engine_status": engine_status,
        "final_verdict": final_verdict,
        "action": user_facing.get("action"),
        "setup_state": user_facing.get("setup_state"),
        "good_idea_now": user_facing.get("good_idea_now"),
    }



def _build_trigger_context_block(
    trigger_state: Dict[str, Any],
    live_map: Dict[str, Any],
) -> Dict[str, Any]:
    trigger_scan = live_map.get("trigger_scan") or {}
    current_bar = trigger_scan.get("current_bar") or {}
    completed_candle = trigger_scan.get("most_recent_completed_candle") or {}

    return {
        "ok": True,
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_reason": trigger_state.get("why"),
        "structure_ready": trigger_state.get("structure_ready"),
        "trigger_style": trigger_state.get("trigger_style"),
        "trigger_level": trigger_state.get("trigger_level"),
        "current_close": trigger_state.get("current_close"),
        "current_bar_raw_trigger_pass": current_bar.get("raw_chart_trigger_pass"),
        "current_bar_gated_trigger_pass": current_bar.get("gated_trigger_pass"),
        "completed_candle_raw_trigger_pass": completed_candle.get("raw_chart_trigger_pass"),
        "completed_candle_gated_trigger_pass": completed_candle.get("gated_trigger_pass"),
        "current_bar_relation_to_trigger_level": current_bar.get("relation_to_trigger_level"),
        "current_bar_relation_to_ema50_1h": current_bar.get("relation_to_ema50_1h"),
        "trigger_scan_status": trigger_scan.get("trigger_scan_status"),
        "why_trigger_scan_passes_or_fails": trigger_scan.get("why_trigger_scan_passes_or_fails"),
    }




def _derive_global_gate_primary_blocker(trigger_reason: Any) -> Optional[str]:
    if trigger_reason == "past_monday_thursday_cutoff":
        return "time_day_gate"
    return None


def _derive_global_gate_next_flip(trigger_reason: Any) -> Optional[str]:
    if trigger_reason == "past_monday_thursday_cutoff":
        return "fresh_entry_allowed"
    return None


def _build_entry_context_block(
    trigger_state: Dict[str, Any],
    live_map: Dict[str, Any],
    checklist_block: Dict[str, Any],
    structure_context: Dict[str, Any],
    user_facing: Dict[str, Any],
) -> Dict[str, Any]:
    trigger_scan = live_map.get("trigger_scan") or {}
    current_bar = trigger_scan.get("current_bar") or {}
    completed_candle = trigger_scan.get("most_recent_completed_candle") or {}
    blockers = list(checklist_block.get("decision_blockers_priority") or checklist_block.get("failed_items") or [])
    gate_blocker = _derive_global_gate_primary_blocker(trigger_state.get("why"))
    if gate_blocker:
        blockers = [gate_blocker] + [item for item in blockers if item != gate_blocker]
    primary_blocker = blockers[0] if blockers else None

    current_bar_raw_trigger_pass = bool(current_bar.get("raw_chart_trigger_pass") is True)
    current_bar_gated_trigger_pass = bool(current_bar.get("gated_trigger_pass") is True)
    completed_candle_raw_trigger_pass = bool(completed_candle.get("raw_chart_trigger_pass") is True)
    completed_candle_gated_trigger_pass = bool(completed_candle.get("gated_trigger_pass") is True)

    if current_bar_gated_trigger_pass:
        mid_candle_entry_state = "APPROVED_NOW"
    elif current_bar_raw_trigger_pass:
        mid_candle_entry_state = "BLOCKED_NOW"
    else:
        mid_candle_entry_state = "NOT_PRESENT"

    if completed_candle_gated_trigger_pass:
        completed_candle_entry_state = "APPROVED_ON_COMPLETED_CANDLE"
    elif completed_candle_raw_trigger_pass:
        completed_candle_entry_state = "BLOCKED_ON_COMPLETED_CANDLE"
    else:
        completed_candle_entry_state = "NOT_PRESENT_ON_COMPLETED_CANDLE"

    return {
        "ok": True,
        "mid_candle_trade_available_now": current_bar_gated_trigger_pass,
        "mid_candle_entry_state": mid_candle_entry_state,
        "mid_candle_raw_trigger_detected_now": current_bar_raw_trigger_pass,
        "mid_candle_block_reason": None if current_bar_gated_trigger_pass else trigger_state.get("why"),
        "completed_candle_trade_available": completed_candle_gated_trigger_pass,
        "completed_candle_entry_state": completed_candle_entry_state,
        "completed_candle_raw_trigger_detected": completed_candle_raw_trigger_pass,
        "completed_candle_block_reason": None if completed_candle_gated_trigger_pass else completed_candle.get("why"),
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_reason": trigger_state.get("why"),
        "structure_ready": trigger_state.get("structure_ready"),
        "trigger_style": trigger_state.get("trigger_style"),
        "trigger_level": trigger_state.get("trigger_level"),
        "current_close": trigger_state.get("current_close"),
        "current_bar_relation_to_trigger_level": current_bar.get("relation_to_trigger_level"),
        "current_bar_relation_to_ema50_1h": current_bar.get("relation_to_ema50_1h"),
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "allowed_setup": structure_context.get("allowed_setup"),
        "setup_type": structure_context.get("setup_type"),
        "room_pass": structure_context.get("room_pass"),
        "extension_blocks_now": structure_context.get("extension_blocks_now"),
        "action": user_facing.get("action"),
        "setup_state": user_facing.get("setup_state"),
        "good_idea_now": user_facing.get("good_idea_now"),
    }


def _build_intrabar_signal_context_block(
    entry_context: Dict[str, Any],
    live_map: Dict[str, Any],
    user_facing: Dict[str, Any],
) -> Dict[str, Any]:
    trigger_scan = live_map.get("trigger_scan") or {}
    current_bar = trigger_scan.get("current_bar") or {}
    completed_candle = trigger_scan.get("most_recent_completed_candle") or {}

    intrabar_trade_available_now = bool(entry_context.get("mid_candle_trade_available_now") is True)
    intrabar_raw_signal_detected = bool(entry_context.get("mid_candle_raw_trigger_detected_now") is True)
    completed_trade_available = bool(entry_context.get("completed_candle_trade_available") is True)
    completed_raw_signal_detected = bool(entry_context.get("completed_candle_raw_trigger_detected") is True)

    if intrabar_trade_available_now:
        intrabar_signal_status = "APPROVED_NOW"
    elif intrabar_raw_signal_detected:
        intrabar_signal_status = "RAW_SIGNAL_BLOCKED_NOW"
    else:
        intrabar_signal_status = "NO_INTRABAR_SIGNAL"

    if completed_trade_available:
        completed_signal_status = "APPROVED_ON_COMPLETED_CANDLE"
    elif completed_raw_signal_detected:
        completed_signal_status = "RAW_SIGNAL_BLOCKED_ON_COMPLETED_CANDLE"
    else:
        completed_signal_status = "NO_COMPLETED_CANDLE_SIGNAL"

    if intrabar_trade_available_now:
        signal_note = "Intrabar SAFE-FAST entry is approved right now."
    elif intrabar_raw_signal_detected:
        signal_note = "Intrabar signal is visible, but SAFE-FAST approval still blocks entry."
    elif completed_trade_available:
        signal_note = "Completed-candle SAFE-FAST entry is approved."
    elif completed_raw_signal_detected:
        signal_note = "Completed-candle signal is visible, but SAFE-FAST approval still blocks entry."
    else:
        signal_note = "No live intrabar or completed-candle signal is currently available."

    return {
        "ok": True,
        "ticker": live_map.get("ticker"),
        "intrabar_signal_status": intrabar_signal_status,
        "intrabar_trade_available_now": intrabar_trade_available_now,
        "intrabar_raw_signal_detected": intrabar_raw_signal_detected,
        "intrabar_block_reason": entry_context.get("mid_candle_block_reason"),
        "intrabar_time_iso": current_bar.get("time_iso"),
        "intrabar_close": current_bar.get("close"),
        "intrabar_trigger_level_relation": current_bar.get("relation_to_trigger_level"),
        "intrabar_ema_relation": current_bar.get("relation_to_ema50_1h"),
        "completed_signal_status": completed_signal_status,
        "completed_trade_available": completed_trade_available,
        "completed_raw_signal_detected": completed_raw_signal_detected,
        "completed_block_reason": entry_context.get("completed_candle_block_reason"),
        "completed_time_iso": completed_candle.get("time_iso"),
        "completed_close": completed_candle.get("close"),
        "primary_blocker": entry_context.get("primary_blocker"),
        "blockers": entry_context.get("blockers"),
        "trigger_present": entry_context.get("trigger_present"),
        "trigger_reason": entry_context.get("trigger_reason"),
        "structure_ready": entry_context.get("structure_ready"),
        "action": user_facing.get("action"),
        "setup_state": user_facing.get("setup_state"),
        "good_idea_now": user_facing.get("good_idea_now"),
        "signal_note": signal_note,
    }



def _build_approval_context_block(
    entry_context: Dict[str, Any],
    intrabar_signal_context: Dict[str, Any],
    checklist_block: Dict[str, Any],
    structure_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    user_facing: Dict[str, Any],
) -> Dict[str, Any]:
    blockers = list(checklist_block.get("decision_blockers_priority") or checklist_block.get("failed_items") or [])
    gate_blocker = _derive_global_gate_primary_blocker(trigger_state.get("why"))
    if gate_blocker:
        blockers = [gate_blocker] + [item for item in blockers if item != gate_blocker]
    primary_blocker = blockers[0] if blockers else None
    next_flip_needed = _derive_global_gate_next_flip(trigger_state.get("why")) or primary_blocker
    intrabar_raw_signal_detected = bool(entry_context.get("mid_candle_raw_trigger_detected_now") is True)
    intrabar_trade_available_now = bool(entry_context.get("mid_candle_trade_available_now") is True)
    completed_raw_signal_detected = bool(entry_context.get("completed_candle_raw_trigger_detected") is True)
    completed_trade_available = bool(entry_context.get("completed_candle_trade_available") is True)

    if intrabar_trade_available_now:
        approval_status = "APPROVED_NOW"
    elif intrabar_raw_signal_detected:
        approval_status = "RAW_SIGNAL_WAITING_FOR_APPROVAL"
    elif completed_trade_available:
        approval_status = "APPROVED_ON_COMPLETED_CANDLE"
    elif completed_raw_signal_detected:
        approval_status = "COMPLETED_SIGNAL_WAITING_FOR_APPROVAL"
    else:
        approval_status = "NO_SIGNAL_TO_APPROVE"

    if intrabar_trade_available_now:
        approval_note = "All SAFE-FAST approval gates pass right now."
    elif intrabar_raw_signal_detected:
        approval_note = "Raw intrabar signal exists, but SAFE-FAST approval gates still block entry."
    elif completed_trade_available:
        approval_note = "Completed-candle signal is approved."
    elif completed_raw_signal_detected:
        approval_note = "Completed-candle raw signal exists, but SAFE-FAST approval gates still block entry."
    else:
        approval_note = "No raw signal is currently waiting for approval."

    return {
        "ok": True,
        "ticker": intrabar_signal_context.get("ticker"),
        "approval_status": approval_status,
        "approval_ready_now": intrabar_trade_available_now,
        "approval_ready_on_completed_candle": completed_trade_available,
        "intrabar_raw_signal_detected": intrabar_raw_signal_detected,
        "completed_raw_signal_detected": completed_raw_signal_detected,
        "structure_ready": trigger_state.get("structure_ready"),
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_reason": trigger_state.get("why"),
        "allowed_setup": structure_context.get("allowed_setup"),
        "setup_type": structure_context.get("setup_type"),
        "room_pass": structure_context.get("room_pass"),
        "extension_blocks_now": structure_context.get("extension_blocks_now"),
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "next_flip_needed": next_flip_needed,
        "action": user_facing.get("action"),
        "setup_state": user_facing.get("setup_state"),
        "good_idea_now": user_facing.get("good_idea_now"),
        "approval_note": approval_note,
    }



def _build_approval_requirements_context_block(
    checklist_block: Dict[str, Any],
    structure_context: Dict[str, Any],
    trigger_state: Dict[str, Any],
    market_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    macro_context: Dict[str, Any],
    liquidity_context: Dict[str, Any],
    approval_context: Dict[str, Any],
) -> Dict[str, Any]:
    checklist_items = {row.get("item"): bool(row.get("yes")) for row in checklist_block.get("items", [])}
    blockers = list(checklist_block.get("decision_blockers_priority") or checklist_block.get("failed_items") or [])
    gate_blocker = _derive_global_gate_primary_blocker(trigger_state.get("why"))
    if gate_blocker:
        blockers = [gate_blocker] + [item for item in blockers if item != gate_blocker]

    gate_statuses = [
        {
            "gate": "allowed_setup_type",
            "ready": _is_allowed_setup_type_name(structure_context.get("setup_type")),
            "current_value": structure_context.get("setup_type"),
            "needed_state": "one of the 3 allowed SAFE-FAST setup types",
        },
        {
            "gate": "room_pass",
            "ready": bool(structure_context.get("room_pass") is True),
            "current_value": structure_context.get("room_pass"),
            "needed_state": True,
        },
        {
            "gate": "extension_clear",
            "ready": bool(structure_context.get("extension_blocks_now") is not True),
            "current_value": structure_context.get("extension_blocks_now"),
            "needed_state": False,
        },
        {
            "gate": "structure_ready",
            "ready": bool(trigger_state.get("structure_ready") is True),
            "current_value": trigger_state.get("structure_ready"),
            "needed_state": True,
        },
        {
            "gate": "trigger_present",
            "ready": bool(trigger_state.get("trigger_present") is True),
            "current_value": trigger_state.get("trigger_present"),
            "needed_state": True,
        },
        {
            "gate": "liquidity_ok",
            "ready": bool(liquidity_context.get("liquidity_pass") is True),
            "current_value": liquidity_context.get("liquidity_pass"),
            "needed_state": True,
        },
        {
            "gate": "market_open",
            "ready": bool(market_context.get("is_open") is True),
            "current_value": market_context.get("is_open"),
            "needed_state": True,
        },
        {
            "gate": "fresh_entry_allowed",
            "ready": bool(time_day_gate.get("fresh_entry_allowed") is True),
            "current_value": time_day_gate.get("fresh_entry_allowed"),
            "needed_state": True,
        },
        {
            "gate": "macro_event_clear",
            "ready": bool(
                not macro_context.get("has_major_event_today")
                and not macro_context.get("has_major_event_tomorrow")
            ),
            "current_value": {
                "has_major_event_today": macro_context.get("has_major_event_today"),
                "has_major_event_tomorrow": macro_context.get("has_major_event_tomorrow"),
            },
            "needed_state": {
                "has_major_event_today": False,
                "has_major_event_tomorrow": False,
            },
        },
    ]

    missing_gates = [row["gate"] for row in gate_statuses if not row["ready"]]
    next_flip_needed = _derive_global_gate_next_flip(trigger_state.get("why")) or (blockers[0] if blockers else (missing_gates[0] if missing_gates else None))

    if approval_context.get("approval_ready_now") is True:
        approval_path_status = "APPROVED_NOW"
    elif approval_context.get("intrabar_raw_signal_detected") is True:
        approval_path_status = "WAITING_FOR_GATES"
    elif approval_context.get("completed_raw_signal_detected") is True:
        approval_path_status = "COMPLETED_SIGNAL_WAITING_FOR_GATES"
    else:
        approval_path_status = "NO_SIGNAL_YET"

    return {
        "ok": True,
        "approval_path_status": approval_path_status,
        "approval_ready_now": approval_context.get("approval_ready_now"),
        "approval_ready_on_completed_candle": approval_context.get("approval_ready_on_completed_candle"),
        "intrabar_raw_signal_detected": approval_context.get("intrabar_raw_signal_detected"),
        "completed_raw_signal_detected": approval_context.get("completed_raw_signal_detected"),
        "next_flip_needed": next_flip_needed,
        "missing_gates": missing_gates,
        "gate_statuses": gate_statuses,
        "checklist_failed_items": checklist_block.get("effective_failed_items", checklist_block.get("failed_items", [])),
        "raw_checklist_failed_items": checklist_block.get("failed_items", []),
        "global_gate_failures": checklist_block.get("global_gate_failures", []),
        "blockers": blockers,
        "allowed_setup": structure_context.get("allowed_setup"),
        "setup_type": structure_context.get("setup_type"),
        "room_pass": structure_context.get("room_pass"),
        "extension_blocks_now": structure_context.get("extension_blocks_now"),
        "structure_ready": trigger_state.get("structure_ready"),
        "trigger_present": trigger_state.get("trigger_present"),
        "trigger_reason": trigger_state.get("why"),
        "liquidity_ok": liquidity_context.get("liquidity_pass"),
        "market_open": market_context.get("is_open"),
        "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
        "macro_event_clear": bool(
            not macro_context.get("has_major_event_today")
            and not macro_context.get("has_major_event_tomorrow")
        ),
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
    selected_reason = selected.get("reason")
    effective_failed_items = _effective_blockers(
        selected_checklist,
        screened_reason=selected_reason,
    )
    effective_primary_blocker = (
        effective_failed_items[0] if effective_failed_items else None
    )
    engine_pick_reason = engine_pick.get("reason") if engine_pick else None
    engine_pick_verdict = engine_pick.get("final_verdict") if engine_pick else None

    normalized_engine_best_ticker = selected.get("symbol")
    return {
        "ok": True,
        "screened_best_ticker": selected.get("symbol"),
        "raw_engine_best_ticker": engine_best_ticker,
        "normalized_engine_best_ticker": normalized_engine_best_ticker,
        "engine_best_ticker": normalized_engine_best_ticker,
        "changed_from_engine_best": selected.get("symbol") != engine_best_ticker,
        "screened_final_verdict": selected.get("final_verdict"),
        "screened_reason": selected_reason,
        "screened_primary_blocker": effective_primary_blocker,
        "screened_checklist_failed_items": effective_failed_items,
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

    if include_chart_checks and symbol:
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
        elif structure_context.get("setup_type_allowed") is False:
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
    trap_check_context = None
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
        trap_check_context = _build_trap_check_context(structure_context)
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

    gate_reason = time_day_gate.get("reason") or trigger_state.get("why")
    effective_blockers = _effective_blockers(
        checklist,
        screened_reason=gate_reason,
        time_gate_reason=time_day_gate.get("reason"),
    )
    effective_primary_blocker = _effective_primary_blocker(
        checklist,
        screened_reason=gate_reason,
        time_gate_reason=time_day_gate.get("reason"),
    )

    return {
        "active": active,
        "ticker": best_ticker,
        "availability_reason": availability_reason,
        "primary_blocker": effective_primary_blocker if active else None,
        "blockers": effective_blockers if active else [],
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
        "trap_check_context": trap_check_context if active else None,
        "trigger_scan": trigger_scan if active else None,
        "primary_entry_zone": primary_entry_zone if active else None,
        "backup_entry_zone": backup_entry_zone if active else None,
        "options": options_block,
        "levels": levels_block,
        "targets": targets_block,
        "primary_candidate": primary_candidate if active else None,
        "backup_candidate": backup_candidate if active else None,
        "invalidation": invalidation_level_1h_ema50 if active else None,
        "checklist_failed_items": effective_blockers if active else [],
        "decision_blockers_priority": effective_blockers if active else [],
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
            "early_enough": "early enough / not overextended",
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


def _normalize_top_level_status(final_verdict: Optional[str]) -> str:
    if final_verdict in {"ACTIVE_NOW", "PENDING", "NO_TRADE", "INVALIDATED"}:
        return str(final_verdict)
    return "NO_TRADE"


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
    failed_items = checklist_block.get("failed_items", [])
    effective_failed_items = checklist_block.get("effective_failed_items", failed_items)
    global_gate_failures = checklist_block.get(
        "global_gate_failures",
        [item for item in effective_failed_items if item not in failed_items],
    )
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
        "failed_items": failed_items,
        "effective_failed_items": effective_failed_items,
        "global_gate_failures": global_gate_failures,
    }


def _build_approval_flip_context_block(
    approval_requirements_context: Dict[str, Any],
    approval_context: Dict[str, Any],
    entry_context: Dict[str, Any],
    intrabar_signal_context: Dict[str, Any],
) -> Dict[str, Any]:
    gate_statuses = approval_requirements_context.get("gate_statuses") or []
    ready_gates = [row.get("gate") for row in gate_statuses if row.get("ready")]
    missing_gates = approval_requirements_context.get("missing_gates") or [
        row.get("gate") for row in gate_statuses if not row.get("ready")
    ]
    next_flip_needed = approval_requirements_context.get("next_flip_needed")
    approval_ready_now = bool(approval_context.get("approval_ready_now") is True)
    approval_ready_on_completed_candle = bool(approval_context.get("approval_ready_on_completed_candle") is True)
    intrabar_raw_signal_detected = bool(approval_context.get("intrabar_raw_signal_detected") is True)
    completed_raw_signal_detected = bool(approval_context.get("completed_raw_signal_detected") is True)

    if approval_ready_now:
        flip_status = "APPROVED_NOW"
    elif approval_ready_on_completed_candle:
        flip_status = "APPROVED_ON_COMPLETED_CANDLE"
    elif intrabar_raw_signal_detected:
        flip_status = "NEXT_GATE_BLOCKING_INTRABAR_ENTRY"
    elif completed_raw_signal_detected:
        flip_status = "NEXT_GATE_BLOCKING_COMPLETED_ENTRY"
    else:
        flip_status = "NO_SIGNAL_TO_APPROVE"

    return {
        "ok": True,
        "flip_status": flip_status,
        "next_flip_needed": next_flip_needed,
        "ready_gate_count": len([gate for gate in ready_gates if gate]),
        "remaining_gate_count": len([gate for gate in missing_gates if gate]),
        "ready_gates": [gate for gate in ready_gates if gate],
        "missing_gates": [gate for gate in missing_gates if gate],
        "intrabar_raw_signal_detected": intrabar_raw_signal_detected,
        "completed_raw_signal_detected": completed_raw_signal_detected,
        "approval_ready_now": approval_ready_now,
        "approval_ready_on_completed_candle": approval_ready_on_completed_candle,
        "mid_candle_trade_available_now": bool(entry_context.get("mid_candle_trade_available_now") is True),
        "completed_candle_trade_available": bool(entry_context.get("completed_candle_trade_available") is True),
        "intrabar_signal_status": intrabar_signal_context.get("intrabar_signal_status"),
        "approval_status": approval_context.get("approval_status"),
        "primary_blocker": approval_context.get("primary_blocker"),
        "blockers": approval_context.get("blockers", []),
        "approval_note": (
            "Flip the next required gate before any raw signal can become an approved SAFE-FAST entry."
            if next_flip_needed
            else "No remaining approval gate is blocking right now."
        ),
    }


def _build_setup_eligibility_context_block(
    structure_context: Dict[str, Any],
    live_map: Dict[str, Any],
    checklist_block: Dict[str, Any],
    approval_requirements_context: Dict[str, Any],
) -> Dict[str, Any]:
    setup_type = structure_context.get("setup_type")
    allowed_setup_type = _is_allowed_setup_type_name(setup_type)
    setup_type_allowed = bool(structure_context.get("setup_type_allowed") is True)
    setup_eligible_now = bool(structure_context.get("setup_eligible_now") is True)
    route = live_map.get("setup_route") or {}
    blockers = list(
        approval_requirements_context.get("blockers")
        or checklist_block.get("decision_blockers_priority")
        or checklist_block.get("failed_items")
        or []
    )
    primary_blocker = blockers[0] if blockers else None

    if not setup_type:
        setup_type_status = "NO_SETUP_TYPE_DETECTED"
    elif setup_eligible_now:
        setup_type_status = "ELIGIBLE_NOW"
    else:
        setup_type_status = "DETECTED_BUT_NOT_ELIGIBLE"

    return {
        "ok": True,
        "setup_type_detected": setup_type,
        "setup_type_status": setup_type_status,
        "allowed_setup": setup_type_allowed,
        "setup_eligible_now": setup_eligible_now,
        "ten_second_check_answer": "YES" if allowed_setup_type else "NO",
        "setup_route_status": route.get("setup_route_status"),
        "setup_route_reason": route.get("why_setup_route_passes_or_fails"),
        "next_flip_needed": approval_requirements_context.get("next_flip_needed") or primary_blocker,
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "approval_path_status": approval_requirements_context.get("approval_path_status"),
        "note": "A setup label can be detected while SAFE-FAST still marks the setup as not eligible."
    }

def _build_setup_check_context_block(

    structure_context: Dict[str, Any],
    ten_second_checklist_block: Dict[str, Any],
    setup_eligibility_context: Dict[str, Any],
) -> Dict[str, Any]:
    setup_type = structure_context.get("setup_type")
    setup_type_allowed = bool(structure_context.get("setup_type_allowed") is True)
    setup_eligible_now = bool(structure_context.get("setup_eligible_now") is True)
    answers = ten_second_checklist_block.get("answers") or []
    allowed_setup_answer = None
    for row in answers:
        if row.get("item") == "allowed_setup_type":
            allowed_setup_answer = row.get("answer")
            break

    setup_type_status = setup_eligibility_context.get("setup_type_status")
    primary_blocker = setup_eligibility_context.get("primary_blocker")
    blockers = setup_eligibility_context.get("blockers") or []

    return {
        "ok": True,
        "setup_type_detected": setup_type,
        "setup_type_status": setup_type_status,
        "allowed_setup": setup_type_allowed,
        "setup_eligible_now": setup_eligible_now,
        "ten_second_check_item": "allowed_setup_type",
        "ten_second_check_answer": allowed_setup_answer,
        "detected_but_not_eligible": bool(_is_allowed_setup_type_name(setup_type) and not setup_eligible_now),
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "note": (
            "Setup detection and setup eligibility are separate. "
            "A route can be labeled while the checklist still says NO."
        ),
    }


def _build_time_gate_check_context_block(
    time_day_gate: Dict[str, Any],
    ten_second_checklist_block: Dict[str, Any],
    checklist_block: Dict[str, Any],
) -> Dict[str, Any]:
    answers = ten_second_checklist_block.get("answers") or []
    early_enough_answer = None
    for row in answers:
        if row.get("item") == "early_enough":
            early_enough_answer = row.get("answer")
            break

    fresh_entry_allowed = bool(time_day_gate.get("fresh_entry_allowed") is True)
    reason = time_day_gate.get("reason")
    cutoff_et = time_day_gate.get("cutoff_et")
    blockers = _effective_blockers(
        checklist_block,
        time_gate_reason=reason,
    )
    primary_blocker = _effective_primary_blocker(
        checklist_block,
        time_gate_reason=reason,
    )

    return {
        "ok": True,
        "entry_window_status": "OPEN" if fresh_entry_allowed else "CLOSED",
        "fresh_entry_allowed": fresh_entry_allowed,
        "time_gate_reason": reason,
        "cutoff_et": cutoff_et,
        "ten_second_check_item": "early_enough",
        "ten_second_check_answer": early_enough_answer,
        "early_enough_fails_from_time_gate": bool(
            early_enough_answer == "NO" and not fresh_entry_allowed
        ),
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "note": (
            "The early_enough checklist item can fail from late extension, "
            "the closed entry window, or both."
        ),
    }

def _build_final_reason_context_block(

    user_facing: Dict[str, Any],
    screened_best_context: Dict[str, Any],
    time_gate_check_context: Dict[str, Any],
    checklist_block: Dict[str, Any],
) -> Dict[str, Any]:
    screened_reason = screened_best_context.get("screened_reason")
    time_gate_reason = time_gate_check_context.get("time_gate_reason")
    blockers = _effective_blockers(
        checklist_block,
        screened_reason=screened_reason,
        time_gate_reason=time_gate_reason,
    )
    primary_blocker = _effective_primary_blocker(
        checklist_block,
        screened_reason=screened_reason,
        time_gate_reason=time_gate_reason,
    )

    return {
        "ok": True,
        "final_reason": user_facing.get("why"),
        "screened_reason": screened_reason,
        "time_gate_reason": time_gate_reason,
        "entry_window_status": time_gate_check_context.get("entry_window_status"),
        "fresh_entry_allowed": time_gate_check_context.get("fresh_entry_allowed"),
        "early_enough_fails_from_time_gate": time_gate_check_context.get("early_enough_fails_from_time_gate"),
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "note": (
            "The final NO_TRADE reason can come from the time/day gate, "
            "structural blockers, or both."
        ),
    }



def _build_reason_stack_context_block(
    final_reason_context: Dict[str, Any],
    checklist_block: Dict[str, Any],
    failed_reasons: List[str],
) -> Dict[str, Any]:
    screened_reason = final_reason_context.get("screened_reason")
    time_gate_reason = final_reason_context.get("time_gate_reason")
    blockers = _effective_blockers(
        checklist_block,
        screened_reason=screened_reason,
        time_gate_reason=time_gate_reason,
    )
    primary_blocker = _effective_primary_blocker(
        checklist_block,
        screened_reason=screened_reason,
        time_gate_reason=time_gate_reason,
    )

    return {
        "ok": True,
        "top_line_reason": final_reason_context.get("final_reason"),
        "screened_reason": screened_reason,
        "time_gate_reason": time_gate_reason,
        "primary_blocker": primary_blocker,
        "blockers": blockers,
        "failed_reasons": failed_reasons,
        "reason_count": len(failed_reasons or []),
        "note": (
            "The top-line NO_TRADE reason is concise. "
            "Use blockers and failed_reasons for the full rejection stack."
        ),
    }



def _build_winner_shift_context_block(
    *,
    raw_engine_winner_ticker: Optional[str],
    raw_engine_winner_status: Optional[str],
    normalized_engine_winner_ticker: Optional[str],
    normalized_engine_winner_status: Optional[str],
    normalized_engine_winner_final_verdict: Optional[str],
    screened_live_winner_ticker: Optional[str],
    screened_live_winner_final_verdict: Optional[str],
    screened_reason: Optional[str],
) -> Dict[str, Any]:
    raw_to_normalized_changed = raw_engine_winner_ticker != normalized_engine_winner_ticker
    normalized_to_screened_changed = normalized_engine_winner_ticker != screened_live_winner_ticker
    any_shift = raw_to_normalized_changed or normalized_to_screened_changed

    if raw_to_normalized_changed and normalized_to_screened_changed:
        shift_path = "RAW_TO_NORMALIZED_TO_SCREENED_SHIFT"
    elif raw_to_normalized_changed:
        shift_path = "RAW_TO_NORMALIZED_SHIFT"
    elif normalized_to_screened_changed:
        shift_path = "NORMALIZED_TO_SCREENED_SHIFT"
    else:
        shift_path = "NO_SHIFT"

    return {
        "ok": True,
        "shift_path": shift_path,
        "raw_engine_winner_ticker": raw_engine_winner_ticker,
        "raw_engine_winner_status": raw_engine_winner_status,
        "normalized_engine_winner_ticker": normalized_engine_winner_ticker,
        "normalized_engine_winner_status": normalized_engine_winner_status,
        "normalized_engine_winner_final_verdict": normalized_engine_winner_final_verdict,
        "screened_live_winner_ticker": screened_live_winner_ticker,
        "screened_live_winner_final_verdict": screened_live_winner_final_verdict,
        "raw_to_normalized_changed": raw_to_normalized_changed,
        "normalized_to_screened_changed": normalized_to_screened_changed,
        "any_shift": any_shift,
        "screened_reason": screened_reason,
        "note": (
            "Raw engine selection, normalized winner, and screened live winner can differ. "
            "Use this block to track exactly where the handoff changed."
        ),
    }



def _json_safe_for_response(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_for_response(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_for_response(item) for item in value]
    if hasattr(value, "model_dump") and callable(getattr(value, "model_dump")):
        try:
            return _json_safe_for_response(value.model_dump())
        except Exception:
            return str(value)
    if hasattr(value, "dict") and callable(getattr(value, "dict")):
        try:
            return _json_safe_for_response(value.dict())
        except Exception:
            return str(value)
    return str(value)

def _coerce_error_reason(value: Any) -> str:
    if value is None:
        return "Candidate engine unavailable for this run."
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except Exception:
        return str(value)


def _build_on_demand_unavailable_payload(
    request: OnDemandRequest,
    *,
    market_context: Dict[str, Any],
    macro_context: Dict[str, Any],
    time_day_gate: Dict[str, Any],
    reason: str,
    error_type: str,
    status_code: int = 503,
) -> Dict[str, Any]:
    reason_text = _coerce_error_reason(reason)
    build_tag = "continuous_compact_ticker_summary_2026_04_13"
    failed_reasons = [reason_text]
    primary_blocker = "data_unavailable"

    simple_output = {
        "design_goal": "complex_inputs_simple_outputs",
        "good_idea_now": "NO",
        "ticker": None,
        "action": "stand down",
        "invalidation": "Unavailable while candidate engine is down for this run.",
        "setup_state": "NO TRADE",
        "why": reason_text,
        "signal_present": False,
    }

    ten_second_answers = [
        {
            "item": "allowed_setup_type",
            "question": "Is this one of the 3 allowed setup types?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "twentyfour_hour_supportive",
            "question": "Is 24H trend/context supportive?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "one_hour_clean_around_ema",
            "question": "Is 1H structure clean around 50 EMA?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "clear_room",
            "question": "Do we have clear room to next level?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "early_enough",
            "question": "Are we early enough, not overextended?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "iv_acceptable",
            "question": "Is IV acceptable for a debit spread?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "clear_trigger",
            "question": "Is there a clear entry trigger?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "invalidation_clear",
            "question": "Is invalidation clear: 1H close beyond 50 EMA?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "fits_risk",
            "question": "Does this fit risk budget?",
            "answer": "UNCONFIRMED",
        },
        {
            "item": "open_trade_already",
            "question": "Do we already have an open trade?",
            "answer": "NO" if request.open_positions == 0 else "YES",
        },
    ]

    checklist_items = [
        {"item": "allowed_setup_type", "yes": None},
        {"item": "twentyfour_hour_supportive", "yes": None},
        {"item": "one_hour_clean_around_ema", "yes": None},
        {"item": "clear_room", "yes": None},
        {"item": "early_enough", "yes": None},
        {"item": "clear_trigger", "yes": None},
        {"item": "liquidity_ok", "yes": None},
        {"item": "invalidation_clear", "yes": None},
        {"item": "fits_risk", "yes": None},
        {"item": "open_trade_already", "yes": request.open_positions > 0},
    ]

    winner_context = {
        "raw_engine_winner_ticker": None,
        "raw_engine_winner_status": "UNCONFIRMED",
        "normalized_engine_winner_ticker": None,
        "normalized_engine_winner_status": "UNCONFIRMED",
        "normalized_engine_winner_final_verdict": "NO_TRADE",
        "screened_live_winner_ticker": None,
        "screened_live_winner_final_verdict": "NO_TRADE",
        "changed_after_screening": False,
        "why_changed_after_screening": error_type,
    }

    engine_context = {
        "ok": False,
        "raw_best_ticker": None,
        "raw_status": "UNCONFIRMED",
        "raw_reason": reason_text,
        "normalized_best_ticker": None,
        "normalized_status": "UNCONFIRMED",
        "normalized_final_verdict": "NO_TRADE",
        "normalized_reason": error_type,
        "changed_from_raw_engine": False,
    }

    decision_context = {
        "ok": True,
        "ticker": None,
        "action": "stand down",
        "setup_state": "NO TRADE",
        "good_idea_now": "NO",
        "raw_engine": {"ticker": None, "status": "UNCONFIRMED", "reason": reason_text},
        "normalized_engine": {
            "ticker": None,
            "status": "UNCONFIRMED",
            "final_verdict": "NO_TRADE",
            "reason": error_type,
        },
        "screened": {"ticker": None, "final_verdict": "NO_TRADE", "reason": error_type},
        "primary_blocker": primary_blocker,
        "blockers": [primary_blocker],
        "failed_reasons": failed_reasons,
        "changed_from_raw_engine": False,
    }

    blocker_context = {
        "ok": True,
        "primary_blocker": primary_blocker,
        "blockers": [primary_blocker],
        "failed_reasons": failed_reasons,
        "trigger_present": False,
        "trigger_reason": error_type,
        "structure_ready": None,
        "setup_type": "UNCONFIRMED",
        "allowed_setup": False,
        "room_pass": False,
        "extension_blocks_now": None,
        "engine_status": "UNCONFIRMED",
        "final_verdict": "NO_TRADE",
        "action": "stand down",
        "setup_state": "NO TRADE",
        "good_idea_now": "NO",
    }

    screened_best_context = {
        "ok": False,
        "screened_best_ticker": None,
        "raw_engine_best_ticker": None,
        "normalized_engine_best_ticker": None,
        "engine_best_ticker": None,
        "changed_from_engine_best": False,
        "screened_final_verdict": "NO_TRADE",
        "screened_reason": error_type,
        "screened_checklist_failed_items": [primary_blocker],
        "engine_best_final_verdict_after_screen": "NO_TRADE",
        "engine_best_reason_after_screen": error_type,
    }

    trigger_state = {
        "ok": False,
        "trigger_present": False,
        "trigger_style": "close_above_recent_high",
        "trigger_level": None,
        "current_close": None,
        "why": error_type,
    }

    empty_context = {
        "ok": False,
        "reason": error_text if (error_text := reason_text) else error_type,
    }

    payload = {
        "ok": True,
        "mode": "on_demand",
        "build_tag": build_tag,
        "source_of_truth": "candidate_engine",
        "read_this_first": "simple_output",
        "engine_status": "UNCONFIRMED",
        "candidate_engine_status": "UNCONFIRMED",
        "final_verdict": "NO_TRADE",
        "best_ticker": None,
        "raw_engine_best_ticker": None,
        "engine_best_ticker": None,
        "winner_context": winner_context,
        "engine_context": engine_context,
        "decision_context": decision_context,
        "blocker_context": blocker_context,
        "live_map": {
            "ticker": None,
            "market_open": market_context.get("is_open"),
            "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
            "why": error_type,
            "backend_error": reason_text,
        },
        "trigger_context": {
            "ok": False,
            "trigger_present": False,
            "trigger_reason": error_type,
            "structure_ready": None,
            "trigger_style": "close_above_recent_high",
            "trigger_level": None,
            "current_close": None,
            "current_bar_raw_trigger_pass": False,
            "current_bar_gated_trigger_pass": False,
            "completed_candle_raw_trigger_pass": False,
            "completed_candle_gated_trigger_pass": False,
            "current_bar_relation_to_trigger_level": None,
            "current_bar_relation_to_ema50_1h": None,
            "trigger_scan_status": "unconfirmed",
            "why_trigger_scan_passes_or_fails": reason_text,
        },
        "entry_context": {
            "ok": False,
            "mid_candle_trade_available_now": False,
            "mid_candle_entry_state": "UNCONFIRMED",
            "mid_candle_raw_trigger_detected_now": False,
            "mid_candle_block_reason": error_type,
            "completed_candle_trade_available": False,
            "completed_candle_entry_state": "UNCONFIRMED",
            "completed_candle_raw_trigger_detected": False,
            "completed_candle_block_reason": error_type,
            "trigger_present": False,
            "trigger_reason": error_type,
            "structure_ready": None,
            "trigger_style": "close_above_recent_high",
            "trigger_level": None,
            "current_close": None,
            "current_bar_relation_to_trigger_level": None,
            "current_bar_relation_to_ema50_1h": None,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "allowed_setup": False,
            "setup_type": "UNCONFIRMED",
            "room_pass": False,
            "extension_blocks_now": None,
            "action": "stand down",
            "setup_state": "NO TRADE",
            "good_idea_now": "NO",
        },
        "intrabar_signal_context": {
            "ok": False,
            "ticker": None,
            "intrabar_signal_status": "UNCONFIRMED",
            "intrabar_trade_available_now": False,
            "intrabar_raw_signal_detected": False,
            "intrabar_block_reason": error_type,
            "intrabar_time_iso": None,
            "intrabar_close": None,
            "intrabar_trigger_level_relation": None,
            "intrabar_ema_relation": None,
            "completed_signal_status": "UNCONFIRMED",
            "completed_trade_available": False,
            "completed_raw_signal_detected": False,
            "completed_block_reason": error_type,
            "completed_time_iso": None,
            "completed_close": None,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "trigger_present": False,
            "trigger_reason": error_type,
            "structure_ready": None,
            "action": "stand down",
            "setup_state": "NO TRADE",
            "good_idea_now": "NO",
            "signal_note": reason_text,
        },
        "approval_context": {
            "ok": False,
            "ticker": None,
            "approval_status": "UNCONFIRMED",
            "approval_ready_now": False,
            "approval_ready_on_completed_candle": False,
            "intrabar_raw_signal_detected": False,
            "completed_raw_signal_detected": False,
            "structure_ready": None,
            "trigger_present": False,
            "trigger_reason": error_type,
            "allowed_setup": False,
            "setup_type": "UNCONFIRMED",
            "room_pass": False,
            "extension_blocks_now": None,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "next_flip_needed": primary_blocker,
            "action": "stand down",
            "setup_state": "NO TRADE",
            "good_idea_now": "NO",
            "approval_note": reason_text,
        },
        "approval_requirements_context": {
            "ok": False,
            "approval_path_status": "UNCONFIRMED",
            "approval_ready_now": False,
            "approval_ready_on_completed_candle": False,
            "intrabar_raw_signal_detected": False,
            "completed_raw_signal_detected": False,
            "next_flip_needed": primary_blocker,
            "missing_gates": [primary_blocker],
            "gate_statuses": [],
            "checklist_failed_items": [primary_blocker],
            "blockers": [primary_blocker],
            "allowed_setup": False,
            "setup_type": "UNCONFIRMED",
            "room_pass": False,
            "extension_blocks_now": None,
            "structure_ready": None,
            "trigger_present": False,
            "trigger_reason": error_type,
            "liquidity_ok": None,
            "market_open": market_context.get("is_open"),
            "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
            "macro_event_clear": None,
        },
        "approval_flip_context": {
            "ok": False,
            "flip_status": "UNCONFIRMED",
            "next_flip_needed": primary_blocker,
            "ready_gate_count": 0,
            "remaining_gate_count": 1,
            "ready_gates": [],
            "missing_gates": [primary_blocker],
            "intrabar_raw_signal_detected": False,
            "completed_raw_signal_detected": False,
            "approval_ready_now": False,
            "approval_ready_on_completed_candle": False,
            "mid_candle_trade_available_now": False,
            "completed_candle_trade_available": False,
            "intrabar_signal_status": "UNCONFIRMED",
            "approval_status": "UNCONFIRMED",
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "approval_note": reason_text,
        },
        "setup_eligibility_context": {
            "ok": False,
            "setup_type_detected": None,
            "setup_type_status": "UNCONFIRMED",
            "allowed_setup": False,
            "ten_second_check_answer": "UNCONFIRMED",
            "setup_route_status": "unconfirmed",
            "setup_route_reason": reason_text,
            "next_flip_needed": primary_blocker,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "approval_path_status": "UNCONFIRMED",
            "note": reason_text,
        },
        "setup_check_context": {
            "ok": False,
            "setup_type_detected": None,
            "setup_type_status": "UNCONFIRMED",
            "allowed_setup": False,
            "ten_second_check_item": "allowed_setup_type",
            "ten_second_check_answer": "UNCONFIRMED",
            "detected_but_not_eligible": False,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "note": reason_text,
        },
        "time_gate_check_context": {
            "ok": True,
            "entry_window_status": "OPEN" if time_day_gate.get("fresh_entry_allowed") else "CLOSED",
            "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
            "time_gate_reason": time_day_gate.get("reason"),
            "cutoff_et": time_day_gate.get("cutoff_et"),
            "ten_second_check_item": "early_enough",
            "ten_second_check_answer": "UNCONFIRMED",
            "early_enough_fails_from_time_gate": False,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "note": reason_text,
        },
        "final_reason_context": {
            "ok": True,
            "final_reason": reason_text,
            "screened_reason": error_type,
            "time_gate_reason": time_day_gate.get("reason"),
            "entry_window_status": "OPEN" if time_day_gate.get("fresh_entry_allowed") else "CLOSED",
            "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
            "early_enough_fails_from_time_gate": False,
            "primary_blocker": primary_blocker,
            "blockers": [primary_blocker],
            "note": reason_text,
        },
        "simple_output": simple_output,
        "screened_best_context": screened_best_context,
        "market_context": market_context,
        "macro_context": macro_context,
        "structure_context": {"ok": False, "why": reason_text},
        "adx_context": {"ok": False, "why": reason_text},
        "time_day_gate": time_day_gate,
        "iv_context": {"ok": False, "status": "unconfirmed", "why": reason_text},
        "python_validation": {
            "ok": False,
            "ticker": None,
            "ticker_allowed": False,
            "risk_preferred_band_ok": None,
            "risk_hard_max_ok": None,
            "open_positions_ok_for_new_trade": request.open_positions == 0,
            "max_one_open_position_rule": request.open_positions <= 1,
            "max_loss_dollars_1lot": None,
            "targets_confirmed": False,
            "target_40_pct_value": None,
            "target_50_pct_value": None,
            "target_60_pct_value": None,
            "target_70_pct_value": None,
            "exit_price_1h_ema50": None,
        },
        "ten_second_checklist": {
            "ok": False,
            "answers": ten_second_answers,
            "failed_items": [primary_blocker],
        },
        "liquidity_context": {"ok": False, "status": "unconfirmed", "why": reason_text},
        "trigger_state": trigger_state,
        "targets": {
            "ok": False,
            "debit": None,
            "max_loss_dollars_1lot": None,
            "target_40_pct_value": None,
            "target_50_pct_value": None,
            "target_60_pct_value": None,
            "target_70_pct_value": None,
        },
        "invalidation_level_1h_ema50": None,
        "checklist": {
            "ok": False,
            "items": checklist_items,
            "failed_items": [primary_blocker],
            "decision_blockers_priority": [primary_blocker],
        },
        "failed_reasons": failed_reasons,
        "other_ticker_candidates": [],
        "request": request.model_dump(),
        "candidate_engine": {
            "ok": False,
            "verdict": "UNCONFIRMED",
            "best_ticker": None,
            "reason": reason_text,
            "selection_mode": "unconfirmed",
            "primary_candidate": None,
            "backup_candidate": None,
            "ticker_summaries": [],
        },
        "candidate_engine_normalized": {
            "ok": False,
            "raw_best_ticker": None,
            "raw_verdict": "UNCONFIRMED",
            "raw_reason": reason_text,
            "normalized_best_ticker": None,
            "normalized_verdict": "UNCONFIRMED",
            "normalized_final_verdict": "NO_TRADE",
            "normalized_reason": error_type,
            "selection_mode": "unconfirmed",
        },
        "chart_check": {"ok": False, "why": reason_text},
        "chart_confirmation": {
            "confirmed": False,
            "message": reason_text,
            "fields": {},
        },
        "universe_chart_confirmation": {
            "ok": False,
            "requested": False,
            "all_tickers_confirmed": False,
            "confirmed_tickers": [],
            "unconfirmed_tickers": list(SYMBOL_ORDER),
            "tickers": [],
            "message": reason_text,
        },
        "user_facing": {
            "good_idea_now": "NO",
            "ticker": None,
            "action": "stand down",
            "invalidation": "Unavailable while candidate engine is down for this run.",
            "setup_state": "NO TRADE",
            "why": reason_text,
        },
        "candidate_context": {
            "active": False,
            "ticker": None,
            "availability_reason": reason_text,
            "good_idea_now": "NO",
            "action": "stand down",
            "setup_state": "NO TRADE",
            "setup_type": "UNCONFIRMED",
            "trend_label": "Unconfirmed",
            "trigger_state": error_type,
            "trigger_style": "close_above_recent_high",
            "trigger_level": None,
            "trigger_candle": None,
            "current_bar_behavior": None,
            "setup_route": {"setup_route_status": "unconfirmed", "why_setup_route_passes_or_fails": reason_text},
            "room_wall": {"room_wall_status": "unconfirmed", "why_room_or_wall_passes_or_fails": reason_text},
            "extension_quality": {"extension_quality_status": "unconfirmed", "why_extension_passes_or_fails": reason_text},
            "execution_quality": {"execution_quality_status": "unconfirmed", "why_execution_quality_passes_or_fails": reason_text},
            "event_gate": {"event_gate_status": "unconfirmed", "why_event_gate_passes_or_fails": reason_text},
            "options_structure": {"options_structure_status": "unconfirmed", "why_options_structure_passes_or_fails": reason_text},
            "wall_thesis_fit": {"wall_thesis_fit_status": "unconfirmed", "why_wall_thesis_fit_passes_or_fails": reason_text},
            "adx_filter": {"adx_filter_status": "unconfirmed", "why_adx_passes_or_fails": reason_text},
            "trigger_scan": {"trigger_scan_status": "unconfirmed", "why_trigger_scan_passes_or_fails": reason_text},
            "primary_entry_zone": None,
            "backup_entry_zone": None,
            "options": None,
            "levels": None,
            "targets": None,
            "primary_candidate": None,
            "backup_candidate": None,
            "invalidation": None,
            "checklist_failed_items": [primary_blocker],
            "decision_blockers_priority": [primary_blocker],
            "execution": {
                "ideal_path": "Retry when backend connectivity is restored.",
                "acceptable_path": "Stand down until candidate engine is reachable again.",
                "invalidation_1h_ema50": None,
                "market_open": market_context.get("is_open"),
                "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
                "macro_risk_level": macro_context.get("risk_level"),
                "major_event_today": macro_context.get("has_major_event_today"),
                "major_event_tomorrow": macro_context.get("has_major_event_tomorrow"),
            },
            "note": reason_text,
        },
        "two_path": {
            "ideal_path": "Retry when backend connectivity is restored.",
            "acceptable_path": "Stand down until candidate engine is reachable again.",
            "invalidation_1h_ema50": None,
        },
        "service_status": {
            "ok": False,
            "error_type": error_type,
            "status_code": status_code,
            "reason": reason_text,
        },
    }
    return payload




async def _build_on_demand_payload(request: OnDemandRequest) -> Dict[str, Any]:

    clean_option_type = _clean_option_type(request.option_type)
    market_context = _market_context_now()
    time_day_gate = _time_day_gate(market_context)
    macro_context = await _build_macro_context(request.macro_context_requested)

    if request.open_positions < 0 or request.open_positions > 1:
        raise HTTPException(status_code=400, detail="open_positions must be 0 or 1")
    if request.weekly_trade_count < 0:
        raise HTTPException(status_code=400, detail="weekly_trade_count must be >= 0")

    try:
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
    except httpx.TimeoutException:
        return _build_on_demand_unavailable_payload(
            request,
            market_context=market_context,
            macro_context=macro_context,
            time_day_gate=time_day_gate,
            reason="Broker auth request timed out. Candidate engine unavailable for this run.",
            error_type="broker_auth_timeout",
            status_code=503,
        )
    except HTTPException as exc:
        return _build_on_demand_unavailable_payload(
            request,
            market_context=market_context,
            macro_context=macro_context,
            time_day_gate=time_day_gate,
            reason=_coerce_error_reason(getattr(exc, "detail", None) or str(exc)),
            error_type="candidate_engine_http_error",
            status_code=getattr(exc, "status_code", 503),
        )
    except httpx.HTTPError as exc:
        return _build_on_demand_unavailable_payload(
            request,
            market_context=market_context,
            macro_context=macro_context,
            time_day_gate=time_day_gate,
            reason=f"Broker request failed: {exc.__class__.__name__}",
            error_type="candidate_engine_http_error",
            status_code=503,
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
    freeze_to_raw_engine = _should_freeze_winner_to_raw_engine(
        summary_payload=summary_payload,
        market_context=market_context,
        time_day_gate=time_day_gate,
    )
    selected = _select_screened_best_candidate(
        screened_candidates,
        raw_engine_best_ticker=summary_payload.get("best_ticker"),
        freeze_to_raw_engine=freeze_to_raw_engine,
    )

    best_ticker = selected.get("symbol") if selected else summary_payload.get("best_ticker")
    raw_engine_status = summary_payload.get("verdict", "NO_TRADE")
    final_verdict = selected.get("final_verdict", "NO_TRADE") if selected else "NO_TRADE"
    engine_status = _normalize_top_level_status(final_verdict)
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
        engine_status=raw_engine_status,
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
    normalized_engine_winner_ticker = best_ticker
    normalized_engine_winner_status = engine_status
    normalized_engine_winner_final_verdict = final_verdict
    screened_live_winner_ticker = best_ticker
    screened_live_winner_final_verdict = final_verdict
    changed_after_screening = raw_engine_winner_ticker != screened_live_winner_ticker
    why_changed_after_screening = (
        selected_reason if changed_after_screening else None
    )
    failed_reasons_block = _failed_reason_messages(
        checklist=checklist_block,
        time_day_gate=time_day_gate,
        market_context=market_context,
        structure_context=structure_context,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
    )

    live_map_block = _build_live_map_block(
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
    )
    trap_check_context_block = live_map_block.get("trap_check_context") or _build_trap_check_context(structure_context)
    entry_context_block = _build_entry_context_block(
        trigger_state=trigger_state,
        live_map=live_map_block,
        checklist_block=checklist_block,
        structure_context=structure_context,
        user_facing=user_facing_block,
    )
    intrabar_signal_context_block = _build_intrabar_signal_context_block(
        entry_context=entry_context_block,
        live_map=live_map_block,
        user_facing=user_facing_block,
    )
    approval_context_block = _build_approval_context_block(
        entry_context=entry_context_block,
        intrabar_signal_context=intrabar_signal_context_block,
        checklist_block=checklist_block,
        structure_context=structure_context,
        trigger_state=trigger_state,
        user_facing=user_facing_block,
    )
    approval_requirements_context_block = _build_approval_requirements_context_block(
        checklist_block=checklist_block,
        structure_context=structure_context,
        trigger_state=trigger_state,
        market_context=market_context,
        time_day_gate=time_day_gate,
        macro_context=macro_context,
        liquidity_context=liquidity_context,
        approval_context=approval_context_block,
    )
    approval_flip_context_block = _build_approval_flip_context_block(
        approval_requirements_context=approval_requirements_context_block,
        approval_context=approval_context_block,
        entry_context=entry_context_block,
        intrabar_signal_context=intrabar_signal_context_block,
    )
    universe_chart_confirmation_block = _build_universe_chart_confirmation_block(
        request=request,
        screened_candidates=screened_candidates,
        include_chart_checks=request.include_chart_checks,
    )
    setup_eligibility_context_block = _build_setup_eligibility_context_block(
        structure_context=structure_context,
        live_map=live_map_block,
        checklist_block=checklist_block,
        approval_requirements_context=approval_requirements_context_block,
    )
    setup_check_context_block = _build_setup_check_context_block(
        structure_context=structure_context,
        ten_second_checklist_block=ten_second_checklist_block,
        setup_eligibility_context=setup_eligibility_context_block,
    )
    time_gate_check_context_block = _build_time_gate_check_context_block(
        time_day_gate=time_day_gate,
        ten_second_checklist_block=ten_second_checklist_block,
        checklist_block=checklist_block,
    )
    screened_best_context_block = _build_screened_best_context(
        selected=selected,
        engine_best_ticker=summary_payload.get("best_ticker"),
        screened_candidates=screened_candidates,
    )
    final_reason_context_block = _build_final_reason_context_block(
        user_facing=user_facing_block,
        screened_best_context=screened_best_context_block,
        time_gate_check_context=time_gate_check_context_block,
        checklist_block=checklist_block,
    )
    reason_stack_context_block = _build_reason_stack_context_block(
        final_reason_context=final_reason_context_block,
        checklist_block=checklist_block,
        failed_reasons=failed_reasons_block,
    )
    winner_shift_context_block = _build_winner_shift_context_block(
        raw_engine_winner_ticker=raw_engine_winner_ticker,
        raw_engine_winner_status=raw_engine_winner_status,
        normalized_engine_winner_ticker=normalized_engine_winner_ticker,
        normalized_engine_winner_status=normalized_engine_winner_status,
        normalized_engine_winner_final_verdict=normalized_engine_winner_final_verdict,
        screened_live_winner_ticker=screened_live_winner_ticker,
        screened_live_winner_final_verdict=screened_live_winner_final_verdict,
        screened_reason=screened_best_context_block.get("screened_reason"),
    )
    effective_payload_checklist_block = dict(checklist_block)
    effective_payload_checklist_block["effective_failed_items"] = _effective_blockers(
        checklist_block,
        screened_reason=screened_best_context_block.get("screened_reason"),
        time_gate_reason=time_day_gate.get("reason"),
    )
    effective_payload_checklist_block["effective_decision_blockers_priority"] = list(
        effective_payload_checklist_block["effective_failed_items"]
    )
    effective_payload_checklist_block["global_gate_failures"] = [
        item
        for item in effective_payload_checklist_block["effective_failed_items"]
        if item not in (checklist_block.get("failed_items") or [])
    ]

    return {
        "ok": True,
        "mode": "on_demand",
        "build_tag": "continuous_compact_ticker_summary_market_closed_tester_2026_04_13",
        "source_of_truth": "candidate_engine",
        "read_this_first": "simple_output",
        "engine_status": engine_status,
        "candidate_engine_status": engine_status,
        "final_verdict": final_verdict,
        "best_ticker": best_ticker,
        "raw_engine_best_ticker": raw_engine_winner_ticker,
        "engine_best_ticker": normalized_engine_winner_ticker,
        "winner_context": {
            "raw_engine_winner_ticker": raw_engine_winner_ticker,
            "raw_engine_winner_status": raw_engine_winner_status,
            "normalized_engine_winner_ticker": normalized_engine_winner_ticker,
            "normalized_engine_winner_status": normalized_engine_winner_status,
            "normalized_engine_winner_final_verdict": normalized_engine_winner_final_verdict,
            "screened_live_winner_ticker": screened_live_winner_ticker,
            "screened_live_winner_final_verdict": screened_live_winner_final_verdict,
            "changed_after_screening": changed_after_screening,
            "why_changed_after_screening": why_changed_after_screening,
        },
        "engine_context": _build_engine_context_block(
            summary_payload=summary_payload,
            selected=selected,
            engine_status=engine_status,
            final_verdict=final_verdict,
            best_ticker=best_ticker,
        ),
        "decision_context": _build_decision_context_block(
            summary_payload=summary_payload,
            selected=selected,
            engine_status=engine_status,
            final_verdict=final_verdict,
            best_ticker=best_ticker,
            checklist_block=checklist_block,
            failed_reasons=failed_reasons_block,
            user_facing=user_facing_block,
        ),
        "blocker_context": _build_blocker_context_block(
            checklist_block=checklist_block,
            failed_reasons=failed_reasons_block,
            trigger_state=trigger_state,
            structure_context=structure_context,
            engine_status=engine_status,
            final_verdict=final_verdict,
            user_facing=user_facing_block,
        ),
        "live_map": live_map_block,
        "trap_check_context": trap_check_context_block,
        "trigger_context": _build_trigger_context_block(
            trigger_state=trigger_state,
            live_map=live_map_block,
        ),
        "entry_context": entry_context_block,
        "intrabar_signal_context": intrabar_signal_context_block,
        "approval_context": approval_context_block,
        "approval_requirements_context": approval_requirements_context_block,
        "approval_flip_context": approval_flip_context_block,
        "setup_eligibility_context": setup_eligibility_context_block,
        "setup_check_context": setup_check_context_block,
        "time_gate_check_context": time_gate_check_context_block,
        "final_reason_context": final_reason_context_block,
        "reason_stack_context": reason_stack_context_block,
        "winner_shift_context": winner_shift_context_block,
        "simple_output": _build_simple_output_block(
            user_facing=user_facing_block,
            trigger_state=trigger_state,
        ),
        "screened_best_context": screened_best_context_block,
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
        "checklist": effective_payload_checklist_block,
        "failed_reasons": failed_reasons_block,
        "compact_ticker_summaries": _build_compact_ticker_summaries(
            screened_candidates,
            time_day_gate=time_day_gate,
        ),
        "other_ticker_candidates": _screened_other_candidates(
            screened_candidates,
            best_ticker,
            request=request,
        ),
        "request": request.model_dump(),
        "candidate_engine": summary_payload,
        "candidate_engine_normalized": _build_candidate_engine_normalized_block(
            summary_payload=summary_payload,
            selected=selected,
            engine_status=engine_status,
            final_verdict=final_verdict,
            best_ticker=best_ticker,
        ),
        "chart_check": chart_check_block,
        "chart_confirmation": _build_chart_confirmation_block(
            request=request,
            chart_check=chart_check,
            chart_check_error=chart_check_error,
            structure_context=structure_context,
        ),
        "universe_chart_confirmation": universe_chart_confirmation_block,
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



class ContinuousShadowRequest(OnDemandRequest):
    profile_name: str = "default"
    persist_state: bool = True


def _model_dump(model: BaseModel) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _sanitize_continuous_profile_name(profile_name: Optional[str]) -> str:
    raw_name = (profile_name or "default").strip()
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw_name).strip("._-")
    return (cleaned or "default")[:64]


def _continuous_state_dir() -> Path:
    state_dir = Path(os.getenv("SAFE_FAST_CONTINUOUS_STATE_DIR", "/tmp/safe_fast_continuous"))
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


def _continuous_shadow_to_on_demand_request(request: ContinuousShadowRequest) -> OnDemandRequest:
    payload = _model_dump(request)
    payload.pop("profile_name", None)
    payload.pop("persist_state", None)
    return OnDemandRequest(**payload)


def _continuous_profile_identity_payload(request: OnDemandRequest) -> Dict[str, Any]:
    request_payload = _model_dump(request)
    stable_payload = dict(request_payload)
    stable_payload.pop("open_positions", None)
    stable_payload.pop("weekly_trade_count", None)
    return stable_payload


def _continuous_profile_key(profile_name: str, request: OnDemandRequest) -> str:
    stable_payload = _continuous_profile_identity_payload(request)
    digest = hashlib.sha1(
        json.dumps(stable_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:12]
    return f"{profile_name}__{digest}"


def _continuous_state_path(profile_key: str) -> Path:
    return _continuous_state_dir() / f"{profile_key}.json"


def _load_continuous_state(profile_key: str) -> Dict[str, Any]:
    state_path = _continuous_state_path(profile_key)
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text())
    except Exception:
        return {}


def _save_continuous_state(profile_key: str, payload: Dict[str, Any]) -> None:
    state_path = _continuous_state_path(profile_key)
    temp_path = state_path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    temp_path.replace(state_path)


_CONTINUOUS_STRUCTURE_BLOCKER_STATE_MAP: Dict[str, str] = {
    "allowed_setup_type": "BLOCKED_SETUP_TYPE",
    "twentyfour_hour_supportive": "BLOCKED_24H_CONTEXT",
    "one_hour_clean_around_ema": "BLOCKED_1H_STRUCTURE",
    "clear_room": "BLOCKED_ROOM",
    "early_enough": "BLOCKED_EXTENSION",
    "clear_trigger": "BLOCKED_TRIGGER",
    "liquidity_ok": "BLOCKED_LIQUIDITY",
    "invalidation_clear": "BLOCKED_INVALIDATION",
    "fits_risk": "BLOCKED_RISK",
}

_CONTINUOUS_STRUCTURE_FAILED_REASON_STATE_MAP: Dict[str, str] = {
    "setup type is not allowed": "BLOCKED_SETUP_TYPE",
    "24h context is not supportive": "BLOCKED_24H_CONTEXT",
    "1h structure around the 50 ema is not clean": "BLOCKED_1H_STRUCTURE",
    "room to the first wall fails": "BLOCKED_ROOM",
    "entry is too late or overextended for safe-fast": "BLOCKED_EXTENSION",
    "no valid live trigger is present": "BLOCKED_TRIGGER",
    "options liquidity is too wide for a clean debit spread entry": "BLOCKED_LIQUIDITY",
    "invalidation is not clear": "BLOCKED_INVALIDATION",
    "risk does not fit the safe-fast budget": "BLOCKED_RISK",
}

_CONTINUOUS_STATE_FAMILY_MAP: Dict[str, str] = {
    "STALE_OR_UNCONFIRMED": "SYSTEM",
    "EXIT_NOW": "EXIT",
    "APPROVAL_READY": "SIGNAL",
    "PENDING_COMPLETED_CANDLE_APPROVAL": "SIGNAL",
    "PENDING_TRIGGER_CONFIRMATION": "SIGNAL",
    "BLOCKED_OPEN_POSITION": "ACCOUNT",
    "BLOCKED_WEEKLY_CAP": "ACCOUNT",
    "BLOCKED_IV_HIGH": "IV",
    "WAIT_MARKET_OPEN": "TIME",
    "BLOCKED_TIME_GATE": "TIME",
    "BLOCKED_SETUP_TYPE": "STRUCTURE",
    "BLOCKED_24H_CONTEXT": "STRUCTURE",
    "BLOCKED_1H_STRUCTURE": "STRUCTURE",
    "BLOCKED_ROOM": "STRUCTURE",
    "BLOCKED_EXTENSION": "STRUCTURE",
    "BLOCKED_TRIGGER": "STRUCTURE",
    "BLOCKED_LIQUIDITY": "STRUCTURE",
    "BLOCKED_INVALIDATION": "STRUCTURE",
    "BLOCKED_RISK": "STRUCTURE",
    "NO_CANDIDATE": "CANDIDATE",
    "BLOCKED_STRUCTURAL": "STRUCTURE",
}


def _ordered_unique_strings(values: List[Any]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _derive_continuous_structure_state(snapshot: Dict[str, Any]) -> Optional[str]:
    primary_blocker = snapshot.get("primary_blocker")
    decision_blockers = _ordered_unique_strings(snapshot.get("decision_blockers") or [])
    failed_reasons = _ordered_unique_strings(snapshot.get("failed_reasons") or [])

    blocker_priority: List[str] = []
    if isinstance(primary_blocker, str) and primary_blocker.strip():
        blocker_priority.append(primary_blocker.strip())
    blocker_priority.extend(
        blocker
        for blocker in decision_blockers
        if blocker not in blocker_priority
    )

    for blocker in blocker_priority:
        mapped_state = _CONTINUOUS_STRUCTURE_BLOCKER_STATE_MAP.get(blocker)
        if mapped_state:
            return mapped_state

    for failed_reason in failed_reasons:
        mapped_state = _CONTINUOUS_STRUCTURE_FAILED_REASON_STATE_MAP.get(
            failed_reason.lower()
        )
        if mapped_state:
            return mapped_state

    return None


def _continuous_state_family(state: Optional[str]) -> str:
    if not state:
        return "UNKNOWN"
    return _CONTINUOUS_STATE_FAMILY_MAP.get(str(state), "UNKNOWN")


def _derive_continuous_state_source(
    snapshot: Dict[str, Any],
    current_state: Optional[str],
    latent_structure_state: Optional[str],
) -> str:
    if current_state in {"BLOCKED_OPEN_POSITION", "BLOCKED_WEEKLY_CAP"}:
        return "account_gate"
    if current_state in {"WAIT_MARKET_OPEN", "BLOCKED_TIME_GATE"}:
        return "time_gate"
    if current_state == "BLOCKED_IV_HIGH":
        return "iv_gate"
    if current_state in {"APPROVAL_READY", "PENDING_COMPLETED_CANDLE_APPROVAL", "PENDING_TRIGGER_CONFIRMATION"}:
        return "signal_state"
    if current_state == "EXIT_NOW":
        return "exit_state"
    if current_state == "NO_CANDIDATE":
        return "candidate_engine"
    if current_state == latent_structure_state and current_state is not None:
        decision_blockers = _ordered_unique_strings(snapshot.get("decision_blockers") or [])
        primary_blocker = snapshot.get("primary_blocker")
        if isinstance(primary_blocker, str) and primary_blocker in _CONTINUOUS_STRUCTURE_BLOCKER_STATE_MAP:
            return "primary_blocker"
        for blocker in decision_blockers:
            if blocker in _CONTINUOUS_STRUCTURE_BLOCKER_STATE_MAP:
                return "decision_blocker"
        return "failed_reason"
    if current_state == "BLOCKED_STRUCTURAL":
        return "structural_fallback"
    return "system"


def _derive_continuous_state_reason(
    snapshot: Dict[str, Any],
    current_state: Optional[str],
    latent_structure_state: Optional[str],
) -> Optional[str]:
    if current_state in {"BLOCKED_OPEN_POSITION", "BLOCKED_WEEKLY_CAP"}:
        return snapshot.get("primary_blocker") or snapshot.get("next_flip_needed")
    if current_state in {"WAIT_MARKET_OPEN", "BLOCKED_TIME_GATE"}:
        return snapshot.get("time_gate_reason") or (snapshot.get("time_day_gate") or {}).get("reason")
    if current_state == "BLOCKED_IV_HIGH":
        return snapshot.get("iv_status")
    if current_state == "EXIT_NOW":
        return "exit_now"
    if current_state in {"APPROVAL_READY", "PENDING_COMPLETED_CANDLE_APPROVAL", "PENDING_TRIGGER_CONFIRMATION"}:
        return current_state.lower()
    if current_state == "NO_CANDIDATE":
        return snapshot.get("primary_blocker") or "no_candidate_available"
    if current_state == latent_structure_state and current_state is not None:
        primary_blocker = snapshot.get("primary_blocker")
        if isinstance(primary_blocker, str) and primary_blocker in _CONTINUOUS_STRUCTURE_BLOCKER_STATE_MAP:
            return primary_blocker
        for blocker in _ordered_unique_strings(snapshot.get("decision_blockers") or []):
            if blocker in _CONTINUOUS_STRUCTURE_BLOCKER_STATE_MAP:
                return blocker
        for failed_reason in _ordered_unique_strings(snapshot.get("failed_reasons") or []):
            if failed_reason.lower() in _CONTINUOUS_STRUCTURE_FAILED_REASON_STATE_MAP:
                return failed_reason
    if current_state == "BLOCKED_STRUCTURAL":
        return snapshot.get("primary_blocker")
    return None


def _derive_continuous_state_from_snapshot(snapshot: Dict[str, Any]) -> str:
    if not snapshot.get("on_demand_ok", False):
        return "STALE_OR_UNCONFIRMED"

    primary_blocker = snapshot.get("primary_blocker")
    next_flip_needed = snapshot.get("next_flip_needed")
    decision_blockers = _ordered_unique_strings(snapshot.get("decision_blockers") or [])
    failed_reasons = _ordered_unique_strings(snapshot.get("failed_reasons") or [])
    summary = snapshot.get("summary") or {}
    iv_status = snapshot.get("iv_status")
    market_open = snapshot.get("market_open")
    fresh_entry_allowed = snapshot.get("fresh_entry_allowed")
    time_gate_reason = snapshot.get("time_gate_reason")

    if primary_blocker == "open_trade_already" or next_flip_needed == "open_trade_already":
        return "BLOCKED_OPEN_POSITION"
    if primary_blocker == "weekly_trade_cap_reached" or next_flip_needed == "weekly_trade_cap_reached":
        return "BLOCKED_WEEKLY_CAP"

    if summary.get("setup_state") == "INVALIDATED" or str(summary.get("action", "")).lower() == "exit now":
        return "EXIT_NOW"
    if snapshot.get("approval_ready_now"):
        return "APPROVAL_READY"
    if snapshot.get("approval_ready_on_completed_candle"):
        return "PENDING_COMPLETED_CANDLE_APPROVAL"
    if snapshot.get("trigger_present"):
        return "PENDING_TRIGGER_CONFIRMATION"

    if iv_status == "high":
        return "BLOCKED_IV_HIGH"

    structure_state = _derive_continuous_structure_state(snapshot)
    if market_open is False and fresh_entry_allowed is False:
        if time_gate_reason == "market_closed":
            if structure_state:
                return structure_state
            return "WAIT_MARKET_OPEN"
        if time_gate_reason:
            return "BLOCKED_TIME_GATE"
    if time_gate_reason == "market_closed":
        if structure_state:
            return structure_state
        return "WAIT_MARKET_OPEN"
    if time_gate_reason:
        return "BLOCKED_TIME_GATE"

    if structure_state:
        return structure_state

    if primary_blocker == "no_candidate_available":
        return "NO_CANDIDATE"
    if summary.get("ticker") == "UNKNOWN" and not primary_blocker and not decision_blockers and not failed_reasons:
        return "NO_CANDIDATE"

    if primary_blocker:
        return "BLOCKED_STRUCTURAL"
    return "STALE_OR_UNCONFIRMED"



def _build_market_closed_tester_block(on_demand_payload: Dict[str, Any]) -> Dict[str, Any]:
    market_context = on_demand_payload.get("market_context") or {}
    time_day_gate = on_demand_payload.get("time_day_gate") or {}
    approval_context = on_demand_payload.get("approval_context") or {}
    approval_requirements_context = on_demand_payload.get("approval_requirements_context") or {}
    decision_context = on_demand_payload.get("decision_context") or {}
    trigger_context = on_demand_payload.get("trigger_context") or {}
    structure_context = on_demand_payload.get("structure_context") or {}
    final_verdict = str(on_demand_payload.get("final_verdict") or "unconfirmed").upper()

    market_closed_context_only = bool(
        market_context.get("is_open") is False
        or time_day_gate.get("reason") == "market_closed"
    )

    structural_blockers = _ordered_unique_strings(
        approval_requirements_context.get("raw_checklist_failed_items")
        or decision_context.get("blockers")
        or []
    )
    structural_blockers = [item for item in structural_blockers if item != "time_day_gate"]
    structural_primary_blocker = structural_blockers[0] if structural_blockers else None

    intrabar_raw_signal_detected = bool(approval_context.get("intrabar_raw_signal_detected") is True)
    completed_raw_signal_detected = bool(approval_context.get("completed_raw_signal_detected") is True)
    raw_signal_present_if_open = intrabar_raw_signal_detected or completed_raw_signal_detected

    if structural_blockers:
        underlying_structural_verdict = "NO_TRADE"
        would_be_trade_if_open = False
        testing_takeaway = (
            "After-hours tester says structure still fails even if the market were open."
        )
    elif raw_signal_present_if_open or final_verdict == "TRADE":
        underlying_structural_verdict = "TRADE"
        would_be_trade_if_open = True
        testing_takeaway = (
            "After-hours tester says this would qualify as a live SAFE-FAST trade if the market were open."
        )
    else:
        underlying_structural_verdict = "PENDING"
        would_be_trade_if_open = False
        testing_takeaway = (
            "After-hours tester says structure is not failing on non-time gates, but no approved live trigger is present yet."
        )

    return {
        "ok": True,
        "market_closed_context_only": market_closed_context_only,
        "underlying_structural_verdict": underlying_structural_verdict,
        "underlying_structural_primary_blocker": structural_primary_blocker,
        "underlying_structural_blockers": structural_blockers,
        "would_be_trade_if_open": would_be_trade_if_open,
        "raw_signal_present_if_open": raw_signal_present_if_open,
        "intrabar_raw_signal_detected": intrabar_raw_signal_detected,
        "completed_raw_signal_detected": completed_raw_signal_detected,
        "setup_type": structure_context.get("setup_type"),
        "market_open": market_context.get("is_open"),
        "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
        "trigger_present_live": trigger_context.get("trigger_present"),
        "trigger_reason_live": trigger_context.get("trigger_reason"),
        "testing_takeaway": testing_takeaway,
    }


def _build_continuous_snapshot(
    *,
    on_demand_payload: Dict[str, Any],
    request: OnDemandRequest,
    profile_name: str,
    profile_key: str,
) -> Dict[str, Any]:
    request_payload = _model_dump(request)
    simple_output = on_demand_payload.get("simple_output") or {}
    user_facing = on_demand_payload.get("user_facing") or {}
    decision_context = on_demand_payload.get("decision_context") or {}
    approval_context = on_demand_payload.get("approval_context") or {}
    approval_requirements_context = on_demand_payload.get("approval_requirements_context") or {}
    trigger_context = on_demand_payload.get("trigger_context") or {}
    market_context = on_demand_payload.get("market_context") or {}
    winner_shift_context = on_demand_payload.get("winner_shift_context") or {}
    iv_context = on_demand_payload.get("iv_context") or {}
    time_day_gate = on_demand_payload.get("time_day_gate") or {}
    market_closed_tester = _build_market_closed_tester_block(on_demand_payload)

    snapshot: Dict[str, Any] = {
        "timestamp_et": market_context.get("as_of_et") or datetime.now(NY_TZ).isoformat(),
        "profile_name": profile_name,
        "profile_key": profile_key,
        "request_profile": request_payload,
        "build_tag": on_demand_payload.get("build_tag"),
        "on_demand_ok": bool(on_demand_payload.get("ok")),
        "best_ticker": on_demand_payload.get("best_ticker"),
        "final_verdict": on_demand_payload.get("final_verdict"),
        "user_facing_why": user_facing.get("why"),
        "primary_blocker": decision_context.get("primary_blocker"),
        "decision_blockers": decision_context.get("blockers") or [],
        "failed_reasons": decision_context.get("failed_reasons") or [],
        "next_flip_needed": approval_context.get("next_flip_needed")
        or approval_requirements_context.get("next_flip_needed"),
        "trigger_present": trigger_context.get("trigger_present"),
        "trigger_reason": trigger_context.get("trigger_reason"),
        "structure_ready": trigger_context.get("structure_ready"),
        "approval_ready_now": approval_context.get("approval_ready_now"),
        "approval_ready_on_completed_candle": approval_context.get("approval_ready_on_completed_candle"),
        "open_positions": request_payload.get("open_positions"),
        "weekly_trade_count": request_payload.get("weekly_trade_count"),
        "invalidation": simple_output.get("invalidation"),
        "invalidation_level_1h_ema50": on_demand_payload.get("invalidation_level_1h_ema50"),
        "targets": on_demand_payload.get("targets") or {},
        "winner_shift_context": winner_shift_context,
        "iv_context": iv_context,
        "iv_status": iv_context.get("status"),
        "market_context": market_context,
        "market_open": market_context.get("is_open"),
        "fresh_entry_allowed": time_day_gate.get("fresh_entry_allowed"),
        "time_gate_reason": time_day_gate.get("reason"),
        "time_day_gate": time_day_gate,
        "summary": {
            "ticker": simple_output.get("ticker"),
            "action": simple_output.get("action"),
            "setup_state": simple_output.get("setup_state"),
            "good_idea_now": simple_output.get("good_idea_now"),
            "why": simple_output.get("why"),
        },
        "market_closed_tester": market_closed_tester,
        "compact_ticker_summaries": on_demand_payload.get("compact_ticker_summaries") or [],
    }
    snapshot["latent_structure_state"] = _derive_continuous_structure_state(snapshot)
    snapshot["current_state"] = _derive_continuous_state_from_snapshot(snapshot)
    snapshot["state_family"] = _continuous_state_family(snapshot.get("current_state"))
    snapshot["state_source"] = _derive_continuous_state_source(
        snapshot,
        snapshot.get("current_state"),
        snapshot.get("latent_structure_state"),
    )
    snapshot["state_reason"] = _derive_continuous_state_reason(
        snapshot,
        snapshot.get("current_state"),
        snapshot.get("latent_structure_state"),
    )
    snapshot["readable_summary"] = _build_continuous_readable_summary(snapshot)
    return snapshot


def _continuous_changed_fields(previous: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    tracked_fields = [
        "current_state",
        "latent_structure_state",
        "state_family",
        "state_source",
        "state_reason",
        "best_ticker",
        "final_verdict",
        "primary_blocker",
        "next_flip_needed",
        "trigger_present",
        "structure_ready",
        "approval_ready_now",
        "approval_ready_on_completed_candle",
        "open_positions",
        "weekly_trade_count",
    ]
    changes: Dict[str, Dict[str, Any]] = {}
    for field in tracked_fields:
        previous_value = previous.get(field)
        current_value = current.get(field)
        if previous_value != current_value:
            changes[field] = {"previous": previous_value, "current": current_value}
    return changes


def _compare_continuous_snapshots(
    previous: Optional[Dict[str, Any]],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    if not previous:
        return {
            "transition_type": "INITIAL_SNAPSHOT",
            "severity": "info",
            "meaningful_transition": False,
            "should_alert_candidate": False,
            "changed_fields": {},
            "summary": "Initial shadow snapshot created.",
        }

    changed_fields = _continuous_changed_fields(previous, current)
    meaningful_transition = bool(changed_fields)

    if not meaningful_transition:
        transition_type = "NO_MEANINGFUL_CHANGE"
        severity = "info"
        summary = "No meaningful state change."
    elif previous.get("current_state") != current.get("current_state"):
        transition_type = "STATE_CHANGED"
        severity = "high" if current.get("current_state") in {"APPROVAL_READY", "EXIT_NOW"} else "medium"
        summary = (
            f"State changed from {previous.get('current_state')} to {current.get('current_state')}."
        )
    elif previous.get("best_ticker") != current.get("best_ticker"):
        transition_type = "WINNER_CHANGED"
        severity = "medium"
        summary = f"Best ticker changed from {previous.get('best_ticker')} to {current.get('best_ticker')}."
    elif previous.get("primary_blocker") != current.get("primary_blocker"):
        transition_type = "PRIMARY_BLOCKER_CHANGED"
        severity = "medium"
        summary = (
            f"Primary blocker changed from {previous.get('primary_blocker')} to {current.get('primary_blocker')}."
        )
    elif previous.get("next_flip_needed") != current.get("next_flip_needed"):
        transition_type = "NEXT_FLIP_CHANGED"
        severity = "medium"
        summary = (
            f"Next flip needed changed from {previous.get('next_flip_needed')} to {current.get('next_flip_needed')}."
        )
    elif (
        previous.get("approval_ready_now") != current.get("approval_ready_now")
        or previous.get("approval_ready_on_completed_candle")
        != current.get("approval_ready_on_completed_candle")
    ):
        transition_type = "APPROVAL_STATE_CHANGED"
        severity = "high" if current.get("approval_ready_now") else "medium"
        summary = "Approval state changed."
    elif previous.get("trigger_present") != current.get("trigger_present"):
        transition_type = "TRIGGER_STATE_CHANGED"
        severity = "medium"
        summary = "Trigger state changed."
    elif (
        previous.get("open_positions") != current.get("open_positions")
        or previous.get("weekly_trade_count") != current.get("weekly_trade_count")
    ):
        transition_type = "ACCOUNT_STATE_CHANGED"
        severity = "medium"
        summary = "Account state changed."
    else:
        transition_type = "DETAIL_CHANGED"
        severity = "info"
        summary = "Tracked shadow fields changed."

    return {
        "transition_type": transition_type,
        "severity": severity,
        "meaningful_transition": meaningful_transition,
        "should_alert_candidate": meaningful_transition,
        "changed_fields": changed_fields,
        "summary": summary,
    }


def _continuous_transition_fingerprint(
    *,
    current_snapshot: Dict[str, Any],
    transition_summary: Dict[str, Any],
) -> str:
    payload = {
        "transition_type": transition_summary.get("transition_type"),
        "current_state": current_snapshot.get("current_state"),
        "best_ticker": current_snapshot.get("best_ticker"),
        "primary_blocker": current_snapshot.get("primary_blocker"),
        "next_flip_needed": current_snapshot.get("next_flip_needed"),
        "trigger_present": current_snapshot.get("trigger_present"),
        "approval_ready_now": current_snapshot.get("approval_ready_now"),
        "approval_ready_on_completed_candle": current_snapshot.get("approval_ready_on_completed_candle"),
        "open_positions": current_snapshot.get("open_positions"),
        "weekly_trade_count": current_snapshot.get("weekly_trade_count"),
    }
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _build_continuous_alert_payload(
    *,
    previous_snapshot: Optional[Dict[str, Any]],
    current_snapshot: Dict[str, Any],
    transition_summary: Dict[str, Any],
    should_alert: bool,
) -> Optional[Dict[str, Any]]:
    if not previous_snapshot or not transition_summary.get("meaningful_transition"):
        return None

    summary = current_snapshot.get("summary") or {}
    return {
        "should_alert": should_alert,
        "transition_type": transition_summary.get("transition_type"),
        "severity": transition_summary.get("severity"),
        "message": transition_summary.get("summary"),
        "ticker": summary.get("ticker"),
        "state": current_snapshot.get("current_state"),
        "primary_blocker": current_snapshot.get("primary_blocker"),
        "next_flip_needed": current_snapshot.get("next_flip_needed"),
        "good_idea_now": summary.get("good_idea_now"),
        "action": summary.get("action"),
    }


def _build_continuous_on_demand_excerpt(on_demand_payload: Dict[str, Any]) -> Dict[str, Any]:
    decision_context = on_demand_payload.get("decision_context") or {}
    approval_context = on_demand_payload.get("approval_context") or {}
    trigger_context = on_demand_payload.get("trigger_context") or {}

    return {
        "build_tag": on_demand_payload.get("build_tag"),
        "simple_output": on_demand_payload.get("simple_output"),
        "user_facing": on_demand_payload.get("user_facing"),
        "decision_context": {
            "primary_blocker": decision_context.get("primary_blocker"),
            "blockers": decision_context.get("blockers"),
            "failed_reasons": decision_context.get("failed_reasons"),
        },
        "approval_context": {
            "primary_blocker": approval_context.get("primary_blocker"),
            "next_flip_needed": approval_context.get("next_flip_needed"),
            "approval_ready_now": approval_context.get("approval_ready_now"),
            "approval_ready_on_completed_candle": approval_context.get(
                "approval_ready_on_completed_candle"
            ),
        },
        "trigger_context": {
            "trigger_present": trigger_context.get("trigger_present"),
            "trigger_reason": trigger_context.get("trigger_reason"),
            "structure_ready": trigger_context.get("structure_ready"),
        },
        "winner_shift_context": on_demand_payload.get("winner_shift_context"),
        "iv_context": on_demand_payload.get("iv_context"),
        "market_context": on_demand_payload.get("market_context"),
        "time_day_gate": on_demand_payload.get("time_day_gate"),
        "market_closed_tester": _build_market_closed_tester_block(on_demand_payload),
        "compact_ticker_summaries": on_demand_payload.get("compact_ticker_summaries") or [],
    }


def _build_continuous_readable_summary(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    current_state = snapshot.get("current_state")
    latent_structure_state = snapshot.get("latent_structure_state")
    primary_blocker = snapshot.get("primary_blocker")
    next_flip_needed = snapshot.get("next_flip_needed")
    summary = snapshot.get("summary") or {}
    time_gate_reason = snapshot.get("time_gate_reason")
    market_open = snapshot.get("market_open")
    decision_blockers = _ordered_unique_strings(snapshot.get("decision_blockers") or [])
    failed_reasons = _ordered_unique_strings(snapshot.get("failed_reasons") or [])
    market_closed_tester = snapshot.get("market_closed_tester") or {}

    underlying_state = current_state
    if current_state in {"WAIT_MARKET_OPEN", "BLOCKED_TIME_GATE"} and latent_structure_state:
        underlying_state = latent_structure_state

    filtered_blockers = [
        blocker for blocker in decision_blockers
        if not (blocker == "time_day_gate" and underlying_state != current_state)
    ]

    top_blockers: List[str] = []
    effective_primary_blocker = primary_blocker
    if effective_primary_blocker == "time_day_gate" and underlying_state != current_state and filtered_blockers:
        effective_primary_blocker = filtered_blockers[0]
    if isinstance(effective_primary_blocker, str) and effective_primary_blocker.strip():
        top_blockers.append(effective_primary_blocker.strip())
    for blocker in filtered_blockers:
        if blocker not in top_blockers:
            top_blockers.append(blocker)
        if len(top_blockers) >= 3:
            break

    summary_note = summary.get("why")
    if current_state in {"WAIT_MARKET_OPEN", "BLOCKED_TIME_GATE"} and underlying_state != current_state:
        summary_note = f"{summary.get('why')} Underneath that, structure is still {underlying_state}."
    elif market_open is False and underlying_state != current_state and underlying_state is not None:
        summary_note = f"Market is closed right now. Underneath that, structure is still {underlying_state}."

    return {
        "ticker": summary.get("ticker"),
        "good_idea_now": summary.get("good_idea_now"),
        "action": summary.get("action"),
        "setup_state": summary.get("setup_state"),
        "now_state": current_state,
        "underlying_state": underlying_state,
        "primary_blocker": primary_blocker,
        "next_flip_needed": next_flip_needed,
        "top_blockers": top_blockers,
        "trigger_present": snapshot.get("trigger_present"),
        "trigger_reason": snapshot.get("trigger_reason"),
        "structure_ready": snapshot.get("structure_ready"),
        "market_open": market_open,
        "time_gate_reason": time_gate_reason,
        "market_closed_context_only": market_closed_tester.get("market_closed_context_only"),
        "underlying_structural_verdict": market_closed_tester.get("underlying_structural_verdict"),
        "would_be_trade_if_open": market_closed_tester.get("would_be_trade_if_open"),
        "why_now": summary_note,
        "first_failed_reason": failed_reasons[0] if failed_reasons else None,
        "invalidation": snapshot.get("invalidation"),
    }


async def _build_continuous_shadow_payload(request: ContinuousShadowRequest) -> Dict[str, Any]:
    profile_name = _sanitize_continuous_profile_name(request.profile_name)
    on_demand_request = _continuous_shadow_to_on_demand_request(request)
    profile_key = _continuous_profile_key(profile_name, on_demand_request)

    stored_state = _load_continuous_state(profile_key) if request.persist_state else {}
    previous_snapshot = stored_state.get("latest_snapshot")

    on_demand_payload = await _build_on_demand_payload(on_demand_request)
    current_snapshot = _build_continuous_snapshot(
        on_demand_payload=on_demand_payload,
        request=on_demand_request,
        profile_name=profile_name,
        profile_key=profile_key,
    )
    transition_summary = _compare_continuous_snapshots(previous_snapshot, current_snapshot)
    transition_fingerprint = _continuous_transition_fingerprint(
        current_snapshot=current_snapshot,
        transition_summary=transition_summary,
    )

    last_alert_fingerprint = stored_state.get("last_alert_fingerprint")
    deduped = bool(
        previous_snapshot
        and transition_summary.get("should_alert_candidate")
        and transition_fingerprint == last_alert_fingerprint
    )
    should_alert = bool(
        previous_snapshot
        and transition_summary.get("should_alert_candidate")
        and not deduped
    )

    alert_payload = _build_continuous_alert_payload(
        previous_snapshot=previous_snapshot,
        current_snapshot=current_snapshot,
        transition_summary=transition_summary,
        should_alert=should_alert,
    )

    persisted = False
    state_file = None
    if request.persist_state:
        persisted = True
        state_file = str(_continuous_state_path(profile_key))
        _save_continuous_state(
            profile_key,
            {
                "profile_name": profile_name,
                "profile_key": profile_key,
                "updated_at": current_snapshot.get("timestamp_et"),
                "latest_snapshot": current_snapshot,
                "previous_snapshot": previous_snapshot,
                "last_transition": transition_summary,
                "last_transition_fingerprint": transition_fingerprint,
                "last_alert_fingerprint": transition_fingerprint if should_alert else last_alert_fingerprint,
                "last_alert_timestamp": current_snapshot.get("timestamp_et") if should_alert else stored_state.get("last_alert_timestamp"),
            },
        )

    response_payload = {
        "ok": bool(on_demand_payload.get("ok")),
        "mode": "continuous_shadow",
        "shadow_mode": "snapshot_compare_only",
        "build_tag": on_demand_payload.get("build_tag"),
        "source_of_truth": "frozen_on_demand_baseline",
        "profile_name": profile_name,
        "profile_key": profile_key,
        "current_snapshot": current_snapshot,
        "previous_snapshot": previous_snapshot,
        "transition_summary": {
            **transition_summary,
            "should_alert": should_alert,
            "deduped": deduped,
            "transition_fingerprint": transition_fingerprint,
        },
        "alert_payload": alert_payload,
        "persistence": {
            "enabled": request.persist_state,
            "persisted": persisted,
            "state_file": state_file,
            "previous_snapshot_found": bool(previous_snapshot),
        },
        "read_this_first": "readable_summary",
        "readable_summary": current_snapshot.get("readable_summary"),
        "market_closed_tester": current_snapshot.get("market_closed_tester"),
        "compact_ticker_summaries": current_snapshot.get("compact_ticker_summaries") or [],
        "on_demand_excerpt": _build_continuous_on_demand_excerpt(on_demand_payload),
    }
    return _json_safe_for_response(response_payload)


@app.post("/safe-fast/continuous/shadow")
async def safe_fast_continuous_shadow(request: ContinuousShadowRequest) -> Any:
    try:
        return await _build_continuous_shadow_payload(request)
    except Exception as e:
        return _json_safe_for_response(
            {
                "ok": False,
                "mode": "continuous_shadow",
                "shadow_mode": "snapshot_compare_only",
                "error_type": "continuous_shadow_runtime_error",
                "reason": str(e),
                "profile_name": _sanitize_continuous_profile_name(request.profile_name),
                "request_profile": _model_dump(request),
            }
        )


@app.get("/safe-fast/continuous/shadow/default")
async def safe_fast_continuous_shadow_default() -> Any:
    try:
        return await _build_continuous_shadow_payload(ContinuousShadowRequest())
    except Exception as e:
        return _json_safe_for_response(
            {
                "ok": False,
                "mode": "continuous_shadow",
                "shadow_mode": "snapshot_compare_only",
                "error_type": "continuous_shadow_runtime_error",
                "reason": str(e),
                "profile_name": "default",
                "request_profile": _model_dump(ContinuousShadowRequest()),
            }
        )

def _default_on_demand_request() -> OnDemandRequest:
    return OnDemandRequest(
        option_type="C",
        open_positions=0,
        weekly_trade_count=0,
    )


@app.get("/safe-fast/on-demand/default")
async def safe_fast_on_demand_default() -> Any:
    return await _build_on_demand_payload(_default_on_demand_request())


@app.get("/safe-fast/on-demand/default/simple")
async def safe_fast_on_demand_default_simple() -> Any:
    payload = await _build_on_demand_payload(_default_on_demand_request())
    return {
        "ok": payload.get("ok"),
        "build_tag": payload.get("build_tag"),
        "read_this_first": payload.get("read_this_first"),
        "simple_output": payload.get("simple_output"),
        "screened_best_context": payload.get("screened_best_context"),
        "failed_reasons": payload.get("failed_reasons"),
    }


@app.post("/safe-fast/on-demand")
async def safe_fast_on_demand(request: OnDemandRequest) -> Any:
    return await _build_on_demand_payload(request)
