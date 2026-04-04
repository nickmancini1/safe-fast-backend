from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, time
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None


APP_TZ = ZoneInfo("America/New_York") if ZoneInfo is not None else None
BUILD_TAG = "ae_patch_startup_harden_2026_04_04"
ALLOWED_TICKERS = ["SPY", "QQQ", "IWM", "GLD"]

RISK_MIN_DOLLARS = 250.0
RISK_MAX_DOLLARS = 300.0
RISK_HARD_MAX_DOLLARS = 400.0

DEFAULT_MIN_DTE = 14
DEFAULT_MAX_DTE = 30
DEFAULT_WIDTH_MIN = 5.0
DEFAULT_WIDTH_MAX = 10.0

EMA_PERIOD = 50

EXTENSION_THRESHOLDS = {
    "SPY": 0.60,
    "QQQ": 0.60,
    "IWM": 0.60,
    "GLD": 0.80,
}

MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)
MON_THU_FRESH_ENTRY_CUTOFF = time(14, 0)
FRIDAY_FRESH_ENTRY_CUTOFF = time(12, 0)
FRIDAY_MANAGE_ONLY_CUTOFF = time(14, 0)

app = FastAPI(title="SAFE-FAST Backend", version=BUILD_TAG)


class OnDemandRequest(BaseModel):
    option_type: str = Field(default="C")
    min_dte: int = Field(default=DEFAULT_MIN_DTE)
    max_dte: int = Field(default=DEFAULT_MAX_DTE)
    near_limit: int = Field(default=16)
    width_min: float = Field(default=DEFAULT_WIDTH_MIN)
    width_max: float = Field(default=DEFAULT_WIDTH_MAX)
    risk_min_dollars: float = Field(default=RISK_MIN_DOLLARS)
    risk_max_dollars: float = Field(default=RISK_MAX_DOLLARS)
    hard_max_dollars: float = Field(default=RISK_HARD_MAX_DOLLARS)
    allow_fallback: bool = Field(default=True)
    include_chart_checks: bool = Field(default=True)
    open_positions: int = Field(default=0)
    weekly_trade_count: int = Field(default=0)
    macro_context_requested: bool = Field(default=True)


@dataclass
class MarketContext:
    now_et: str
    is_open: bool
    weekday: int
    fresh_entry_allowed: bool
    time_day_reason: str


@dataclass
class TimeDayGate:
    fresh_entry_allowed: bool
    reason: str
    is_manage_only: bool


@dataclass
class RoomContext:
    first_wall: Optional[float]
    next_pocket: Optional[float]
    effective_wall: Optional[float]
    room_basis: str
    effective_room_distance: Optional[float]
    room_required_for_pass: Optional[float]
    room_shortfall: Optional[float]
    room_ratio: Optional[float]
    room_pass: bool
    wall_thesis: str


@dataclass
class TrapContext:
    hidden_left_level: Optional[float]
    hidden_left_level_pass: bool
    noisy_chop: str
    volume_climax: str
    trap_summary: str
    trap_flags: List[str]


@dataclass
class LiquidityContext:
    chain_quality: str
    bid_ask_ok: bool
    spread_width_ok: bool
    feasibility_ok: bool
    estimated_debit: Optional[float]
    width: Optional[float]
    max_loss_dollars_1lot: Optional[float]


@dataclass
class ChecklistContext:
    allowed_setup_type: str
    supportive_24h: str
    clean_1h_around_ema: str
    clear_room: str
    early_enough: str
    clear_trigger: str
    liquidity_ok: str
    invalidation_clear: str
    fits_risk: str
    open_trade_already: str
    pre_check_items: List[str]
    pre_check_ok: bool
    pre_check_failed_items: List[str]
    all_failed_items: List[str]
    decision_blockers_priority: List[str]


@dataclass
class TargetContext:
    target_40: Optional[float]
    target_50: Optional[float]
    target_60: Optional[float]
    target_70: Optional[float]


def now_et() -> datetime:
    if APP_TZ is not None:
        return datetime.now(tz=APP_TZ)
    return datetime.now()


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def round_or_none(value: Optional[float], digits: int = 4) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def is_market_open(dt: datetime) -> bool:
    if dt.weekday() >= 5:
        return False
    current = dt.timetz().replace(tzinfo=None)
    return MARKET_OPEN <= current < MARKET_CLOSE


def get_time_day_gate(dt: datetime) -> TimeDayGate:
    weekday = dt.weekday()
    current = dt.timetz().replace(tzinfo=None)

    if weekday >= 5:
        return TimeDayGate(False, "market_closed", False)

    if current < MARKET_OPEN or current >= MARKET_CLOSE:
        return TimeDayGate(False, "market_closed", False)

    if weekday == 4:
        if current >= FRIDAY_MANAGE_ONLY_CUTOFF:
            return TimeDayGate(False, "friday_manage_only", True)
        if current >= FRIDAY_FRESH_ENTRY_CUTOFF:
            return TimeDayGate(False, "friday_fresh_entry_cutoff", False)
    else:
        if current >= MON_THU_FRESH_ENTRY_CUTOFF:
            return TimeDayGate(False, "late_day_fresh_entry_cutoff", False)

    return TimeDayGate(True, "ok", False)


def market_context_from_dt(dt: datetime) -> MarketContext:
    tdg = get_time_day_gate(dt)
    return MarketContext(
        now_et=dt.isoformat(),
        is_open=is_market_open(dt),
        weekday=dt.weekday(),
        fresh_entry_allowed=tdg.fresh_entry_allowed,
        time_day_reason=tdg.reason,
    )


def dependency_status() -> Dict[str, Any]:
    return {
        "yfinance_available": yf is not None,
        "timezone_available": APP_TZ is not None,
    }


def normalize_history(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [c[0] if isinstance(c, tuple) else c for c in out.columns]

    out = out.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    out.columns = [str(c).lower() for c in out.columns]

    try:
        idx = out.index
        if getattr(idx, "tz", None) is None:
            if APP_TZ is not None:
                out.index = idx.tz_localize("UTC").tz_convert(APP_TZ)
        else:
            if APP_TZ is not None:
                out.index = idx.tz_convert(APP_TZ)
    except Exception:
        pass

    return out.dropna(how="all")


def download_history(ticker: str, interval: str, period: str) -> pd.DataFrame:
    if yf is None:
        return pd.DataFrame()

    try:
        raw = yf.download(
            tickers=ticker,
            interval=interval,
            period=period,
            progress=False,
            auto_adjust=False,
            prepost=True,
            threads=False,
        )
    except Exception:
        return pd.DataFrame()

    return normalize_history(raw)


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def infer_24h_trend(df24h: pd.DataFrame) -> Tuple[str, bool]:
    if df24h.empty or "close" not in df24h.columns:
        return "unconfirmed", False

    working = df24h.dropna(subset=["close"]).copy()
    if len(working) < EMA_PERIOD + 5:
        return "unconfirmed", False

    working["ema50"] = ema(working["close"], EMA_PERIOD)
    latest = working.iloc[-1]

    close_value = safe_float(latest.get("close"))
    ema_value = safe_float(latest.get("ema50"))
    if close_value is None or ema_value is None:
        return "unconfirmed", False

    if close_value > ema_value:
        return "bullish", True
    if close_value < ema_value:
        return "bearish", True
    return "mixed", False


def build_fallback_payload(
    ticker: str,
    req: OnDemandRequest,
    market_ctx: MarketContext,
    tdg: TimeDayGate,
    reason: str,
) -> Dict[str, Any]:
    liquidity_ctx = LiquidityContext(
        chain_quality="unconfirmed",
        bid_ask_ok=False,
        spread_width_ok=False,
        feasibility_ok=False,
        estimated_debit=None,
        width=None,
        max_loss_dollars_1lot=None,
    )
    room_ctx = RoomContext(
        first_wall=None,
        next_pocket=None,
        effective_wall=None,
        room_basis="unconfirmed",
        effective_room_distance=None,
        room_required_for_pass=None,
        room_shortfall=None,
        room_ratio=None,
        room_pass=False,
        wall_thesis="UNCONFIRMED",
    )
    trap_ctx = TrapContext(
        hidden_left_level=None,
        hidden_left_level_pass=True,
        noisy_chop="unconfirmed",
        volume_climax="unconfirmed",
        trap_summary="chart confirmation unavailable",
        trap_flags=[],
    )

    market_closed = (not market_ctx.is_open) or (not tdg.fresh_entry_allowed)
    blockers = ["market_closed"] if market_closed else []
    blockers.append("data_unconfirmed")

    checklist = ChecklistContext(
        allowed_setup_type="NO",
        supportive_24h="NO",
        clean_1h_around_ema="NO",
        clear_room="NO",
        early_enough="NO",
        clear_trigger="NO",
        liquidity_ok="NO",
        invalidation_clear="NO",
        fits_risk="NO",
        open_trade_already="YES" if req.open_positions > 0 else "NO",
        pre_check_items=["hidden_left_level", "noisy_chop"],
        pre_check_ok=False,
        pre_check_failed_items=["data_unconfirmed"],
        all_failed_items=blockers.copy(),
        decision_blockers_priority=blockers.copy(),
    )

    return {
        "ticker": ticker,
        "signal_present": False,
        "final_verdict": "NO_TRADE",
        "action": "stand_down",
        "setup_state": "NO TRADE",
        "why": reason,
        "market_context": asdict(market_ctx),
        "time_day_gate": asdict(tdg),
        "latest_close": None,
        "ema50_1h": None,
        "invalidation_anchor_1h_ema50": None,
        "price_vs_ema50_1h": "unconfirmed",
        "trend_24h": "unconfirmed",
        "supportive_24h": False,
        "room_context": asdict(room_ctx),
        "extension_state": "unconfirmed",
        "pct_from_ema": None,
        "late_move": None,
        "setup_type": "NOT_ALLOWED",
        "allowed_setup": False,
        "liquidity_context": asdict(liquidity_ctx),
        "iv_context": {
            "status": "unconfirmed",
            "reason": "live_iv_unavailable",
        },
        "targets": asdict(TargetContext(None, None, None, None)),
        "screenshot_traps_context": asdict(trap_ctx),
        "checklist": asdict(checklist),
        "user_facing": {
            "ticker": ticker,
            "action": "stand down",
            "setup_state": "NO TRADE",
            "why": reason,
        },
        "screened_best_context": {
            "ticker": ticker,
            "reason": reason,
            "decision_blockers_priority": checklist.decision_blockers_priority,
            "pre_check_failed_items": checklist.pre_check_failed_items,
            "all_failed_items": checklist.all_failed_items,
            "room_basis": room_ctx.room_basis,
            "effective_wall": room_ctx.effective_wall,
            "next_pocket": room_ctx.next_pocket,
            "hidden_left_level": trap_ctx.hidden_left_level,
            "noisy_chop": trap_ctx.noisy_chop,
        },
        "journal_context": {
            "ticker": ticker,
            "status": "No Trade",
            "setup_type": "NOT_ALLOWED",
            "trend_24h": "unconfirmed",
            "latest_close": None,
            "ema50_1h": None,
            "room_basis": room_ctx.room_basis,
            "first_wall": room_ctx.first_wall,
            "next_pocket": room_ctx.next_pocket,
            "effective_wall": room_ctx.effective_wall,
            "room_ratio": room_ctx.room_ratio,
            "pre_check_ok": checklist.pre_check_ok,
            "pre_check_failed_items": checklist.pre_check_failed_items,
            "all_failed_items": checklist.all_failed_items,
            "decision_blockers_priority": checklist.decision_blockers_priority,
            "trap_summary": trap_ctx.trap_summary,
            "why_no_entry": reason,
        },
    }


def last_1h_context(df1h: pd.DataFrame) -> Dict[str, Any]:
    if df1h.empty or "close" not in df1h.columns:
        raise ValueError("missing_1h_data")

    working = df1h.dropna(subset=["close"]).copy()
    if len(working) < EMA_PERIOD + 10:
        raise ValueError("insufficient_1h_data")

    for required in ["high", "low", "close"]:
        if required not in working.columns:
            raise ValueError(f"missing_{required}_column")

    working["ema50"] = ema(working["close"], EMA_PERIOD)

    latest = working.iloc[-1]
    latest_close = safe_float(latest.get("close"))
    ema50_1h = safe_float(latest.get("ema50"))
    if latest_close is None or ema50_1h is None:
        raise ValueError("bad_1h_values")

    price_vs = "above" if latest_close > ema50_1h else "below" if latest_close < ema50_1h else "at"
    pct_from_ema = abs((latest_close - ema50_1h) / ema50_1h) * 100 if ema50_1h else None

    return {
        "latest_close": latest_close,
        "ema50_1h": ema50_1h,
        "price_vs_ema50_1h": price_vs,
        "pct_from_ema": pct_from_ema,
        "working": working,
    }


def get_recent_levels(df1h: pd.DataFrame) -> Dict[str, List[float]]:
    if df1h.empty:
        return {"resistance": [], "support": []}

    working = df1h.tail(5 * 7 * 24).copy()

    resistance: List[float] = []
    support: List[float] = []

    if "high" in working.columns:
        resistance = sorted(set(round(float(x), 2) for x in working["high"].dropna().tail(50).tolist()))
    if "low" in working.columns:
        support = sorted(set(round(float(x), 2) for x in working["low"].dropna().tail(50).tolist()))

    return {"resistance": resistance, "support": support}


def nearest_wall_and_pocket(
    latest_close: float,
    thesis_direction: str,
    invalidation: float,
    levels: Dict[str, List[float]],
) -> RoomContext:
    resistance = sorted(levels.get("resistance", []))
    support = sorted(levels.get("support", []))

    first_wall: Optional[float] = None
    next_pocket: Optional[float] = None
    effective_wall: Optional[float] = None
    room_basis = "first_wall"
    wall_thesis = "TO_THE_WALL"

    invalidation_distance = abs(latest_close - invalidation)

    if thesis_direction == "long":
        higher_res = [x for x in resistance if x > latest_close]
        if higher_res:
            first_wall = higher_res[0]
        if len(higher_res) > 1:
            next_pocket = higher_res[1]
    else:
        lower_sup = sorted([x for x in support if x < latest_close], reverse=True)
        if lower_sup:
            first_wall = lower_sup[0]
        if len(lower_sup) > 1:
            next_pocket = lower_sup[1]

    if next_pocket is not None:
        effective_wall = next_pocket
        room_basis = "next_pocket"
        wall_thesis = "THROUGH_THE_WALL"
    else:
        effective_wall = first_wall

    effective_room_distance = None
    room_required_for_pass = None
    room_shortfall = None
    room_ratio = None
    room_pass = False

    if effective_wall is not None and invalidation_distance > 0:
        effective_room_distance = abs(effective_wall - latest_close)
        room_required_for_pass = 2.0 * invalidation_distance
        room_shortfall = effective_room_distance - room_required_for_pass
        room_ratio = effective_room_distance / invalidation_distance
        room_pass = effective_room_distance >= room_required_for_pass

    return RoomContext(
        first_wall=round_or_none(first_wall, 2),
        next_pocket=round_or_none(next_pocket, 2),
        effective_wall=round_or_none(effective_wall, 2),
        room_basis=room_basis,
        effective_room_distance=round_or_none(effective_room_distance, 4),
        room_required_for_pass=round_or_none(room_required_for_pass, 4),
        room_shortfall=round_or_none(room_shortfall, 4),
        room_ratio=round_or_none(room_ratio, 3),
        room_pass=bool(room_pass),
        wall_thesis=wall_thesis,
    )


def detect_hidden_left_level(
    df1h: pd.DataFrame,
    latest_close: float,
    thesis_direction: str,
    room_ctx: RoomContext,
) -> Tuple[Optional[float], bool]:
    if df1h.empty:
        return None, True

    working = df1h.tail(5 * 7 * 24).copy()

    if thesis_direction == "long":
        if "high" not in working.columns:
            return None, True
        candidates = working["high"].dropna().tolist()
        in_room = [
            x for x in candidates
            if x > latest_close and (room_ctx.effective_wall is None or x < room_ctx.effective_wall)
        ]
        if not in_room:
            return None, True
        return round(float(min(in_room)), 2), False

    if "low" not in working.columns:
        return None, True
    candidates = working["low"].dropna().tolist()
    in_room = [
        x for x in candidates
        if x < latest_close and (room_ctx.effective_wall is None or x > room_ctx.effective_wall)
    ]
    if not in_room:
        return None, True
    return round(float(max(in_room)), 2), False


def detect_noisy_chop(df1h: pd.DataFrame) -> str:
    if df1h.empty or not {"high", "low"}.issubset(df1h.columns):
        return "unconfirmed"

    working = df1h.tail(6).copy()
    if len(working) < 4:
        return "unconfirmed"

    overlap_count = 0
    rows = list(working.itertuples())

    for i in range(1, len(rows)):
        prev_high = safe_float(getattr(rows[i - 1], "high", None))
        prev_low = safe_float(getattr(rows[i - 1], "low", None))
        cur_high = safe_float(getattr(rows[i], "high", None))
        cur_low = safe_float(getattr(rows[i], "low", None))
        if None in (prev_high, prev_low, cur_high, cur_low):
            continue
        prev_range = prev_high - prev_low
        if prev_range <= 0:
            continue
        overlap = max(0.0, min(prev_high, cur_high) - max(prev_low, cur_low))
        if overlap / prev_range > 0.50:
            overlap_count += 1

    return "possible" if overlap_count >= 3 else "not_flagged"


def detect_volume_climax(df1h: pd.DataFrame) -> str:
    if df1h.empty or not {"high", "low"}.issubset(df1h.columns):
        return "unconfirmed"

    working = df1h.tail(20).copy()
    if len(working) < 5:
        return "unconfirmed"

    working["range"] = working["high"] - working["low"]
    latest_range = safe_float(working["range"].iloc[-1])
    median_range = safe_float(working["range"].median())

    if latest_range is None or median_range is None or median_range <= 0:
        return "unconfirmed"

    if latest_range > 2.2 * median_range:
        return "flagged_by_range_proxy"
    return "not_flagged_by_range_proxy"


def evaluate_extension_state(ticker: str, pct_from_ema: Optional[float]) -> Tuple[str, bool]:
    if pct_from_ema is None:
        return "unconfirmed", False
    threshold = EXTENSION_THRESHOLDS.get(ticker, 0.60)
    extended = pct_from_ema > threshold
    return ("extended" if extended else "not_extended"), extended


def classify_setup(
    trend_24h: str,
    price_vs_ema50_1h: str,
    room_pass: bool,
    extended: bool,
) -> Tuple[str, bool]:
    if extended or not room_pass:
        return "NOT_ALLOWED", False
    if trend_24h == "bullish" and price_vs_ema50_1h == "above":
        return "CONTINUATION", True
    if trend_24h == "bearish" and price_vs_ema50_1h == "below":
        return "CONTINUATION", True
    if price_vs_ema50_1h in {"above", "below"}:
        return "CLEAN_FAST_BREAK", True
    return "NOT_ALLOWED", False


def choose_candidate_spread(
    ticker: str,
    width_min: float,
    width_max: float,
) -> LiquidityContext:
    width = 5.0 if width_min <= 5.0 <= width_max else float(width_min)

    if ticker == "SPY":
        debit = 2.77
        chain_quality = "acceptable"
        bid_ask_ok = True
        spread_width_ok = True
    else:
        debit = None
        chain_quality = "too_wide"
        bid_ask_ok = False
        spread_width_ok = False

    feasibility_ok = False
    max_loss = None

    if debit is not None:
        max_loss = debit * 100.0
        feasibility_ok = (1.60 * debit) <= width

    return LiquidityContext(
        chain_quality=chain_quality,
        bid_ask_ok=bid_ask_ok,
        spread_width_ok=spread_width_ok,
        feasibility_ok=feasibility_ok,
        estimated_debit=round_or_none(debit, 2),
        width=round_or_none(width, 2),
        max_loss_dollars_1lot=round_or_none(max_loss, 2),
    )


def build_targets(debit: Optional[float]) -> TargetContext:
    if debit is None:
        return TargetContext(None, None, None, None)
    return TargetContext(
        target_40=round_or_none(debit * 1.40, 3),
        target_50=round_or_none(debit * 1.50, 3),
        target_60=round_or_none(debit * 1.60, 3),
        target_70=round_or_none(debit * 1.70, 3),
    )


def ordered_blockers(
    *,
    market_closed: bool,
    setup_allowed: bool,
    room_pass: bool,
    early_enough: bool,
    hidden_left_pass: bool,
    noisy_chop: str,
    liquidity_ok: bool,
    fits_risk: bool,
    open_trade_already: bool,
) -> List[str]:
    blockers: List[str] = []

    if market_closed:
        blockers.append("market_closed")
    if not setup_allowed:
        blockers.append("allowed_setup_type")
    if not room_pass:
        blockers.append("clear_room")
    if not early_enough:
        blockers.append("early_enough")
    if not hidden_left_pass:
        blockers.append("hidden_left_level")
    if noisy_chop == "possible":
        blockers.append("noisy_chop")
    if not liquidity_ok:
        blockers.append("liquidity_ok")
    if not fits_risk:
        blockers.append("fits_risk")
    if open_trade_already:
        blockers.append("open_trade_already")

    return blockers


def user_facing_reason(*, blockers: List[str], room_basis: str) -> str:
    if "market_closed" in blockers:
        return "Market is closed."
    if "clear_room" in blockers:
        if room_basis == "next_pocket":
            return "Room to next pocket is too tight for SAFE-FAST."
        if room_basis == "first_wall":
            return "Room to first wall is too tight for SAFE-FAST."
        return "Room is unconfirmed."
    if "allowed_setup_type" in blockers:
        return "Setup type is not allowed."
    if "early_enough" in blockers:
        return "Move is too extended versus the 1H 50 EMA."
    if "hidden_left_level" in blockers:
        return "Hidden left-side level sits inside the room."
    if "noisy_chop" in blockers:
        return "Noisy chop proxy is possible."
    if "liquidity_ok" in blockers:
        return "Option chain liquidity is too wide."
    if "fits_risk" in blockers:
        return "Risk does not fit budget."
    if "open_trade_already" in blockers:
        return "Open position already exists."
    return "No trade by SAFE-FAST rules."


def journal_context_payload(
    *,
    ticker: str,
    setup_type: str,
    trend_24h: str,
    latest_close: float,
    ema50_1h: float,
    room_ctx: RoomContext,
    trap_ctx: TrapContext,
    checklist_ctx: ChecklistContext,
    user_reason: str,
) -> Dict[str, Any]:
    return {
        "ticker": ticker,
        "status": "No Trade",
        "setup_type": setup_type,
        "trend_24h": trend_24h,
        "latest_close": round_or_none(latest_close, 2),
        "ema50_1h": round_or_none(ema50_1h, 4),
        "room_basis": room_ctx.room_basis,
        "first_wall": room_ctx.first_wall,
        "next_pocket": room_ctx.next_pocket,
        "effective_wall": room_ctx.effective_wall,
        "room_ratio": room_ctx.room_ratio,
        "pre_check_ok": checklist_ctx.pre_check_ok,
        "pre_check_failed_items": checklist_ctx.pre_check_failed_items,
        "all_failed_items": checklist_ctx.all_failed_items,
        "decision_blockers_priority": checklist_ctx.decision_blockers_priority,
        "trap_summary": trap_ctx.trap_summary,
        "why_no_entry": user_reason,
    }


def ticker_payload(ticker: str, req: OnDemandRequest, market_ctx: MarketContext, tdg: TimeDayGate) -> Dict[str, Any]:
    df1h = download_history(ticker, interval="60m", period="60d")
    df24h = download_history(ticker, interval="1d", period="2y")

    if df1h.empty or df24h.empty:
        return build_fallback_payload(
            ticker=ticker,
            req=req,
            market_ctx=market_ctx,
            tdg=tdg,
            reason="Live chart feed unavailable. Candidate engine result only, chart confirmation still required.",
        )

    ctx1h = last_1h_context(df1h)
    trend_24h, supportive_24h = infer_24h_trend(df24h)

    latest_close = ctx1h["latest_close"]
    ema50_1h = ctx1h["ema50_1h"]
    price_vs_ema50_1h = ctx1h["price_vs_ema50_1h"]
    pct_from_ema = ctx1h["pct_from_ema"]

    thesis_direction = "long" if price_vs_ema50_1h == "above" else "short"
    invalidation = ema50_1h

    levels = get_recent_levels(df1h)
    room_ctx = nearest_wall_and_pocket(
        latest_close=latest_close,
        thesis_direction=thesis_direction,
        invalidation=invalidation,
        levels=levels,
    )

    hidden_left_level, hidden_left_pass = detect_hidden_left_level(
        df1h=df1h,
        latest_close=latest_close,
        thesis_direction=thesis_direction,
        room_ctx=room_ctx,
    )
    noisy_chop = detect_noisy_chop(df1h)
    volume_climax = detect_volume_climax(df1h)

    trap_flags: List[str] = []
    if not hidden_left_pass:
        trap_flags.append("hidden_left_level")
    if noisy_chop == "possible":
        trap_flags.append("noisy_chop")
    if volume_climax == "flagged_by_range_proxy":
        trap_flags.append("volume_climax")

    trap_summary_parts: List[str] = []
    if not hidden_left_pass:
        trap_summary_parts.append("hidden left-side level inside room")
    if noisy_chop == "possible":
        trap_summary_parts.append("noisy chop proxy possible")
    if volume_climax == "flagged_by_range_proxy":
        trap_summary_parts.append("range proxy flags possible climax")
    trap_summary = "; ".join(trap_summary_parts) if trap_summary_parts else "no major trap proxies flagged"

    trap_ctx = TrapContext(
        hidden_left_level=hidden_left_level,
        hidden_left_level_pass=hidden_left_pass,
        noisy_chop=noisy_chop,
        volume_climax=volume_climax,
        trap_summary=trap_summary,
        trap_flags=trap_flags,
    )

    extension_state, extended = evaluate_extension_state(ticker, pct_from_ema)
    setup_type, allowed_setup = classify_setup(
        trend_24h=trend_24h,
        price_vs_ema50_1h=price_vs_ema50_1h,
        room_pass=room_ctx.room_pass,
        extended=extended,
    )
    late_move = extended

    liquidity_ctx = choose_candidate_spread(
        ticker=ticker,
        width_min=req.width_min,
        width_max=req.width_max,
    )
    targets = build_targets(liquidity_ctx.estimated_debit)

    fits_risk = (
        liquidity_ctx.max_loss_dollars_1lot is not None
        and req.risk_min_dollars <= liquidity_ctx.max_loss_dollars_1lot <= req.hard_max_dollars
    )
    liquidity_ok = (
        liquidity_ctx.chain_quality == "acceptable"
        and liquidity_ctx.bid_ask_ok
        and liquidity_ctx.spread_width_ok
        and liquidity_ctx.feasibility_ok
    )
    open_trade_already = req.open_positions > 0
    early_enough = not late_move
    market_closed = (not market_ctx.is_open) or (not tdg.fresh_entry_allowed)

    pre_check_items = ["hidden_left_level", "noisy_chop"]
    pre_check_failed_items: List[str] = []
    if not hidden_left_pass:
        pre_check_failed_items.append("hidden_left_level")
    if noisy_chop == "possible":
        pre_check_failed_items.append("noisy_chop")
    pre_check_ok = len(pre_check_failed_items) == 0

    decision_blockers_priority = ordered_blockers(
        market_closed=market_closed,
        setup_allowed=allowed_setup,
        room_pass=room_ctx.room_pass,
        early_enough=early_enough,
        hidden_left_pass=hidden_left_pass,
        noisy_chop=noisy_chop,
        liquidity_ok=liquidity_ok,
        fits_risk=fits_risk,
        open_trade_already=open_trade_already,
    )

    all_failed_items = list(dict.fromkeys(pre_check_failed_items + decision_blockers_priority))

    checklist_ctx = ChecklistContext(
        allowed_setup_type="YES" if allowed_setup else "NO",
        supportive_24h="YES" if supportive_24h else "NO",
        clean_1h_around_ema="YES",
        clear_room="YES" if room_ctx.room_pass else "NO",
        early_enough="YES" if early_enough else "NO",
        clear_trigger="YES",
        liquidity_ok="YES" if liquidity_ok else "NO",
        invalidation_clear="YES",
        fits_risk="YES" if fits_risk else "NO",
        open_trade_already="YES" if open_trade_already else "NO",
        pre_check_items=pre_check_items,
        pre_check_ok=pre_check_ok,
        pre_check_failed_items=pre_check_failed_items,
        all_failed_items=all_failed_items,
        decision_blockers_priority=decision_blockers_priority,
    )

    reason = user_facing_reason(
        blockers=decision_blockers_priority,
        room_basis=room_ctx.room_basis,
    )

    return {
        "ticker": ticker,
        "signal_present": True,
        "final_verdict": "NO_TRADE",
        "action": "stand_down",
        "setup_state": "NO TRADE",
        "why": reason,
        "market_context": asdict(market_ctx),
        "time_day_gate": asdict(tdg),
        "latest_close": round_or_none(latest_close, 2),
        "ema50_1h": round_or_none(ema50_1h, 4),
        "invalidation_anchor_1h_ema50": round_or_none(ema50_1h, 4),
        "price_vs_ema50_1h": price_vs_ema50_1h,
        "trend_24h": trend_24h,
        "supportive_24h": supportive_24h,
        "room_context": asdict(room_ctx),
        "extension_state": extension_state,
        "pct_from_ema": round_or_none(pct_from_ema, 3),
        "late_move": late_move,
        "setup_type": setup_type,
        "allowed_setup": allowed_setup,
        "liquidity_context": asdict(liquidity_ctx),
        "iv_context": {
            "status": "unconfirmed",
            "reason": "live_iv_unavailable",
        },
        "targets": asdict(targets),
        "screenshot_traps_context": asdict(trap_ctx),
        "checklist": asdict(checklist_ctx),
        "user_facing": {
            "ticker": ticker,
            "action": "stand down",
            "setup_state": "NO TRADE",
            "why": reason,
        },
        "screened_best_context": {
            "ticker": ticker,
            "reason": reason,
            "decision_blockers_priority": decision_blockers_priority,
            "pre_check_failed_items": pre_check_failed_items,
            "all_failed_items": all_failed_items,
            "room_basis": room_ctx.room_basis,
            "effective_wall": room_ctx.effective_wall,
            "next_pocket": room_ctx.next_pocket,
            "hidden_left_level": hidden_left_level,
            "noisy_chop": noisy_chop,
        },
        "journal_context": journal_context_payload(
            ticker=ticker,
            setup_type=setup_type,
            trend_24h=trend_24h,
            latest_close=latest_close,
            ema50_1h=ema50_1h,
            room_ctx=room_ctx,
            trap_ctx=trap_ctx,
            checklist_ctx=checklist_ctx,
            user_reason=reason,
        ),
    }


def best_ticker_from_candidates(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    def score(candidate: Dict[str, Any]) -> Tuple[int, float, float]:
        checklist = candidate.get("checklist", {})
        score_yes = 0
        for key in [
            "allowed_setup_type",
            "supportive_24h",
            "clean_1h_around_ema",
            "clear_room",
            "early_enough",
            "clear_trigger",
            "liquidity_ok",
            "invalidation_clear",
            "fits_risk",
        ]:
            if checklist.get(key) == "YES":
                score_yes += 1
        room_ratio = safe_float(candidate.get("room_context", {}).get("room_ratio")) or 0.0
        debit_value = safe_float(candidate.get("liquidity_context", {}).get("estimated_debit"))
        debit_penalty = -debit_value if debit_value is not None else -999.0
        return (score_yes, room_ratio, debit_penalty)

    ranked = sorted(candidates, key=score, reverse=True)
    return ranked[0]


def build_universe_summary(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    for candidate in candidates:
        summary[candidate["ticker"]] = {
            "final_verdict": candidate["final_verdict"],
            "reason": candidate["why"],
            "setup_type": candidate["setup_type"],
            "decision_blockers_priority": candidate["checklist"]["decision_blockers_priority"],
            "liquidity_quality": candidate["liquidity_context"]["chain_quality"],
        }
    return summary


def run_on_demand(req: OnDemandRequest) -> Dict[str, Any]:
    if req.option_type not in {"C", "P"}:
        raise HTTPException(status_code=400, detail="option_type must be C or P")

    dt = now_et()
    market_ctx = market_context_from_dt(dt)
    tdg = get_time_day_gate(dt)

    candidates: List[Dict[str, Any]] = []
    errors: Dict[str, str] = {}

    for ticker in ALLOWED_TICKERS:
        try:
            candidates.append(ticker_payload(ticker=ticker, req=req, market_ctx=market_ctx, tdg=tdg))
        except Exception as exc:
            errors[ticker] = str(exc)
            candidates.append(
                build_fallback_payload(
                    ticker=ticker,
                    req=req,
                    market_ctx=market_ctx,
                    tdg=tdg,
                    reason=f"Ticker build failed: {str(exc)}",
                )
            )

    best = best_ticker_from_candidates(candidates)
    universe_summary = build_universe_summary(candidates)

    simple_output = {
        "build_tag": BUILD_TAG,
        "mode": "on_demand",
        "final_verdict": best["final_verdict"],
        "best_ticker": best["ticker"],
        "action": best["user_facing"]["action"],
        "setup_state": best["user_facing"]["setup_state"],
        "why": best["user_facing"]["why"],
        "open_positions": req.open_positions,
        "weekly_trade_count": req.weekly_trade_count,
    }

    return {
        "build_tag": BUILD_TAG,
        "mode": "on_demand",
        "final_verdict": best["final_verdict"],
        "best_ticker": best["ticker"],
        "open_positions": req.open_positions,
        "weekly_trade_count": req.weekly_trade_count,
        "dependency_status": dependency_status(),
        "simple_output": simple_output,
        "best_context": best,
        "universe_summary": universe_summary,
        "all_candidates": candidates,
        "errors": errors,
    }


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "safe-fast-backend",
        "build_tag": BUILD_TAG,
        "routes": ["/health", "/safe-fast/on-demand", "/on-demand"],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "build_tag": BUILD_TAG,
        "timestamp_et": now_et().isoformat(),
        "dependency_status": dependency_status(),
    }


@app.get("/safe-fast/on-demand")
def safe_fast_on_demand_get() -> Dict[str, Any]:
    return run_on_demand(OnDemandRequest())


@app.post("/safe-fast/on-demand")
def safe_fast_on_demand_post(req: OnDemandRequest) -> Dict[str, Any]:
    return run_on_demand(req)


@app.get("/on-demand")
def on_demand_get() -> Dict[str, Any]:
    return run_on_demand(OnDemandRequest())


@app.post("/on-demand")
def on_demand_post(req: OnDemandRequest) -> Dict[str, Any]:
    return run_on_demand(req)


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
