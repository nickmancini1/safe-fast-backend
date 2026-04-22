
"""
SAFE-FAST pending-next-session carry-forward patch.

Purpose
-------
When a continuation setup has already produced a valid *completed* trigger candle
but the market is closed, do not leave the user with a vague "NO_TRADE / no_valid_trigger"
message. Preserve the real state as a carry-forward:

    PENDING_NEXT_SESSION

This patch is intentionally conservative:
- It does NOT force a live entry while the market is closed.
- It preserves `final_verdict = "NO_TRADE"` by default for closed-session reads.
- It adds/updates explicit carry-forward fields and user-facing messaging so the
  next-session plan is unambiguous.
- It only upgrades messaging when the completed structural trigger is truly present.

Intended integration points
---------------------------
Apply `apply_pending_next_session_patch(result)` after:
- structure / trigger evaluation
- approval / gating assembly
- simple_output assembly (or immediately before final response serialization)

The function mutates and returns the payload dict.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, Optional


HARD_TRAP_KEYS = {
    "hidden_left_structure",
    "volume_climax_exhaustion",
    "parabolic_exhaustion",
}


def _nested_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _nested_set(d: Dict[str, Any], path: Iterable[str], value: Any) -> None:
    cur = d
    path = list(path)
    for key in path[:-1]:
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[key] = nxt
        cur = nxt
    cur[path[-1]] = value


def _ensure_dict(parent: Dict[str, Any], key: str) -> Dict[str, Any]:
    child = parent.get(key)
    if not isinstance(child, dict):
        child = {}
        parent[key] = child
    return child


def _has_hard_trap(result: Dict[str, Any]) -> bool:
    trap_ctx = result.get("trap_check_context")
    if not isinstance(trap_ctx, dict):
        return False

    checks = trap_ctx.get("checks")
    if not isinstance(checks, dict):
        return False

    for trap_name in HARD_TRAP_KEYS:
        trap_block = checks.get(trap_name)
        if isinstance(trap_block, dict) and trap_block.get("status") == "fail":
            return True
    return False


def _completed_trigger_detected(result: Dict[str, Any]) -> bool:
    candidates = [
        _nested_get(result, "trigger_context", "completed_candle_raw_trigger_pass"),
        _nested_get(result, "entry_context", "completed_candle_raw_trigger_detected"),
        _nested_get(result, "intrabar_signal_context", "completed_raw_signal_detected"),
        _nested_get(result, "approval_context", "completed_raw_signal_detected"),
    ]
    return any(v is True for v in candidates)


def _structure_ready(result: Dict[str, Any]) -> bool:
    candidates = [
        _nested_get(result, "trigger_context", "structure_ready"),
        _nested_get(result, "entry_context", "structure_ready"),
        _nested_get(result, "approval_context", "structure_ready"),
        _nested_get(result, "blocker_context", "structure_ready"),
    ]
    return any(v is True for v in candidates)


def _continuation_ok(result: Dict[str, Any]) -> bool:
    setup_type = _nested_get(result, "blocker_context", "setup_type") or _nested_get(
        result, "structure_context", "setup_type"
    )
    allowed_setup = _nested_get(result, "blocker_context", "allowed_setup")
    room_pass = _nested_get(result, "blocker_context", "room_pass")
    reclaim_hold = _nested_get(result, "live_map", "continuation", "reclaim_hold_proven")
    shelf_proven = _nested_get(result, "live_map", "continuation", "shelf_proven")

    return (
        setup_type == "Continuation"
        and allowed_setup is True
        and room_pass is True
        and reclaim_hold is True
        and shelf_proven is True
    )


def _market_closed(result: Dict[str, Any]) -> bool:
    market_open_flags = [
        _nested_get(result, "market_context", "is_open"),
        _nested_get(result, "time_day_gate", "fresh_entry_allowed"),
        _nested_get(result, "entry_context", "live_entry_requires_market_open"),
    ]
    market_is_open = _nested_get(result, "market_context", "is_open")
    fresh_entry_allowed = _nested_get(result, "time_day_gate", "fresh_entry_allowed")
    if market_is_open is False:
        return True
    if fresh_entry_allowed is False:
        return True
    return False


def should_mark_pending_next_session(result: Dict[str, Any]) -> bool:
    """
    True only when a completed continuation trigger exists structurally,
    but the entry is blocked because the market is closed / fresh entry is unavailable.
    """
    if not isinstance(result, dict):
        return False

    if not _market_closed(result):
        return False

    if not _continuation_ok(result):
        return False

    if not _structure_ready(result):
        return False

    if not _completed_trigger_detected(result):
        return False

    if _has_hard_trap(result):
        return False

    # Do NOT carry forward if room fails.
    if _nested_get(result, "blocker_context", "room_pass") is not True:
        return False

    # If the setup already explicitly says no trigger due to market closed or similar, good.
    # If not, the structural trigger plus closed market is still sufficient for carry-forward.
    return True


def apply_pending_next_session_patch(
    result: Dict[str, Any],
    *,
    preserve_top_level_final_verdict: bool = True,
) -> Dict[str, Any]:
    """
    Mutates and returns the SAFE-FAST result dict.
    """
    if not isinstance(result, dict):
        raise TypeError("result must be a dict")

    if not should_mark_pending_next_session(result):
        return result

    ticker = result.get("best_ticker") or _nested_get(result, "decision_context", "ticker")
    trigger_level = _nested_get(result, "trigger_context", "trigger_level")
    current_close = _nested_get(result, "trigger_context", "current_close")
    invalidation = result.get("invalidation_level_1h_ema50")
    setup_type = _nested_get(result, "blocker_context", "setup_type", default="Continuation")

    carry_forward_note = (
        f"Completed {setup_type} trigger is already locked from the last completed 1H candle, "
        f"but the market is closed. Re-check next session open before entry."
    )

    carry_ctx = _ensure_dict(result, "carry_forward_context")
    carry_ctx.update(
        {
            "status": "PENDING_NEXT_SESSION",
            "ticker": ticker,
            "valid_completed_trigger_locked": True,
            "next_session_open_check_required": True,
            "entry_live_now": False,
            "reason": "completed_candle_trigger_market_closed",
            "carry_forward_note": carry_forward_note,
            "trigger_level": trigger_level,
            "current_close": current_close,
            "invalidation_1h_ema50": invalidation,
            "open_check_items": [
                "market_open",
                "fresh_entry_allowed",
                "one_hour_clean_around_ema",
                "early_enough",
                "clear_trigger",
            ],
        }
    )

    # Promote clearer trigger semantics across key contexts.
    for ctx_name in (
        "blocker_context",
        "trigger_context",
        "entry_context",
        "intrabar_signal_context",
        "approval_context",
        "approval_requirements_context",
        "approval_flip_context",
        "trigger_state",
    ):
        ctx = result.get(ctx_name)
        if not isinstance(ctx, dict):
            continue

        if ctx_name in {"trigger_context", "trigger_state"}:
            ctx["structural_trigger_present"] = True

        if "structure_ready" in ctx:
            ctx["structure_ready"] = True

        if "trigger_reason" in ctx:
            ctx["trigger_reason"] = "completed_candle_trigger_market_closed"

        if "trigger_present" in ctx:
            ctx["trigger_present"] = False

        if "live_entry_waiting_on" in ctx:
            ctx["live_entry_waiting_on"] = "market_open"

    # Approval / entry path messaging
    for ctx_name in ("approval_context", "entry_context", "intrabar_signal_context"):
        ctx = result.get(ctx_name)
        if not isinstance(ctx, dict):
            continue
        ctx["pending_next_session"] = True

    approval_ctx = _ensure_dict(result, "approval_context")
    approval_ctx["approval_status"] = "PENDING_NEXT_SESSION"
    approval_ctx["approval_note"] = carry_forward_note
    approval_ctx["next_flip_needed"] = "market_open"

    entry_ctx = _ensure_dict(result, "entry_context")
    entry_ctx["mid_candle_entry_state"] = "PENDING_NEXT_SESSION"
    entry_ctx["completed_candle_entry_state"] = "PENDING_NEXT_SESSION"
    entry_ctx["completed_candle_trade_available"] = False
    entry_ctx["live_entry_available_now"] = False

    trigger_ctx = _ensure_dict(result, "trigger_context")
    trigger_ctx["structural_trigger_present"] = True
    trigger_ctx["completed_candle_trigger_present"] = True
    trigger_ctx["current_bar_trigger_present"] = False

    approval_reqs = _ensure_dict(result, "approval_requirements_context")
    approval_reqs["approval_path_status"] = "PENDING_NEXT_SESSION"
    approval_reqs["next_flip_needed"] = "market_open"

    # Keep final verdict conservative for closed market, but surface the real state.
    if not preserve_top_level_final_verdict:
        result["engine_status"] = "PENDING_NEXT_SESSION"
        result["candidate_engine_status"] = "PENDING_NEXT_SESSION"
        result["final_verdict"] = "PENDING_NEXT_SESSION"

    # Decision / simple output should stop saying generic NO TRADE.
    decision_ctx = _ensure_dict(result, "decision_context")
    decision_ctx["setup_state"] = "PENDING NEXT SESSION"
    decision_ctx["action"] = "recheck next session open"
    decision_ctx["good_idea_now"] = "NO"
    decision_ctx["primary_blocker"] = "market_closed_after_completed_trigger"

    simple = _ensure_dict(result, "simple_output")
    simple["good_idea_now"] = "NO"
    simple["ticker"] = ticker
    simple["action"] = "recheck next session open"
    simple["setup_state"] = "PENDING NEXT SESSION"
    simple["headline"] = "Completed trigger locked after hours."
    simple["why"] = (
        f"Completed 1H trigger is already locked above {trigger_level}, "
        "but the market is closed. Re-check next session open before entry."
    )
    simple["signal_present"] = True
    simple["primary_blocker"] = "market closed after completed trigger"
    simple["next_flip_needed"] = "market open"
    simple["top_blockers"] = [
        "market open",
        "clean 1H structure around the 50 EMA",
        "early entry quality",
    ]
    simple["primary_blocker_key"] = "completed_candle_trigger_market_closed"
    simple["next_flip_needed_key"] = "market_open"
    simple["top_blocker_keys"] = [
        "market_open",
        "one_hour_clean_around_ema",
        "early_enough",
    ]
    simple["also_failing"] = "market is closed; 1H structure around the 50 EMA is not clean."
    simple["trap_line"] = "overextension vs 1H 50 EMA."
    simple["watchouts"] = (
        "market is closed; 1H structure around the 50 EMA is not clean; "
        "overextension vs 1H 50 EMA."
    )
    simple["next_step"] = "Re-check at next session open."
    simple["what_matters_next_session"] = (
        "If price is not too extended and market is open, the completed trigger can carry forward."
    )
    simple["response_lines"] = [
        "Completed trigger locked after hours.",
        f"Ticker: {ticker}",
        "Action: recheck next session open",
        f"Reason: Completed 1H trigger is already locked above {trigger_level}, but the market is closed.",
        "Watchouts: market is closed; 1H structure around the 50 EMA is not clean; overextension vs 1H 50 EMA.",
        "Next session: Re-check at open before entry.",
        f"Invalidation: 1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {invalidation}.",
    ]
    simple["response_text"] = "\n".join(simple["response_lines"])

    # Final reason stack / surfaced explanation
    final_reason_ctx = _ensure_dict(result, "final_reason_context")
    final_reason_ctx["final_reason"] = (
        f"After-hours structural read: Completed 1H trigger is already locked above {trigger_level}, "
        "but the market is closed."
    )

    reason_stack = _ensure_dict(result, "reason_stack_context")
    reason_stack["top_line_reason"] = final_reason_ctx["final_reason"]

    return result


def demo() -> None:
    sample = {
        "market_context": {"is_open": False},
        "time_day_gate": {"fresh_entry_allowed": False},
        "blocker_context": {
            "setup_type": "Continuation",
            "allowed_setup": True,
            "room_pass": True,
            "structure_ready": True,
            "trigger_reason": "completed_candle_trigger_market_closed",
        },
        "live_map": {
            "continuation": {
                "reclaim_hold_proven": True,
                "shelf_proven": True,
            }
        },
        "trigger_context": {
            "completed_candle_raw_trigger_pass": True,
            "structure_ready": True,
            "trigger_level": 653.73,
            "current_close": 655.08,
        },
        "entry_context": {
            "completed_candle_raw_trigger_detected": True,
            "structure_ready": True,
        },
        "approval_context": {
            "completed_raw_signal_detected": True,
            "structure_ready": True,
        },
        "trap_check_context": {
            "checks": {
                "hidden_left_structure": {"status": "pass"},
                "volume_climax_exhaustion": {"status": "pass"},
                "parabolic_exhaustion": {"status": "pass"},
            }
        },
        "best_ticker": "QQQ",
        "invalidation_level_1h_ema50": 639.6553,
    }
    patched = apply_pending_next_session_patch(deepcopy(sample))
    import json
    print(json.dumps(patched, indent=2))


if __name__ == "__main__":
    demo()
