"""
SAFE-FAST continuous runner core loop v1

Source scope:
- SAFE_FAST_STANDALONE_HANDOFF_v1.md
- main_macro_surface_v25.py
- runner artifacts built in this chat

This file implements the first runner MVP module:
- call /safe-fast/continuous
- normalize current snapshot
- compare prior vs current meaningful state
- suppress initial-snapshot noise
- apply simple dedupe by meaningful-state fingerprint
- persist runner-owned state
- emit alert/no-alert output for a later delivery module

Unconfirmed items remain caller-supplied:
- endpoint base URL
- polling cadence
- alert delivery transport/channel
- market-hours window
- advanced re-arm policy
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import httpx


MEANINGFUL_FIELDS = (
    "final_verdict",
    "primary_blocker",
    "approval_ready_now",
    "approval_ready_on_completed_candle",
    "invalidation",
    "invalidation_hit",
    "breakout_hold_pending",
    "thesis_gate_pending",
)

DEFAULT_REQUEST_BODY = {
    "option_type": "C",
    "open_positions": 0,
    "weekly_trade_count": 0,
    # runner owns persistence by default; endpoint-side persistence is not required here
    "persist_state": False,
}


@dataclass
class RunnerConfig:
    endpoint_url: str
    state_file: str
    poll_seconds: Optional[float] = None  # unconfirmed; caller must supply for loop mode
    profile_name: str = "runner_mvp"
    option_type: str = "C"
    open_positions: int = 0
    weekly_trade_count: int = 0
    timeout_seconds: float = 15.0


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _fingerprint(payload: Dict[str, Any]) -> str:
    packed = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()


def _snapshot_from_response(response_payload: Dict[str, Any]) -> Dict[str, Any]:
    current_snapshot = response_payload.get("current_snapshot")
    if isinstance(current_snapshot, dict):
        return current_snapshot
    return response_payload


def _normalized_state(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    state = {
        "timestamp_et": snapshot.get("timestamp_et"),
        "best_ticker": snapshot.get("best_ticker"),
        "final_verdict": snapshot.get("final_verdict"),
        "primary_blocker": snapshot.get("primary_blocker"),
        "approval_ready_now": snapshot.get("approval_ready_now"),
        "approval_ready_on_completed_candle": snapshot.get("approval_ready_on_completed_candle"),
        "invalidation": snapshot.get("invalidation"),
        "invalidation_hit": snapshot.get("invalidation_hit"),
        "breakout_hold_pending": snapshot.get("breakout_hold_pending"),
        "thesis_gate_pending": snapshot.get("thesis_gate_pending"),
        "summary": snapshot.get("summary") or {},
        "readable_summary": snapshot.get("readable_summary"),
        "alert_stage": snapshot.get("alert_stage"),
        "alert_reason": snapshot.get("alert_reason"),
        "alert_severity": snapshot.get("alert_severity"),
        "current_state": snapshot.get("current_state"),
        "next_flip_needed": snapshot.get("next_flip_needed"),
    }
    return state


def _changed_fields(previous_state: Dict[str, Any], current_state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    changes: Dict[str, Dict[str, Any]] = {}
    for field in MEANINGFUL_FIELDS:
        if previous_state.get(field) != current_state.get(field):
            changes[field] = {
                "previous": previous_state.get(field),
                "current": current_state.get(field),
            }
    return changes


def _state_key(state: Dict[str, Any]) -> str:
    comparable = {field: state.get(field) for field in MEANINGFUL_FIELDS}
    return _fingerprint(comparable)


def _build_user_facing_alert(current_state: Dict[str, Any], changed_fields: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    verdict = str(current_state.get("final_verdict") or "").upper()
    best_ticker = current_state.get("best_ticker")
    summary = current_state.get("summary") or {}
    trade_now = "This is a trade now" if verdict == "TRADE" else "This is not a trade now"

    if changed_fields:
        changed_names = ", ".join(changed_fields.keys())
        what_changed = f"Meaningful state changed: {changed_names}."
    else:
        what_changed = "No meaningful state change."

    why = current_state.get("alert_reason") or summary.get("why") or current_state.get("readable_summary") or "unconfirmed"
    what_matters_now = current_state.get("invalidation") or current_state.get("next_flip_needed") or current_state.get("current_state") or "unconfirmed"

    return {
        "trade_now_text": trade_now,
        "ticker": best_ticker,
        "what_changed": what_changed,
        "why": why,
        "what_matters_now": what_matters_now,
    }


def _build_request_body(config: RunnerConfig) -> Dict[str, Any]:
    body = dict(DEFAULT_REQUEST_BODY)
    body.update(
        {
            "profile_name": config.profile_name,
            "option_type": config.option_type,
            "open_positions": config.open_positions,
            "weekly_trade_count": config.weekly_trade_count,
        }
    )
    return body


async def fetch_continuous_payload(config: RunnerConfig) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
        response = await client.post(config.endpoint_url, json=_build_request_body(config))
        response.raise_for_status()
        return response.json()


def evaluate_transition(
    previous_state: Optional[Dict[str, Any]],
    current_state: Dict[str, Any],
    last_alerted_state_key: Optional[str],
) -> Dict[str, Any]:
    current_state_key = _state_key(current_state)

    if not previous_state:
        return {
            "transition_type": "INITIAL_SNAPSHOT",
            "should_alert": False,
            "deduped": False,
            "changed_fields": {},
            "state_key": current_state_key,
            "reason": "Initial snapshot noise suppressed.",
        }

    changed_fields = _changed_fields(previous_state, current_state)
    if not changed_fields:
        return {
            "transition_type": "NO_MEANINGFUL_CHANGE",
            "should_alert": False,
            "deduped": False,
            "changed_fields": {},
            "state_key": current_state_key,
            "reason": "No meaningful state change.",
        }

    deduped = bool(last_alerted_state_key and current_state_key == last_alerted_state_key)
    should_alert = not deduped

    if current_state.get("invalidation_hit"):
        transition_type = "INVALIDATION_HIT"
    elif "final_verdict" in changed_fields:
        transition_type = "FINAL_VERDICT_CHANGED"
    elif "primary_blocker" in changed_fields:
        transition_type = "PRIMARY_BLOCKER_CHANGED"
    elif "approval_ready_on_completed_candle" in changed_fields:
        transition_type = "COMPLETED_CANDLE_APPROVAL_CHANGED"
    elif "approval_ready_now" in changed_fields:
        transition_type = "INTRABAR_APPROVAL_CHANGED"
    elif "breakout_hold_pending" in changed_fields:
        transition_type = "BREAKOUT_HOLD_CHANGED"
    elif "thesis_gate_pending" in changed_fields:
        transition_type = "THESIS_GATE_CHANGED"
    else:
        transition_type = "DETAIL_CHANGED"

    return {
        "transition_type": transition_type,
        "should_alert": should_alert,
        "deduped": deduped,
        "changed_fields": changed_fields,
        "state_key": current_state_key,
        "reason": "Meaningful new state." if should_alert else "Duplicate meaningful state suppressed.",
    }


async def run_once(config: RunnerConfig) -> Dict[str, Any]:
    state_path = Path(config.state_file)
    stored = _read_json(state_path)

    response_payload = await fetch_continuous_payload(config)
    snapshot = _snapshot_from_response(response_payload)
    current_state = _normalized_state(snapshot)

    previous_state = stored.get("current_state")
    last_alerted_state_key = stored.get("last_alerted_state_key")
    transition = evaluate_transition(previous_state, current_state, last_alerted_state_key)

    alert_payload = _build_user_facing_alert(current_state, transition["changed_fields"])
    should_alert = bool(transition["should_alert"])

    new_stored_state = {
        "runner_state_version": 1,
        "config": {
            "profile_name": config.profile_name,
            "option_type": config.option_type,
            "open_positions": config.open_positions,
            "weekly_trade_count": config.weekly_trade_count,
        },
        "current_state": current_state,
        "previous_state": previous_state,
        "last_transition": transition,
        "last_alerted_state_key": transition["state_key"] if should_alert else last_alerted_state_key,
        "last_alert_payload": alert_payload if should_alert else stored.get("last_alert_payload"),
        "last_poll_timestamp_et": current_state.get("timestamp_et"),
    }
    _write_json(state_path, new_stored_state)

    return {
        "ok": True,
        "endpoint_url": config.endpoint_url,
        "transition": transition,
        "should_alert": should_alert,
        "alert_payload": alert_payload,
        "current_state": current_state,
        "state_file": str(state_path),
    }


async def loop_forever(config: RunnerConfig) -> None:
    if config.poll_seconds is None or config.poll_seconds <= 0:
        raise ValueError("poll_seconds is required for loop mode. Polling cadence is unconfirmed and must be supplied.")

    while True:
        result = await run_once(config)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        await asyncio.sleep(config.poll_seconds)


def _parse_args() -> RunnerConfig:
    parser = argparse.ArgumentParser(description="SAFE-FAST continuous runner core loop v1")
    parser.add_argument("--endpoint-url", required=True, help="Full POST URL for /safe-fast/continuous")
    parser.add_argument("--state-file", required=True, help="Runner-owned JSON state file path")
    parser.add_argument("--poll-seconds", type=float, default=None, help="Polling cadence in seconds (unconfirmed; required for --loop)")
    parser.add_argument("--profile-name", default="runner_mvp")
    parser.add_argument("--option-type", default="C", choices=["C", "P"])
    parser.add_argument("--open-positions", type=int, default=0)
    parser.add_argument("--weekly-trade-count", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    args = parser.parse_args()
    return RunnerConfig(
        endpoint_url=args.endpoint_url,
        state_file=args.state_file,
        poll_seconds=args.poll_seconds,
        profile_name=args.profile_name,
        option_type=args.option_type,
        open_positions=args.open_positions,
        weekly_trade_count=args.weekly_trade_count,
        timeout_seconds=args.timeout_seconds,
    )


async def _async_main() -> None:
    import sys

    loop_mode = "--loop" in sys.argv
    if loop_mode:
        sys.argv.remove("--loop")

    config = _parse_args()
    if loop_mode:
        await loop_forever(config)
        return

    result = await run_once(config)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    asyncio.run(_async_main())
