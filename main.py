from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException


app = FastAPI(title="SAFE-FAST Continuous Runner", version="1.0.1")


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
    "persist_state": False,
}


_runner_task: Optional[asyncio.Task] = None
_last_result: Dict[str, Any] = {}
_last_error: Optional[str] = None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


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
    return {
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
    trade_now = "This is a trade now" if verdict == "TRADE" else "This is not a trade now"
    summary = current_state.get("summary") or {}

    if changed_fields:
        changed_names = ", ".join(changed_fields.keys())
        what_changed = f"Meaningful state changed: {changed_names}."
    else:
        what_changed = "No meaningful state change."

    return {
        "trade_now_text": trade_now,
        "ticker": current_state.get("best_ticker"),
        "what_changed": what_changed,
        "why": current_state.get("alert_reason") or summary.get("why") or current_state.get("readable_summary") or "unconfirmed",
        "what_matters_now": current_state.get("invalidation") or current_state.get("next_flip_needed") or current_state.get("current_state") or "unconfirmed",
    }


def _load_config_from_env() -> Dict[str, Any]:
    endpoint_url = os.getenv("SAFE_FAST_CONTINUOUS_URL", "").strip()
    if not endpoint_url:
        raise RuntimeError("SAFE_FAST_CONTINUOUS_URL is required.")

    poll_seconds_raw = os.getenv("SAFE_FAST_POLL_SECONDS", "").strip()
    poll_seconds: Optional[float] = None
    if poll_seconds_raw:
        try:
            poll_seconds = float(poll_seconds_raw)
        except ValueError as exc:
            raise RuntimeError("SAFE_FAST_POLL_SECONDS must be numeric.") from exc

    return {
        "endpoint_url": endpoint_url,
        "state_file": os.getenv("SAFE_FAST_STATE_FILE", "/tmp/safe_fast_continuous_runner_state.json").strip(),
        "poll_seconds": poll_seconds,
        "profile_name": os.getenv("SAFE_FAST_PROFILE_NAME", "runner_mvp").strip() or "runner_mvp",
        "option_type": os.getenv("SAFE_FAST_OPTION_TYPE", "C").strip().upper() or "C",
        "open_positions": int(os.getenv("SAFE_FAST_OPEN_POSITIONS", "0")),
        "weekly_trade_count": int(os.getenv("SAFE_FAST_WEEKLY_TRADE_COUNT", "0")),
        "timeout_seconds": float(os.getenv("SAFE_FAST_TIMEOUT_SECONDS", "15")),
    }


def _build_request_body(config: Dict[str, Any]) -> Dict[str, Any]:
    body = dict(DEFAULT_REQUEST_BODY)
    body.update(
        {
            "profile_name": config["profile_name"],
            "option_type": config["option_type"],
            "open_positions": config["open_positions"],
            "weekly_trade_count": config["weekly_trade_count"],
        }
    )
    return body


async def fetch_continuous_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=config["timeout_seconds"]) as client:
        response = await client.post(config["endpoint_url"], json=_build_request_body(config))
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


async def run_once(config: Dict[str, Any]) -> Dict[str, Any]:
    state_path = Path(config["state_file"])
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
            "profile_name": config["profile_name"],
            "option_type": config["option_type"],
            "open_positions": config["open_positions"],
            "weekly_trade_count": config["weekly_trade_count"],
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
        "endpoint_url": config["endpoint_url"],
        "transition": transition,
        "should_alert": should_alert,
        "alert_payload": alert_payload,
        "current_state": current_state,
        "state_file": str(state_path),
    }


async def _runner_loop() -> None:
    global _last_result, _last_error

    while True:
        try:
            config = _load_config_from_env()
            poll_seconds = config["poll_seconds"]
            if poll_seconds is None or poll_seconds <= 0:
                raise RuntimeError("SAFE_FAST_POLL_SECONDS is required for auto-start loop mode.")
            _last_result = await run_once(config)
            _last_error = None
            await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _last_error = str(exc)
            await asyncio.sleep(5)


@app.on_event("startup")
async def _startup() -> None:
    global _runner_task
    if _env_bool("SAFE_FAST_RUNNER_AUTO_START", default=False):
        _runner_task = asyncio.create_task(_runner_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _runner_task
    if _runner_task is not None:
        _runner_task.cancel()
        try:
            await _runner_task
        except asyncio.CancelledError:
            pass
        finally:
            _runner_task = None


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "safe_fast_continuous_runner",
        "runner_auto_start": _env_bool("SAFE_FAST_RUNNER_AUTO_START", default=False),
        "runner_task_active": bool(_runner_task and not _runner_task.done()),
        "last_error": _last_error,
    }


@app.get("/runner/config")
async def runner_config() -> Dict[str, Any]:
    try:
        return _load_config_from_env()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/runner/poll-once")
async def runner_poll_once() -> Dict[str, Any]:
    global _last_result, _last_error
    try:
        config = _load_config_from_env()
        result = await run_once(config)
        _last_result = result
        _last_error = None
        return result
    except Exception as exc:
        _last_error = str(exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/runner/state")
async def runner_state() -> Dict[str, Any]:
    try:
        config = _load_config_from_env()
        stored = _read_json(Path(config["state_file"]))
        return {
            "last_result": _last_result,
            "stored_state": stored,
            "last_error": _last_error,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "safe_fast_continuous_runner",
        "health": "/healthz",
        "poll_once": "/runner/poll-once",
        "state": "/runner/state",
    }
