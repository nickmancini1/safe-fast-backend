from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException

from safe_fast_continuous_runner_core_v1 import RunnerConfig, run_once


app = FastAPI(title="SAFE-FAST Continuous Runner", version="1.0.0")


_runner_task: Optional[asyncio.Task] = None
_last_result: Dict[str, Any] = {}
_last_error: Optional[str] = None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _load_config_from_env() -> RunnerConfig:
    endpoint_url = os.getenv("SAFE_FAST_CONTINUOUS_URL", "").strip()
    if not endpoint_url:
        raise RuntimeError("SAFE_FAST_CONTINUOUS_URL is required.")

    state_file = os.getenv("SAFE_FAST_STATE_FILE", "/data/safe_fast_continuous_runner_state.json").strip()
    poll_seconds_raw = os.getenv("SAFE_FAST_POLL_SECONDS", "").strip()

    poll_seconds = None
    if poll_seconds_raw:
        try:
            poll_seconds = float(poll_seconds_raw)
        except ValueError as exc:
            raise RuntimeError("SAFE_FAST_POLL_SECONDS must be numeric.") from exc

    return RunnerConfig(
        endpoint_url=endpoint_url,
        state_file=state_file,
        poll_seconds=poll_seconds,
        profile_name=os.getenv("SAFE_FAST_PROFILE_NAME", "runner_mvp").strip() or "runner_mvp",
        option_type=os.getenv("SAFE_FAST_OPTION_TYPE", "C").strip().upper() or "C",
        open_positions=int(os.getenv("SAFE_FAST_OPEN_POSITIONS", "0")),
        weekly_trade_count=int(os.getenv("SAFE_FAST_WEEKLY_TRADE_COUNT", "0")),
        timeout_seconds=float(os.getenv("SAFE_FAST_TIMEOUT_SECONDS", "15")),
    )


def _read_state_file(path: str) -> Dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    return json.loads(state_path.read_text(encoding="utf-8"))


async def _runner_loop() -> None:
    global _last_result, _last_error
    config = _load_config_from_env()

    if config.poll_seconds is None or config.poll_seconds <= 0:
        raise RuntimeError("SAFE_FAST_POLL_SECONDS is required for auto-start loop mode.")

    while True:
        try:
            _last_result = await run_once(config)
            _last_error = None
        except Exception as exc:
            _last_error = str(exc)
        await asyncio.sleep(config.poll_seconds)


@app.on_event("startup")
async def _startup() -> None:
    global _runner_task, _last_error
    if _env_bool("SAFE_FAST_RUNNER_AUTO_START", default=False):
        try:
            _runner_task = asyncio.create_task(_runner_loop())
        except Exception as exc:
            _last_error = str(exc)


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
    runner_enabled = _env_bool("SAFE_FAST_RUNNER_AUTO_START", default=False)
    return {
        "ok": True,
        "service": "safe_fast_continuous_runner",
        "runner_auto_start": runner_enabled,
        "runner_task_active": bool(_runner_task and not _runner_task.done()),
        "last_error": _last_error,
    }


@app.get("/runner/config")
async def runner_config() -> Dict[str, Any]:
    try:
        config = _load_config_from_env()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "endpoint_url": config.endpoint_url,
        "state_file": config.state_file,
        "poll_seconds": config.poll_seconds,
        "profile_name": config.profile_name,
        "option_type": config.option_type,
        "open_positions": config.open_positions,
        "weekly_trade_count": config.weekly_trade_count,
        "timeout_seconds": config.timeout_seconds,
    }


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
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    stored = _read_state_file(config.state_file)
    return {
        "last_result": _last_result,
        "stored_state": stored,
        "last_error": _last_error,
    }


@app.get("/")
async def root() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "safe_fast_continuous_runner",
        "health": "/healthz",
        "poll_once": "/runner/poll-once",
        "state": "/runner/state",
    }
