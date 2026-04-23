from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

CANONICAL_ENGINE_FILENAME = "main_remaining_reason_tail_cleanup_patch1_full(1).py"
CANONICAL_ENGINE_PATH = Path(__file__).resolve().parent / CANONICAL_ENGINE_FILENAME


def _error_result(
    *,
    engine_path: Path,
    error_stage: str,
    error_type: str,
    error_message: str,
    traceback_text: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "engine_ok": False,
        "engine_path": str(engine_path),
        "error_stage": error_stage,
        "engine_error_type": error_type,
        "engine_error_message": error_message,
        "traceback": traceback_text,
        "raw_result": None,
    }


def _success_result(*, engine_path: Path, raw_result: Any) -> Dict[str, Any]:
    return {
        "engine_ok": True,
        "engine_path": str(engine_path),
        "error_stage": None,
        "engine_error_type": None,
        "engine_error_message": None,
        "traceback": None,
        "raw_result": raw_result,
    }


def _load_engine_module(engine_path: Path):
    if not engine_path.exists():
        raise FileNotFoundError(f"Canonical engine file not found: {engine_path}")

    module_name = "safe_fast_canonical_engine_attached"
    spec = importlib.util.spec_from_file_location(module_name, str(engine_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create import spec for {engine_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


async def _run_engine_async(module: Any) -> Any:
    if hasattr(module, "_default_on_demand_request") and callable(module._default_on_demand_request):
        request = module._default_on_demand_request()
    elif hasattr(module, "OnDemandRequest"):
        request = module.OnDemandRequest(
            option_type="C",
            open_positions=0,
            weekly_trade_count=0,
        )
    else:
        raise AttributeError("Canonical engine is missing OnDemandRequest and _default_on_demand_request")

    if hasattr(module, "safe_fast_on_demand") and callable(module.safe_fast_on_demand):
        return await module.safe_fast_on_demand(request)

    if hasattr(module, "_build_on_demand_payload") and callable(module._build_on_demand_payload):
        payload = await module._build_on_demand_payload(request)
        if hasattr(module, "_ensure_contracts_surface") and callable(module._ensure_contracts_surface):
            return module._ensure_contracts_surface(payload)
        return payload

    raise AttributeError("Canonical engine is missing safe_fast_on_demand and _build_on_demand_payload")


def run_canonical_engine(engine_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Success schema:
    {
      "engine_ok": True,
      "engine_path": "<str>",
      "error_stage": None,
      "engine_error_type": None,
      "engine_error_message": None,
      "traceback": None,
      "raw_result": <Any>
    }

    Error schema:
    {
      "engine_ok": False,
      "engine_path": "<str>",
      "error_stage": "load_module|execute_engine",
      "engine_error_type": "<str>",
      "engine_error_message": "<str>",
      "traceback": "<str|None>",
      "raw_result": None
    }
    """
    resolved_path = Path(engine_path) if engine_path is not None else CANONICAL_ENGINE_PATH

    try:
        module = _load_engine_module(resolved_path)
    except Exception as exc:
        return _error_result(
            engine_path=resolved_path,
            error_stage="load_module",
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
        )

    try:
        raw_result = asyncio.run(_run_engine_async(module))
        return _success_result(engine_path=resolved_path, raw_result=raw_result)
    except Exception as exc:
        return _error_result(
            engine_path=resolved_path,
            error_stage="execute_engine",
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
        )


if __name__ == "__main__":
    result = run_canonical_engine()
    print(json.dumps(result, indent=2, default=str))
