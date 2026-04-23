from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import traceback
import types
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

CANONICAL_ENGINE_FILENAME = "main_remaining_reason_tail_cleanup_patch1_full(1).py"
CANONICAL_ENGINE_PATH = Path(__file__).resolve().parent / CANONICAL_ENGINE_FILENAME

# TEST-ONLY / LOADER-ONLY.
# This must stay OFF by default so a missing live dependency never gets silently faked.
TEST_ONLY_DXLINK_SHIM_ENV = "SAFE_FAST_ALLOW_TEST_ONLY_DXLINK_SHIM"


def _error_result(
    *,
    engine_path: Path,
    error_stage: str,
    error_type: str,
    error_message: str,
    traceback_text: Optional[str] = None,
    test_only_dxlink_shim_active: bool = False,
) -> Dict[str, Any]:
    return {
        "engine_ok": False,
        "engine_path": str(engine_path),
        "error_stage": error_stage,
        "engine_error_type": error_type,
        "engine_error_message": error_message,
        "traceback": traceback_text,
        "test_only_dxlink_shim_active": test_only_dxlink_shim_active,
        "raw_result": None,
    }


def _success_result(
    *,
    engine_path: Path,
    raw_result: Any,
    test_only_dxlink_shim_active: bool = False,
) -> Dict[str, Any]:
    return {
        "engine_ok": True,
        "engine_path": str(engine_path),
        "error_stage": None,
        "engine_error_type": None,
        "engine_error_message": None,
        "traceback": None,
        "test_only_dxlink_shim_active": test_only_dxlink_shim_active,
        "raw_result": raw_result,
    }


def _test_only_dxlink_shim_enabled() -> bool:
    return os.getenv(TEST_ONLY_DXLINK_SHIM_ENV, "0").strip() == "1"


def _install_test_only_dxlink_shim_if_allowed() -> bool:
    """
    TEST-ONLY / LOADER-ONLY path.
    Returns True only when the explicit shim path is active.
    """
    existing_module = sys.modules.get("dxlink_candles")
    if existing_module is not None:
        return bool(getattr(existing_module, "__SAFE_FAST_TEST_ONLY_SHIM__", False))

    if importlib.util.find_spec("dxlink_candles") is not None:
        return False

    if not _test_only_dxlink_shim_enabled():
        raise ModuleNotFoundError(
            "Missing dependency: dxlink_candles. "
            "Live mode is blocked because the TEST-ONLY shim is disabled. "
            f"Set {TEST_ONLY_DXLINK_SHIM_ENV}=1 only for loader/testing use."
        )

    shim = types.ModuleType("dxlink_candles")
    shim.__SAFE_FAST_TEST_ONLY_SHIM__ = True
    shim.__SAFE_FAST_TEST_ONLY_SHIM_REASON__ = (
        "TEST-ONLY / LOADER-ONLY shim active because real dxlink_candles "
        "is unavailable and explicit opt-in was enabled."
    )

    async def get_1h_ema50_snapshot(*args: Any, **kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError(
            "TEST-ONLY dxlink_candles shim is active. "
            "Real market-data dependency is not wired."
        )

    shim.get_1h_ema50_snapshot = get_1h_ema50_snapshot
    sys.modules["dxlink_candles"] = shim
    return True


def _load_engine_module(engine_path: Path) -> Tuple[Any, bool]:
    if not engine_path.exists():
        raise FileNotFoundError(f"Canonical engine file not found: {engine_path}")

    test_only_dxlink_shim_active = _install_test_only_dxlink_shim_if_allowed()

    module_name = "safe_fast_canonical_engine_attached"
    spec = importlib.util.spec_from_file_location(module_name, str(engine_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create import spec for {engine_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module, test_only_dxlink_shim_active


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
    resolved_path = Path(engine_path) if engine_path is not None else CANONICAL_ENGINE_PATH

    try:
        module, test_only_dxlink_shim_active = _load_engine_module(resolved_path)
    except Exception as exc:
        return _error_result(
            engine_path=resolved_path,
            error_stage="load_module",
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
            test_only_dxlink_shim_active=bool(
                getattr(sys.modules.get("dxlink_candles"), "__SAFE_FAST_TEST_ONLY_SHIM__", False)
            ),
        )

    try:
        raw_result = asyncio.run(_run_engine_async(module))
        return _success_result(
            engine_path=resolved_path,
            raw_result=raw_result,
            test_only_dxlink_shim_active=test_only_dxlink_shim_active,
        )
    except Exception as exc:
        return _error_result(
            engine_path=resolved_path,
            error_stage="execute_engine",
            error_type=type(exc).__name__,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
            test_only_dxlink_shim_active=test_only_dxlink_shim_active,
        )


if __name__ == "__main__":
    result = run_canonical_engine()
    print(json.dumps(result, indent=2, default=str))
