#!/usr/bin/env python3
"""
Validation script for the SAFE-FAST Railway endpoint.

This is intentionally strict enough to catch the kinds of failures you were
manually checking for:
- app crashes / non-JSON responses
- missing top-level keys
- broken simple_output / user_facing blocks
- no-candidate context regressions on the current cleanup track

Set these env vars in the GitHub Action:
  SAFE_FAST_APP_URL
  SAFE_FAST_ENDPOINT_PATH
  SAFE_FAST_OPTION_TYPE
  SAFE_FAST_OPEN_POSITIONS
  SAFE_FAST_WEEKLY_TRADE_COUNT
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any


def fail(message: str) -> None:
    print(f"VALIDATION FAILED: {message}", file=sys.stderr)
    raise SystemExit(1)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", "replace")
            require(resp.status == 200, f"Expected HTTP 200, got {resp.status}: {raw}")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        fail(f"HTTP {exc.code} from endpoint: {raw}")
    except urllib.error.URLError as exc:
        fail(f"Endpoint request failed: {exc}")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        fail(f"Response was not valid JSON: {exc}: {raw}")

    require(isinstance(data, dict), "Top-level response must be a JSON object.")
    return data


def validate_common_shape(data: dict[str, Any]) -> None:
    for key in [
        "ok",
        "mode",
        "build_tag",
        "source_of_truth",
        "simple_output",
        "user_facing",
        "final_verdict",
        "best_ticker",
        "candidate_engine",
    ]:
        require(key in data, f"Missing top-level key: {key}")

    require(data["ok"] is True, "Expected ok == true.")
    require(data["mode"] == "on_demand", "Expected mode == 'on_demand'.")

    simple = data["simple_output"]
    user = data["user_facing"]
    require(isinstance(simple, dict), "simple_output must be an object.")
    require(isinstance(user, dict), "user_facing must be an object.")

    for block_name, block in [("simple_output", simple), ("user_facing", user)]:
        for key in ["good_idea_now", "ticker", "action", "invalidation", "setup_state", "why"]:
            require(key in block, f"Missing {key} in {block_name}")

    require(simple["ticker"] == user["ticker"], "simple_output.ticker and user_facing.ticker must match.")
    require(simple["action"] == user["action"], "simple_output.action and user_facing.action must match.")
    require(simple["why"] == user["why"], "simple_output.why and user_facing.why must match.")
    require(simple["invalidation"] == user["invalidation"], "simple_output.invalidation and user_facing.invalidation must match.")


def validate_current_no_candidate_track(data: dict[str, Any]) -> None:
    """
    This matches the branch you are actively cleaning up right now.
    If the response is *not* in the no-candidate path, this block will not fail.
    """
    no_candidate = data.get("no_candidate_context")
    if not isinstance(no_candidate, dict):
        print("INFO: no_candidate_context absent; skipping no-candidate branch validation.")
        return

    require(no_candidate.get("active") is True, "no_candidate_context.active should be true.")
    require(no_candidate.get("reason") == "No feasible candidates found for the current filters.",
            "Unexpected no_candidate_context.reason.")
    require(no_candidate.get("selection_mode") == "none",
            "Expected no_candidate_context.selection_mode == 'none'.")
    require(no_candidate.get("best_ticker") == data.get("best_ticker"),
            "no_candidate_context.best_ticker must match top-level best_ticker.")
    require(no_candidate.get("chart_check_status") == "skipped_no_candidate",
            "Expected chart_check_status == 'skipped_no_candidate'.")
    require(no_candidate.get("trigger_state") == "NO_CANDIDATE",
            "Expected trigger_state == 'NO_CANDIDATE'.")
    require(no_candidate.get("structure_status") == "no candidate available",
            "Expected structure_status == 'no candidate available'.")

    chart_check = data.get("chart_check", {})
    if isinstance(chart_check, dict):
        require(chart_check.get("status") == "skipped_no_candidate",
                "chart_check.status should be 'skipped_no_candidate' on the no-candidate branch.")

    failed_reasons = data.get("failed_reasons")
    if isinstance(failed_reasons, list):
        require(
            failed_reasons[:2] == [
                "No feasible candidates found for the current filters.",
                "market is closed",
            ],
            "Top failed_reasons ordering regressed."
        )


def main() -> None:
    app_url = os.environ["SAFE_FAST_APP_URL"].rstrip("/")
    endpoint_path = os.environ.get("SAFE_FAST_ENDPOINT_PATH", "/safe-fast/on-demand")
    url = f"{app_url}{endpoint_path}"

    payload = {
        "option_type": os.environ.get("SAFE_FAST_OPTION_TYPE", "C"),
        "open_positions": int(os.environ.get("SAFE_FAST_OPEN_POSITIONS", "0")),
        "weekly_trade_count": int(os.environ.get("SAFE_FAST_WEEKLY_TRADE_COUNT", "0")),
    }

    print("POST", url)
    print("PAYLOAD", json.dumps(payload, indent=2))
    data = post_json(url, payload)

    validate_common_shape(data)
    validate_current_no_candidate_track(data)

    print("VALIDATION PASSED")
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
