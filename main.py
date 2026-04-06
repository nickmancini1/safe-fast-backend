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
    screenshot_traps_context: Dict[str, Any],
    checklist: Optional[Dict[str, Any]] = None,
    trigger_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ticker = best_ticker or "UNKNOWN"
    ema_text = str(chart_check.get("ema50_1h")) if chart_check and chart_check.get("ok") else "unconfirmed"
    no_candidate_mode = liquidity_context.get("why") == "No candidate available."

    base_invalidation_text = (
        f"1H close beyond EMA50 against thesis. Current EMA50_1h anchor: {ema_text}."
        if best_ticker and chart_check and chart_check.get("ok")
        else "No valid new entry from the current combined read."
    )

    if no_candidate_mode and not (chart_check and chart_check.get("ok")):
        base_invalidation_text = "No valid new entry from the current combined read."

    trigger_state = trigger_state or {}
    primary_blocker_text = _priority_blocker_user_text(
        checklist=checklist,
        structure_context=structure_context,
        liquidity_context=liquidity_context,
        trigger_state=trigger_state,
        screenshot_traps_context=screenshot_traps_context,
        chart_check_error=chart_check_error,
        engine_reason=engine_reason,
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

    if not market_context["is_open"]:
        blocking_reasons: List[str] = []

        if liquidity_context.get("liquidity_pass") is False:
            blocking_reasons.append(
                liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry."
            )

        if chart_check_error:
            blocking_reasons.append("Chart check failed in this run.")

        if structure_context.get("ok"):
            if structure_context.get("room_pass") is False:
                blocking_reasons.append(_room_failure_user_text(structure_context))
            if structure_context.get("extension_state") == "extended":
                blocking_reasons.append("Move is too extended from the 1H 50 EMA.")
            if structure_context.get("allowed_setup") is False:
                blocking_reasons.append(f"Setup type is {structure_context.get('setup_type')}, which is not tradable now.")
            if structure_context.get("wall_pass") is False:
                blocking_reasons.append("Wall thesis and strike placement do not match.")
        elif chart_check_error:
            blocking_reasons.append("Candidate engine result only - chart confirmation still required.")

        if _trap_blocks_trade(screenshot_traps_context):
            blocking_reasons.append(_trap_failure_user_text(screenshot_traps_context))

        if primary_blocker_text or blocking_reasons:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": primary_blocker_text or blocking_reasons[0],
            }

        return {
            "good_idea_now": "WAIT",
            "ticker": ticker,
            "action": "wait for next regular session",
            "invalidation": base_invalidation_text,
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
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": macro_context.get("note") or "Major event risk is inside the expected hold window.",
        }

    if not time_day_gate.get("fresh_entry_allowed"):
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": f"Time/day filter fails: {time_day_gate.get('reason')}.",
        }

    if liquidity_context.get("liquidity_pass") is False:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": liquidity_context.get("why") or "Options liquidity is too wide for a clean SAFE-FAST entry.",
        }

    if not best_ticker:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": "No valid candidate engine setup is available.",
            "setup_state": "NO TRADE",
            "why": engine_reason,
        }

    if primary_blocker_text:
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": primary_blocker_text,
        }

    if structure_context.get("ok"):
        if structure_context.get("room_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": _room_failure_user_text(structure_context),
            }
        if structure_context.get("wall_pass") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": "Wall thesis and strike placement do not match.",
            }
        if structure_context.get("extension_state") == "extended":
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": "Move is extended vs the 1H 50 EMA or too late relative to the first wall.",
            }
        if structure_context.get("allowed_setup") is False:
            return {
                "good_idea_now": "NO",
                "ticker": ticker,
                "action": "stand down",
                "invalidation": base_invalidation_text,
                "setup_state": "NO TRADE",
                "why": f"Setup type is {structure_context.get('setup_type')}, which is not tradable now.",
            }

    if final_verdict == "NO_TRADE":
        why = "Best ticker failed the 1H EMA alignment check."
        if chart_check_error:
            why = "Chart check failed in this run."
        invalidation_text = (
            base_invalidation_text
            if best_ticker and chart_check and chart_check.get("ok")
            else "No valid new entry from the current combined read."
        )
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": invalidation_text,
            "setup_state": "NO TRADE",
            "why": why,
        }

    if trigger_state.get("entry_state") == "ACTIVE_NOW" and trigger_state.get("trigger_present") is True:
        live_reason = "Mapped trigger is live now and SAFE-FAST gates pass."
        if structure_context.get("trend_label"):
            live_reason = f"{structure_context.get('trend_label')}, mapped trigger is live now and not blocked."
        return {
            "good_idea_now": "YES",
            "ticker": ticker,
            "action": "enter",
            "invalidation": base_invalidation_text,
            "setup_state": "ACTIVE NOW",
            "why": live_reason,
        }

    if trigger_state.get("entry_state") == "PENDING_TRIGGER":
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "wait",
            "invalidation": base_invalidation_text,
            "setup_state": "PENDING",
            "why": "Candidate engine is valid, but the live close trigger is not present yet.",
        }

    if trigger_state.get("entry_state") == "SIGNAL_PRESENT_BUT_BLOCKED":
        return {
            "good_idea_now": "NO",
            "ticker": ticker,
            "action": "stand down",
            "invalidation": base_invalidation_text,
            "setup_state": "NO TRADE",
            "why": primary_blocker_text or "Signal is present, but SAFE-FAST blockers still fail.",
        }

    return {
        "good_idea_now": "NO",
        "ticker": ticker,
        "action": "wait",
        "invalidation": base_invalidation_text,
        "setup_state": "PENDING",
        "why": "Candidate engine is valid, but trigger/entry-zone timing still needs confirmation.",
    }
