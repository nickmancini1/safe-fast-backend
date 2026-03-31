import os
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dxlink_candles import get_1h_ema50_snapshot

app = FastAPI()

ALLOWED_SYMBOLS = {"SPY", "QQQ", "IWM", "GLD"}


# =========================
# REQUEST MODEL
# =========================
class OnDemandRequest(BaseModel):
    option_type: str = "C"
    min_dte: int = 14
    max_dte: int = 30
    near_limit: int = 16
    width_min: float = 5
    width_max: float = 10
    risk_min_dollars: float = 250
    risk_max_dollars: float = 300
    hard_max_dollars: float = 400
    allow_fallback: bool = True

    include_chart_checks: bool = True
    open_positions: int = 0
    weekly_trade_count: int = 0


# =========================
# CORE ENGINE CALL
# =========================
async def call_candidate_engine(req: OnDemandRequest):
    url = "https://safe-fast-backend-production.up.railway.app/getSafeFastSummaryCompact"

    params = {
        "option_type": req.option_type,
        "min_dte": req.min_dte,
        "max_dte": req.max_dte,
        "near_limit": req.near_limit,
        "width_min": req.width_min,
        "width_max": req.width_max,
        "risk_min_dollars": req.risk_min_dollars,
        "risk_max_dollars": req.risk_max_dollars,
        "hard_max_dollars": req.hard_max_dollars,
        "allow_fallback": req.allow_fallback,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)

    if r.status_code != 200:
        raise HTTPException(status_code=500, detail="candidate engine failed")

    return r.json()


# =========================
# HELPER
# =========================
def build_chart_block(chart_data: Optional[dict]):
    if not chart_data:
        return {
            "confirmed": False,
            "message": "chart check unavailable",
            "fields": {
                "one_hour_50_ema": {"status": "unconfirmed", "value": None},
                "price_vs_ema": {"status": "unconfirmed", "value": None},
            },
        }

    return {
        "confirmed": False,
        "message": "chart check present but not validated",
        "fields": {
            "one_hour_50_ema": {
                "status": "confirmed",
                "value": chart_data.get("ema50_1h"),
            },
            "price_vs_ema": {
                "status": "confirmed",
                "value": chart_data.get("price_vs_ema50_1h"),
            },
        },
    }


# =========================
# MAIN ENDPOINT
# =========================
@app.post("/safe-fast/on-demand")
async def safe_fast_on_demand(req: OnDemandRequest):

    engine = await call_candidate_engine(req)

    best = engine.get("best_ticker")
    primary = engine.get("primary_candidate")

    chart_data = None

    # =========================
    # OPTIONAL CHART CHECK
    # =========================
    if req.include_chart_checks and best in ALLOWED_SYMBOLS:
        try:
            chart_data = await get_1h_ema50_snapshot(
                symbol=best,
                access_token=os.getenv("DXLINK_TOKEN"),
                api_base="https://api.tastyworks.com",
                user_agent="safe-fast",
            )
        except Exception:
            chart_data = None

    # =========================
    # FINAL VERDICT LOGIC
    # =========================
    if req.open_positions > 0:
        final_verdict = "NO_TRADE"
        action = "stand_down"

    elif engine.get("verdict") == "ACTIVE_NOW":
        final_verdict = "TRADE"
        action = "enter"

    else:
        final_verdict = "NO_TRADE"
        action = "wait"

    # =========================
    # RESPONSE
    # =========================
    return {
        "mode": "on_demand",
        "engine": engine,

        "decision": {
            "ticker": best,
            "verdict": final_verdict,
            "action": action,
            "reason": engine.get("reason"),
        },

        "candidate": primary,

        "chart_confirmation": build_chart_block(chart_data),

        "user_facing": {
            "good_idea_now": "YES" if final_verdict == "TRADE" else "NO",
            "ticker": best,
            "action": action,
            "setup_state": final_verdict,
            "why": engine.get("reason"),
        },
    }
