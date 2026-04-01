import os
from typing import Dict, Any, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dxlink_candles import get_1h_ema50_snapshot

app = FastAPI()

ALLOWED_SYMBOLS = {"SPY", "QQQ", "IWM", "GLD"}
SYMBOL_ORDER = ["SPY", "QQQ", "IWM", "GLD"]


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
