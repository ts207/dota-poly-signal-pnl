from __future__ import annotations

import json
import os
import time
from pathlib import Path

LIVE_STATE_PATH = "logs/live_state.json"

def load_live_state() -> dict:
    """Load persisted live risk state from disk."""
    default = {
        "total_submitted_usd": 0.0,
        "total_filled_usd": 0.0,
        "open_positions": 0,
        "updated_at_ns": 0
    }
    if not os.path.exists(LIVE_STATE_PATH):
        return default
    try:
        with open(LIVE_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Ensure all keys exist
            for k, v in default.items():
                if k not in data:
                    data[k] = v
            return data
    except Exception:
        return default

def save_live_state(total_submitted_usd: float, total_filled_usd: float, open_positions: int):
    """Persist live risk state to disk."""
    os.makedirs(os.path.dirname(LIVE_STATE_PATH), exist_ok=True)
    state = {
        "total_submitted_usd": float(total_submitted_usd),
        "total_filled_usd": float(total_filled_usd),
        "open_positions": int(open_positions),
        "updated_at_ns": time.time_ns()
    }
    with open(LIVE_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
