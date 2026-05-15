from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

LIVE_POSITIONS_PATH = "logs/live_positions.json"


@dataclass
class LivePosition:
    position_id: str
    state: str
    token_id: str
    opposing_token_id: str
    match_id: str
    market_name: str | None
    side: str
    entry_price: float
    shares: float
    cost_usd: float
    entry_time_ns: int
    entry_game_time_sec: int | None
    event_type: str
    expected_move: float
    fair_price: float
    exit_attempts: int = 0
    last_exit_attempt_ns: int | None = None
    exit_reason: str | None = None


class LivePositionStore:
    def __init__(self, path: str = LIVE_POSITIONS_PATH):
        self.path = path
        self.positions: dict[str, LivePosition] = {}
        self.load()

    def load(self) -> None:
        if not os.path.exists(self.path):
            self.positions = {}
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.positions = {
                row["position_id"]: LivePosition(**row)
                for row in data.get("positions", [])
            }
        except (json.JSONDecodeError, KeyError, TypeError):
            self.positions = {}

    def save(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        data = {
            "updated_at_ns": time.time_ns(),
            "positions": [asdict(p) for p in self.positions.values()],
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)

    def add(self, pos: LivePosition) -> None:
        self.positions[pos.position_id] = pos
        self.save()

    def open_positions(self) -> list[LivePosition]:
        return [
            p for p in self.positions.values()
            if p.state in {"OPEN", "PARTIALLY_EXITED"}
        ]

    def mark_exiting(self, position_id: str, reason: str) -> None:
        if position_id not in self.positions:
            return
        p = self.positions[position_id]
        p.state = "EXITING"
        p.exit_reason = reason
        p.exit_attempts += 1
        p.last_exit_attempt_ns = time.time_ns()
        self.save()

    def mark_closed(self, position_id: str) -> None:
        if position_id not in self.positions:
            return
        p = self.positions[position_id]
        p.state = "CLOSED"
        self.save()

    def mark_open_again(self, position_id: str) -> None:
        if position_id not in self.positions:
            return
        p = self.positions[position_id]
        p.state = "OPEN"
        self.save()
