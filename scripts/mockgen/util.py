"""Tiện ích nhỏ dùng chung cho mock generator."""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Sequence, TypeVar

T = TypeVar("T")


def log(message: str) -> None:
    """Log 1 dòng có timestamp UTC (giữ format cũ để dễ grep)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {message}")


def weighted_choice(options: Sequence[tuple[T, float]]) -> T:
    """Chọn một phần tử theo trọng số (tổng trọng số không cần bằng 1)."""
    roll = random.random() * sum(weight for _, weight in options)
    for value, weight in options:
        roll -= weight
        if roll <= 0:
            return value
    return options[-1][0]
