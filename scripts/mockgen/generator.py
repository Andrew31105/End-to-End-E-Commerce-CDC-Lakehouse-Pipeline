"""Vòng lặp traffic: đơn hàng + thay đổi bảng chiều."""
from __future__ import annotations

import random
import time

from .dimensions import DimensionMutator, DimensionValues
from .orders import OrderTraffic
from .sources import OlistOrders
from .state import DbState
from .util import log


class TrafficGenerator:
    def __init__(
        self,
        conn,
        cur,
        state: DbState,
        orders: OlistOrders | None,
        values: DimensionValues,
        transitions: dict[str, list[tuple[str, float]]],
        *,
        verbose: bool = False,
        dim_ratio: float = 0.2,
        stock_ratio: float = 0.0,
        pool_ttl: float = 15.0,
        pool_max: int = 50_000,
    ) -> None:
        self.dim_ratio = dim_ratio
        self.stock_ratio = stock_ratio
        self.order_traffic = OrderTraffic(
            conn, cur, state, orders, transitions,
            verbose=verbose, pool_ttl=pool_ttl, pool_max=pool_max,
        )
        self.dimensions = DimensionMutator(conn, cur, state, values, verbose=verbose)

    def run(
        self,
        update_ratio: float,
        rate: float = 0.0,
        interval: float = 2.0,
        duration: float = 0.0,
    ) -> None:
        """Chạy đến khi Ctrl+C hoặc hết ``duration``.

        * ``rate > 0``     : số event/giây (bù trừ thời gian xử lý)
        * ``rate == 0``    : nghỉ ``interval`` giây giữa 2 event
        * ``update_ratio`` : tỉ lệ UPDATE so với INSERT
        """
        target_dt = 1.0 / rate if rate > 0 else max(interval, 0.001)
        mode = f"rate {rate:g} event/s" if rate > 0 else f"interval {interval}s"
        log(f" Traffic Generator started | {mode} | update_ratio {update_ratio} "
            f"| Ctrl+C để dừng")

        started = time.monotonic()
        next_report = started + 5.0
        counts = {"insert": 0, "update": 0, "dim": 0}
        events = 0

        try:
            while True:
                cycle_start = time.monotonic()

                outcome = self.order_traffic.step(update_ratio)
                if outcome == "insert":
                    counts["insert"] += 1
                elif outcome == "update":
                    counts["update"] += 1
                events += 1

                # Thay đổi bảng chiều → CDC → Silver SCD2
                if (self.dim_ratio > 0 and random.random() < self.dim_ratio
                        and self.dimensions.mutate()):
                    counts["dim"] += 1

                # Cột biến động nhanh (Type 1, mặc định tắt)
                if (self.stock_ratio > 0 and random.random() < self.stock_ratio
                        and self.dimensions.mutate_stock()):
                    counts["dim"] += 1

                now = time.monotonic()
                if now >= next_report:
                    elapsed = now - started
                    log(f"  {events} event | {events / elapsed:.1f} event/s "
                        f"| INSERT {counts['insert']} / UPDATE {counts['update']} "
                        f"| DIM {counts['dim']} | pool {self.order_traffic.pool_size}")
                    next_report = now + 5.0

                if duration and now - started >= duration:
                    break

                # Bù trừ thời gian xử lý để giữ đúng rate
                sleep_for = target_dt - (time.monotonic() - cycle_start)
                if sleep_for > 0:
                    time.sleep(sleep_for)

        except KeyboardInterrupt:
            log(" Stopped by user.")
        finally:
            elapsed = time.monotonic() - started
            if elapsed > 0:
                log(f" Tổng kết: {events} event trong {elapsed:.1f}s "
                    f"≈ {events / elapsed:.1f} event/s | INSERT {counts['insert']} / "
                    f"UPDATE {counts['update']} | DIM {counts['dim']}")
