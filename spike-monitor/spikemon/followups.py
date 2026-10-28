"""Follow-through price recorder.

For every spike it records the traded price (and BTC's price) at fixed delays
after detection. The interval-close and next-interval-close marks are emitted
by the engine when those intervals end.
"""

from __future__ import annotations

import heapq
import itertools


def label_for(h: float) -> str:
    h = int(h)
    if h < 60:
        return f"{h}s"
    if h % 60 == 0:
        return f"{h // 60}m"
    return f"{h}s"


class FollowUps:
    def __init__(self, horizons_s, get_price, get_btc, emit):
        self.horizons = list(horizons_s)
        self.get_price = get_price
        self.get_btc = get_btc
        self.emit = emit
        self._heap: list = []
        self._seq = itertools.count()

    @property
    def pending(self) -> int:
        return len(self._heap)

    def register(self, event_id: int, symbol: str, t0_ms: int) -> None:
        for h in self.horizons:
            heapq.heappush(
                self._heap,
                (t0_ms + int(h * 1000), next(self._seq), event_id, symbol, h),
            )

    def tick(self, now_ms: int) -> int:
        n = 0
        while self._heap and self._heap[0][0] <= now_ms:
            due, _, event_id, symbol, h = heapq.heappop(self._heap)
            price = self.get_price(symbol)
            if not price:
                continue
            self.emit({
                "event_id": event_id, "label": label_for(h), "horizon_s": float(h),
                "due_ms": due, "actual_ms": now_ms, "price": price,
                "btc_price": self.get_btc(),
            })
            n += 1
        return n
