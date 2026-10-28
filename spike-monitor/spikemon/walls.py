"""Order book wall tracker (watchlist coins only).

Uses Binance's partial book depth snapshots (top 20 levels per side). A level
is a wall when its notional is at least `median_multiple` times the median
level on that side AND above a dollar floor (higher for $1B+ coins). A wall is
"confirmed" after it persists for `confirm_snapshots` consecutive snapshots,
which filters flicker. When a confirmed wall disappears from a price that is
still inside the visible book, it is labelled:

* eaten:  trades hit its side at or through its price for at least
          `eaten_fraction` of its size
* pulled: it vanished without being traded (cancelled)
"""

from __future__ import annotations

import statistics


class Wall:
    __slots__ = ("side", "price", "first_ms", "notional", "peak", "seen",
                 "confirmed", "traded")

    def __init__(self, side, price, ts_ms, notional):
        self.side = side
        self.price = price
        self.first_ms = ts_ms
        self.notional = notional
        self.peak = notional
        self.seen = 1
        self.confirmed = False
        self.traded = 0.0


class WallTracker:
    def __init__(self, cfg, is_big, on_event):
        w = cfg.walls
        self.min_big = float(w.min_usd_big)
        self.min_small = float(w.min_usd_small)
        self.mult = float(w.median_multiple)
        self.confirm = int(w.confirm_snapshots)
        self.eaten = float(w.eaten_fraction)
        self.is_big = is_big
        self.on_event = on_event
        self.walls: dict[str, dict] = {}
        self.events = 0

    def drop(self, sym: str) -> None:
        self.walls.pop(sym, None)

    def active(self, sym: str) -> int:
        return sum(1 for w in self.walls.get(sym, {}).values() if w.confirmed)

    def on_depth(self, sym: str, bids, asks, ts_ms: int) -> None:
        floor = self.min_big if self.is_big(sym) else self.min_small
        cur = self.walls.setdefault(sym, {})
        seen: set = set()
        levels_by_side: dict = {}
        thr_by_side: dict = {}
        range_by_side: dict = {}
        for side, levels in (("bid", bids), ("ask", asks)):
            if len(levels) < 5:
                continue
            parsed = [(float(p), float(q)) for p, q in levels]
            notionals = [p * q for p, q in parsed]
            thr = max(floor, self.mult * statistics.median(notionals))
            prices = [p for p, _ in parsed]
            levels_by_side[side] = {p: n for (p, _), n in zip(parsed, notionals)}
            thr_by_side[side] = thr
            range_by_side[side] = (min(prices), max(prices))
            for (p, _), n in zip(parsed, notionals):
                if n < thr:
                    continue
                key = (side, p)
                seen.add(key)
                w = cur.get(key)
                if w is None:
                    cur[key] = Wall(side, p, ts_ms, n)
                else:
                    w.notional = n
                    w.peak = max(w.peak, n)
                    w.seen += 1
                    if not w.confirmed and w.seen >= self.confirm:
                        w.confirmed = True
                        self._emit(sym, w, "appeared", ts_ms)
        for key, w in list(cur.items()):
            if key in seen:
                continue
            side, p = key
            if side not in levels_by_side:
                continue
            lo, hi = range_by_side[side]
            if p < lo or p > hi:
                del cur[key]  # fell out of the visible window, not a cancel
                continue
            still = levels_by_side[side].get(p)
            if still is not None and still >= 0.5 * thr_by_side[side]:
                w.notional = still  # shrunk but still a wall
                continue
            del cur[key]
            if not w.confirmed:
                continue
            frac = min(w.traded, w.peak) / w.peak if w.peak > 0 else 0.0
            self._emit(sym, w, "eaten" if frac >= self.eaten else "pulled", ts_ms)

    def on_trade(self, sym: str, price: float, notional: float, is_sell: bool) -> None:
        cur = self.walls.get(sym)
        if not cur:
            return
        for (side, p), w in cur.items():
            if side == "bid" and is_sell and price <= p:
                w.traded += notional
            elif side == "ask" and not is_sell and price >= p:
                w.traded += notional

    def _emit(self, sym: str, w: Wall, kind: str, ts_ms: int) -> None:
        self.events += 1
        traded = 0.0 if kind == "appeared" else min(w.traded, w.peak)
        self.on_event({
            "ts_ms": ts_ms, "symbol": sym, "side": w.side, "price": w.price,
            "notional": w.peak if kind != "appeared" else w.notional, "kind": kind,
            "traded_notional": traded,
            "lifetime_s": 0.0 if kind == "appeared" else (ts_ms - w.first_ms) / 1000.0,
        })
