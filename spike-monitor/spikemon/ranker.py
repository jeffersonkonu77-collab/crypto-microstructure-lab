"""Watchlist ranking: top by 24h volume plus top 24h gainers and losers.

Spike scanning covers every crypto perpetual regardless of the watchlist. The
watchlist decides which coins get order book depth (walls) and what the
terminal dashboard focuses on. Entry is at rank <= N, exit only past
rank N + hysteresis_extra, so coins hovering at the cutoff do not churn.
"""

from __future__ import annotations


class Ranker:
    def __init__(self, cfg, universe):
        self.w = cfg.watchlist
        self.universe = set(universe)
        self.t: dict[str, tuple[float, float, float]] = {}
        self.members = {"V": set(), "G": set(), "L": set()}
        self.forced: dict[str, int] = {}
        self.watch: dict[str, str] = {}

    def update(self, items) -> None:
        """items: iterable of (symbol, last, quote_volume, pct_change_24h)."""
        for sym, last, qv, pct in items:
            if sym in self.universe:
                self.t[sym] = (last, qv, pct)

    def force(self, sym: str, now_ms: int) -> None:
        self.forced[sym] = now_ms + int(self.w.forced_hold_minutes * 60_000)

    def _pick(self, key: str, ranked: list[str], n: int) -> set:
        extra = int(self.w.hysteresis_extra)
        keep = {s for s in ranked[: n + extra] if s in self.members[key]}
        return keep | set(ranked[:n])

    def refresh(self, now_ms: int) -> tuple[set, set]:
        by_vol = sorted(self.t, key=lambda s: self.t[s][1], reverse=True)
        minv = float(self.w.min_volume_usd_movers)
        liquid = [s for s in self.t if self.t[s][1] >= minv]
        gainers = sorted((s for s in liquid if self.t[s][2] > 0),
                         key=lambda s: self.t[s][2], reverse=True)
        losers = sorted((s for s in liquid if self.t[s][2] < 0),
                        key=lambda s: self.t[s][2])
        self.members["V"] = self._pick("V", by_vol, int(self.w.top_volume))
        self.members["G"] = self._pick("G", gainers, int(self.w.top_gainers))
        self.members["L"] = self._pick("L", losers, int(self.w.top_losers))
        for s in [s for s, until in self.forced.items() if until < now_ms]:
            del self.forced[s]
        new: dict[str, str] = {}
        for tag, group in self.members.items():
            for s in group:
                new[s] = new.get(s, "") + tag
        for s in self.forced:
            if s in self.universe:
                new[s] = new.get(s, "") + "S"
        old_keys, new_keys = set(self.watch), set(new)
        self.watch = new
        return new_keys - old_keys, old_keys - new_keys
