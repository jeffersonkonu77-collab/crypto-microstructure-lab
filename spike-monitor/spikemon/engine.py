"""Spike engine: per-symbol 15-minute interval state and trigger logic.

Definitions
-----------
* Intervals sit on UTC clock boundaries (:00, :15, :30, :45), like any 15m bar.
* The move is the return from the interval's first trade price to the live price.
* Trigger: 3% for coins with 24h quote volume >= $1B, 5% otherwise. The tier
  follows the live 24h volume.
* One alert per coin per interval. If the coin later also crosses the trigger in
  the opposite direction inside the same interval, a single VERY VOLATILE
  upgrade alert follows (or one VOLATILE alert straight away if both sides were
  already crossed when the first alert fires).
* z-score = move / sigma, where sigma = 1.4826 * median(|15m open-to-close
  return|) over the last 96 completed intervals. A median is used so earlier
  spikes cannot inflate the baseline. The constant 1.4826 makes sigma equal the
  standard deviation for normally distributed returns. Crypto returns are
  fat-tailed, so z is a ranking tool, not a probability.
"""

from __future__ import annotations

import itertools
import statistics
import time
from collections import deque

SIGMA_K = 1.4826
TOP_KEEP = 5


class SymbolState:
    __slots__ = (
        "symbol", "idx", "start_ms", "open", "high", "low", "last", "last_ms", "empty",
        "vol_base", "buy", "sell", "n", "liq_long", "liq_short", "top", "top_min",
        "alert_dir", "volatile_sent", "partial", "gap", "hist", "vol24h", "chg24h",
        "thr_pct", "tier", "in_watch", "watch_tags", "n_events", "walls",
        "pending_close", "pending_next_close",
    )

    def __init__(self, symbol: str, hist_len: int):
        self.symbol = symbol
        self.idx: int | None = None
        self.start_ms = 0
        self.open = self.high = self.low = self.last = 0.0
        self.last_ms = 0
        self.empty = True
        self.vol_base = 0.0
        self.buy = 0.0
        self.sell = 0.0
        self.n = 0
        self.liq_long = 0.0
        self.liq_short = 0.0
        self.top: list = []
        self.top_min = 0.0
        self.alert_dir = 0
        self.volatile_sent = False
        self.partial = False
        self.gap = False
        self.hist: deque = deque(maxlen=hist_len)
        self.vol24h = 0.0
        self.chg24h = 0.0
        self.thr_pct = 5.0
        self.tier = "lt1b"
        self.in_watch = False
        self.watch_tags = ""
        self.n_events = 0
        self.walls: list = []
        self.pending_close: list = []
        self.pending_next_close: list = []


class SpikeEngine:
    def __init__(self, cfg, on_spike, on_interval, on_followup, next_id=None,
                 clock=time.time):
        s = cfg.spike
        self.iv = int(s.interval_minutes * 60_000)
        self.big_vol = float(s.big_tier_volume_usd)
        self.big_thr = float(s.big_tier_threshold_pct)
        self.small_thr = float(s.small_tier_threshold_pct)
        self.hist_len = int(s.history_intervals)
        self.min_base = int(s.min_baseline_intervals)
        self.flag_pct = float(s.order_flag_pct_of_volume)
        self.n_top = int(s.top_orders)
        self.volatile_upgrade = bool(s.volatile_upgrade)
        self.grace_ms = int(s.roll_grace_s * 1000)
        self.btc = cfg.universe.btc_symbol
        self.on_spike = on_spike
        self.on_interval = on_interval
        self.on_followup = on_followup
        self.next_id = next_id or itertools.count(1).__next__
        self.clock = clock
        self.states: dict[str, SymbolState] = {}
        self.armed = False
        self.trade_count = 0
        self.btc_close: dict[int, float] = {}
        self.spikes_total = 0
        self.volatile_total = 0

    # ---- setup ----------------------------------------------------------------
    def add_symbols(self, symbols) -> None:
        for sym in symbols:
            if sym not in self.states:
                self.states[sym] = SymbolState(sym, self.hist_len)

    def load_klines(self, sym: str, rows: list, now_ms: int) -> list[dict]:
        """Seed baseline and the in-progress interval from REST klines.
        Returns interval rows for completed history so they can be stored."""
        st = self.states.get(sym)
        if st is None:
            return []
        cur_idx = now_ms // self.iv
        out = []
        for r in rows:
            idx = int(r[0]) // self.iv
            o, h, l, c = float(r[1]), float(r[2]), float(r[3]), float(r[4])
            qv = float(r[7])
            buyq = float(r[10]) if len(r) > 10 else 0.0
            if idx < cur_idx:
                st.hist.append((abs(c / o - 1.0) if o > 0 else 0.0, qv))
                out.append({
                    "symbol": sym, "start_ms": idx * self.iv, "open": o, "high": h,
                    "low": l, "close": c, "quote_volume": qv, "buy_notional": buyq,
                    "sell_notional": max(qv - buyq, 0.0), "liq_long": 0.0,
                    "liq_short": 0.0, "n_trades": int(r[8]) if len(r) > 8 else 0,
                    "partial": 0, "source": "kline",
                })
            elif idx == cur_idx and st.idx is None:
                self._reset(st, idx, o, empty=False)
                st.high, st.low, st.last = h, l, c
                st.vol_base = qv
                st.partial = True
        return out

    # ---- feeds ----------------------------------------------------------------
    def on_ticker(self, sym: str, last: float, vol24h: float, chg_pct: float) -> None:
        st = self.states.get(sym)
        if st is None:
            return
        st.vol24h = vol24h
        st.chg24h = chg_pct
        if vol24h >= self.big_vol:
            st.thr_pct, st.tier = self.big_thr, "ge1b"
        else:
            st.thr_pct, st.tier = self.small_thr, "lt1b"
        if st.last == 0.0 and last > 0:
            st.last = last

    def on_liquidation(self, sym: str, side: str, notional: float) -> None:
        st = self.states.get(sym)
        if st is None:
            return
        if side == "SELL":  # forced sell = a long was liquidated
            st.liq_long += notional
        else:               # forced buy = a short was liquidated
            st.liq_short += notional

    def add_wall_tag(self, sym: str, tag: dict) -> None:
        st = self.states.get(sym)
        if st is not None:
            st.walls.append(tag)
            if len(st.walls) > 8:
                del st.walls[0]

    def mark_gap(self) -> None:
        for st in self.states.values():
            st.gap = True

    def on_trade(self, sym: str, price: float, qty: float, is_sell: bool,
                 trade_ms: int, recv_ms: int) -> None:
        st = self.states.get(sym)
        if st is None or price <= 0:
            return
        self.trade_count += 1
        idx = trade_ms // self.iv
        if st.idx is None:
            self._reset(st, idx, price, empty=False)
        elif idx != st.idx:
            if idx < st.idx:
                return  # late trade from an interval we already closed
            self._close(st)
            self._reset(st, idx, price, empty=False)
        elif st.empty:
            st.open = st.high = st.low = price
            st.empty = False

        notional = price * qty
        st.last = price
        st.last_ms = trade_ms
        if price > st.high:
            st.high = price
        elif price < st.low:
            st.low = price
        if is_sell:
            st.sell += notional
        else:
            st.buy += notional
        st.n += 1

        top = st.top
        if len(top) < TOP_KEEP:
            top.append((notional, is_sell, price, trade_ms))
            st.top_min = min(t[0] for t in top) if len(top) == TOP_KEEP else 0.0
        elif notional > st.top_min:
            i = min(range(TOP_KEEP), key=lambda k: top[k][0])
            top[i] = (notional, is_sell, price, trade_ms)
            st.top_min = min(t[0] for t in top)

        if not self.armed or st.open <= 0 or st.volatile_sent:
            return
        thr = st.thr_pct / 100.0
        up_x = st.high / st.open - 1.0 >= thr
        dn_x = st.low / st.open - 1.0 <= -thr
        if not (up_x or dn_x):
            return
        both = up_x and dn_x and self.volatile_upgrade
        if st.alert_dir == 0:
            ret = price / st.open - 1.0
            direction = (1 if ret >= 0 else -1) if both else (1 if up_x else -1)
            self._fire(st, "VOLATILE" if both else "SPIKE", direction, trade_ms, recv_ms)
        elif both:
            ret = price / st.open - 1.0
            self._fire(st, "VOLATILE", 1 if ret >= 0 else -1, trade_ms, recv_ms)

    # ---- interval lifecycle -----------------------------------------------------
    def sweep(self, now_ms: int) -> None:
        """Close intervals for symbols that had no new trade after the boundary."""
        idx_now = (now_ms - self.grace_ms) // self.iv
        btc = self.states.get(self.btc)
        order = ([btc] if btc is not None else []) + [
            s for s in self.states.values() if s is not btc
        ]
        for st in order:
            if st.idx is not None and idx_now > st.idx:
                self._close(st)
                self._reset(st, idx_now, st.last, empty=True)

    def _reset(self, st: SymbolState, idx: int, open_price: float, empty: bool) -> None:
        st.idx = idx
        st.start_ms = idx * self.iv
        st.open = st.high = st.low = open_price
        st.empty = empty
        st.vol_base = 0.0
        st.buy = st.sell = 0.0
        st.n = 0
        st.liq_long = st.liq_short = 0.0
        st.top = []
        st.top_min = 0.0
        st.alert_dir = 0
        st.volatile_sent = False
        st.partial = False
        st.gap = False
        st.walls = []

    def _close(self, st: SymbolState) -> None:
        close = st.last if st.last > 0 else st.open
        qvol = st.vol_base + st.buy + st.sell
        ret = close / st.open - 1.0 if st.open > 0 else 0.0
        st.hist.append((abs(ret), qvol))
        end_ms = st.start_ms + self.iv
        if st.symbol == self.btc:
            self.btc_close[st.idx] = close
            for k in [k for k in self.btc_close if k < st.idx - 3]:
                del self.btc_close[k]
        btc_px = self.btc_close.get(st.idx)
        if btc_px is None:
            b = self.states.get(self.btc)
            btc_px = b.last if b is not None and b.last > 0 else None
        self.on_interval({
            "symbol": st.symbol, "start_ms": st.start_ms, "open": st.open,
            "high": st.high, "low": st.low, "close": close, "quote_volume": qvol,
            "buy_notional": st.buy, "sell_notional": st.sell, "liq_long": st.liq_long,
            "liq_short": st.liq_short, "n_trades": st.n,
            "partial": int(st.partial or st.gap), "source": "live",
        })
        for eid in st.pending_next_close:
            self.on_followup({"event_id": eid, "label": "next_close", "horizon_s": None,
                              "due_ms": end_ms, "actual_ms": end_ms, "price": close,
                              "btc_price": btc_px})
        for eid in st.pending_close:
            self.on_followup({"event_id": eid, "label": "int_close", "horizon_s": None,
                              "due_ms": end_ms, "actual_ms": end_ms, "price": close,
                              "btc_price": btc_px})
        st.pending_next_close = st.pending_close
        st.pending_close = []

    # ---- statistics ---------------------------------------------------------------
    def sigma(self, st: SymbolState) -> float | None:
        if len(st.hist) < self.min_base:
            return None
        med = statistics.median(a for a, _ in st.hist)
        s = SIGMA_K * med
        return s if s > 0 else None

    def median_volume(self, st: SymbolState) -> float:
        return statistics.median(v for _, v in st.hist) if st.hist else 0.0

    def z_score(self, st: SymbolState) -> float | None:
        sg = self.sigma(st)
        if sg is None or st.open <= 0 or st.last <= 0:
            return None
        return (st.last / st.open - 1.0) / sg

    def btc_price(self) -> float | None:
        b = self.states.get(self.btc)
        return b.last if b is not None and b.last > 0 else None

    # ---- firing ---------------------------------------------------------------
    def _fire(self, st: SymbolState, kind: str, direction: int, trade_ms: int,
              recv_ms: int) -> None:
        price = st.last
        ret = price / st.open - 1.0
        sg = self.sigma(st)
        z = ret / sg if sg else None
        volume = st.vol_base + st.buy + st.sell
        med_vol = self.median_volume(st)
        elapsed_s = max((trade_ms - st.start_ms) / 1000.0, 30.0)
        expected = med_vol * elapsed_s / (self.iv / 1000.0)
        vol_mult = volume / expected if expected > 0 else None

        top_sorted = sorted(st.top, key=lambda t: t[0], reverse=True)
        orders = []
        n_flagged = 0
        for notional, is_sell, px_, ts in top_sorted:
            pct = notional / volume * 100.0 if volume > 0 else 0.0
            flag = pct >= self.flag_pct
            n_flagged += int(flag)
            orders.append({"side": "SELL" if is_sell else "BUY", "price": px_,
                           "notional": notional, "pct_of_volume": pct,
                           "flagged": int(flag), "ts_ms": ts})

        ev = {
            "id": self.next_id(), "symbol": st.symbol, "kind": kind,
            "direction": direction, "interval_start_ms": st.start_ms,
            "interval_ms": self.iv, "trade_ms": trade_ms, "recv_ms": recv_ms,
            "detect_ms": int(self.clock() * 1000), "minute": (trade_ms - st.start_ms) / 60000.0,
            "open": st.open, "price": price, "ret": ret,
            "high_ret": st.high / st.open - 1.0, "low_ret": st.low / st.open - 1.0,
            "threshold": st.thr_pct / 100.0, "tier": st.tier, "vol24h": st.vol24h,
            "sigma": sg, "z": z, "buy_notional": st.buy, "sell_notional": st.sell,
            "interval_volume": volume, "vol_mult": vol_mult, "liq_long": st.liq_long,
            "liq_short": st.liq_short, "flagged_5pct": n_flagged,
            "partial": int(st.partial or st.gap), "btc_price": self.btc_price(),
            "in_watchlist": int(st.in_watch),
            "top_orders": orders, "n_top_shown": self.n_top,
            "walls": list(st.walls[-3:]),
        }
        st.alert_dir = direction if st.alert_dir == 0 else st.alert_dir
        if kind == "VOLATILE":
            st.volatile_sent = True
            self.volatile_total += 1
        st.n_events += 1
        self.spikes_total += 1
        st.pending_close.append(ev["id"])
        self.on_spike(ev)
