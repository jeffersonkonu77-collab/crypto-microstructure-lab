#!/usr/bin/env python3
"""Generate a SYNTHETIC spikemon database to test the analysis pipeline.

Nothing here is market data. It exists so event_study.py can be run and
checked before real data has been collected. With --edge-bps 0 (default) there
is no continuation or fade, so a correct analysis should find no edge. With a
positive --edge-bps a continuation effect is injected so you can confirm the
analysis detects it.

    python analysis/simulate_db.py synthetic.sqlite3 --events 4000 --edge-bps 0
    python analysis/event_study.py --db synthetic.sqlite3
"""

from __future__ import annotations

import argparse
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from spikemon.db import init_db  # noqa: E402

IV = 900_000
HORIZONS = {"1m": 60, "5m": 300, "30m": 1800}


def simulate(path: str, events: int = 3000, days: int = 14, edge_bps: float = 0.0,
             seed: int = 1) -> None:
    rng = np.random.default_rng(seed)
    if os.path.exists(path):
        os.remove(path)
    con = init_db(path)
    t0 = 1_760_000_000_000
    n_intervals = days * 96
    symbols = [f"SIM{i:03d}USDT" for i in range(80)] + ["BTCUSDT"]
    big = {s: (i % 5 == 0) for i, s in enumerate(symbols)}
    con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?)",
                (1, t0, t0 + days * 86_400_000, "synthetic", "synthetic", "{}"))
    # market factor per interval: events in the same interval share it
    mkt = rng.normal(0, 1, size=n_intervals)
    interval_pick = np.sort(rng.integers(0, n_intervals, size=events))
    eid = 0
    btc_px = 60_000.0
    for k in interval_pick:
        eid += 1
        start = t0 + int(k) * IV
        minute = float(rng.uniform(0.5, 14.5))
        trade_ms = start + int(minute * 60_000)
        sym = symbols[rng.integers(0, len(symbols) - 1)] if rng.random() > 0.01 else "BTCUSDT"
        is_big = big.get(sym, False)
        thr = 0.03 if is_big else 0.05
        direction = 1 if rng.random() < 0.5 else -1
        ret = direction * thr * float(rng.uniform(1.0, 2.0))
        sigma = float(rng.uniform(0.003, 0.012))
        z = ret / sigma
        volume = float(rng.lognormal(15.5, 0.8))
        buy = volume * (0.7 if direction > 0 else 0.3)
        price = float(rng.uniform(0.05, 50))
        flagged = int(rng.random() < 0.35)
        liq = volume * float(rng.uniform(0.0, 0.25)) if rng.random() < 0.3 else 0.0
        detect = trade_ms + 40
        sent = detect + int(rng.integers(30, 400))
        con.execute(
            "INSERT INTO spikes(id,run_id,symbol,kind,direction,interval_start_ms,interval_ms,trade_ms,"
            "recv_ms,detect_ms,sent_ms,clock_offset_ms,minute,open,price,ret,high_ret,low_ret,threshold,"
            "tier,vol24h,sigma,z,buy_notional,sell_notional,interval_volume,vol_mult,liq_long,liq_short,"
            "flagged_5pct,partial,btc_price,in_watchlist,delivery) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (eid, 1, sym, "SPIKE", direction, start, IV, trade_ms, trade_ms + 20, detect, sent, 0.0,
             minute, price / (1 + ret), price, ret, max(ret, 0), min(ret, 0), thr,
             "ge1b" if is_big else "lt1b", 2e9 if is_big else 2e8, sigma, z, buy, volume - buy,
             volume, float(rng.uniform(1, 8)), liq if direction < 0 else 0.0,
             liq if direction > 0 else 0.0, flagged, 0, btc_px, 1, "practice"))
        # follow-through prices
        sim_detect = detect
        p_entry_noise = rng.normal(0, 3e-4)
        coin_p = price * (1 + p_entry_noise)
        btc_p = btc_px * (1 + rng.normal(0, 1e-4))
        beta = 0.0 if sym == "BTCUSDT" else float(rng.uniform(0.6, 1.4))
        rows = [("1s", 1, price * (1 + rng.normal(0, 2e-4)), btc_px)]
        rows.append(("5s", 5, coin_p, btc_p))
        for label, h in HORIZONS.items():
            vol = 0.0020 * np.sqrt(h / 60.0)
            m = float(mkt[k]) * 0.0010 * np.sqrt(h / 60.0)
            idio = rng.normal(0, vol)
            drift = direction * edge_bps / 1e4 * min(h / 300.0, 1.0)
            coin_ret = beta * m + idio + drift
            btc_ret = m if sym != "BTCUSDT" else coin_ret
            rows.append((label, h, coin_p * (1 + coin_ret), btc_p * (1 + btc_ret)))
        # interval close
        h_ic = (start + IV - sim_detect) / 1000.0
        vol = 0.0020 * np.sqrt(max(h_ic, 1) / 60.0)
        m = float(mkt[k]) * 0.0010 * np.sqrt(max(h_ic, 1) / 60.0)
        coin_ret = beta * m + rng.normal(0, vol) + direction * edge_bps / 1e4 * min(h_ic / 300.0, 1.0)
        rows.append(("int_close", None, coin_p * (1 + coin_ret), btc_p * (1 + m)))
        for label, h, cp, bp in rows:
            due = sim_detect + int((h or 0) * 1000) if h else start + IV
            actual = due + (int(rng.integers(0, 600)) if h else 0)
            con.execute("INSERT INTO followups VALUES (?,?,?,?,?,?,?)",
                        (eid, label, h, due, actual, float(cp), float(bp)))
    con.execute("INSERT INTO gaps VALUES (?,?,?,?)", (t0 + 5 * 86_400_000, "market", 4200, "reconnect"))
    con.commit()
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("path", nargs="?", default="synthetic.sqlite3")
    ap.add_argument("--events", type=int, default=3000)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--edge-bps", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    simulate(a.path, a.events, a.days, a.edge_bps, a.seed)
    print(f"wrote SYNTHETIC database {a.path} ({a.events} events, edge {a.edge_bps} bps)")
