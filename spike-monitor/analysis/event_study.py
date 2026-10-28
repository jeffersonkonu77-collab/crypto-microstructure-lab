#!/usr/bin/env python3
"""Event study: after a spike alert, does price continue or fade, and does it
survive trading costs?

Method (the short version)
--------------------------
* Entry is the traded price 5 seconds AFTER the alert (the earliest anyone could
  realistically act). The move that triggered the alert is never counted.
* Exit is the traded price at +1m, +5m, the end of the 15m interval, and +30m
  after the alert.
* Returns are direction-adjusted: positive means the spike CONTINUED, negative
  means it FADED.
* Three numbers per slice, in basis points:
    gross = direction-adjusted raw coin return
    adj   = gross minus BTC's direction-adjusted return over the same window
            (strips out market-wide moves; BTCUSDT events themselves are skipped)
    net   = adj minus round-trip costs on both legs (coin and BTC hedge)
* Spikes cluster (many coins trip in the same interval), so confidence intervals
  come from a CLUSTER bootstrap that resamples whole 15m intervals.
* Slices with fewer than --min-n events report "not enough data".
* Every slice tested is counted and reported. With 95% intervals, about 5% of
  tests will show an effect by luck alone.
* A first-half vs second-half split shows whether a pattern persists.

Run:  python analysis/event_study.py --db spikemon.sqlite3
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import numpy as np
import pandas as pd

ENTRY = "5s"
EXITS = ["1m", "5m", "int_close", "30m"]
EXIT_NAMES = {"1m": "+1m", "5m": "+5m", "int_close": "interval close", "30m": "+30m"}
MIN_AFTER_ENTRY_MS = 10_000  # interval-close exit must be at least 10s after entry


# ----------------------------------------------------------------------------
# loading and return construction
# ----------------------------------------------------------------------------
def load(con, include_volatile: bool, max_late_s: float):
    spikes = pd.read_sql("SELECT * FROM spikes", con)
    fu = pd.read_sql("SELECT event_id,label,due_ms,actual_ms,price,btc_price FROM followups", con)
    if not spikes.empty and not include_volatile:
        spikes = spikes[spikes["kind"] == "SPIKE"]
    spikes = spikes[spikes["direction"].isin([1, -1])].copy()
    if spikes.empty:
        return spikes, fu
    timed = fu["label"].isin([ENTRY, "1m", "5m", "30m"])
    late = (fu["actual_ms"] - fu["due_ms"]) / 1000.0
    fu = fu[~(timed & (late > max_late_s))]
    df = spikes.set_index("id")
    for label in [ENTRY] + EXITS:
        sub = fu[fu["label"] == label].drop_duplicates("event_id").set_index("event_id")
        df[f"p_{label}"] = sub["price"]
        df[f"b_{label}"] = sub["btc_price"]
        df[f"t_{label}"] = sub["actual_ms"]
    df = df.reset_index()
    return df, fu


def build_returns(df: pd.DataFrame, a) -> pd.DataFrame:
    df = df.dropna(subset=[f"p_{ENTRY}"]).copy()
    d = df["direction"].astype(float)
    slip_coin = np.where(df["tier"] == "ge1b", a.slip_bps_big, a.slip_bps_small)
    cost_bps = 2 * (a.fee_bps + slip_coin) + 2 * (a.fee_bps + a.slip_bps_btc)
    is_btc = df["symbol"] == "BTCUSDT"
    for L in EXITS:
        p0, p1 = df[f"p_{ENTRY}"], df[f"p_{L}"]
        raw = d * (p1 / p0 - 1.0) * 1e4
        b0, b1 = df[f"b_{ENTRY}"], df[f"b_{L}"]
        btc = d * (b1 / b0 - 1.0) * 1e4
        adj = raw - btc
        adj = adj.where(~is_btc)
        valid = p1.notna() & b1.notna() & b0.notna()
        if L == "int_close":
            valid &= (df["t_int_close"] - (df["detect_ms"] + 5000)) >= MIN_AFTER_ENTRY_MS
        df[f"gross_{L}"] = raw.where(valid)
        df[f"adj_{L}"] = adj.where(valid)
        df[f"net_{L}"] = (adj - cost_bps).where(valid)
    return df


# ----------------------------------------------------------------------------
# slicing and statistics
# ----------------------------------------------------------------------------
def add_slices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["s_tier"] = np.where(df["tier"] == "ge1b", "tier >=$1B (3%)", "tier <$1B (5%)")
    df["s_direction"] = np.where(df["direction"] > 0, "up spikes", "down spikes")
    z = df["z"]
    df["s_z"] = np.select(
        [z.isna(), z.abs() < 3, z.abs() < 5], ["z n/a", "|z| < 3", "|z| 3 to 5"], "|z| >= 5"
    )
    df["s_bigorder"] = np.where(df["flagged_5pct"].fillna(0) > 0, "5%+ order present", "no 5%+ order")
    liq = (df["liq_long"].fillna(0) + df["liq_short"].fillna(0)) / df["interval_volume"].replace(0, np.nan)
    df["s_liq"] = np.where(liq >= 0.10, "liquidation-driven (>=10% of vol)", "not liquidation-driven")
    vm = df["vol_mult"]
    df["s_vol"] = np.select(
        [vm.isna(), vm < 2, vm < 5], ["vol n/a", "volume <2x pace", "volume 2 to 5x pace"], "volume >=5x pace"
    )
    return df


DIMS = [("all events", None), ("tier", "s_tier"), ("direction", "s_direction"), ("z band", "s_z"),
        ("big order", "s_bigorder"), ("liquidations", "s_liq"), ("volume", "s_vol")]


def cluster_ci(values: np.ndarray, clusters: np.ndarray, B: int, rng) -> tuple[float, float]:
    g = pd.DataFrame({"v": values, "c": clusters}).groupby("c")["v"].agg(["sum", "count"])
    sums = g["sum"].to_numpy()
    counts = g["count"].to_numpy(dtype=float)
    k = len(sums)
    if k < 2:
        return (np.nan, np.nan)
    out = []
    step = max(1, 4_000_000 // k)
    for start in range(0, B, step):
        m = min(step, B - start)
        idx = rng.integers(0, k, size=(m, k))
        out.append(sums[idx].sum(axis=1) / counts[idx].sum(axis=1))
    means = np.concatenate(out)
    lo, hi = np.percentile(means, [2.5, 97.5])
    return float(lo), float(hi)


def verdict(adj_mean, adj_lo, adj_hi, net_lo) -> str:
    if adj_lo > 0 or adj_hi < 0:
        kind = "continuation" if adj_mean > 0 else "fade"
        return f"{kind} before costs" + (", survives costs" if net_lo > 0 else ", not tradable after costs")
    return "no detectable effect"


def slice_table(df: pd.DataFrame, a) -> pd.DataFrame:
    rng = np.random.default_rng(a.seed)
    split_ms = df["trade_ms"].median() if len(df) else 0
    rows = []
    for L in EXITS:
        for dim_name, col in DIMS:
            levels = [("all", None)] if col is None else [(lv, lv) for lv in sorted(df[col].unique())]
            for level, key in levels:
                sub_all = df if col is None else df[df[col] == key]
                sub = sub_all[sub_all[f"net_{L}"].notna()]
                n = len(sub)
                row = {"horizon": EXIT_NAMES[L], "dimension": dim_name, "level": level, "n": n,
                       "clusters": int(sub["interval_start_ms"].nunique()) if n else 0}
                if n < a.min_n:
                    row["verdict"] = "not enough data"
                    rows.append(row)
                    continue
                adj = sub[f"adj_{L}"].to_numpy()
                net = sub[f"net_{L}"].to_numpy()
                cl = sub["interval_start_ms"].to_numpy()
                adj_lo, adj_hi = cluster_ci(adj, cl, a.bootstrap, rng)
                net_lo, net_hi = cluster_ci(net, cl, a.bootstrap, rng)
                h1 = sub[sub["trade_ms"] <= split_ms][f"adj_{L}"]
                h2 = sub[sub["trade_ms"] > split_ms][f"adj_{L}"]
                if len(h1) >= 10 and len(h2) >= 10:
                    same = np.sign(h1.mean()) == np.sign(h2.mean()) and np.sign(h1.mean()) != 0
                    persists = "same sign" if same else "sign flips"
                else:
                    persists = "n/a"
                row.update({
                    "gross_bps": sub[f"gross_{L}"].mean(), "adj_bps": float(adj.mean()),
                    "adj_ci_lo": adj_lo, "adj_ci_hi": adj_hi,
                    "net_bps": float(net.mean()), "net_ci_lo": net_lo, "net_ci_hi": net_hi,
                    "hit_adj_pct": float((adj > 0).mean() * 100),
                    "hit_net_pct": float((net > 0).mean() * 100),
                    "first_half_adj_bps": float(h1.mean()) if len(h1) else np.nan,
                    "second_half_adj_bps": float(h2.mean()) if len(h2) else np.nan,
                    "half_check": persists,
                    "verdict": verdict(float(adj.mean()), adj_lo, adj_hi, net_lo),
                })
                rows.append(row)
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------
# data health
# ----------------------------------------------------------------------------
def pct_table(series: pd.Series) -> str:
    s = series.dropna()
    if s.empty:
        return "n/a"
    q = np.percentile(s, [50, 90, 99])
    return f"p50 {q[0]:.0f}ms  p90 {q[1]:.0f}ms  p99 {q[2]:.0f}ms  max {s.max():.0f}ms  (n={len(s)})"


def data_health(con) -> list[str]:
    out = []
    sp = pd.read_sql("SELECT * FROM spikes", con)
    runs = pd.read_sql("SELECT * FROM runs", con)
    gaps = pd.read_sql("SELECT * FROM gaps", con)
    iv = con.execute("SELECT COUNT(*) FROM intervals").fetchone()[0]
    hours = 0.0
    for _, r in runs.iterrows():
        end = r["end_ms"] if pd.notna(r["end_ms"]) else (sp["trade_ms"].max() if len(sp) else r["start_ms"])
        hours += max(end - r["start_ms"], 0) / 3.6e6
    out.append(f"Runs: {len(runs)} | monitored hours: {hours:.1f} | spike events: {len(sp)} | interval rows: {iv:,}")
    if len(sp):
        out.append("Delivery: " + ", ".join(f"{k} {v}" for k, v in sp["delivery"].fillna("unsent").value_counts().items()))
        out.append(f"Partial-interval events: {int(sp['partial'].fillna(0).sum())} of {len(sp)}")
        sent = sp[sp["sent_ms"].notna()]
        out.append("Latency recv -> detect: " + pct_table(sp["detect_ms"] - sp["recv_ms"]))
        out.append("Latency detect -> sent: " + pct_table(sent["sent_ms"] - sent["detect_ms"]))
        total = sent["sent_ms"] - sent["trade_ms"] - sent["clock_offset_ms"].fillna(0)
        out.append("Latency exchange trade -> sent (clock-offset adjusted): " + pct_table(total))
        live = sent[sent["delivery"].isin(["alert", "digest"])]
        if len(live):
            tl = live["sent_ms"] - live["trade_ms"] - live["clock_offset_ms"].fillna(0)
            out.append(f"Live Telegram deliveries within 2.0s of the triggering trade: {(tl <= 2000).mean() * 100:.1f}% (n={len(live)})")
        else:
            out.append("No live Telegram deliveries recorded yet (practice mode only), so the 2s claim is untested.")
    if len(gaps):
        out.append(f"Websocket gaps: {len(gaps)} totalling {gaps['duration_ms'].sum() / 1000:.0f}s "
                   + str(gaps.groupby('stream')['duration_ms'].count().to_dict()))
    else:
        out.append("Websocket gaps: none recorded")
    return out


# ----------------------------------------------------------------------------
# reporting
# ----------------------------------------------------------------------------
def fmt(df: pd.DataFrame) -> str:
    show = df.copy()
    for c in show.columns:
        if show[c].dtype.kind == "f":
            show[c] = show[c].map(lambda v: "" if pd.isna(v) else f"{v:.1f}")
    return show.to_string(index=False)


def is_synthetic(con) -> bool:
    row = con.execute("SELECT COUNT(*) FROM runs WHERE mode='synthetic'").fetchone()
    return bool(row and row[0])


def write_markdown(path, a, table, health, n_events, n_tests, synthetic=False) -> None:
    lines = ["# Spike follow-through: event study", "",
             "**WARNING: this database is SYNTHETIC test data. None of these numbers are real.**" if synthetic else "",
             f"Events analysed: {n_events}. Entry = price 5s after the alert. Returns are in basis points, "
             "direction-adjusted (positive = continuation, negative = fade).", "",
             "Costs assumed (per side): "
             f"fee {a.fee_bps} bps, slippage {a.slip_bps_big} bps for $1B+ coins, {a.slip_bps_small} bps below, "
             f"{a.slip_bps_btc} bps on the BTC hedge. Costs are charged on both legs, round trip.", "",
             "## Headline (all events)", ""]
    head = table[(table["dimension"] == "all events")]
    cols = ["horizon", "n", "clusters", "gross_bps", "adj_bps", "adj_ci_lo", "adj_ci_hi", "net_bps", "net_ci_hi", "verdict"]
    lines.append("```")
    lines.append(fmt(head[[c for c in cols if c in head.columns]]))
    lines.append("```")
    lines += ["", f"Slices tested: {n_tests}. About {n_tests * 0.05:.0f} would show a before-cost effect by chance alone.",
              "", "## Slices with a detectable effect before costs (treat with suspicion)", ""]
    hits = table[table["verdict"].str.contains("before costs", na=False)]
    if hits.empty:
        lines.append("None.")
    else:
        lines += ["```", fmt(hits[[c for c in ["horizon", "dimension", "level", "n", "clusters", "adj_bps", "adj_ci_lo", "adj_ci_hi", "net_bps", "half_check", "verdict"] if c in hits.columns]]), "```"]
    surv = table[table["verdict"].str.contains("survives costs", na=False)]
    lines += ["", f"Slices that survive costs: {len(surv)}."]
    lines += ["", "## Data health", ""] + [f"- {h}" for h in health]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Spike follow-through event study")
    p.add_argument("--db", default="spikemon.sqlite3")
    p.add_argument("--out", default="analysis_output")
    p.add_argument("--fee-bps", type=float, default=5.0, help="taker fee per side")
    p.add_argument("--slip-bps-big", type=float, default=2.0, help="slippage per side, $1B+ coins")
    p.add_argument("--slip-bps-small", type=float, default=8.0, help="slippage per side, smaller coins")
    p.add_argument("--slip-bps-btc", type=float, default=1.0, help="slippage per side on the BTC hedge")
    p.add_argument("--min-n", type=int, default=30)
    p.add_argument("--bootstrap", type=int, default=2000)
    p.add_argument("--max-late-s", type=float, default=5.0, help="drop follow-up prices recorded this late")
    p.add_argument("--include-volatile", action="store_true")
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args(argv)

    if not os.path.exists(a.db):
        print(f"database not found: {a.db}", file=sys.stderr)
        return 2
    con = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    os.makedirs(a.out, exist_ok=True)
    health = data_health(con)
    df, _ = load(con, a.include_volatile, a.max_late_s)
    print("DATA HEALTH")
    print("\n".join("  " + h for h in health))
    if df.empty:
        print("\nNo spike events with follow-ups yet. Let the monitor run, then re-run this.")
        return 0
    df = add_slices(build_returns(df, a))
    table = slice_table(df, a)
    n_tests = int((table["verdict"] != "not enough data").sum())
    table.to_csv(os.path.join(a.out, "slices.csv"), index=False)
    synthetic = is_synthetic(con)
    write_markdown(os.path.join(a.out, "summary.md"), a, table, health, len(df), n_tests, synthetic)
    if synthetic:
        print("\n*** SYNTHETIC test database: none of the numbers below are real ***")

    print(f"\nEVENT STUDY: {len(df)} spike events with a 5s entry price "
          f"(assumed costs: fee {a.fee_bps}bps/side, slippage {a.slip_bps_big}/{a.slip_bps_small}bps by tier, BTC hedge {a.slip_bps_btc}bps)")
    cols = ["horizon", "dimension", "level", "n", "clusters", "gross_bps", "adj_bps", "adj_ci_lo",
            "adj_ci_hi", "net_bps", "hit_adj_pct", "half_check", "verdict"]
    print(fmt(table[[c for c in cols if c in table.columns]]))
    print(f"\nSlices with enough data: {n_tests}. With 95% intervals expect about {n_tests * 0.05:.0f} "
          "to show an effect by luck alone.")
    print(f"Wrote {os.path.join(a.out, 'slices.csv')} and {os.path.join(a.out, 'summary.md')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
