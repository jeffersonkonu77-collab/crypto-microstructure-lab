import importlib.util
import os
import sqlite3

import pandas as pd

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def _load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, "analysis", f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sim = _load("simulate_db")
study = _load("event_study")


def run(tmp_path, edge, events=1800):
    db = str(tmp_path / f"s{edge}.sqlite3")
    out = str(tmp_path / f"out{edge}")
    sim.simulate(db, events=events, days=10, edge_bps=edge, seed=3)
    rc = study.main(["--db", db, "--out", out, "--bootstrap", "300"])
    assert rc == 0
    return pd.read_csv(os.path.join(out, "slices.csv")), out


def test_null_data_shows_no_edge_after_costs(tmp_path):
    t, out = run(tmp_path, 0.0)
    head = t[t["dimension"] == "all events"]
    assert (head["net_bps"] < -20).all()                          # costs make a null result negative
    assert not t["verdict"].fillna("").str.contains("survives costs").any()
    tested = t[t["verdict"] != "not enough data"]
    false_pos = tested["verdict"].str.contains("before costs").sum()
    assert false_pos <= 0.15 * len(tested)                        # about 5% expected by chance alone
    assert "SYNTHETIC" in open(os.path.join(out, "summary.md"), encoding="utf-8").read()


def test_injected_continuation_is_detected_and_survives_costs(tmp_path):
    t, _ = run(tmp_path, 60.0)
    head = t[(t["dimension"] == "all events") & (t["horizon"] == "+5m")].iloc[0]
    assert head["verdict"] == "continuation before costs, survives costs"
    assert 50 < head["adj_bps"] < 70 and head["half_check"] == "same sign"


def test_small_slices_report_not_enough_data(tmp_path):
    t, _ = run(tmp_path, 0.0, events=400)
    small = t[t["n"] < 30]
    assert len(small) > 0 and (small["verdict"] == "not enough data").all()
    assert small["net_bps"].isna().all()


def test_entry_is_after_alert_and_btc_events_are_not_market_adjusted(tmp_path):
    db = str(tmp_path / "e.sqlite3")
    sim.simulate(db, events=600, days=5, edge_bps=0.0, seed=5)
    con = sqlite3.connect(db)
    a = study.argparse.Namespace(fee_bps=5, slip_bps_big=2, slip_bps_small=8, slip_bps_btc=1)
    df, _ = study.load(con, False, 5.0)
    df = study.build_returns(df, a)
    assert df["p_5s"].notna().all()                               # entry price exists for every row kept
    btc = df[df["symbol"] == "BTCUSDT"]
    assert len(btc) > 0 and btc["adj_5m"].isna().all() and btc["net_5m"].isna().all()
    # interval-close exit within 10s of entry is invalid
    late = df[(df["t_int_close"] - (df["detect_ms"] + 5000)) < 10_000]
    assert late["net_int_close"].isna().all()


def test_empty_database_is_handled(tmp_path):
    from spikemon.db import init_db
    db = str(tmp_path / "empty.sqlite3")
    init_db(db).close()
    assert study.main(["--db", db, "--out", str(tmp_path / "o")]) == 0
