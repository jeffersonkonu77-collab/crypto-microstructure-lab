import sqlite3
import time

from spikemon.config import load_config
from spikemon.db import SPIKE_COLS, Store
from spikemon.followups import FollowUps, label_for
from spikemon.ranker import Ranker


def test_store_batches_updates_replace_ignore_and_ids_continue(tmp_path):
    path = str(tmp_path / "t.sqlite3")
    s = Store(path, batch_seconds=0.05)
    s.start()
    ids = [s.next_id() for _ in range(3)]
    assert ids == [1, 2, 3]
    row = {k: None for k in SPIKE_COLS}
    row.update(id=ids[0], symbol="AAAUSDT", kind="SPIKE", direction=1)
    s.insert("spikes", row)
    s.execute("UPDATE spikes SET sent_ms=?, delivery=? WHERE id=?", (123, "practice", ids[0]))
    now = int(time.time() * 1000)
    base = {"symbol": "A", "start_ms": now, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
            "quote_volume": 5.0, "buy_notional": 0.0, "sell_notional": 0.0, "liq_long": 0.0,
            "liq_short": 0.0, "n_trades": 1, "partial": 0, "source": "kline"}
    s.insert("intervals", base, "INSERT OR IGNORE")
    s.insert("intervals", {**base, "quote_volume": 99.0}, "INSERT OR IGNORE")   # ignored
    s.insert("intervals", {**base, "quote_volume": 7.0, "source": "live"}, "INSERT OR REPLACE")
    for i in range(500):
        s.insert("gaps", {"ts_ms": now + i, "stream": "market", "duration_ms": i, "note": "x"})
    s.close()
    con = sqlite3.connect(path)
    assert con.execute("SELECT sent_ms, delivery FROM spikes WHERE id=1").fetchone() == (123, "practice")
    assert con.execute("SELECT quote_volume, source FROM intervals").fetchall() == [(7.0, "live")]
    assert con.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 500
    assert s.errors == 0
    s2 = Store(path)
    assert s2.next_id() == 2                                   # continues after the stored max id


def test_store_survives_a_bad_row(tmp_path):
    s = Store(str(tmp_path / "b.sqlite3"), batch_seconds=0.05)
    s.start()
    now = int(time.time() * 1000)
    s.insert("gaps", {"ts_ms": now, "stream": "m", "duration_ms": 1, "note": "ok"})
    s.execute("INSERT INTO nonexistent_table VALUES (1)")
    s.insert("gaps", {"ts_ms": now + 1, "stream": "m", "duration_ms": 1, "note": "ok"})
    s.close()
    con = sqlite3.connect(str(tmp_path / "b.sqlite3"))
    assert con.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 2 and s.errors == 1


def test_retention_prune_keeps_spikes_and_drops_old_telemetry(tmp_path):
    path = str(tmp_path / "r.sqlite3")
    s = Store(path, batch_seconds=0.05, retention_days=1)
    now = int(time.time() * 1000)
    row = {k: None for k in SPIKE_COLS}
    row.update(id=1, symbol="A", trade_ms=1)                    # ancient, but spikes are kept forever
    s.start()
    s.insert("spikes", row)
    s.insert("gaps", {"ts_ms": 5, "stream": "m", "duration_ms": 1, "note": "old"})
    s.insert("gaps", {"ts_ms": now, "stream": "m", "duration_ms": 1, "note": "new"})
    s.close()
    con = sqlite3.connect(path)
    assert [r[0] for r in con.execute("SELECT note FROM gaps")] == ["new"]
    assert con.execute("SELECT COUNT(*) FROM spikes").fetchone()[0] == 1


def test_ranker_union_hysteresis_and_min_volume():
    cfg = load_config(None, {"watchlist": {"top_volume": 3, "top_gainers": 2, "top_losers": 2,
                                           "hysteresis_extra": 2, "min_volume_usd_movers": 1e7}})
    syms = [f"C{i}USDT" for i in range(12)]
    r = Ranker(cfg, syms)
    # C0..C2 biggest volume; gainers C3,C4 (+30%,+20%); losers C5,C6; C11 has +90% but illiquid
    data = {s: (1.0, 1e9 - i * 1e7 - 5e8, 0.0) for i, s in enumerate(syms)}
    data["C3USDT"] = (1.0, 5e7, 30.0)
    data["C4USDT"] = (1.0, 5e7, 20.0)
    data["C5USDT"] = (1.0, 5e7, -25.0)
    data["C6USDT"] = (1.0, 5e7, -15.0)
    data["C11USDT"] = (1.0, 1e6, 90.0)
    r.update((s, *v) for s, v in data.items())
    added, removed = r.refresh(0)
    assert {"C0USDT", "C1USDT", "C2USDT"} <= set(r.watch)
    assert {"C3USDT", "C4USDT", "C5USDT", "C6USDT"} <= set(r.watch)
    assert "C11USDT" not in r.watch                             # illiquid mover excluded
    assert r.watch["C3USDT"] == "G" and r.watch["C5USDT"] == "L"
    # C4 slips to rank 3 of gainers (inside N+extra): it stays. A new #1 gainer enters at once.
    data["C7USDT"] = (1.0, 5e7, 50.0)
    data["C8USDT"] = (1.0, 5e7, 40.0)
    r.update((s, *v) for s, v in data.items())
    added, removed = r.refresh(1)
    assert {"C7USDT", "C8USDT"} <= added and "C4USDT" in r.watch
    # C4 falls beyond rank N+extra = 4 of gainers: now it drops
    data["C9USDT"] = (1.0, 5e7, 25.0)
    data["C10USDT"] = (1.0, 5e7, 22.0)
    r.update((s, *v) for s, v in data.items())
    added, removed = r.refresh(2)
    assert "C4USDT" in removed


def test_ranker_forced_symbols_expire():
    cfg = load_config(None, {"watchlist": {"forced_hold_minutes": 1, "top_volume": 1,
                                           "top_gainers": 0, "top_losers": 0}})
    r = Ranker(cfg, ["XUSDT", "YUSDT"])
    r.update([("XUSDT", 1.0, 5e9, 0.0), ("YUSDT", 1.0, 1e5, 0.0)])
    r.refresh(0)
    r.force("YUSDT", 0)
    added, _ = r.refresh(1000)
    assert "YUSDT" in r.watch and "S" in r.watch["YUSDT"]
    _, removed = r.refresh(61_000)
    assert "YUSDT" in removed


def test_followups_fire_in_order_and_label():
    out = []
    price = {"A": 10.0}
    f = FollowUps([1, 5, 60, 300, 1800], lambda s: price[s], lambda: 100.0, out.append)
    f.register(7, "A", 1_000_000)
    assert f.tick(1_000_500) == 0
    assert f.tick(1_001_100) == 1
    price["A"] = 11.0
    assert f.tick(1_005_200) == 1 and f.tick(1_060_100) == 1
    assert [o["label"] for o in out] == ["1s", "5s", "1m"] and out[1]["price"] == 11.0
    assert out[0]["btc_price"] == 100.0 and f.pending == 2
    assert label_for(300) == "5m" and label_for(1800) == "30m" and label_for(30) == "30s"
