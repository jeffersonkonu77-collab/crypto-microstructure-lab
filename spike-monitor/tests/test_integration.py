"""End-to-end: the real app against a scripted fake Binance."""

import asyncio
import os
import sqlite3
import time

os.environ["NO_PROXY"] = "127.0.0.1,localhost"
os.environ["no_proxy"] = "127.0.0.1,localhost"

from spikemon.app import App  # noqa: E402
from spikemon.config import load_config  # noqa: E402

from tests.fake_exchange import IV, start_fake  # noqa: E402


def wait_for_safe_window(seconds=25):
    """Avoid running across a 15m boundary, which would legitimately re-open intervals."""
    now = time.time() * 1000
    left = (IV - now % IV) / 1000
    if left < seconds:
        time.sleep(left + 1.5)


def test_full_run_against_fake_exchange(tmp_path):
    wait_for_safe_window()
    db = str(tmp_path / "e2e.sqlite3")

    async def go():
        runner, server, rest_port, ws_port = await start_fake()
        cfg = load_config(None, {
            "exchange": {"rest_base": f"http://127.0.0.1:{rest_port}",
                         "ws_market": f"ws://127.0.0.1:{ws_port}/market",
                         "ws_public": f"ws://127.0.0.1:{ws_port}/public"},
            "storage": {"db_path": db, "batch_seconds": 0.1, "alert_log_path": str(tmp_path / "alerts.log")},
            "watchlist": {"refresh_seconds": 1},
            "followups": {"horizons_s": [1, 5]},
            "runtime": {"subscribe_pause_s": 0.02},
        })
        app = App(cfg, live=False, ui=False, duration=11.5)
        try:
            await app.run()
        finally:
            server.close()
            await runner.cleanup()
        return app

    app = asyncio.run(go())

    # universe: crypto USDT perpetuals only (stock, commodity, quarterly, settling excluded)
    assert sorted(app.universe) == ["AAAUSDT", "BBBUSDT", "BTCUSDT"]

    evs = app.spike_log
    by = {(e["symbol"], e["kind"]): e for e in evs}
    assert set(by) == {("BBBUSDT", "SPIKE"), ("BBBUSDT", "VOLATILE"), ("AAAUSDT", "SPIKE")}, \
        [(e["symbol"], e["kind"], round(e["ret"], 4)) for e in evs]
    assert len(evs) == 3                                  # exactly one per coin and kind, BTC silent

    bbb = by[("BBBUSDT", "SPIKE")]
    assert bbb["tier"] == "ge1b" and bbb["threshold"] == 0.03 and 0.03 <= bbb["ret"] < 0.035
    assert bbb["liq_long"] == 100_000 and bbb["liq_short"] == 0
    assert bbb["top_orders"][0]["notional"] > 590_000 and bbb["top_orders"][0]["side"] == "BUY"
    assert bbb["top_orders"][0]["flagged"] == 1 and bbb["flagged_5pct"] >= 1
    assert bbb["z"] is not None and bbb["sigma"] > 0 and bbb["vol_mult"] > 0
    assert bbb["partial"] == 1                            # started mid-interval, so it says so
    vol = by[("BBBUSDT", "VOLATILE")]
    assert vol["high_ret"] >= 0.03 and vol["low_ret"] <= -0.03

    aaa = by[("AAAUSDT", "SPIKE")]
    assert aaa["tier"] == "lt1b" and aaa["threshold"] == 0.05 and aaa["ret"] >= 0.05

    import io
    from rich.console import Console
    from spikemon.ui import build_view
    con_ = Console(width=170, file=io.StringIO(), record=True)
    con_.print(build_view(app))
    shown = con_.export_text()
    assert "Latest alert" in shown and "VERY VOLATILE BBBUSDT" in shown and "Alert feed" in shown

    # latency bookkeeping: everything fast, and 'sent' recorded for every event
    for e in evs:
        assert 0 <= e["detect_ms"] - e["recv_ms"] < 250, e["detect_ms"] - e["recv_ms"]
        assert e["sent_ms"] is not None and e["sent_ms"] - e["detect_ms"] < 1500
        assert e["delivery"] == "practice"

    log = (tmp_path / "alerts.log").read_text(encoding="utf-8")
    assert log.count("UTC] alert #") == 3 and "BBBUSDT" in log and "VERY VOLATILE" in log

    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM spikes").fetchone()[0] == 3
    assert con.execute("SELECT COUNT(*) FROM spikes WHERE sent_ms IS NOT NULL").fetchone()[0] == 3
    assert con.execute("SELECT COUNT(*) FROM big_orders").fetchone()[0] >= 3
    assert con.execute("SELECT COUNT(*) FROM intervals WHERE source='kline'").fetchone()[0] >= 3 * 90
    labels = {r[0] for r in con.execute("SELECT label FROM followups")}
    assert {"1s", "5s"} <= labels
    five = con.execute("SELECT price, btc_price FROM followups WHERE label='5s' LIMIT 1").fetchone()
    assert five[0] > 0 and five[1] > 0
    walls = [r[0] for r in con.execute(
        "SELECT kind FROM wall_events WHERE symbol='BBBUSDT' ORDER BY ts_ms")]
    assert walls[:1] == ["appeared"] and "pulled" in walls
    assert con.execute("SELECT COUNT(*) FROM watchlist_changes WHERE action='add'").fetchone()[0] >= 3
    run = con.execute("SELECT end_ms, mode FROM runs").fetchone()
    assert run[0] is not None and run[1] == "practice"   # clean shutdown stamped the run
    assert app.store.errors == 0, app.store.last_error
    assert app.market_feed.handler_errors == 0 and app.depth_feed.handler_errors == 0
