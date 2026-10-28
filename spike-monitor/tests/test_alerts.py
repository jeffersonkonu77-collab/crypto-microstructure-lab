import asyncio
import time

from spikemon.alerts import Alerter, format_alert, format_digest, usd
from spikemon.config import load_config

IV = 900_000
T0 = (1_760_000_000_000 // IV) * IV
START = (T0 // 86_400_000) * 86_400_000 + 14 * 3_600_000 + 15 * 60_000   # 14:15 UTC


def ev(i=1, kind="SPIKE", ret=0.068, z=7.4, tier="lt1b", sym="WIFUSDT", partial=0):
    return {
        "id": i, "symbol": sym, "kind": kind, "direction": 1 if ret > 0 else -1,
        "interval_start_ms": START,
        "interval_ms": IV, "trade_ms": START + 9 * 60_000, "recv_ms": START + 9 * 60_000 + 20,
        "detect_ms": int(time.time() * 1000), "minute": 9.0, "open": 2.41, "price": 2.574,
        "ret": ret, "high_ret": 0.053, "low_ret": -0.051, "threshold": 0.05, "tier": tier,
        "vol24h": 6.4e8, "sigma": 0.0092, "z": z, "buy_notional": 7.6e6, "sell_notional": 2.5e6,
        "interval_volume": 10.1e6, "vol_mult": 6.2, "liq_long": 0.0, "liq_short": 2.3e6,
        "flagged_5pct": 1, "partial": partial, "n_top_shown": 3,
        "top_orders": [
            {"side": "BUY", "price": 2.5, "notional": 1.4e6, "pct_of_volume": 14.0, "flagged": 1, "ts_ms": 0},
            {"side": "BUY", "price": 2.5, "notional": 7.8e5, "pct_of_volume": 7.7, "flagged": 1, "ts_ms": 0},
            {"side": "SELL", "price": 2.5, "notional": 3.1e5, "pct_of_volume": 3.0, "flagged": 0, "ts_ms": 0},
        ],
        "walls": [{"side": "ask", "price": 2.6, "notional": 2.1e6, "kind": "eaten"}],
    }


def test_usd_formatting():
    assert usd(1_400_000) == "$1.4M" and usd(860_000) == "$860K" and usd(2.4e9) == "$2.40B"
    assert usd(-5000) == "-$5K"


def test_spike_alert_text_is_quant_style_and_states_the_window():
    t = format_alert(ev())
    assert "WIFUSDT +6.8%" in t and "15m interval" in t and "UTC, minute 9.0" in t
    assert "Tier <$1B (trigger 5%)" in t
    assert "z +7.4 (typical 15m move 0.92%)" in t
    assert "Net flow +$5.1M (buy $7.6M / sell $2.5M)" in t
    assert "6.2x normal pace" in t
    assert "BUY $1.4M (14% of vol) 🏷️" in t and "SELL $310K (3% of vol)" in t
    assert "Liquidations: $2.30M shorts".replace("2.30M", "2.3M") in t
    assert "ask $2.1M @ 2.6 eaten" in t
    assert "partial" not in t


def test_volatile_alert_shows_both_extremes():
    t = format_alert(ev(kind="VOLATILE", ret=-0.02))
    assert t.startswith("⚠️ VERY VOLATILE WIFUSDT")
    assert "+5.3% high / -5.1% low" in t and "now -2.0%" in t


def test_no_z_when_history_is_short_and_partial_note():
    e = ev(partial=1)
    e["z"], e["sigma"] = None, None
    t = format_alert(e)
    assert "z n/a" in t and "partial interval" in t


def test_no_em_or_en_dashes_in_output():
    text = format_alert(ev()) + format_digest([ev(i) for i in range(5)])
    assert chr(0x2014) not in text and chr(0x2013) not in text


def test_digest_orders_by_strength_and_caps_lines():
    evs = [ev(i, z=float(i), sym=f"S{i}USDT") for i in range(1, 13)]
    evs.append(ev(99, kind="VOLATILE", z=0.5, sym="VOLUSDT"))
    d = format_digest(evs, 8)
    lines = d.split("\n")
    assert "13 spike alerts" in lines[0]
    assert lines[1].startswith("VOLATILE VOLUSDT")        # volatile first
    assert lines[2].startswith("S12USDT")                 # then strongest z
    assert len(lines) == 1 + 8 + 1 and "+5 more" in lines[-1]


def make_alerter(live, post, **over):
    cfg = load_config(None, {"alerts": {"min_interval_s": 0.05, **over}})
    sent = []
    a = Alerter(cfg, lambda evs, ms, d: sent.append((evs, ms, d)), live=live, post=post)
    return a, sent


def test_single_alert_leaves_fast_and_practice_mode_sends_nothing():
    async def go():
        posts = []

        async def post(text):
            posts.append((time.monotonic(), text))
            return True, None, None

        a, sent = make_alerter(True, post)
        task = asyncio.create_task(a.run())
        t0 = time.monotonic()
        a.submit(ev(1))
        await asyncio.sleep(0.2)
        task.cancel()
        assert len(posts) == 1 and posts[0][0] - t0 < 0.1
        assert sent[0][2] == "alert"

        posts2 = []

        async def post2(text):
            posts2.append(text)
            return True, None, None

        a2, sent2 = make_alerter(False, post2)
        t2 = asyncio.create_task(a2.run())
        a2.submit(ev(2))
        await asyncio.sleep(0.1)
        t2.cancel()
        assert posts2 == [] and sent2[0][2] == "practice"

    asyncio.run(go())


def test_storm_collapses_into_digests_and_respects_rate_limit():
    async def go():
        times = []

        async def post(text):
            times.append(time.monotonic())
            return True, None, None

        a, sent = make_alerter(True, post, min_interval_s=0.1)
        task = asyncio.create_task(a.run())
        for i in range(28):
            a.submit(ev(i, z=float(i)))
        await asyncio.sleep(0.6)
        task.cancel()
        assert 1 <= len(times) <= 4                      # far fewer messages than alerts
        assert a.digests >= 1
        assert sum(len(e) for e, _, _ in sent) == 28     # every event accounted for
        gaps = [b - a_ for a_, b in zip(times, times[1:])]
        assert all(g >= 0.09 for g in gaps)

    asyncio.run(go())


def test_telegram_429_is_retried_then_succeeds_and_failures_are_marked():
    async def go():
        calls = []

        async def post(text):
            calls.append(1)
            if len(calls) == 1:
                return False, 0.01, "HTTP 429"
            return True, None, None

        a, sent = make_alerter(True, post)
        task = asyncio.create_task(a.run())
        a.submit(ev(1))
        await asyncio.sleep(0.3)
        task.cancel()
        assert len(calls) == 2 and a.retries == 1 and sent[0][2] == "alert"

        async def bad(text):
            return False, None, "HTTP 500"

        b, sent_b = make_alerter(True, bad)
        t2 = asyncio.create_task(b.run())
        b.submit(ev(2))
        await asyncio.sleep(2.0)
        t2.cancel()
        assert sent_b[0][2] == "failed" and b.failed == 1

    asyncio.run(go())
