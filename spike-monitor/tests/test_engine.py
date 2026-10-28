import itertools
import statistics

from spikemon.config import load_config
from spikemon.engine import SIGMA_K, SpikeEngine

IV = 900_000
T0 = (1_760_000_000_000 // IV) * IV


def make(overrides=None, symbols=("BTCUSDT", "AAAUSDT"), armed=True):
    cfg = load_config(None, overrides)
    spikes, intervals, follows = [], [], []
    eng = SpikeEngine(cfg, spikes.append, intervals.append, follows.append,
                      next_id=itertools.count(1).__next__)
    eng.add_symbols(symbols)
    eng.armed = armed
    return eng, spikes, intervals, follows


def seed_hist(eng, sym, n=30, r=0.005, vol=1e6):
    for _ in range(n):
        eng.states[sym].hist.append((r, vol))


def trade(eng, sym, price, t_ms, qty=1.0, sell=False):
    eng.on_trade(sym, price, qty, sell, t_ms, t_ms + 5)


def set_tier(eng, sym, vol24h):
    eng.on_ticker(sym, 100.0, vol24h, 0.0)


def test_big_tier_triggers_at_3pct_small_tier_needs_5pct():
    eng, spikes, *_ = make(symbols=("BIGUSDT", "SMLUSDT"))
    set_tier(eng, "BIGUSDT", 1.5e9)
    set_tier(eng, "SMLUSDT", 5e8)
    for sym in ("BIGUSDT", "SMLUSDT"):
        trade(eng, sym, 100.0, T0 + 1000)
        trade(eng, sym, 102.9, T0 + 2000)   # +2.9%: nobody triggers
    assert spikes == []
    for sym in ("BIGUSDT", "SMLUSDT"):
        trade(eng, sym, 103.0, T0 + 3000)   # +3.0%: only the big tier
    assert [s["symbol"] for s in spikes] == ["BIGUSDT"]
    assert spikes[0]["threshold"] == 0.03 and spikes[0]["tier"] == "ge1b"
    trade(eng, "SMLUSDT", 104.9, T0 + 4000)
    assert len(spikes) == 1
    trade(eng, "SMLUSDT", 105.0, T0 + 5000)  # +5.0%
    assert [s["symbol"] for s in spikes] == ["BIGUSDT", "SMLUSDT"]
    assert spikes[1]["threshold"] == 0.05 and spikes[1]["tier"] == "lt1b"


def test_down_move_triggers_with_negative_direction():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 94.9, T0 + 2000)
    assert len(spikes) == 1
    assert spikes[0]["direction"] == -1 and spikes[0]["ret"] < -0.05


def test_move_is_measured_from_the_interval_open_and_rolls_on_boundary():
    eng, spikes, intervals, _ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 104.0, T0 + 800_000)       # +4%, below 5%
    trade(eng, "AAAUSDT", 104.5, T0 + IV + 1000)     # new interval: opens at 104.5
    st = eng.states["AAAUSDT"]
    assert st.open == 104.5 and st.start_ms == T0 + IV
    assert len(intervals) == 1 and intervals[0]["close"] == 104.0 and intervals[0]["open"] == 100.0
    trade(eng, "AAAUSDT", 108.0, T0 + IV + 2000)     # +3.3% vs new open: no alert
    assert spikes == []
    trade(eng, "AAAUSDT", 109.8, T0 + IV + 3000)     # +5.07% vs new open
    assert len(spikes) == 1 and spikes[0]["interval_start_ms"] == T0 + IV


def test_one_alert_per_interval_then_again_next_interval():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 105.0, T0 + 2000)
    trade(eng, "AAAUSDT", 107.0, T0 + 3000)
    trade(eng, "AAAUSDT", 110.0, T0 + 4000)
    assert len(spikes) == 1
    trade(eng, "AAAUSDT", 100.0, T0 + IV + 1000)
    trade(eng, "AAAUSDT", 106.0, T0 + IV + 2000)
    assert len(spikes) == 2


def test_volatile_upgrade_after_opposite_cross_and_only_once():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 105.5, T0 + 2000)
    assert [s["kind"] for s in spikes] == ["SPIKE"]
    trade(eng, "AAAUSDT", 99.0, T0 + 3000)           # back through the open, not yet -5%
    assert len(spikes) == 1
    trade(eng, "AAAUSDT", 94.5, T0 + 4000)           # now -5.5% vs open
    assert [s["kind"] for s in spikes] == ["SPIKE", "VOLATILE"]
    v = spikes[1]
    assert v["high_ret"] > 0.05 and v["low_ret"] < -0.05 and v["direction"] == -1
    trade(eng, "AAAUSDT", 120.0, T0 + 5000)
    trade(eng, "AAAUSDT", 80.0, T0 + 6000)
    assert len(spikes) == 2                          # nothing after the volatile alert
    eng.volatile_total == 1


def test_volatile_upgrade_can_be_disabled():
    eng, spikes, *_ = make({"spike": {"volatile_upgrade": False}})
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 105.5, T0 + 2000)
    trade(eng, "AAAUSDT", 94.0, T0 + 3000)
    assert [s["kind"] for s in spikes] == ["SPIKE"]


def test_no_alerts_until_armed():
    eng, spikes, *_ = make(armed=False)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 110.0, T0 + 2000)
    assert spikes == []


def test_tier_follows_live_volume():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 9e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 103.5, T0 + 2000)
    assert spikes == []
    set_tier(eng, "AAAUSDT", 1.2e9)                  # crosses $1B: threshold drops to 3%
    trade(eng, "AAAUSDT", 103.6, T0 + 3000)
    assert len(spikes) == 1 and spikes[0]["tier"] == "ge1b"


def test_robust_sigma_ignores_outlier_intervals():
    eng, *_ = make()
    st = eng.states["AAAUSDT"]
    for r in [0.004, 0.006, 0.005, 0.005, 0.004, 0.006] * 5:
        st.hist.append((r, 1e6))
    clean = eng.sigma(st)
    assert abs(clean - SIGMA_K * 0.005) < 1e-9
    st.hist.append((0.15, 1e6))                      # one wild 15% interval
    st.hist.append((0.12, 1e6))
    noisy = eng.sigma(st)
    assert abs(noisy - clean) / clean < 0.15         # a std-dev baseline would blow up here
    assert statistics.pstdev([a for a, _ in st.hist]) > 3 * clean


def test_z_score_value_and_short_history():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 105.0, T0 + 2000)
    assert spikes[0]["z"] is None and spikes[0]["sigma"] is None      # no history yet
    eng2, spikes2, *_ = make()
    set_tier(eng2, "AAAUSDT", 1e8)
    seed_hist(eng2, "AAAUSDT", 30, 0.005)
    trade(eng2, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng2, "AAAUSDT", 105.0, T0 + 2000)
    expect = 0.05 / (SIGMA_K * 0.005)
    assert abs(spikes2[0]["z"] - expect) < 1e-6


def test_top_orders_flag_and_net_flow():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 500, qty=1)                       # tiny
    for i in range(60):
        trade(eng, "AAAUSDT", 100.0, T0 + 600 + i, qty=10, sell=(i % 2 == 0))   # $1000 each
    trade(eng, "AAAUSDT", 101.0, T0 + 1000, qty=400)                    # $40.4k buy
    trade(eng, "AAAUSDT", 101.0, T0 + 1100, qty=200, sell=True)         # $20.2k sell
    trade(eng, "AAAUSDT", 105.1, T0 + 2000, qty=100)                    # $10.5k buy, trips 5%
    ev = spikes[0]
    top = ev["top_orders"]
    assert [round(o["notional"]) for o in top[:3]] == [40400, 20200, 10510]
    assert top[0]["side"] == "BUY" and top[1]["side"] == "SELL"
    vol = ev["buy_notional"] + ev["sell_notional"]
    assert abs(ev["interval_volume"] - vol) < 1e-6
    assert top[0]["pct_of_volume"] > 5 and top[0]["flagged"] == 1
    assert ev["flagged_5pct"] >= 2                                      # 40.4k and 20.2k exceed 5% of ~$140k
    assert ev["buy_notional"] > ev["sell_notional"]


def test_liquidations_are_split_by_side_and_reset_each_interval():
    eng, spikes, *_ = make()
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    eng.on_liquidation("AAAUSDT", "SELL", 250_000)      # a long was liquidated
    eng.on_liquidation("AAAUSDT", "BUY", 900_000)       # shorts were liquidated
    trade(eng, "AAAUSDT", 105.0, T0 + 2000)
    assert spikes[0]["liq_long"] == 250_000 and spikes[0]["liq_short"] == 900_000
    trade(eng, "AAAUSDT", 105.0, T0 + IV + 1000)
    assert eng.states["AAAUSDT"].liq_long == 0 and eng.states["AAAUSDT"].liq_short == 0


def test_sweep_closes_quiet_intervals_and_feeds_baseline():
    eng, spikes, intervals, _ = make()
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    eng.sweep(T0 + IV + 1000)                           # inside grace: nothing yet
    assert intervals == []
    eng.sweep(T0 + IV + 5000)
    assert len(intervals) == 1
    st = eng.states["AAAUSDT"]
    assert st.idx == (T0 + IV) // IV and st.empty
    assert len(st.hist) == 1
    trade(eng, "AAAUSDT", 101.0, T0 + IV + 6000)        # first trade defines the new open
    assert st.open == 101.0 and not st.empty


def test_late_trade_from_closed_interval_is_ignored():
    eng, spikes, intervals, _ = make()
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 100.5, T0 + IV + 1000)
    trade(eng, "AAAUSDT", 200.0, T0 + IV - 10)          # late print for the old interval
    assert eng.states["AAAUSDT"].last == 100.5


def test_int_close_and_next_close_marks():
    eng, spikes, intervals, follows = make(symbols=("BTCUSDT", "AAAUSDT"))
    set_tier(eng, "AAAUSDT", 1e8)
    trade(eng, "BTCUSDT", 60000.0, T0 + 500)
    trade(eng, "AAAUSDT", 100.0, T0 + 1000)
    trade(eng, "AAAUSDT", 105.0, T0 + 2000)
    eid = spikes[0]["id"]
    eng.sweep(T0 + IV + 5000)
    marks = [(f["label"], f["price"]) for f in follows if f["event_id"] == eid]
    assert marks == [("int_close", 105.0)]
    assert follows[0]["btc_price"] == 60000.0
    eng.sweep(T0 + 2 * IV + 5000)
    marks = [f["label"] for f in follows if f["event_id"] == eid]
    assert marks == ["int_close", "next_close"]


def test_load_klines_seeds_history_and_current_interval():
    eng, *_ = make()
    now = T0 + 300_000
    rows = []
    for k in range(25, 0, -1):
        o = 100.0
        rows.append([T0 - k * IV, str(o), "101", "99", str(o * 1.004), "10", 0, "2000000", 50, "5", "900000", "0"])
    rows.append([T0, "100.0", "102", "99.5", "101.0", "10", 0, "750000", 30, "5", "300000", "0"])
    out = eng.load_klines("AAAUSDT", rows, now)
    st = eng.states["AAAUSDT"]
    assert len(out) == 25 and len(st.hist) == 25
    assert out[0]["source"] == "kline" and out[0]["buy_notional"] == 900_000
    assert st.idx == T0 // IV and st.open == 100.0 and st.vol_base == 750_000 and st.partial
    assert eng.sigma(st) is not None
    trade(eng, "AAAUSDT", 103.0, now + 1000, qty=10)
    assert st.open == 100.0                              # kline open kept, not replaced by first trade
