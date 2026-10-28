from spikemon.config import load_config
from spikemon.walls import WallTracker


def book(wall_bid_idx=None, wall_ask_idx=None, wall_usd=2_000_000, shift=0):
    """20 levels per side, ~$20k each, optionally one wall level per side."""
    bids, asks = [], []
    for i in range(20):
        pb = round(100.0 - 0.01 * (i + 1) - shift, 2)
        pa = round(100.0 + 0.01 * (i + 1) + shift, 2)
        qb = wall_usd / pb if i == wall_bid_idx else 200.0
        qa = wall_usd / pa if i == wall_ask_idx else 200.0
        bids.append([str(pb), str(qb)])
        asks.append([str(pa), str(qa)])
    return bids, asks


def make(big=False):
    events = []
    wt = WallTracker(load_config(None), lambda s: big, events.append)
    return wt, events


def test_wall_needs_confirmation_then_pulled_when_not_traded():
    wt, ev = make()
    b, a = book(wall_ask_idx=5)
    wt.on_depth("X", b, a, 1000)
    assert ev == []                                   # one snapshot is flicker
    wt.on_depth("X", b, a, 1500)
    assert [e["kind"] for e in ev] == ["appeared"] and ev[0]["side"] == "ask"
    b2, a2 = book()
    wt.on_depth("X", b2, a2, 3000)                    # gone, still in the visible range
    assert [e["kind"] for e in ev] == ["appeared", "pulled"]
    assert ev[1]["traded_notional"] == 0 and ev[1]["lifetime_s"] == 2.0


def test_wall_that_is_traded_through_is_eaten():
    wt, ev = make()
    b, a = book(wall_bid_idx=3)
    wt.on_depth("X", b, a, 1000)
    wt.on_depth("X", b, a, 1500)
    wall_px = float(b[3][0])
    wt.on_trade("X", wall_px, 1_200_000, True)         # sells hit the bid wall (60% of $2M)
    wt.on_trade("X", wall_px + 0.5, 900_000, True)     # above the wall: not part of it
    b2, a2 = book()
    wt.on_depth("X", b2, a2, 2500)
    assert ev[-1]["kind"] == "eaten" and ev[-1]["side"] == "bid"
    assert 1_100_000 < ev[-1]["traded_notional"] <= 2_000_000


def test_unconfirmed_wall_vanishing_is_silent():
    wt, ev = make()
    b, a = book(wall_ask_idx=2)
    wt.on_depth("X", b, a, 1000)
    b2, a2 = book()
    wt.on_depth("X", b2, a2, 1500)
    assert ev == []


def test_wall_leaving_visible_window_is_not_reported_as_pulled():
    wt, ev = make()
    b, a = book(wall_ask_idx=19)                       # sits at the deepest visible ask level
    wt.on_depth("X", b, a, 1000)
    wt.on_depth("X", b, a, 1500)
    assert [e["kind"] for e in ev] == ["appeared"]
    b2, a2 = book(shift=-0.1)                          # price drops, window moves down, wall out of range
    wt.on_depth("X", b2, a2, 2000)
    assert [e["kind"] for e in ev] == ["appeared"]


def test_dollar_floor_scales_with_tier():
    small, ev_s = make(big=False)
    big, ev_b = make(big=True)
    b, a = book(wall_ask_idx=4, wall_usd=400_000)      # above the $150k floor, under the $1M floor
    for wt in (small, big):
        wt.on_depth("X", b, a, 1000)
        wt.on_depth("X", b, a, 1500)
    assert len(ev_s) == 1 and ev_b == []


def test_shrunk_wall_is_not_counted_as_pulled():
    wt, ev = make()
    b, a = book(wall_ask_idx=5, wall_usd=2_000_000)
    wt.on_depth("X", b, a, 1000)
    wt.on_depth("X", b, a, 1500)
    b2, a2 = book(wall_ask_idx=5, wall_usd=800_000)    # shrank but still above half the threshold
    wt.on_depth("X", b2, a2, 2000)
    assert [e["kind"] for e in ev] == ["appeared"]
