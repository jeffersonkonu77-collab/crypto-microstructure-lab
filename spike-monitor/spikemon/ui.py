"""Terminal dashboard (rich). Plain figures only: no charts, no sparklines."""

from __future__ import annotations

import time

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .alerts import pct, px, sgn_usd, usd


def _pctl(values, q: float):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(q * (len(v) - 1) + 0.5))]


def _feed_stat(feed) -> str:
    if feed is None:
        return "off"
    state = "up" if feed.connected else "DOWN"
    return f"{state} msgs {feed.msgs:,} reconnects {feed.reconnects}"


def build_view(app) -> Group:
    cfg = app.cfg
    eng = app.engine
    up = time.time() - app.started
    h, rem = divmod(int(up), 3600)
    m, s = divmod(rem, 60)
    mode = "LIVE (Telegram on)" if app.live else "PRACTICE (nothing is sent)"

    lat = list(app.lat_detect_sent)
    p50, p95 = _pctl(lat, 0.5), _pctl(lat, 0.95)
    lat_txt = f"detect->sent p50 {p50:.0f}ms p95 {p95:.0f}ms" if p50 is not None else "detect->sent n/a"

    status = Table.grid(padding=(0, 2))
    status.add_row(
        f"Mode: {mode}", f"Uptime {h}h{m:02d}m{s:02d}s", f"Coins {len(eng.states)}",
        f"Trades/s {app.trades_per_s:,.0f}",
    )
    status.add_row(
        f"Market ws: {_feed_stat(app.market_feed)}",
        f"Depth ws: {_feed_stat(app.depth_feed)}", f"Gaps {app.gap_count}",
        f"REST weight {app.rest.used_weight if app.rest else 0}",
    )
    status.add_row(
        f"Spikes {eng.spikes_total} (volatile {eng.volatile_total})",
        f"Msgs sent {app.alerter.sent_msgs} digests {app.alerter.digests} failed {app.alerter.failed}",
        lat_txt,
        f"DB queue {app.store.queue_size} rows {app.store.rows_written:,} err {app.store.errors}",
    )
    status.add_row(
        f"Walls tracked {sum(app.walls.active(s) for s in app.walls.walls)}",
        f"Follow-ups pending {app.followups.pending}",
        f"Watchlist {len(app.ranker.watch)}",
        "Ctrl+C to stop (flushes the database)",
    )

    rows = []
    for st in eng.states.values():
        if st.idx is None or st.open <= 0:
            continue
        ret = st.last / st.open - 1.0 if st.last > 0 else 0.0
        rows.append((abs(ret) / (st.thr_pct / 100.0), ret, st))

    leader = Table(title="Closest to trigger (all coins, ranked by move / trigger)", expand=True)
    for c in ("Symbol", "Ret", "Trigger", "Used", "z", "Net flow", "Vol 24h"):
        leader.add_column(c, justify="right" if c != "Symbol" else "left")
    for used, ret, st in sorted(rows, key=lambda r: r[0], reverse=True)[: int(cfg.ui.leader_rows)]:
        z = eng.z_score(st)
        leader.add_row(
            st.symbol, pct(ret, 2), f"{st.thr_pct:g}%", f"{used * 100:.0f}%",
            f"{z:+.1f}" if z is not None else "n/a",
            sgn_usd(st.buy - st.sell), usd(st.vol24h),
        )

    watch = Table(title="Watchlist (V volume, G gainers, L losers, S spike-added)", expand=True)
    for c in ("Symbol", "Src", "Price", "24h", "Vol 24h", "Int ret", "z", "Events", "Walls"):
        watch.add_column(c, justify="right" if c not in ("Symbol", "Src") else "left")
    wrows = [r for r in rows if r[2].symbol in app.ranker.watch]
    for used, ret, st in sorted(wrows, key=lambda r: r[0], reverse=True)[: int(cfg.ui.watch_rows)]:
        z = eng.z_score(st)
        watch.add_row(
            st.symbol, app.ranker.watch.get(st.symbol, ""), px(st.last),
            f"{st.chg24h:+.1f}%", usd(st.vol24h), pct(ret, 2),
            f"{z:+.1f}" if z is not None else "n/a", str(st.n_events),
            str(app.walls.active(st.symbol)),
        )

    feed = Text()
    for line in list(app.feed)[-int(cfg.ui.feed_lines):]:
        feed.append(line + "\n")
    if not feed.plain:
        feed.append("No spikes yet. Triggers: 3% for $1B+ volume coins, 5% below, within a 15m interval.")

    latest = Text(app.last_alert_text or "No alerts yet. The full text of the newest alert appears here.")
    return Group(
        Panel(status, title="spikemon: Binance USDT-M perpetuals, 15m interval spike monitor"),
        leader, watch, Panel(feed, title="Alert feed"),
        Panel(latest, title="Latest alert (full text, also appended to alerts.log)"),
    )
