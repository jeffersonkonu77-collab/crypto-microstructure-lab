"""Application wiring: startup, streaming, loops, graceful shutdown."""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from collections import deque

import aiohttp

from . import __version__
from .alerts import Alerter, format_alert, pct, usd
from .binance import Rest, WsFeed, filter_universe, make_session, now_ms
from .config import to_dict
from .db import SPIKE_COLS, Store
from .engine import SpikeEngine
from .followups import FollowUps
from .ranker import Ranker
from .walls import WallTracker


class App:
    def __init__(self, cfg, live: bool = False, ui: bool = True, duration: float | None = None,
                 max_symbols: int | None = None, post=None):
        self.cfg = cfg
        self.live = live
        self.use_ui = ui
        self.duration = duration
        self.max_symbols = max_symbols
        self.started = time.time()
        self.clock_offset_ms = 0.0
        self.run_id = int(time.time() * 1000)
        self.stop_event = asyncio.Event()

        s = cfg.storage
        self.store = Store(s.db_path, s.batch_seconds, s.retention_days)
        self.engine = SpikeEngine(cfg, self._on_spike, self._on_interval, self._on_followup,
                                  next_id=self.store.next_id)
        self.ranker = Ranker(cfg, [])
        self.walls = WallTracker(cfg, lambda sym: self._is_big(sym), self._on_wall)
        self.followups = FollowUps(
            cfg.followups.horizons_s,
            lambda sym: (self.engine.states[sym].last if sym in self.engine.states else 0.0),
            self.engine.btc_price, self._on_followup,
        )
        self.alerter = Alerter(cfg, self._on_sent, live=live, post=post)

        self.session: aiohttp.ClientSession | None = None
        self.rest: Rest | None = None
        self.market_feed: WsFeed | None = None
        self.depth_feed: WsFeed | None = None
        self.depth_syms: set[str] = set()
        self.last_ticker = 0.0
        self.gap_count = 0
        self.trades_per_s = 0.0
        self.feed: deque = deque(maxlen=200)
        self.lat_detect_sent: deque = deque(maxlen=500)
        self.events: dict[int, dict] = {}
        self.universe: list[str] = []
        self.tasks: list[asyncio.Task] = []
        self.log_lines: deque = deque(maxlen=50)
        self.spike_log: list[dict] = []  # kept for tests and the no-UI printer
        self.alert_log = None
        self.last_alert_text = ""
        path = getattr(cfg.storage, 'alert_log_path', '')
        if path:
            self.alert_log = open(path, 'a', encoding='utf-8', buffering=1)

    # ------------------------------------------------------------------ helpers
    def exchange_ms(self) -> int:
        return int(time.time() * 1000 - self.clock_offset_ms)

    def _is_big(self, sym: str) -> bool:
        st = self.engine.states.get(sym)
        return st is not None and st.tier == "ge1b"

    def say(self, msg: str) -> None:
        self.log_lines.append(msg)
        if not self.use_ui:
            print(msg, flush=True)

    # --------------------------------------------------------------- engine hooks
    def _on_spike(self, ev: dict) -> None:
        ev["run_id"] = self.run_id
        ev["clock_offset_ms"] = self.clock_offset_ms
        ev["sent_ms"] = None
        ev["delivery"] = None
        self.events[ev["id"]] = ev
        self.spike_log.append(ev)
        self.store.insert("spikes", {k: ev.get(k) for k in SPIKE_COLS})
        for o in ev["top_orders"]:
            self.store.insert("big_orders", {
                "event_id": ev["id"], "symbol": ev["symbol"], "ts_ms": o["ts_ms"],
                "side": o["side"], "price": o["price"], "notional": o["notional"],
                "pct_of_volume": o["pct_of_volume"], "flagged": o["flagged"],
            })
        self.followups.register(ev["id"], ev["symbol"], ev["detect_ms"])
        self.alerter.submit(ev)
        if ev["symbol"] not in self.ranker.watch:
            self.ranker.force(ev["symbol"], self.exchange_ms())
        z = f"z{ev['z']:+.1f}" if ev.get("z") is not None else "z n/a"
        tag = "VOLATILE" if ev["kind"] == "VOLATILE" else "SPIKE"
        stamp = time.strftime("%H:%M:%S", time.gmtime(ev["trade_ms"] / 1000))
        line = (f"{stamp} {tag} {ev['symbol']} {pct(ev['ret'], 2)} {z} "
                f"flow {usd(ev['buy_notional'] - ev['sell_notional'])}")
        if ev.get("vol_mult") is not None:
            line += f" vol {ev['vol_mult']:.1f}x"
        self.feed.append(line)
        self.last_alert_text = format_alert(ev)
        self._write_alert_log(ev)
        if not self.use_ui:
            print("\n" + format_alert(ev), flush=True)

    def _write_alert_log(self, ev: dict) -> None:
        if self.alert_log is None:
            return
        try:
            stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ev['detect_ms'] / 1000))
            self.alert_log.write(f"[{stamp} UTC] alert #{ev['id']}\n{format_alert(ev)}\n\n")
            self.alert_log.flush()
        except Exception as exc:
            self.say(f'[alert log] {exc!r}')

    def _on_interval(self, row: dict) -> None:
        mode = "INSERT OR IGNORE" if row["partial"] else "INSERT OR REPLACE"
        self.store.insert("intervals", row, mode)

    def _on_followup(self, row: dict) -> None:
        self.store.insert("followups", row)

    def _on_wall(self, ev: dict) -> None:
        self.store.insert("wall_events", ev)
        self.engine.add_wall_tag(ev["symbol"], {
            "side": ev["side"], "price": ev["price"], "notional": ev["notional"],
            "kind": ev["kind"],
        })

    def _on_sent(self, events: list[dict], sent_ms: int, delivery: str) -> None:
        for ev in events:
            self.store.execute("UPDATE spikes SET sent_ms=?, delivery=? WHERE id=?",
                               (sent_ms, delivery, ev["id"]))
            self.lat_detect_sent.append(sent_ms - ev["detect_ms"])
            ev["sent_ms"], ev["delivery"] = sent_ms, delivery
            self.events.pop(ev["id"], None)

    def _on_gap(self, name: str, start_ms: int, end_ms: int) -> None:
        self.gap_count += 1
        self.store.insert("gaps", {"ts_ms": start_ms, "stream": name,
                                   "duration_ms": end_ms - start_ms, "note": "reconnect"})
        if name == "market":
            self.engine.mark_gap()
        self.say(f"[gap] {name} stream was down {(end_ms - start_ms) / 1000:.1f}s")

    # ------------------------------------------------------------ stream handlers
    def _apply_tickers(self, rows) -> None:
        eng = self.engine
        batch = []
        for sym, last, qv, chg in rows:
            eng.on_ticker(sym, last, qv, chg)
            batch.append((sym, last, qv, chg))
        self.ranker.update(batch)

    def _on_market(self, data) -> None:
        if isinstance(data, list):
            self.last_ticker = time.time()
            self._apply_tickers(
                (t["s"], float(t["c"]), float(t["q"]), float(t["P"])) for t in data
            )
            return
        e = data.get("e")
        if e == "aggTrade":
            sym = data["s"]
            price = float(data["p"])
            qty = float(data["q"])
            is_sell = data["m"]
            self.engine.on_trade(sym, price, qty, is_sell, data["T"], int(time.time() * 1000))
            if sym in self.depth_syms:
                self.walls.on_trade(sym, price, price * qty, is_sell)
        elif e == "forceOrder":
            o = data["o"]
            qty = float(o.get("z") or o.get("q") or 0)
            p = float(o.get("ap") or o.get("p") or 0)
            self.engine.on_liquidation(o["s"], o["S"], qty * p)
        elif e == "24hrTicker":
            self._apply_tickers([(data["s"], float(data["c"]), float(data["q"]), float(data["P"]))])

    def _on_depth(self, data) -> None:
        if isinstance(data, dict) and data.get("e") == "depthUpdate":
            self.walls.on_depth(data["s"], data["b"], data["a"], data.get("T") or self.exchange_ms())

    # ------------------------------------------------------------------- startup
    async def _startup(self) -> None:
        cfg = self.cfg
        self.say(f"spikemon {__version__} starting ({'LIVE' if self.live else 'PRACTICE'} mode)")
        self.store.start()
        self.store.insert("runs", {
            "id": self.run_id, "start_ms": int(time.time() * 1000), "end_ms": None,
            "version": __version__, "mode": "live" if self.live else "practice",
            "config_json": json.dumps(to_dict(cfg), default=str),
        })
        self.session = make_session(20)
        self.rest = Rest(cfg.exchange.rest_base, self.session, cfg.runtime.rest_weight_soft_limit)

        t0 = time.time() * 1000
        srv = await self.rest.server_time()
        t1 = time.time() * 1000
        self.clock_offset_ms = (t0 + t1) / 2 - srv
        self.say(f"clock offset vs Binance: {self.clock_offset_ms:+.0f} ms")

        info = await self.rest.exchange_info()
        universe, excluded = filter_universe(info, cfg.universe)
        tick = await self.rest.ticker24h()
        vol = {t["symbol"]: float(t["quoteVolume"]) for t in tick}
        if self.max_symbols:
            universe = sorted(universe, key=lambda s: vol.get(s, 0), reverse=True)[: self.max_symbols]
        self.universe = universe
        self.ranker.universe = set(universe)
        self.engine.add_symbols(universe)
        self.say(f"universe: {len(universe)} crypto USDT perpetuals (excluded {excluded})")
        self._apply_tickers(
            (t["symbol"], float(t["lastPrice"]), float(t["quoteVolume"]), float(t["priceChangePercent"]))
            for t in tick if t["symbol"] in self.ranker.universe
        )
        await self._load_history()
        await self._refresh_watchlist(initial=True)
        self.engine.armed = True
        self.say("history loaded, spike detection armed")

        r = cfg.runtime
        self.market_feed = WsFeed(
            "market", cfg.exchange.ws_market, self._on_market,
            base_streams=["!ticker@arr", "!forceOrder@arr"], on_gap=self._on_gap,
            chunk=r.subscribe_chunk, pause=r.subscribe_pause_s, stale_s=r.stale_stream_s,
            max_backoff=r.reconnect_max_s, max_age_s=r.max_connection_age_s,
        )
        self.market_feed.desired.update(f"{s.lower()}@aggTrade" for s in universe)
        if cfg.walls.enabled:
            self.depth_feed = WsFeed(
                "depth", cfg.exchange.ws_public, self._on_depth, on_gap=self._on_gap,
                chunk=r.subscribe_chunk, pause=r.subscribe_pause_s, stale_s=r.stale_stream_s,
                max_backoff=r.reconnect_max_s, max_age_s=r.max_connection_age_s,
            )
            self.depth_feed.desired.update(self._depth_stream(s) for s in self.depth_syms)

    def _depth_stream(self, sym: str) -> str:
        w = self.cfg.walls
        return f"{sym.lower()}@depth{int(w.levels)}@{int(w.update_ms)}ms"

    async def _load_history(self) -> None:
        cfg = self.cfg
        n = len(self.universe)
        limit = int(cfg.spike.history_intervals) + 1
        sem = asyncio.Semaphore(int(cfg.runtime.history_concurrency))
        done = 0
        failed: list[str] = []
        now = self.exchange_ms()

        async def one(sym: str) -> None:
            nonlocal done
            async with sem:
                try:
                    rows = await self.rest.klines(sym, "15m", limit)
                    for row in self.engine.load_klines(sym, rows, now):
                        self.store.insert("intervals", row, "INSERT OR IGNORE")
                except Exception as exc:
                    failed.append(f"{sym}: {exc!r}"[:80])
                done += 1
                if done % 100 == 0 or done == n:
                    self.say(f"history {done}/{n}")

        self.say(f"loading ~24h of 15m history for {n} coins (baselines for z-scores)...")
        await asyncio.gather(*(one(s) for s in self.universe))
        if failed:
            self.say(f"history failed for {len(failed)} coins (they alert without z): {failed[:3]}")

    async def _refresh_watchlist(self, initial: bool = False) -> None:
        now = self.exchange_ms()
        added, removed = self.ranker.refresh(now)
        for sym, tags in self.ranker.watch.items():
            st = self.engine.states.get(sym)
            if st is not None:
                st.in_watch, st.watch_tags = True, tags
        for sym in removed:
            st = self.engine.states.get(sym)
            if st is not None:
                st.in_watch, st.watch_tags = False, ""
            self.walls.drop(sym)
        for sym in added:
            self.store.insert("watchlist_changes", {"ts_ms": now, "symbol": sym, "action": "add",
                                                     "source": self.ranker.watch.get(sym, "")})
        for sym in removed:
            self.store.insert("watchlist_changes", {"ts_ms": now, "symbol": sym, "action": "remove",
                                                     "source": ""})
        self.depth_syms = set(self.ranker.watch)
        if self.cfg.walls.enabled and self.depth_feed is not None and not initial:
            if added:
                await self.depth_feed.subscribe([self._depth_stream(s) for s in added])
            if removed:
                await self.depth_feed.unsubscribe([self._depth_stream(s) for s in removed])

    # --------------------------------------------------------------------- loops
    async def _poll_tickers(self) -> None:
        tick = await self.rest.ticker24h()
        self._apply_tickers(
            (t["symbol"], float(t["lastPrice"]), float(t["quoteVolume"]), float(t["priceChangePercent"]))
            for t in tick
        )

    async def _ranker_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.watchlist.refresh_seconds)
            try:
                if time.time() - self.last_ticker > 15:
                    await self._poll_tickers()  # ticker stream silent: REST fallback
                await self._refresh_watchlist()
            except Exception as exc:
                self.say(f"[ranker] {exc!r}")

    async def _sweep_loop(self) -> None:
        last_n, last_t = 0, time.time()
        while True:
            await asyncio.sleep(0.5)
            self.engine.sweep(self.exchange_ms())
            self.followups.tick(int(time.time() * 1000))
            now = time.time()
            if now - last_t >= 2.0:
                self.trades_per_s = (self.engine.trade_count - last_n) / (now - last_t)
                last_n, last_t = self.engine.trade_count, now

    async def _clock_loop(self) -> None:
        while True:
            await asyncio.sleep(300)
            try:
                t0 = time.time() * 1000
                srv = await self.rest.server_time()
                self.clock_offset_ms = (t0 + time.time() * 1000) / 2 - srv
            except Exception:
                pass

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            self.store.insert("health", {
                "ts_ms": int(time.time() * 1000), "trades_per_s": self.trades_per_s,
                "market_msgs": self.market_feed.msgs if self.market_feed else 0,
                "depth_msgs": self.depth_feed.msgs if self.depth_feed else 0,
                "writer_queue": self.store.queue_size, "spikes_total": self.engine.spikes_total,
                "alerts_sent": self.alerter.sent_msgs, "rest_weight": self.rest.used_weight,
            })

    async def _status_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            self.say(
                f"[status] trades/s {self.trades_per_s:,.0f} | spikes {self.engine.spikes_total} | "
                f"msgs sent {self.alerter.sent_msgs} | db queue {self.store.queue_size} | gaps {self.gap_count}"
            )

    async def _ui_loop(self) -> None:
        from rich.live import Live
        from .ui import build_view
        hz = float(self.cfg.ui.refresh_hz)
        with Live(build_view(self), refresh_per_second=max(hz, 1), screen=False) as live:
            while True:
                await asyncio.sleep(1.0 / hz)
                live.update(build_view(self))

    # ----------------------------------------------------------------------- run
    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except (NotImplementedError, RuntimeError):
                try:  # Windows has no loop signal handlers: use the plain signal module
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(self.stop_event.set))
                except (ValueError, OSError):
                    pass
        try:
            await self._startup()
            coros = [self.market_feed.run(), self.alerter.run(), self._ranker_loop(),
                     self._sweep_loop(), self._clock_loop(), self._health_loop()]
            if self.depth_feed is not None:
                coros.append(self.depth_feed.run())
            coros.append(self._ui_loop() if self.use_ui else self._status_loop())
            self.tasks = [asyncio.create_task(c) for c in coros]
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.duration)
            except asyncio.TimeoutError:
                pass
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        for f in (self.market_feed, self.depth_feed):
            if f is not None:
                f.stop()
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        try:
            await self.alerter.drain()
            await self.alerter.close()
        except Exception:
            pass
        if self.alert_log is not None:
            try:
                self.alert_log.close()
            except Exception:
                pass
        self.store.execute("UPDATE runs SET end_ms=? WHERE id=?", (int(time.time() * 1000), self.run_id))
        await asyncio.to_thread(self.store.close)
        if self.session is not None:
            await self.session.close()
        print(f"\nstopped cleanly: {self.engine.spikes_total} spikes, "
              f"{self.store.rows_written:,} rows written to {self.cfg.storage.db_path}", file=sys.stderr)
