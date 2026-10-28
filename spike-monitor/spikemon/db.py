"""SQLite storage. All writes go through one background thread in batches so
the database can never block the websocket handlers."""

from __future__ import annotations

import itertools
import queue
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY, start_ms INTEGER, end_ms INTEGER,
  version TEXT, mode TEXT, config_json TEXT);

CREATE TABLE IF NOT EXISTS spikes(
  id INTEGER PRIMARY KEY, run_id INTEGER, symbol TEXT, kind TEXT, direction INTEGER,
  interval_start_ms INTEGER, interval_ms INTEGER,
  trade_ms INTEGER, recv_ms INTEGER, detect_ms INTEGER, sent_ms INTEGER, clock_offset_ms REAL,
  minute REAL, open REAL, price REAL, ret REAL, high_ret REAL, low_ret REAL,
  threshold REAL, tier TEXT, vol24h REAL, sigma REAL, z REAL,
  buy_notional REAL, sell_notional REAL, interval_volume REAL, vol_mult REAL,
  liq_long REAL, liq_short REAL, flagged_5pct INTEGER, partial INTEGER,
  btc_price REAL, in_watchlist INTEGER, delivery TEXT);
CREATE INDEX IF NOT EXISTS ix_spikes_sym ON spikes(symbol, interval_start_ms);
CREATE INDEX IF NOT EXISTS ix_spikes_time ON spikes(trade_ms);

CREATE TABLE IF NOT EXISTS big_orders(
  event_id INTEGER, symbol TEXT, ts_ms INTEGER, side TEXT, price REAL,
  notional REAL, pct_of_volume REAL, flagged INTEGER);
CREATE INDEX IF NOT EXISTS ix_big_event ON big_orders(event_id);

CREATE TABLE IF NOT EXISTS followups(
  event_id INTEGER, label TEXT, horizon_s REAL, due_ms INTEGER, actual_ms INTEGER,
  price REAL, btc_price REAL);
CREATE INDEX IF NOT EXISTS ix_follow_event ON followups(event_id);

CREATE TABLE IF NOT EXISTS intervals(
  symbol TEXT, start_ms INTEGER, open REAL, high REAL, low REAL, close REAL,
  quote_volume REAL, buy_notional REAL, sell_notional REAL,
  liq_long REAL, liq_short REAL, n_trades INTEGER, partial INTEGER, source TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS ux_intervals ON intervals(symbol, start_ms);

CREATE TABLE IF NOT EXISTS wall_events(
  ts_ms INTEGER, symbol TEXT, side TEXT, price REAL, notional REAL,
  kind TEXT, traded_notional REAL, lifetime_s REAL);
CREATE INDEX IF NOT EXISTS ix_wall_sym ON wall_events(symbol, ts_ms);

CREATE TABLE IF NOT EXISTS gaps(ts_ms INTEGER, stream TEXT, duration_ms INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS watchlist_changes(ts_ms INTEGER, symbol TEXT, action TEXT, source TEXT);
CREATE TABLE IF NOT EXISTS health(
  ts_ms INTEGER, trades_per_s REAL, market_msgs INTEGER, depth_msgs INTEGER,
  writer_queue INTEGER, spikes_total INTEGER, alerts_sent INTEGER, rest_weight INTEGER);
"""

SPIKE_COLS = (
    "id", "run_id", "symbol", "kind", "direction", "interval_start_ms", "interval_ms",
    "trade_ms", "recv_ms", "detect_ms", "sent_ms", "clock_offset_ms", "minute", "open",
    "price", "ret", "high_ret", "low_ret", "threshold", "tier", "vol24h", "sigma", "z",
    "buy_notional", "sell_notional", "interval_volume", "vol_mult", "liq_long",
    "liq_short", "flagged_5pct", "partial", "btc_price", "in_watchlist", "delivery",
)

_retention_tables = {
    "intervals": "start_ms",
    "wall_events": "ts_ms",
    "gaps": "ts_ms",
    "watchlist_changes": "ts_ms",
    "health": "ts_ms",
}


def init_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(SCHEMA)
    con.commit()
    return con


class Store:
    def __init__(self, path: str, batch_seconds: float = 0.5, retention_days: int = 30):
        self.path = path
        self.batch_seconds = batch_seconds
        self.retention_days = retention_days
        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.rows_written = 0
        self.errors = 0
        self.last_error: str | None = None
        self._sql_cache: dict = {}
        con = init_db(path)
        row = con.execute("SELECT MAX(id) FROM spikes").fetchone()
        self._ids = itertools.count((row[0] or 0) + 1)
        con.close()

    # ---- public API (called from the event loop thread) -------------------
    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="db-writer", daemon=True)
        self._thread.start()

    def next_id(self) -> int:
        return next(self._ids)

    @property
    def queue_size(self) -> int:
        return self._q.qsize()

    def insert(self, table: str, row: dict, mode: str = "INSERT") -> None:
        keys = tuple(row.keys())
        key = (mode, table, keys)
        sql = self._sql_cache.get(key)
        if sql is None:
            cols = ",".join(keys)
            marks = ",".join("?" * len(keys))
            sql = f"{mode} INTO {table}({cols}) VALUES ({marks})"
            self._sql_cache[key] = sql
        self._q.put((sql, tuple(row[k] for k in keys)))

    def execute(self, sql: str, params: tuple = ()) -> None:
        self._q.put((sql, params))

    def close(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=15)

    # ---- writer thread -----------------------------------------------------
    def _drain(self) -> list:
        items = []
        while len(items) < 20000:
            try:
                items.append(self._q.get_nowait())
            except queue.Empty:
                break
        return items

    def _loop(self) -> None:
        con = sqlite3.connect(self.path, timeout=30)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        last_prune = 0.0
        while True:
            self._stop.wait(self.batch_seconds)
            items = self._drain()
            if items:
                self._flush(con, items)
            if time.time() - last_prune > 3600:
                self._prune(con)
                last_prune = time.time()
            if self._stop.is_set():
                items = self._drain()
                if items:
                    self._flush(con, items)
                break
        con.close()

    def _flush(self, con: sqlite3.Connection, items: list) -> None:
        try:
            for sql, group in itertools.groupby(items, key=lambda it: it[0]):
                con.executemany(sql, [g[1] for g in group])
            con.commit()
            self.rows_written += len(items)
        except Exception as exc:  # isolate a bad row instead of losing the batch
            con.rollback()
            self.last_error = repr(exc)
            for sql, params in items:
                try:
                    con.execute(sql, params)
                    self.rows_written += 1
                except Exception as exc2:
                    self.errors += 1
                    self.last_error = repr(exc2)
            con.commit()

    def _prune(self, con: sqlite3.Connection) -> None:
        if self.retention_days <= 0:
            return
        cutoff = int((time.time() - self.retention_days * 86400) * 1000)
        try:
            for table, col in _retention_tables.items():
                con.execute(f"DELETE FROM {table} WHERE {col} < ?", (cutoff,))
            con.commit()
        except Exception as exc:
            self.last_error = repr(exc)
