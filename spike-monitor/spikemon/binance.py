"""Binance USDT-M futures access: REST client, universe filter, websocket feed.

Market data is public: no API key, no signature, no cost.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter

import aiohttp
import websockets

try:  # optional speedup
    import orjson

    def _loads(s):
        return orjson.loads(s)
except Exception:  # pragma: no cover
    _loads = json.loads


def make_session(total: float = 20) -> aiohttp.ClientSession:
    """HTTP session using the operating system resolver (getaddrinfo in a thread).
    Avoids aiodns/c-ares, which fails to resolve on some Windows setups."""
    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=total),
        connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver()),
    )


def now_ms() -> int:
    return int(time.time() * 1000)


# --------------------------------------------------------------------------
# REST
# --------------------------------------------------------------------------
class Rest:
    def __init__(self, base: str, session: aiohttp.ClientSession, soft_limit: int = 1800):
        self.base = base.rstrip("/")
        self.session = session
        self.soft_limit = soft_limit
        self.used_weight = 0
        self._ban_until = 0.0

    async def get(self, path: str, params: dict | None = None):
        for attempt in range(4):
            wait = self._ban_until - time.time()
            if wait > 0:
                await asyncio.sleep(min(wait, 120))
            if self.used_weight >= self.soft_limit:
                # weight window is one minute; back off until it resets
                await asyncio.sleep(5)
                self.used_weight = max(0, self.used_weight - 600)
            async with self.session.get(self.base + path, params=params) as resp:
                used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                if used is not None:
                    try:
                        self.used_weight = int(used)
                    except ValueError:
                        pass
                if resp.status in (418, 429):
                    retry = float(resp.headers.get("Retry-After", "10"))
                    self._ban_until = time.time() + retry
                    continue
                if resp.status >= 400:
                    text = await resp.text()
                    raise RuntimeError(f"REST {path} -> HTTP {resp.status}: {text[:200]}")
                return await resp.json()
        raise RuntimeError(f"REST {path} rate limited repeatedly")

    async def server_time(self) -> int:
        return int((await self.get("/fapi/v1/time"))["serverTime"])

    async def exchange_info(self) -> list[dict]:
        return (await self.get("/fapi/v1/exchangeInfo"))["symbols"]

    async def ticker24h(self) -> list[dict]:
        return await self.get("/fapi/v1/ticker/24hr")

    async def klines(self, symbol: str, interval: str, limit: int) -> list:
        return await self.get(
            "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit}
        )


def filter_universe(info: list[dict], ucfg) -> tuple[list[str], dict]:
    """Return (included symbols, summary of exclusions) for crypto USDT perpetuals."""
    included: list[str] = []
    excluded: Counter = Counter()
    allowed_types = {t.upper() for t in ucfg.underlying_types}
    bad_sub = {t.upper() for t in ucfg.exclude_subtypes}
    manual = set(ucfg.exclude)
    for s in info:
        sym = s.get("symbol", "")
        if s.get("contractType") != "PERPETUAL":
            excluded["not_perpetual"] += 1
            continue
        if s.get("quoteAsset") != ucfg.quote:
            excluded["other_quote"] += 1
            continue
        if s.get("status") != "TRADING":
            excluded["not_trading"] += 1
            continue
        utype = str(s.get("underlyingType", "COIN")).upper()
        subs = {str(x).upper() for x in (s.get("underlyingSubType") or [])}
        if utype not in allowed_types:
            excluded[f"type:{utype}"] += 1
            continue
        hit = subs & bad_sub
        if hit:
            excluded[f"subtype:{sorted(hit)[0]}"] += 1
            continue
        if sym in manual:
            excluded["manual"] += 1
            continue
        included.append(sym)
    return sorted(included), dict(excluded)


# --------------------------------------------------------------------------
# Websocket feed
# --------------------------------------------------------------------------
class WsFeed:
    """One websocket connection with dynamic SUBSCRIBE/UNSUBSCRIBE, automatic
    reconnect with backoff, staleness watchdog, proactive refresh before
    Binance's 24h connection cap, and gap reporting.

    Messages arrive raw (the /ws path), so every payload carries its own event
    type: dicts with "e", or a list for the all-market ticker.
    """

    def __init__(
        self,
        name: str,
        base_url: str,
        on_msg,
        base_streams: list[str] | None = None,
        on_gap=None,
        chunk: int = 100,
        pause: float = 0.12,
        stale_s: float = 30,
        max_backoff: float = 30,
        max_age_s: float = 82800,
    ):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.on_msg = on_msg
        self.base_streams = list(base_streams or [])
        self.on_gap = on_gap
        self.chunk = chunk
        self.pause = pause
        self.stale_s = stale_s
        self.max_backoff = max_backoff
        self.max_age_s = max_age_s
        self.desired: set[str] = set(self.base_streams)
        self.ws = None
        self.connected = False
        self.msgs = 0
        self.handler_errors = 0
        self.reconnects = 0
        self.last_msg = 0.0
        self.last_error: str | None = None
        self._req_id = 0
        self._stop = False
        self._sub_lock = asyncio.Lock()

    # ---- subscription management -------------------------------------------
    async def subscribe(self, streams) -> None:
        new = [s for s in streams if s not in self.desired]
        self.desired.update(new)
        if self.ws is not None and self.connected and new:
            await self._send_chunks("SUBSCRIBE", new)

    async def unsubscribe(self, streams) -> None:
        gone = [s for s in streams if s in self.desired]
        for s in gone:
            self.desired.discard(s)
        if self.ws is not None and self.connected and gone:
            await self._send_chunks("UNSUBSCRIBE", gone)

    async def _send_chunks(self, method: str, streams: list[str]) -> None:
        async with self._sub_lock:
            for i in range(0, len(streams), self.chunk):
                part = streams[i : i + self.chunk]
                self._req_id += 1
                try:
                    await self.ws.send(
                        json.dumps({"method": method, "params": part, "id": self._req_id})
                    )
                except Exception as exc:
                    self.last_error = f"send failed: {exc!r}"
                    return
                await asyncio.sleep(self.pause)  # stay under 10 incoming msgs/s

    # ---- run loop -----------------------------------------------------------
    def stop(self) -> None:
        self._stop = True
        if self.ws is not None:
            asyncio.ensure_future(self.ws.close())

    def _url(self) -> tuple[str, list[str]]:
        initial = list(self.base_streams) or sorted(self.desired)[:1]
        url = self.base_url + "/ws" + ("/" + "/".join(initial) if initial else "")
        return url, initial

    async def run(self) -> None:
        backoff = 1.0
        disconnected_at: int | None = None
        first = True
        while not self._stop:
            url, initial = self._url()
            try:
                async with websockets.connect(
                    url, max_size=None, ping_interval=20, ping_timeout=20, open_timeout=15
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    self.last_msg = time.time()
                    connected_at = time.time()
                    if not first and disconnected_at is not None and self.on_gap:
                        self.on_gap(self.name, disconnected_at, now_ms())
                    first = False
                    backoff = 1.0
                    rest = [s for s in sorted(self.desired) if s not in initial]
                    if rest:
                        asyncio.ensure_future(self._send_chunks("SUBSCRIBE", rest))
                    watchdog = asyncio.ensure_future(self._watchdog(ws, connected_at))
                    try:
                        async for raw in ws:
                            self.msgs += 1
                            self.last_msg = time.time()
                            try:
                                data = _loads(raw)
                            except Exception:
                                continue
                            if isinstance(data, dict) and "e" not in data and "result" in data:
                                continue  # subscribe ack
                            try:
                                self.on_msg(data)
                            except Exception as exc:
                                self.handler_errors += 1
                                self.last_error = f"handler: {exc!r}"
                    finally:
                        watchdog.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = repr(exc)
            finally:
                if self.connected:
                    disconnected_at = now_ms()
                self.connected = False
                self.ws = None
            if self._stop:
                break
            self.reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.max_backoff)

    async def _watchdog(self, ws, connected_at: float) -> None:
        while True:
            await asyncio.sleep(5)
            if time.time() - self.last_msg > self.stale_s:
                self.last_error = f"stale > {self.stale_s}s, reconnecting"
                await ws.close()
                return
            if time.time() - connected_at > self.max_age_s:
                self.last_error = "proactive reconnect before 24h cap"
                await ws.close()
                return
