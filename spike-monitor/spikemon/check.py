"""Connectivity check: python -m spikemon --check

Tests Binance REST and both websocket endpoints from this machine, reports how
many crypto perpetuals were included and what was excluded, and checks the
Telegram bot if one is configured.
"""

from __future__ import annotations

import asyncio
import json
import time

import aiohttp
import websockets

from .binance import Rest, filter_universe, make_session


async def _ws_probe(url: str, subscribe: list[str] | None, timeout: float = 12.0):
    t0 = time.time()
    try:
        async with websockets.connect(url, open_timeout=10, max_size=None) as ws:
            if subscribe:
                await ws.send(json.dumps({"method": "SUBSCRIBE", "params": subscribe, "id": 1}))
            got_ack = False
            while time.time() - t0 < timeout:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except asyncio.TimeoutError:
                    break
                msg = json.loads(raw)
                if isinstance(msg, dict) and "result" in msg and "e" not in msg:
                    got_ack = True
                    continue
                return True, f"data received after {time.time() - t0:.1f}s"
            return (got_ack, "connected and subscribed, no data yet" if got_ack else "no data")
    except Exception as exc:
        return False, repr(exc)[:160]


async def run_check(cfg) -> int:
    fails = 0

    def line(ok, name, detail=""):
        nonlocal fails
        fails += 0 if ok else 1
        print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f": {detail}" if detail else ""))

    async with make_session(20) as session:
        rest = Rest(cfg.exchange.rest_base, session)
        try:
            t0 = time.time() * 1000
            srv = await rest.server_time()
            t1 = time.time() * 1000
            line(True, "REST server time", f"clock offset {((t0 + t1) / 2 - srv):+.0f} ms, rtt {t1 - t0:.0f} ms")
        except Exception as exc:
            line(False, "REST server time", repr(exc)[:160])
            print("Binance REST is unreachable from here (network or region block). Stopping.")
            return fails
        symbols = []
        try:
            info = await rest.exchange_info()
            symbols, excluded = filter_universe(info, cfg.universe)
            line(True, "Universe", f"{len(symbols)} crypto USDT perpetuals included")
            print(f"       excluded: {excluded}")
        except Exception as exc:
            line(False, "exchangeInfo", repr(exc)[:160])
        try:
            tick = await rest.ticker24h()
            big = sum(1 for t in tick if t["symbol"] in set(symbols) and float(t["quoteVolume"]) >= cfg.spike.big_tier_volume_usd)
            line(True, "24h tickers", f"{len(tick)} rows, {big} coins at or above the $1B tier (3% trigger)")
        except Exception as exc:
            line(False, "24h tickers", repr(exc)[:160])
        try:
            kl = await rest.klines("BTCUSDT", "15m", 97)
            line(len(kl) >= 90, "15m klines", f"{len(kl)} rows for BTCUSDT")
        except Exception as exc:
            line(False, "15m klines", repr(exc)[:160])

        ok, d = await _ws_probe(cfg.exchange.ws_market + "/ws/btcusdt@aggTrade", None)
        line(ok, "Market ws path /market (aggTrade)", d)
        ok, d = await _ws_probe(cfg.exchange.ws_market + "/ws", ["!ticker@arr"])
        line(ok, "Market ws all-coin ticker (!ticker@arr)", d + " (if this fails the app polls REST instead)")
        ok, d = await _ws_probe(cfg.exchange.ws_market + "/ws", ["!forceOrder@arr"], timeout=10)
        line(ok, "Market ws liquidations (!forceOrder@arr)", d + " (quiet minutes are normal)")
        ok, d = await _ws_probe(cfg.exchange.ws_public + "/ws/btcusdt@depth20@500ms", None)
        line(ok, "Public ws path /public (partial depth)", d)

        tok = cfg.alerts.telegram_token
        if tok:
            try:
                async with session.get(f"https://api.telegram.org/bot{tok}/getMe") as r:
                    data = await r.json(content_type=None)
                line(bool(data.get("ok")), "Telegram bot", data.get("result", {}).get("username", str(data)[:120]))
            except Exception as exc:
                line(False, "Telegram bot", repr(exc)[:160])
        else:
            print("[SKIP] Telegram: no token configured yet (practice mode works without it)")
    print("\nAll checks passed." if fails == 0 else f"\n{fails} check(s) failed.")
    return fails
