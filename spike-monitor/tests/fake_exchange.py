"""A scripted fake Binance for end-to-end tests.

REST: /fapi/v1/time, exchangeInfo, ticker/24hr, klines.
WS:   /market/ws/... (ticker, forceOrder, aggTrade) and /public/ws/... (depth).

Script (seconds after the streams start):
  BTCUSDT  flat.
  BBBUSDT  24h volume $2B (3% tier). Long liquidation $100k at 0.8s, one $600k buy at 1.0s,
           ramps to +3.5% by 2.0s (SPIKE at +3%), flat, then falls to -3.5% by 5.5s
           (VOLATILE upgrade). A $4M ask wall sits in the book from 1.5s to 4.0s.
  AAAUSDT  24h volume $300M (5% tier). Ramps to +4% (no alert), then +5.5% (one SPIKE).
"""

from __future__ import annotations

import asyncio
import json
import random
import time

import websockets
from aiohttp import web

IV = 900_000
PRICES0 = {"BTCUSDT": 60000.0, "BBBUSDT": 50.0, "AAAUSDT": 100.0}
QVOL = {"BTCUSDT": 5e9, "BBBUSDT": 2e9, "AAAUSDT": 3e8, "TSLAUSDT": 1e9, "XAUUSDT": 1e9}
SCRIPT_START_DELAY = 1.5
SCRIPT_LENGTH = 7.0


def exchange_info():
    def sym(name, ctype="PERPETUAL", utype="COIN", sub=None, status="TRADING"):
        return {"symbol": name, "contractType": ctype, "quoteAsset": "USDT", "status": status,
                "underlyingType": utype, "underlyingSubType": sub or []}
    return [sym("BTCUSDT"), sym("BBBUSDT"), sym("AAAUSDT"),
            sym("TSLAUSDT", utype="EQUITY"), sym("XAUUSDT", utype="COMMODITY", sub=["TradFi"]),
            sym("BTCUSDT_261225", ctype="CURRENT_QUARTER"), sym("OLDUSDT", status="SETTLING")]


def path_price(sym: str, t: float) -> float:
    p0 = PRICES0[sym]
    if sym == "BBBUSDT":
        if t < 0.5:
            return p0
        if t < 2.0:
            return p0 * (1 + 0.035 * (t - 0.5) / 1.5)
        if t < 3.5:
            return p0 * 1.035
        if t < 5.5:
            return p0 * (1.035 - 0.07 * (t - 3.5) / 2.0)
        return p0 * 0.965
    if sym == "AAAUSDT":
        if t < 0.5:
            return p0
        if t < 2.0:
            return p0 * (1 + 0.04 * (t - 0.5) / 1.5)
        if t < 3.0:
            return p0 * 1.04
        if t < 4.0:
            return p0 * (1.04 + 0.015 * (t - 3.0))
        return p0 * 1.055
    return p0


def make_rest_app() -> web.Application:
    routes = web.RouteTableDef()

    @routes.get("/fapi/v1/time")
    async def time_(req):
        return web.json_response({"serverTime": int(time.time() * 1000)})

    @routes.get("/fapi/v1/exchangeInfo")
    async def info(req):
        return web.json_response({"symbols": exchange_info()})

    @routes.get("/fapi/v1/ticker/24hr")
    async def ticker(req):
        rows = [{"symbol": s, "lastPrice": str(PRICES0.get(s, 10.0)), "quoteVolume": str(v),
                 "priceChangePercent": "1.0"} for s, v in QVOL.items()]
        return web.json_response(rows)

    @routes.get("/fapi/v1/klines")
    async def klines(req):
        sym = req.query["symbol"]
        limit = int(req.query["limit"])
        rng = random.Random(sym)
        cur = int(time.time() * 1000) // IV * IV
        p0 = PRICES0.get(sym, 10.0)
        out = []
        for k in range(limit - 1, -1, -1):
            o = p0
            c = o * (1 + rng.uniform(-0.003, 0.003)) if k else o
            out.append([cur - k * IV, str(o), str(max(o, c)), str(min(o, c)), str(c), "10",
                        cur - k * IV + IV - 1, "1000000", 100, "5", "500000", "0"])
        return web.json_response(out)

    app = web.Application()
    app.add_routes(routes)
    return app


async def ws_handler(ws):
    path = ws.request.path
    kind = "market" if path.startswith("/market/ws") else "public"
    initial = path.split("/ws/", 1)[1].split("/") if "/ws/" in path else []
    subs = set(initial)
    state = {"liq": False, "big": False, "wall_on": False}

    async def reader():
        async for raw in ws:
            m = json.loads(raw)
            if m.get("method") == "SUBSCRIBE":
                subs.update(m["params"])
                await ws.send(json.dumps({"result": None, "id": m["id"]}))
            elif m.get("method") == "UNSUBSCRIBE":
                subs.difference_update(m["params"])
                await ws.send(json.dumps({"result": None, "id": m["id"]}))

    async def market_tick(t, i):
        ms = int(time.time() * 1000)
        if i % 10 == 0 and "!ticker@arr" in subs:
            rows = [{"e": "24hrTicker", "s": s, "c": str(path_price(s, t)), "P": "1.0",
                     "q": str(QVOL[s])} for s in PRICES0]
            await ws.send(json.dumps(rows))
        if t >= 0.8 and not state["liq"]:
            state["liq"] = True
            await ws.send(json.dumps({"e": "forceOrder", "E": ms, "o": {
                "s": "BBBUSDT", "S": "SELL", "q": "2000", "p": "50", "ap": "50", "z": "2000",
                "X": "FILLED", "T": ms}}))
        for s in PRICES0:
            if f"{s.lower()}@aggTrade" not in subs:
                continue
            price = path_price(s, t)
            qty = 0.01 if s == "BTCUSDT" else 2000.0 / price
            sell = bool(i % 2)
            if s == "BBBUSDT" and t >= 1.0 and not state["big"]:
                state["big"] = True
                qty, sell = 600_000.0 / price, False
            await ws.send(json.dumps({"e": "aggTrade", "E": ms, "s": s, "a": i, "p": f"{price:.6f}",
                                      "q": f"{qty:.6f}", "f": i, "l": i, "T": ms, "m": sell}))

    async def depth_tick(t, i):
        ms = int(time.time() * 1000)
        for stream in list(subs):
            sym = stream.split("@")[0].upper()
            if sym not in PRICES0:
                continue
            p = PRICES0[sym]
            bids = [[f"{p - 0.02 * (k + 1):.2f}", f"{20000.0 / p:.4f}"] for k in range(20)]
            asks = [[f"{p + 0.02 * (k + 1):.2f}", f"{20000.0 / p:.4f}"] for k in range(20)]
            if sym == "BBBUSDT" and 1.5 <= t < 4.0:
                price = asks[5][0]
                asks[5] = [price, f"{4_000_000.0 / float(price):.4f}"]
            await ws.send(json.dumps({"e": "depthUpdate", "E": ms, "T": ms, "s": sym,
                                      "U": i, "u": i, "pu": i - 1, "b": bids, "a": asks}))

    async def driver():
        await asyncio.sleep(SCRIPT_START_DELAY)
        t0 = time.time()
        i = 0
        step = 0.05 if kind == "market" else 0.25
        while time.time() - t0 < SCRIPT_LENGTH:
            t = time.time() - t0
            await (market_tick(t, i) if kind == "market" else depth_tick(t, i))
            i += 1
            await asyncio.sleep(step)
        await asyncio.sleep(60)

    try:
        await asyncio.gather(reader(), driver())
    except websockets.ConnectionClosed:
        pass


async def start_fake():
    runner = web.AppRunner(make_rest_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    rest_port = site._server.sockets[0].getsockname()[1]
    server = await websockets.serve(ws_handler, "127.0.0.1", 0)
    ws_port = server.sockets[0].getsockname()[1]
    return runner, server, rest_port, ws_port
