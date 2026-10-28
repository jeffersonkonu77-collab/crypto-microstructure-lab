#!/usr/bin/env python3
"""Binance USDT-M futures L2 imbalance scanner.
Install: pip install textual websockets aiohttp   |   Run: python fetcher.py
Keys: s/1/5 = CVD window | b = band % (type a whole number 1-10) | o = sort | Enter = coin details | d = open a coin | x = clear ended Triggered rows | Tab = switch table | o/r/u = sort / reverse / reset sort of the focused table | c = copy row | q = quit
"""
import asyncio, csv, os, sqlite3, json, time, collections, itertools, traceback, socket, threading, urllib.parse, urllib.request, aiohttp, websockets
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import App
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Static, Footer, Input, OptionList
from textual.widgets.option_list import Option

# ---------- DNS workaround (copied from your cross_scanner.py idea, extended) ----------
# Some ISPs (e.g. in Nigeria) block DNS lookups for Binance, giving "getaddrinfo failed".
# We resolve *.binance.com ourselves through DNS-over-HTTPS (travels to the IP 1.1.1.1 / 8.8.8.8, so ISP DNS
# filtering never sees it) and hand the IPs to Python's socket layer. The hostname is still used for TLS/Host,
# so Binance sees a normal connection. Everything else uses the normal system DNS. If DoH itself fails we fall
# back to the system resolver. This fixes DNS-level blocking only; if connections still get reset, use a VPN/server.
DNS_HOSTS = ("binance.com",)
DNS_TTL = 300
DOH = [("https://1.1.1.1/dns-query", {"accept": "application/dns-json"}),
       ("https://8.8.8.8/resolve", {"accept": "application/dns-json"})]
DNS = {"ok": None, "last": ""}
_dns_cache = {}; _dns_lock = threading.Lock(); _orig_gai = socket.getaddrinfo


def _doh(host):
    err = None
    for url, hdr in DOH:
        try:
            req = urllib.request.Request(url + "?" + urllib.parse.urlencode({"name": host, "type": "A"}), headers=hdr)
            with urllib.request.urlopen(req, timeout=5) as r: j = json.load(r)
            ips = [a["data"] for a in j.get("Answer", []) if a.get("type") == 1]
            if ips: return ips
        except Exception as e:
            err = e
    raise RuntimeError(f"DoH failed for {host}: {err}")


def _gai(host, port, *a, **k):
    if isinstance(host, str) and host.endswith(DNS_HOSTS):
        with _dns_lock:
            ent = _dns_cache.get(host)
            if not ent or time.time() - ent[0] > DNS_TTL:
                try: ent = _dns_cache[host] = (time.time(), _doh(host)); DNS["ok"] = True; DNS["last"] = f"{host} -> {len(ent[1])} IPs"
                except Exception as e: DNS["ok"] = bool(ent); DNS["last"] = str(e)[:80]      # keep stale IPs if we have them
        if ent:
            out = []
            for ip in ent[1]:
                try: out += _orig_gai(ip, port, *a, **k)
                except OSError: pass
            if out: return out
    return _orig_gai(host, port, *a, **k)


socket.getaddrinfo = _gai


# ---------- config ----------
MIN_VOL = 3_000_000          # min 24h USDT volume
TOP_N = 20
LIVE_EXTRA = 8               # live-streamed coins beyond the visible table (buffer so rows do not flap)
DEMOTE_AFTER = 60            # seconds a coin must stay out of the table before its live stream is dropped
DEPTH_MS = 500               # 100 / 250 / 500 (500 = half the CPU load of 250)
BAND = [1]                   # chosen band in whole percent (1-10). Drives Bids$/Asks$/ratios, walls and the streak. Change with the b key
BAND_GEN = [0]               # bumped on every band change so walls and streaks restart cleanly
DET_BANDS = (0.001, 0.0025, 0.005)   # extra fixed bands (10 / 25 / 50 bps of price) shown on the detail screen
NB = 1 + len(DET_BANDS)      # band slot 0 = chosen band, slots 1.. = DET_BANDS
THRESH, GRACE = 1.5, 10      # streak: ratio threshold, seconds of grace before reset
WALL_MIN, WALL_X, YOUNG = 250_000, 8, 60   # wall = >=$250k and >=8x avg level; "young" (<60s) walls excluded from clean ratio
CANCEL_MIN = 25_000          # min $ to log as a "last cancel"
PX_MIN, OI_MIN = 0.0005, 0.002   # min price / OI move (over the OI window) to label positioning
WARM = 240                   # seconds before NEW tags appear
# --- drop-from-peak watchlist ---
POOL_N = 30                  # pool = the N coins (above MIN_VOL) with the biggest 24h peak gain (24h high vs price 24h ago)
DROP_WATCH, DROP_EXIT = 0.03, 0.0299        # Watchlist: enters 3% below the 24h high, leaves once back above -2.99%
DROP_TRIG, DROP_TRIG_EXIT = 0.05, 0.0499    # Triggered: 5%+ below the high AND asks > bids AND sells > buys
DROP_MAX = 0.15              # Watchlist only shows coins at most this far below the high (Triggered has no cap)
HOLD = 10                    # seconds a condition must stay broken before a coin leaves a list (stops flicker)
STALL_MIN, STALL_NEAR, STALL_RANGE = 3600, 0.005, 0.01   # stall: 60m+ inside a 1% range, price within 0.5% of the 24h high
KL_REFRESH = 15              # seconds between 1m candle refreshes per pool coin
STALE_S = 60                 # a live row with no depth update for this long is treated as stale
BIG_TRADE = 50_000           # trades at or above this many dollars are highlighted in the detail tape
HORIZ = (60, 300, 900, 3600) # seconds after a list entry at which the outcome is logged
EV_LOG, OUT_LOG = "watchlist_events.csv", "watchlist_outcomes.csv"
DB_FILE = "watchlist.db"      # SQLite copy of the same events and outcomes (tables: events, outcomes)
REST = "https://fapi.binance.com"
WS = {"pub": "wss://fstream.binance.com/public/ws", "mkt": "wss://fstream.binance.com/market/ws"}
MODES = ("clean ratio", "|CVD|", "streak time", "24h volume")

def dstream(n): return n.lower() + "@depth" + ("" if DEPTH_MS == 250 else f"@{DEPTH_MS}ms")


SY, W = {}, {}
STAT = {"n": 0, "fail": 0, "t": "", "e": "", "last": "", "down": {}, "rec": 0, "rcoins": 0, "hist": collections.deque(maxlen=4)}   # websocket reconnect diagnostics
LAST = {"pub": 0.0, "mkt": 0.0}; CNT = {"pub": 0, "mkt": 0}; LAG = [0.0]; TOT = {"msgs": 0, "bytes": 0}     # last message time per websocket
WANT = set(); NEXT = [0.0]
PRI = set()                         # pinned + visible coins: re-synced first, refreshed every second
SEQ = itertools.count(); ID = itertools.count()
POOL = []; POOLSET = set()          # watchlist pool (ordered by peak gain) and the same names as a set
KEEP = set()                        # coins that must stay live: on a list, with pending outcomes, or open in the detail screen
KLREQ = {}                          # coins whose candles the detail screen wants {name: last request time}
DET = [None]                        # coin currently open in the detail screen


def log(msg):
    try:
        with open("fetcher.log", "a", encoding="utf-8") as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + msg + "\n")
    except Exception:
        pass


def enq(x): SQ.put_nowait((0 if x.n in PRI else 1, next(SEQ), x))
SQ = LK = HS = None


class S:
    def __init__(s, n):
        s.n = n; s.id = next(ID); s.bids = {}; s.asks = {}; s.u = 0
        s.lev = s.dead = 0.0; s.mode = "scan"; s.snap_t = 0.0; s.stale = False; s.ok = s.fresh = s.live = s.subd = False; s.syncing = True; s.buf = []
        s.tq = collections.defaultdict(float); s.tdq = collections.deque(); s.pend = collections.deque()
        s.buck = {}; s.cb = s.cs = s.b0 = s.s0 = 0.0
        s.last_a = 0; s.gapfill = 0.0; s.gap_lo = 0; s.tbuf = []; s.gaptr = collections.defaultdict(float)
        s.tinfo = s.binfo = s.gapinfo = None; s.gdrops = []
        s.dir = 0; s.t0 = s.last = 0.0
        s.walls = {}; s.cx = collections.defaultdict(float)
        s.spoof = collections.deque(maxlen=50); s.sp = None; s.lc = None
        s.vol = s.chg = s.mid = 0.0; s.oih = collections.deque(maxlen=200)   # (time, OI, price); windows are cut by timestamp, see oiref()
        s.raw = [[0] * NB, [0] * NB]; s.cln = [[0] * NB, [0] * NB]; s.bm = [(0, 0), (0, 0)]
        s.bgen = -1; s.cov = 0.0; s.bb = s.ba = 0.0          # band generation seen, how far the book reaches (fraction of price), best bid / ask
        s.hi = s.op = s.pgain = 0.0                           # 24h high, price 24h ago, peak gain (hi / op - 1)
        s.kl = collections.deque(maxlen=1500); s.kl_t = 0.0; s.klf = False    # 1m candles (open ms, high, low, close)
        s.pk = s.pkt = s.stall = 0.0; s.t3 = s.t5 = None      # candle based 24h peak, its time, stall seconds, seconds from peak to -3% / -5%
        s.fail = {}; s.in_drop = s.in_trig = s.in_stall = False; s.state = ""; s.state_t = 0.0
        s.liq = collections.deque(maxlen=500); s.tape = collections.deque(maxlen=40)
        s.fund = s.mark = s.index = 0.0


# ---------- REST (global pacing ~<=2000 weight/min) ----------
async def rest(path, params, w):
    for _ in range(3):
        async with LK:                       # reserve a time slot (global pacing ~1700 weight/min max)
            t = max(NEXT[0], time.time()); NEXT[0] = t + 0.035 * w
        if t > time.time(): await asyncio.sleep(t - time.time())
        async with HS.get(REST + path, params=params) as r:
            if r.status in (418, 429):
                ra = min(int(r.headers.get("Retry-After", 10)), 300); log(f"REST {r.status} on {path}, sleeping {ra}s")
                NEXT[0] = max(NEXT[0], time.time() + ra); await asyncio.sleep(ra); continue
            used = int(r.headers.get("x-mbx-used-weight-1m", 0)); j = await r.json(content_type=None)
        if used > 2000: NEXT[0] = max(NEXT[0], time.time() + 8)
        return j
    raise RuntimeError("rate limited")


async def sub(key, streams, m="SUBSCRIBE"):
    ws = W.get(key)
    if not ws: return False
    try:
        for i in range(0, len(streams), 50):
            await ws.send(json.dumps({"method": m, "params": streams[i:i + 50], "id": i + 1}))
            if i + 50 < len(streams): await asyncio.sleep(0.2)
        return True
    except Exception:
        return False


# ---------- book / trade handling ----------
def resync(x):
    x.dead = x.dead or time.time(); x.ok = False; x.stale = x.live; x.syncing = True; x.buf = []; x.pend.clear()
    if x.mode == "live": enq(x)


def apply(x, d, rec=True):
    now = time.time()
    for sd, book, k in ((1, x.bids, "b"), (2, x.asks, "a")):
        for p, q in d[k]:
            p = float(p); q = float(q); old = book.get(p, 0)
            if q: book[p] = q
            else: book.pop(p, None)
            if rec and q < old and (old - q) * p >= 2000: x.pend.append((now, sd, p, old - q))
    x.u = d["u"]


def on_depth(d):
    x = SY.get(d.get("s"))
    if not x or x.mode != "live": return
    if not x.ok:
        if x.syncing: x.buf.append(d)
        return
    if x.fresh:
        if d["u"] < x.u: return
        if d["U"] > x.u: resync(x); return      # first event must straddle the snapshot id, otherwise updates were missed
        x.fresh = False
    elif d["pu"] != x.u:
        resync(x); return
    apply(x, d); x.lev = time.time()


def count(x, d, hist=False):
    """Add one trade to totals / time buckets. hist=True = recovered after a gap (not used for fill matching)."""
    p = float(d["p"]); q = float(d["q"]); u = p * q
    sec = int(d["T"] / 1000) if "T" in d else int(time.time())
    if hist: x.gaptr[p] += q
    else: now = time.time(); x.tdq.append((now, p, q)); x.tq[p] += q; x.tape.append((now, p, u, "S" if d["m"] else "B"))
    b, s_ = (0, u) if d["m"] else (u, 0)   # m=True -> buyer is maker -> taker SELL
    x.cb += b; x.cs += s_
    bk = x.buck.get(sec)
    if bk is None: bk = x.buck[sec] = [0, 0]
    bk[0] += b; bk[1] += s_
    if d["a"] > x.last_a: x.last_a = d["a"]
    return b, s_


def on_liq(d):
    """Forced liquidation order (Binance sends at most one per coin per second, so totals are a minimum)."""
    o = d.get("o") or {}; x = SY.get(o.get("s"))
    if not x: return
    try:
        p = float(o.get("ap") or o["p"]); q = float(o.get("z") or o["q"])
    except Exception:
        return
    x.liq.append((time.time(), "L" if o.get("S") == "SELL" else "S", p * q, p))   # SELL = a long got liquidated


def on_trade(d):
    if d.get("e") == "forceOrder": on_liq(d); return
    x = SY.get(d.get("s"))
    if not x or x.mode != "live": return
    if x.gapfill: x.tbuf.append(d); return      # recovering a gap: hold live trades so order/dedupe stay exact
    count(x, d)


def settle(x, now):
    """Split book decreases into fills (matched by trades) vs estimated cancels."""
    while x.tdq and now - x.tdq[0][0] > 8:
        _, p, q = x.tdq.popleft(); v = x.tq[p] - q
        if v > 1e-12: x.tq[p] = v
        else: x.tq.pop(p, None)
    for k in [k for k in x.buck if k < now - 330]: del x.buck[k]
    pd = x.pend
    while pd and now - pd[0][0] >= 0.5:
        t, sd, p, c = pd.popleft(); f = min(c, x.tq.get(p, 0))
        if f: x.tq[p] -= f
        cn = (c - f) * p
        if cn >= 1000:
            x.cx[(sd, p)] += cn
            if cn >= CANCEL_MIN: x.lc = (t, cn, p, sd)


def metrics(x, now):
    settle(x, now)
    if not x.ok: return                      # resyncing: keep showing last known values (marked ~)
    if not x.bids or not x.asks: x.live = False; return
    bb = max(x.bids); ba = min(x.asks)
    if bb >= ba: resync(x); return
    if x.bgen != BAND_GEN[0]:                # band changed: walls were measured on the old band, so restart them and the streak
        x.bgen = BAND_GEN[0]; x.walls.clear(); x.cx.clear(); x.dir = 0; x.t0 = x.last = 0.0
    mid = (bb + ba) / 2; bl = (BAND[0] / 100.0,) + DET_BANDS; lim = max(bl); x.mid = mid; x.bb, x.ba = bb, ba
    x.cov = min(mid - min(x.bids), max(x.asks) - mid) / mid     # how far from price the book really reaches (snapshot holds 500 levels)
    L = [(1, [(p, p * q) for p, q in x.bids.items() if mid - p <= lim]),
         (2, [(p, p * q) for p, q in x.asks.items() if p - mid <= lim])]
    tot = sum(u for _, l in L for _, u in l); n = sum(len(l) for _, l in L)
    thr = max(WALL_MIN, WALL_X * tot / max(n, 1))
    raw = [[0] * NB, [0] * NB]; cln = [[0] * NB, [0] * NB]; seen = set(); bm = [(0, 0), (0, 0)]
    for i, (sd, l) in enumerate(L):
        for p, u in l:
            young = False; dd = abs(p - mid) / mid
            if u > bm[i][0]: bm[i] = (u, p)
            if u >= thr:
                k = (sd, p); w = x.walls.get(k)
                if w is None: w = x.walls[k] = [now, u, dd, dd]   # [first seen, peak $, dist at birth, latest dist]
                else: w[1] = max(w[1], u); w[3] = dd
                seen.add(k); young = now - w[0] < YOUNG
            for j, b in enumerate(bl):
                if dd <= b:
                    raw[i][j] += u
                    if not young: cln[i][j] += u
    for k in [k for k in x.walls if k not in seen]:      # wall vanished: pulled (spoof) or filled?
        w = x.walls.pop(k); c = x.cx.pop(k, 0)
        approached = w[3] < 0.6 * w[2] or w[3] < 0.003   # price closed in on the wall (or sat within 0.3%)
        if now - w[0] < 120 and c >= 0.6 * w[1] and approached: x.spoof.append(now); x.sp = (now, w[1], k[1])
    if len(x.cx) > 3000: x.cx.clear()
    x.raw, x.cln, x.bm, x.live, x.stale = raw, cln, bm, True, False
    rs = (cln[0][0] + 1) / (cln[1][0] + 1); st = max(rs, 1 / rs)   # streak follows the chosen band (clean ratio)
    d = (1 if rs > 1 else -1) if st >= THRESH else 0
    if d and d != x.dir: x.dir, x.t0, x.last, x.b0, x.s0 = d, now, now, x.cb, x.cs
    elif d: x.last = now
    elif x.dir and now - x.last > GRACE: x.dir = 0


def flow(x, w, now):
    if x.mode != "live": return None
    if w == "s":
        return (x.cb - x.b0, x.cs - x.s0) if x.dir else None
    cut = now - w; b = s = 0
    for sec, (bb, ss) in x.buck.items():
        if sec >= cut: b += bb; s += ss
    return b, s


# ---------- background tasks ----------
def subscribed(key):
    if key == "pub": return any(x.mode == "live" and x.subd for x in SY.values())
    return any(x.mode == "live" for x in SY.values())


async def watchdog(key, ws):
    while True:
        await asyncio.sleep(10)
        if not subscribed(key): LAST[key] = time.time(); continue     # nothing requested = silence is normal
        if time.time() - LAST[key] > 40:
            log(f"{key} silent for 40s, reconnecting"); await ws.close(); return


async def ws_loop(key, handler):
    errs = fails = 0
    while True:
        err = "connection closed"; wd = None; t_conn = time.time(); CNT[key] = 0; up_ok = False
        try:
            async with websockets.connect(WS[key], max_size=None, ping_interval=None, max_queue=None,
                                          compression=None) as ws:
                W[key] = ws; t_conn = LAST[key] = time.time(); wd = asyncio.create_task(watchdog(key, ws)); up_ok = True
                if STAT["down"].get(key):
                    gs = time.time() - STAT["down"].pop(key); log(f"{key} reconnected after {gs:.0f}s offline")
                    STAT["last"] = f"{time.strftime('%H:%M:%S')} {key} back after {gs:.0f}s"
                if key == "pub":
                    for x in list(SY.values()):
                        if x.mode == "live": x.subd = False; resync(x)
                else:
                    await sub("mkt", [f"{n.lower()}@aggTrade" for n, x in SY.items() if x.mode == "live"] + ["!forceOrder@arr"])
                async for m in ws:
                    LAST[key] = time.time(); fails = 0; CNT[key] += 1; TOT["msgs"] += 1; TOT["bytes"] += len(m)
                    try:
                        d = json.loads(m)
                        if "e" in d: handler(d)
                    except Exception:
                        errs += 1
                        if errs <= 20: log(f"{key} handler error: {traceback.format_exc()[-400:]}")
        except Exception as e:
            err = f"{type(e).__name__} {str(e)[:60]}"
        if wd: wd.cancel()
        W[key] = None
        up = time.time() - t_conn; n = CNT[key]
        if up_ok:                                   # a real drop of an established connection
            STAT["n"] += 1; STAT["t"] = time.strftime("%H:%M:%S"); STAT["e"] = f"{key}: {err}"; STAT["hist"].append(f"{STAT['t']} {key}")
            STAT["down"][key] = time.time()
            if key == "mkt":                        # remember where each coin's trades stopped so we can backfill
                for x in SY.values():
                    if x.mode == "live" and x.last_a and not x.gapfill: x.gapfill = time.time(); x.gap_lo = x.last_a + 1; x.tbuf = []; x.gaptr.clear()
            else:
                for x in SY.values():
                    if x.mode == "live": x.dead = x.dead or time.time()
        else:
            STAT["fail"] += 1; STAT["down"].setdefault(key, time.time())     # could not connect (e.g. no internet / DNS)
        log(f"{key} disconnected: {err} | up {up:.0f}s, {n} msgs ({n/max(up,1):.0f}/s), worst loop lag {LAG[0]:.1f}s"); LAG[0] = 0.0
        fails += 1; await asyncio.sleep(min(3 * fails, 30))


def promote(x):
    x.mode = "live"; x.ok = False; x.stale = x.live; x.syncing = True; x.buf = []; x.subd = False
    x.b0, x.s0 = x.cb, x.cs; enq(x)          # CVD-since-streak counts from when live trades start


async def demote(xs):
    for x in xs:
        x.mode = "scan"; x.ok = bool(x.bids); x.syncing = False; x.subd = False; x.buf = []; x.stale = False
        x.snap_t = x.lev or 0.0
    await sub("pub", [dstream(x.n) for x in xs], "UNSUBSCRIBE")
    await sub("mkt", [f"{x.n.lower()}@aggTrade" for x in xs], "UNSUBSCRIBE")


async def live_manager():
    """Only the coins on screen (+pinned +buffer) get live streams; everything else is scanned by REST."""
    out = {}
    while True:
        await asyncio.sleep(2); now = time.time(); want = set(WANT) | POOLSET | KEEP
        up = [SY[n] for n in want if n in SY and SY[n].mode != "live"][:8]
        for x in up: promote(x)
        if up: await sub("mkt", [f"{x.n.lower()}@aggTrade" for x in up])
        down = []
        for x in list(SY.values()):
            if x.mode != "live": continue
            if x.n in want: out.pop(x.n, None)
            elif now - out.setdefault(x.n, now) > DEMOTE_AFTER: down.append(x); out.pop(x.n, None)
        if down: await demote(down)


async def sweeper():
    """Round-robin REST snapshots of every non-live coin, stalest first."""
    sem = asyncio.Semaphore(4)

    async def one(x):
        async with sem:
            try:
                j = await rest("/fapi/v1/depth", {"symbol": x.n, "limit": 500}, 10)
                if x.mode != "scan" or SY.get(x.n) is not x: return
                x.bids = {float(p): float(q) for p, q in j["bids"]}; x.asks = {float(p): float(q) for p, q in j["asks"]}
                x.ok = True; x.snap_t = x.lev = time.time(); metrics(x, x.snap_t)
            except Exception:
                x.snap_t = time.time() - 60

    while True:
        cand = sorted((x for x in SY.values() if x.mode == "scan"), key=lambda x: x.snap_t)[:8]
        if not cand: await asyncio.sleep(1); continue
        await asyncio.gather(*[one(x) for x in cand])


def try_finish(x):
    """Build the 'what happened during the gap' summary once both the trade backfill and book resync are done."""
    if x.gapfill or x.dead or not (x.tinfo or x.binfo): return
    parts = []; now = time.time()
    if x.tinfo:
        t0, secs, n, bu, se, ok = x.tinfo
        parts.append(f"{secs:.0f}s trade gap: +${M(bu)} buys / ${M(se)} sells recovered ({n} trades)" + ("" if ok else " PARTIAL"))
    if x.binfo:
        _, secs, added = x.binfo; canc = fill = 0.0; best = None
        for sd, p, qty in x.gdrops:
            f = min(qty, x.gaptr.get(p, 0.0)) if x.tinfo else 0.0
            c = (qty - f) * p; canc += c; fill += f * p
            if c >= CANCEL_MIN and (best is None or c > best[1]): best = (now, c, p, sd)
        if best: x.lc = best
        if x.tinfo: parts.append(f"book: {len(x.gdrops)} big levels gone (cancel ~${M(canc)}, filled ~${M(fill)}), new ${M(added)}")
        else: parts.append(f"book resynced after {secs:.0f}s: {len(x.gdrops)} big levels gone (${M(canc)}), new ${M(added)}")
    x.gapinfo = (now, " | ".join(parts)); log(f"{x.n} GAP {x.gapinfo[1]}")
    x.tinfo = x.binfo = None; x.gdrops = []; x.gaptr.clear()


async def trade_backfill():
    """After the trade stream reconnects, fetch the trades each coin missed (by aggregate id) and add them back."""
    while True:
        await asyncio.sleep(1)
        if not W.get("mkt"): continue
        todo = sorted((x for x in SY.values() if x.gapfill and x.mode == "live"), key=lambda x: (x.n not in PRI, x.gapfill))
        for x in todo[:4]:
            try:
                t0 = x.gapfill; n = 0; bu = se = 0.0; ok = True; lo = x.gap_lo
                for _ in range(5):
                    j = await rest("/fapi/v1/aggTrades", {"symbol": x.n, "fromId": lo, "limit": 1000}, 20)
                    if not isinstance(j, list): ok = False; break
                    for t in j:
                        if t["a"] > x.last_a:
                            b, s_ = count(x, t, hist=True); bu += b; se += s_; n += 1
                    if len(j) < 1000: break
                    lo = j[-1]["a"] + 1
                else: ok = False
                for d in x.tbuf:                                    # now release the held live trades (deduped by id)
                    if d["a"] > x.last_a: count(x, d)
                x.tbuf = []; x.gapfill = 0.0
                x.tinfo = (t0, time.time() - t0, n, bu, se, ok); STAT["rec"] += n; STAT["rcoins"] += 1
                try_finish(x)
            except Exception:
                if time.time() - x.gapfill > 90:                    # give up: release held trades, mark unrecovered
                    for d in x.tbuf:
                        if d["a"] > x.last_a: count(x, d)
                    x.tbuf = []; x.gapfill = 0.0; x.tinfo = (0, 0, 0, 0.0, 0.0, False); log(f"{x.n} trade gap NOT recovered"); try_finish(x)


async def lagmon():
    while True:
        t = time.time(); await asyncio.sleep(0.5); lag = time.time() - t - 0.5
        LAG[0] = max(LAG[0], lag)
        if lag > 2: log(f"event loop lagged {lag:.1f}s (program frozen/overloaded)")


async def supervise(fn, *a):
    while True:
        try: await fn(*a)
        except asyncio.CancelledError: raise
        except Exception: log(f"task {fn.__name__} died: {traceback.format_exc()[-400:]}"); await asyncio.sleep(2)


async def snap_worker():
    while True:
        _, _, x = await SQ.get()
        if SY.get(x.n) is not x or x.ok or x.mode != "live": continue
        try:
            if not x.subd:
                if not await sub("pub", [dstream(x.n)]):
                    await asyncio.sleep(2); enq(x); continue
                x.subd = True; await asyncio.sleep(0.5)
            j = await rest("/fapi/v1/depth", {"symbol": x.n, "limit": 500}, 10)
            nb = {float(p): float(q) for p, q in j["bids"]}; na = {float(p): float(q) for p, q in j["asks"]}
            if x.dead and (x.bids or x.asks):                    # book was blind: record what changed
                drops = []; added = 0.0
                for sd, old, new in ((1, x.bids, nb), (2, x.asks, na)):
                    for p, q in old.items():
                        nq = new.get(p, 0.0)
                        if nq < q and (q - nq) * p >= CANCEL_MIN: drops.append((sd, p, q - nq))
                    for p, q in new.items():
                        oq = old.get(p, 0.0)
                        if q > oq and (q - oq) * p >= CANCEL_MIN: added += (q - oq) * p
                x.gdrops = drops; x.binfo = (time.time(), time.time() - x.dead, added)
            x.bids, x.asks = nb, na; x.dead = 0.0
            lid = j["lastUpdateId"]; x.u = lid; first = True; x.fresh = True
            for e in x.buf:
                if e["u"] < lid: continue
                if first and e["U"] > lid: raise ValueError("missed updates before the first event")
                if not first and e["pu"] != x.u: raise ValueError("gap")
                first = False; x.fresh = False; apply(x, e, rec=False)
            x.buf = []; x.ok = True; x.lev = time.time(); try_finish(x)
        except Exception:
            await asyncio.sleep(1); x.buf = []; enq(x)


async def universe():
    perps = None
    while True:
        try:
            if perps is None:
                info = await rest("/fapi/v1/exchangeInfo", {}, 1)
                perps = {s["symbol"] for s in info["symbols"] if s["contractType"] == "PERPETUAL"
                         and s["status"] == "TRADING" and s["quoteAsset"] == "USDT"}
            tk = await rest("/fapi/v1/ticker/24hr", {}, 40); new = []
            for t in tk:
                n = t["symbol"]
                if n not in perps: continue
                v = float(t["quoteVolume"]); c = float(t["priceChangePercent"]); x = SY.get(n)
                hi = float(t["highPrice"]); op = float(t["openPrice"])
                if x: x.vol, x.chg, x.hi, x.op = v, c, hi, op
                elif v >= MIN_VOL: new.append((v, n, c, hi, op))
            for v, n, c, hi, op in sorted(new, reverse=True):
                x = SY[n] = S(n); x.vol, x.chg, x.hi, x.op = v, c, hi, op
            for n, x in list(SY.items()):
                if x.vol < MIN_VOL * 0.8:
                    del SY[n]
                    if x.mode == "live":
                        await sub("pub", [dstream(n)], "UNSUBSCRIBE"); await sub("mkt", [f"{n.lower()}@aggTrade"], "UNSUBSCRIBE")
            # pool: the biggest 24h peak gain (24h high vs the price 24h ago) among coins that pass the volume rule
            for x in SY.values(): x.pgain = x.hi / x.op - 1 if x.op else 0.0
            rank = sorted((x for x in SY.values() if x.vol >= MIN_VOL), key=lambda x: x.pgain, reverse=True)[:POOL_N]
            POOL[:] = [x.n for x in rank]; POOLSET.clear(); POOLSET.update(POOL)
            try:                                  # funding rate and mark / index price for every coin in one call
                for t in await rest("/fapi/v1/premiumIndex", {}, 10):
                    x = SY.get(t["symbol"])
                    if x: x.fund = float(t.get("lastFundingRate") or 0); x.mark = float(t["markPrice"]); x.index = float(t["indexPrice"])
            except Exception:
                pass
        except Exception:
            log("universe error: " + traceback.format_exc()[-300:])
        await asyncio.sleep(30)


async def oi_loop():
    while True:
        for x in list(SY.values()):
            if SY.get(x.n) is not x or not x.mid: continue   # need a live price to pair with OI
            try:
                j = await rest("/fapi/v1/openInterest", {"symbol": x.n}, 1)
                x.oih.append((time.time(), float(j["openInterest"]), x.mid))
            except Exception:
                pass
        await asyncio.sleep(1)


# ---------- drop-from-peak watchlist logic ----------
def ago(s):
    s = int(max(s, 0))
    if s < 60: return f"{s}s"
    if s < 3600: return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def fmt_t(v): return "<1m" if v < 60 else ago(v)


def pstate(x, now):
    """Where price sits versus the 24h high right now: (peak price, peak time or 0, drop fraction), or None."""
    pk, pkt = x.pk, x.pkt
    if not pk: pk, pkt = x.hi, 0.0            # candles not loaded yet: use the ticker's 24h high (time unknown)
    if not pk or not x.mid: return None
    if x.mid > pk: pk, pkt = x.mid, now       # a fresh high made after the last candle refresh
    return pk, pkt, max(0.0, 1 - x.mid / pk)


def weff(x, win):
    """CVD window actually used for conditions: 'streak' falls back to 5m when no streak is active."""
    return 300 if win == "s" and not x.dir else win


def conds(x, now, win):
    ps = pstate(x, now); fl = flow(x, weff(x, win), now)
    return ps, fl, x.raw[1][0] > x.raw[0][0], fl is not None and fl[1] > fl[0]     # asks > bids, sells > buys


def drop_of(x, now):
    ps = pstate(x, now)
    return ps[2] if ps else 0.0


def liq_sum(x, now, w):
    l = s = 0.0
    for t, sd, u, p in x.liq:
        if now - t <= w:
            if sd == "L": l += u
            else: s += u
    return l, s


def tt(x, f, drop, now, pkt):
    """Seconds the coin took to fall f% from its 24h high (from candles; falls back to time since peak if the drop is fresh)."""
    v = x.t3 if f == 3 else x.t5
    if pkt and x.pkt and pkt != x.pkt: v = None          # a newer high was made, the old timing no longer applies
    if v is None and drop >= f / 100.0 and pkt: v = max(0.0, now - pkt)
    return v


def kl_calc(x, now):
    """From 1m candles: 24h peak and when it happened, time from peak to -3% / -5%, and how long price has stalled."""
    cut = (now - 86400) * 1000
    kl = [c for c in x.kl if c[0] >= cut]
    if not kl: return
    pi = 0; ph = kl[0][1]
    for i, c in enumerate(kl):
        if c[1] >= ph: ph, pi = c[1], i
    x.pk = ph; x.pkt = kl[pi][0] / 1000.0

    def first_below(f):
        lvl = ph * (1 - f)
        for j in range(pi, len(kl)):
            if kl[j][2] <= lvl: return (kl[j][0] - kl[pi][0]) / 1000.0
        return None
    x.t3 = first_below(0.03); x.t5 = first_below(0.05)
    hi = lo = first = None
    for c in reversed(kl):                    # walk back while the whole span stays inside STALL_RANGE
        nh = c[1] if hi is None else max(hi, c[1]); nl = c[2] if lo is None else min(lo, c[2])
        if nl <= 0 or nh / nl - 1 > STALL_RANGE: break
        hi, lo, first = nh, nl, c[0]
    x.stall = ((kl[-1][0] - first) / 1000.0 + 60) if first is not None and hi >= ph * (1 - STALL_NEAR) else 0.0


async def kline_loop():
    """1m candles for pool coins (and any coin on a list or open in the detail screen). Full 24h once, then small top-ups."""
    sem = asyncio.Semaphore(3)

    async def one(x):
        async with sem:
            try:
                gap = time.time() * 1000 - x.kl[-1][0] if x.kl else 1e18
                if not x.klf or gap > 15 * 60 * 1000:
                    j = await rest("/fapi/v1/klines", {"symbol": x.n, "interval": "1m", "limit": 1500}, 10)
                else:
                    j = await rest("/fapi/v1/klines", {"symbol": x.n, "interval": "1m", "startTime": int(x.kl[-1][0]), "limit": 20}, 1)
                if not isinstance(j, list): raise ValueError("bad klines reply")
                for k in j:
                    c = (int(k[0]), float(k[2]), float(k[3]), float(k[4]))      # open time ms, high, low, close
                    if x.kl and x.kl[-1][0] == c[0]: x.kl[-1] = c
                    elif not x.kl or c[0] > x.kl[-1][0]: x.kl.append(c)
                x.klf = True; x.kl_t = time.time(); kl_calc(x, x.kl_t)
            except Exception:
                x.kl_t = time.time() - KL_REFRESH + 10                           # retry in 10s

    while True:
        now = time.time(); want = []
        for n in list(POOL) + list(KEEP) + [n for n, t in list(KLREQ.items()) if now - t < 10]:
            x = SY.get(n)
            if x is not None and x not in want: want.append(x)
        todo = sorted((x for x in want if now - x.kl_t >= KL_REFRESH), key=lambda x: x.kl_t)[:10]
        if todo: await asyncio.gather(*[one(x) for x in todo])
        await asyncio.sleep(1)


def held(x, key, ok, now):
    """True while the condition holds, and for HOLD seconds after it breaks (prevents flicker around a threshold)."""
    if ok: x.fail.pop(key, None); return True
    return now - x.fail.setdefault(key, now) < HOLD


def update_state(x, now, win):
    """Decide list membership for one coin. Returns '', 'drop', 'stall' or 'trig'."""
    ps, fl, ab, sb = conds(x, now, win)
    fresh = x.live and x.ok and x.mode == "live" and now - x.lev < STALE_S
    if not (fresh and ps):
        x.in_trig = False                      # stale data never counts as Triggered
        return "drop" if x.in_drop else "stall" if x.in_stall else ""
    pk, pkt, drop = ps
    if x.in_trig: x.in_trig = held(x, "t", drop >= DROP_TRIG_EXIT and ab and sb, now)
    elif drop >= DROP_TRIG and ab and sb: x.in_trig = True; x.fail.pop("t", None)
    if x.in_drop: x.in_drop = held(x, "d", DROP_EXIT <= drop <= DROP_MAX, now)
    elif DROP_WATCH <= drop <= DROP_MAX: x.in_drop = True; x.fail.pop("d", None)
    st = x.stall >= STALL_MIN and drop <= STALL_NEAR
    if x.in_stall: x.in_stall = held(x, "s", st, now)
    elif st: x.in_stall = True; x.fail.pop("s", None)
    return "trig" if x.in_trig else "drop" if x.in_drop else "stall" if x.in_stall else ""


def oiref(x, w, now=None):
    """Open interest reading at or before `w` seconds ago (oldest reading is accepted if it is at least half that old)."""
    if len(x.oih) < 2: return None
    now = now or time.time(); cut = now - w; ref = None
    for r in x.oih:
        if r[0] <= cut: ref = r
        else: break
    if ref is None and now - x.oih[0][0] >= 0.5 * w: ref = x.oih[0]
    return ref


def oichg(x, w=300):
    ref = oiref(x, w)
    return x.oih[-1][1] / ref[1] - 1 if ref and ref[1] else 0.0


DBC = [None]


def db_add(table, header, row):
    """Mirror one logged row into SQLite (same columns as the CSV; empty values become NULL)."""
    try:
        c = DBC[0]
        if c is None:
            c = DBC[0] = sqlite3.connect(DB_FILE, check_same_thread=False)
            for t, cols in (("events", EV_COLS), ("outcomes", OUT_COLS)):
                c.execute(f"CREATE TABLE IF NOT EXISTS {t} (" + ", ".join(f'"{k}"' for k in cols) + ")")
            c.execute("CREATE INDEX IF NOT EXISTS ix_outcomes_id ON outcomes(id)")
            c.commit()
        c.execute(f"INSERT INTO {table} VALUES ({','.join('?' * len(header))})", [None if v == "" else v for v in row])
        c.commit()
    except Exception:
        log(f"sqlite write failed for {table}: {traceback.format_exc()[-200:]}")


def csv_add(path, header, row):
    db_add("events" if path == EV_LOG else "outcomes", header, row)
    try:
        new = not os.path.exists(path) or os.path.getsize(path) == 0
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new: w.writerow(header)
            w.writerow(row)
    except Exception:
        log(f"csv write failed for {path}: {traceback.format_exc()[-200:]}")


EV_COLS = ["id", "time", "coin", "event", "price", "peak_price", "peak_time", "drop_pct", "peak_ago_s", "to_3pct_s", "to_5pct_s",
           "peak_gain_pct", "vol24h_usd", "band_pct", "cvd_window", "bids_usd", "asks_usd", "buys_usd", "sells_usd", "cvd_usd",
           "stall_s", "streak", "streak_s", "oi_chg_5m_pct"]
OUT_COLS = ["id", "coin", "event", "after", "time", "price", "change_pct", "max_up_pct", "max_down_pct"]
HL = {60: "1m", 300: "5m", 900: "15m", 3600: "60m"}
KIND = {"drop": "WATCH_DROP", "stall": "WATCH_STALL", "trig": "TRIGGERED"}
STATE_TXT = {"": "not on a list", "drop": "WATCHLIST (dropping from the high)", "stall": "WATCHLIST (stalled at the high)", "trig": "TRIGGERED"}


def tstr(t): return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) if t else ""


# ---------- UI ----------
def M(v):
    a = abs(v)
    return f"{v/1e9:.2f}B" if a >= 1e9 else f"{v/1e6:.2f}M" if a >= 1e6 else f"{v/1e3:.0f}K" if a >= 1e3 else f"{v:.0f}"


def RT(r, part=False):
    s = max(r, 1 / r)
    return Text(f"{s:.1f}:1{'▲' if r > 1 else '▼'}{'~' if part else ''}", style=("green" if r > 1 else "red") if s >= THRESH else "dim")


COLS = [("rank", "#", 4), ("d", "Δ", 4), ("coin", "Coin", 11), ("src", "Data", 6), ("chg", "24h%", 8), ("vol", "Vol24h", 9),
        ("bids", "Bids$", 8), ("asks", "Asks$", 8), ("raw", "Ratio", 8), ("ratio", "Clean", 8), ("streak", "Streak", 9),
        ("buys", "Buys$", 8), ("sells", "Sells$", 8), ("cvd", "CVD", 17), ("oi", "OI$", 9), ("oid", "OIΔ", 8),
        ("pos", "Positioning", 16), ("bw", "Bid wall", 18), ("aw", "Ask wall", 18), ("lc", "Last cancel", 24),
        ("sp", "Spoof", 28), ("gap", "Gap / recovered", 60)]
# columns shared by the Watchlist and Triggered tables (identical widths so the two tables line up)
WCOLS = [("coin", "Coin", 11), ("why", "Why", 12), ("src", "Data", 6), ("px", "Price", 11), ("pk", "24h high", 11),
         ("sp", "Since peak", 13), ("pa", "Peak ago", 11), ("t3", "To -3%", 10), ("t5", "To -5%", 10), ("a3", "-3% ago", 9), ("a5", "-5% ago", 9), ("gain", "Peak gain", 9),
         ("vol", "Vol24h", 9), ("bids", "Bids$", 8), ("asks", "Asks$", 8), ("ratio", "Clean", 8), ("buys", "Buys$", 8),
         ("sells", "Sells$", 8), ("cvd", "CVD", 14), ("stk", "Streak", 9), ("stall", "Stall", 17), ("since", "Entered", 10)]
WL = {"s": "streak", 60: "1m", 300: "5m"}
PINS = "fetcher_pins.json"
SPARK = "▁▂▃▄▅▆▇█"
# Watchlist / Triggered sorts (column key -> menu text, and whether the natural order is high-to-low)
LSORT_MENU = {"pa": "Peak ago (most recent first)", "t3": "To -3% (fastest first)", "t5": "To -5% (fastest first)",
              "sp": "Since peak (deepest first)", "since": "Entered (newest first)"}
LS_DESC = {"pa": False, "t3": False, "t5": False, "sp": True, "since": True}
FOCUS = {"main": "top 20", "wl": "Watchlist", "tg": "Triggered"}


# it = (x, clean bid/ask, raw bid/ask, flow, cvd, band slot)   ->   key: (menu label, sort value, optional filter)
SORTS = {
    "ratio": ("Strongest imbalance (either side)", lambda it, now: max(it[1], 1 / it[1]), None),
    "bidside": ("Bid-heavy only (more resting buys)", lambda it, now: it[1], lambda it: it[1] > 1),
    "askside": ("Ask-heavy only (more resting sells)", lambda it, now: 1 / it[1], lambda it: it[1] < 1),
    "buyers": ("Net buyers only (CVD positive)", lambda it, now: it[4] or 0, lambda it: (it[4] or 0) > 0),
    "sellers": ("Net sellers only (CVD negative)", lambda it, now: -(it[4] or 0), lambda it: (it[4] or 0) < 0),
    "raw": ("Raw ratio (includes fresh walls)", lambda it, now: max(it[2], 1 / it[2]), None),
    "cvd": ("CVD (net buying to net selling)", lambda it, now: it[4] or 0, None),
    "acvd": ("CVD size (either side)", lambda it, now: abs(it[4] or 0), None),
    "streak": ("Streak time", lambda it, now: now - it[0].t0 if it[0].dir else 0, None),
    "chg": ("24h % change", lambda it, now: it[0].chg, None),
    "vol": ("24h volume", lambda it, now: it[0].vol, None),
    "oid": ("Open interest change (5m)", lambda it, now: oichg(it[0]), None),
    "bids": ("Bids $", lambda it, now: it[0].raw[0][it[5]], None),
    "asks": ("Asks $", lambda it, now: it[0].raw[1][it[5]], None),
    "spread": ("Spread (bps)", lambda it, now: (it[0].ba - it[0].bb) / it[0].mid * 1e4 if it[0].mid else 0, None),
    "dropfrom": ("Drop from 24h high", lambda it, now: drop_of(it[0], now), None),
    "peakgain": ("24h peak gain (high vs price 24h ago)", lambda it, now: it[0].pgain, None),
    "liq": ("Liquidations last 5m ($)", lambda it, now: sum(liq_sum(it[0], now, 300)), None),
}


def clip(t):
    import subprocess, sys, shutil
    try:
        if sys.platform == "win32": subprocess.run("clip", input=t.encode("utf-16"), check=True, shell=True)
        elif sys.platform == "darwin": subprocess.run("pbcopy", input=t.encode(), check=True)
        else:
            c = next(([b] + a for b, a in (("wl-copy", []), ("xclip", ["-selection", "clipboard"]), ("xsel", ["-b"]))
                      if shutil.which(b)), None)
            if not c: return False
            subprocess.run(c, input=t.encode(), check=True)
        return True
    except Exception:
        return False


def find(q):
    q = "".join(ch for ch in q.upper() if ch.isalnum())
    if q.endswith("USDT") and len(q) > 4: q = q[:-4]
    names = {n[:-4]: n for n in SY}
    if not q or not names: return []
    if q in names: return [names[q]]
    c = sorted(b for b in names if b.startswith(q)) or sorted(b for b in names if q in b)
    return [names[b] for b in c[:15]]


def pos_label(x, now):
    ref = oiref(x, 300, now)
    if ref is None or not ref[2]: return Text("warming up", style="dim")
    pc = x.mid / ref[2] - 1; od = oichg(x)
    if abs(pc) >= PX_MIN and abs(od) >= OI_MIN:
        return Text(("NEW LONGS" if od > 0 else "SHORTS COVERING") if pc > 0 else
                    ("NEW SHORTS" if od > 0 else "LONGS CLOSING"), style="green" if pc > 0 else "red")
    return Text("flat", style="dim")


# ---------- detail screen content ----------
def spark(vals):
    if not vals: return "-"
    lo, hi = min(vals), max(vals)
    if hi - lo < 1e-18: return SPARK[3] * len(vals)
    return "".join(SPARK[min(7, int((v - lo) / (hi - lo) * 7.999))] for v in vals)


def bucketed(vals, n):
    if len(vals) <= n: return list(vals)
    step = len(vals) / n
    return [vals[min(len(vals) - 1, int((i + 1) * step) - 1)] for i in range(n)]


def mk_bar(v, vmax, width=16): return "█" * int(round(width * v / vmax)) if vmax > 0 else ""


def top_levels(x, n): return sorted(x.bids.items(), reverse=True)[:n], sorted(x.asks.items())[:n]


def zones(x):
    """Crowded zones: the chosen band is cut into 10 slices per side; the 3 heaviest slices are reported."""
    mid = x.mid; lim = BAND[0] / 100.0; w = lim / 10.0; out = []
    for sd, book in ((2, x.asks), (1, x.bids)):
        bk = collections.defaultdict(float); tot = 0.0
        for p, q in book.items():
            d = abs(p - mid) / mid
            if d <= lim: u = p * q; bk[min(9, int(d / w))] += u; tot += u
        out.append((sd, tot, [(i * w, (i + 1) * w, u) for i, u in sorted(bk.items(), key=lambda kv: -kv[1])[:3]]))
    return out


def liq_clusters(x, now, span=3600):
    if not x.mid: return []
    step = x.mid * 0.002; bk = {}
    for t, sd, u, p in x.liq:
        if now - t <= span:
            r = bk.setdefault((sd, int(p / step)), [0.0, 0]); r[0] += u; r[1] += 1
    return [(sd, (i + 0.5) * step, r[0], r[1]) for (sd, i), r in sorted(bk.items(), key=lambda kv: -kv[1][0])[:3]]


def cvd_series(x, now, span=300, step=10):
    n = span // step; net = [0.0] * n; base = int(now) - span
    for sec, (b, s_) in list(x.buck.items()):
        i = (sec - base) // step
        if 0 <= i < n: net[i] += b - s_
    return list(itertools.accumulate(net))


class Lines:
    def __init__(s): s.l = []
    def add(s, *parts): s.l.append(Text.assemble(*parts))
    def text(s):
        t = Text("\n").join(s.l) if s.l else Text("-", style="dim")
        t.no_wrap = True; t.overflow = "ellipsis"
        return t


def detail_view(x, now, win, pinned):
    """Everything the detail screen shows for one coin. Returns (header, summary, grid renderable, plain text report)."""
    band = BAND[0]; ps = pstate(x, now); mid = x.mid
    fresh = x.live and x.ok and now - x.lev < STALE_S
    pk, pkt, drop = ps if ps else (0.0, 0.0, 0.0)
    we = weff(x, win); fl = flow(x, we, now)
    ab = x.raw[1][0] > x.raw[0][0]; sb = fl is not None and fl[1] > fl[0]
    part = band / 100.0 > x.cov + 1e-9

    if x.mode == "live" and x.ok and fresh: dtag = ("LIVE", "green")
    elif x.mode == "live": dtag = ("SYNCING", "yellow")
    else: dtag = (f"SCANNED {ago(now - x.snap_t)} ago" if x.snap_t else "NOT LOADED", "dim")
    issues = []
    if x.mode != "live": issues.append("not streaming live")
    elif not x.ok: issues.append("book syncing")
    elif not fresh: issues.append("no recent depth updates")
    if x.gapfill: issues.append("filling trade gap")
    if x.dead: issues.append("book gap")
    qual = ("OK", "green") if not issues else (", ".join(issues), "yellow")
    head = Text.assemble((("★ " if pinned else "") + x.n[:-4] + "/USDT   ", "bold yellow"), (f"{mid:.8g}   " if mid else "-   ", "bold"),
                         (f"{x.chg:+.1f}% 24h   ", "green" if x.chg >= 0 else "red"), f"Vol ${M(x.vol)}   Data: ", dtag, "   Quality: ", qual)

    bits = []
    if ps:
        if drop > 0: bits.append(f"{drop * 100:.1f}% below its 24h high" + (f" (peak {ago(now - pkt)} ago)" if pkt else ""))
        else: bits.append("at its 24h high")
    if x.live:
        r = (x.raw[0][0] + 1) / (x.raw[1][0] + 1)
        bits.append(f"{'bids' if r > 1 else 'asks'} outweigh {'asks' if r > 1 else 'bids'} {max(r, 1 / r):.1f}:1 (±{band}%)")
    if fl: bits.append(f"{'buyers' if fl[0] > fl[1] else 'sellers'} ahead by ${M(abs(fl[0] - fl[1]))} ({WL[we]})")
    if x.stall >= STALL_MIN and drop <= STALL_NEAR: bits.append(f"stalled at the high for {ago(x.stall)}")
    summ = Text.assemble(("Summary: ", "bold"), ". ".join(bits) + ("." if bits else "no data yet."), "  Status: ", (STATE_TXT[x.state], "bold yellow"))

    secs = {}

    # --- price vs 24h high
    L = Lines()
    if ps:
        L.add("Now ", (f"{mid:.8g}", "bold"), "   24h high ", (f"{pk:.8g}", "bold yellow"), "   ",
              (f"▼{drop * 100:.2f}% below", "red") if drop > 0 else ("at the high", "green"))
        L.add(("Peak " + ago(now - pkt) + " ago" if pkt else "Peak time loading (candles)", "dim"))
        v3 = tt(x, 3, drop, now, pkt); v5 = tt(x, 5, drop, now, pkt)
        L.add("Time to -3%: ", (fmt_t(v3) if v3 is not None else "-", "bold"), (f" (hit {fmt_t(max(0.0, now - pkt - v3))} ago)" if v3 is not None and pkt else "", "dim"),
              "    Time to -5%: ", (fmt_t(v5) if v5 is not None else "-", "bold"), (f" (hit {fmt_t(max(0.0, now - pkt - v5))} ago)" if v5 is not None and pkt else "", "dim"))
        L.add(f"Peak gain vs 24h ago: +{x.pgain * 100:.1f}%    Pool rank: " + (f"{POOL.index(x.n) + 1}/{POOL_N}" if x.n in POOLSET else "not in pool"))
    cl = [c[3] for c in x.kl]
    if len(cl) >= 5:
        L.add(("1h   ", "dim"), spark(cl[-60:]))
        day = bucketed(cl[-1440:], 60)
        L.add(("24h  ", "dim"), spark(day))
        t0 = x.kl[max(0, len(x.kl) - 1440)][0]; t1 = x.kl[-1][0] + 60000
        if pkt and pkt * 1000 >= t0:
            L.add(("     " + " " * min(len(day) - 1, int((pkt * 1000 - t0) / max(t1 - t0, 1) * len(day))) + "^ peak", "yellow"))
    else: L.add(("candles loading...", "dim"))
    secs["PRICE vs 24H HIGH"] = (L.text(), "yellow")

    # --- setup checklist
    L = Lines()

    def ck(ok, label, val): L.add((" ✓ " if ok else " ✗ ", "bold green" if ok else "bold red"), f"{label:<30}", (val, "dim"))
    ck(x.n in POOLSET, f"In pool (top {POOL_N} peak gain)", f"rank {POOL.index(x.n) + 1}" if x.n in POOLSET else "no")
    ck(x.vol >= MIN_VOL, "Volume rule", f"${M(x.vol)} (min ${M(MIN_VOL)})")
    ck(drop >= DROP_WATCH, f"{DROP_WATCH * 100:g}%+ below high", f"{drop * 100:.2f}%  -> Watchlist")
    ck(drop >= DROP_TRIG, f"{DROP_TRIG * 100:g}%+ below high", f"{drop * 100:.2f}%  -> Triggered needs this")
    ck(ab, "Asks above bids", f"asks ${M(x.raw[1][0])} vs bids ${M(x.raw[0][0])} (±{band}%)")
    ck(sb, "Sells above buys", f"sells ${M(fl[1])} vs buys ${M(fl[0])} ({WL[we]})" if fl else "no flow data")
    ck(x.stall >= STALL_MIN and drop <= STALL_NEAR, f"Stalled at high {STALL_MIN // 60}m+", f"{ago(x.stall)} in a {STALL_RANGE * 100:g}% range")
    L.add(("Result: ", "bold"), (STATE_TXT[x.state], "bold yellow"))
    secs["SETUP CHECKLIST"] = (L.text(), "green")

    # --- ladder
    L = Lines(); b, a = top_levels(x, 8)
    if a and b and mid:
        ca = list(itertools.accumulate(p * q for p, q in a)); cb = list(itertools.accumulate(p * q for p, q in b)); vm = max(ca[-1], cb[-1], 1)
        for i in range(len(a) - 1, -1, -1):
            p, q = a[i]; w = x.walls.get((2, p))
            L.add((f"ASK {p:>11.8g} ${M(p * q):>7} ", "red"), (mk_bar(ca[i], vm), "red"), (f" WALL {ago(now - w[0])}" if w else "", "bold yellow"))
        qb = x.bids.get(x.bb, 0); qa = x.asks.get(x.ba, 0)
        micro = (x.bb * qa + x.ba * qb) / (qa + qb) if qa + qb else mid
        L.add((f"---- spread {(x.ba - x.bb) / mid * 1e4:.1f} bps | microprice {micro:.8g} ({(micro / mid - 1) * 1e4:+.1f} bps) ----", "dim"))
        for i, (p, q) in enumerate(b):
            w = x.walls.get((1, p))
            L.add((f"BID {p:>11.8g} ${M(p * q):>7} ", "green"), (mk_bar(cb[i], vm), "green"), (f" WALL {ago(now - w[0])}" if w else "", "bold yellow"))
        L.add(("bars show cumulative dollars from the inside", "dim"))
    else: L.add(("no live book yet", "dim"))
    secs["ORDER BOOK LADDER"] = (L.text(), "cyan")

    # --- imbalance
    L = Lines(); L.add((f"{'band':<14}{'bids':>9}{'asks':>9}  {'raw':<9}{'clean':<9}", "dim"))
    labs = [f"±{band}% chosen"] + [f"±{int(round(d * 1e4))} bps" for d in DET_BANDS]
    for i, lab in enumerate(labs):
        r = (x.raw[0][i] + 1) / (x.raw[1][i] + 1); rc = (x.cln[0][i] + 1) / (x.cln[1][i] + 1)
        t = Text(f"{lab:<14}{'$' + M(x.raw[0][i]):>9}{'$' + M(x.raw[1][i]):>9}  "); t.append_text(RT(r, i == 0 and part)); t.append("   "); t.append_text(RT(rc, i == 0 and part))
        L.l.append(t)
    b10, a10 = top_levels(x, 10)
    for n in (5, 10):
        def sm(lst, sd, clean): return sum(p * q for p, q in lst[:n] if not (clean and x.walls.get((sd, p)) and now - x.walls[(sd, p)][0] < YOUNG))
        bi, ai, bc, ac = sm(b10, 1, False), sm(a10, 2, False), sm(b10, 1, True), sm(a10, 2, True)
        t = Text(f"{'top ' + str(n) + ' levels':<14}{'$' + M(bi):>9}{'$' + M(ai):>9}  "); t.append_text(RT((bi + 1) / (ai + 1))); t.append("   "); t.append_text(RT((bc + 1) / (ac + 1)))
        L.l.append(t)
    L.add((f"Book reaches ±{x.cov * 100:.2f}% from price (500 level snapshot)" + ("; chosen band is PARTIAL (~)" if part else ""), "yellow" if part else "dim"))
    secs["IMBALANCE"] = (L.text(), "magenta")

    # --- trade flow + cvd
    L = Lines(); L.add((f"{'window':<8}{'buys':>10}{'sells':>10}{'CVD':>11}  buy share", "dim"))
    for lab, w in (("10s", 10), ("1m", 60), ("5m", 300), ("streak", "s")):
        f = flow(x, w, now)
        if f is None:
            L.add(f"{lab:<8}", ("-", "dim")); continue
        cv = f[0] - f[1]; tot = f[0] + f[1]
        L.add(f"{lab:<8}", (f"${M(f[0]):>9}", "green"), (f"${M(f[1]):>9}", "red"), (f"{'+' if cv >= 0 else '-'}${M(abs(cv)):>9}", "green" if cv >= 0 else "red"),
              f"  {100 * f[0] / tot:.0f}%" if tot else "")
    cs = cvd_series(x, now)
    L.add(("CVD 5m  ", "dim"), spark(cs), (f"  {'+' if cs[-1] >= 0 else '-'}${M(abs(cs[-1]))}" if cs else "", "dim"))
    secs["TRADE FLOW"] = (L.text(), "blue")

    # --- tape
    L = Lines()
    for t, p, u, sd in list(x.tape)[-10:][::-1]:
        big = u >= BIG_TRADE
        L.add((f"{time.strftime('%H:%M:%S', time.localtime(t))} {'BUY ' if sd == 'B' else 'SELL'} {p:>11.8g} ${M(u):>7}" + ("  BIG" if big else ""),
               ("bold " if big else "") + ("green" if sd == "B" else "red")))
    if not x.tape: L.add(("no trades yet", "dim"))
    secs["LIVE TAPE"] = (L.text(), "blue")

    # --- positioning
    L = Lines()
    if x.oih and mid:
        L.add(f"Open interest ${M(x.oih[-1][1] * mid)}")
        for lab, w in (("1m", 60), ("5m", 300), ("15m", 900)):
            if oiref(x, w, now) is None: L.add(f"  change {lab:<4}", ("warming up", "dim"))
            else:
                od = oichg(x, w); L.add(f"  change {lab:<4}", (f"{od * 100:+.2f}%", "green" if od >= 0 else "red"))
        t = Text("Positioning (5m): "); t.append_text(pos_label(x, now)); L.l.append(t)
    else: L.add(("open interest loading...", "dim"))
    if x.index: L.add(f"Funding {x.fund * 100:+.4f}%   mark vs index {(x.mark / x.index - 1) * 1e4:+.1f} bps")
    secs["POSITIONING"] = (L.text(), "white")

    # --- crowded zones
    L = Lines()
    if mid and x.ok:
        for sd, tot, zs in zones(x):
            L.add((f"{'Asks above' if sd == 2 else 'Bids below'} (±{band}%, total ${M(tot)})", "bold red" if sd == 2 else "bold green"))
            for lo, hi, u in zs:
                pl, ph = (mid * (1 + lo), mid * (1 + hi)) if sd == 2 else (mid * (1 - hi), mid * (1 - lo))
                L.add((f"  {pl:.8g} to {ph:.8g}  ${M(u)}  {100 * u / tot:.0f}% of side  {lo * 1e4:.0f}-{hi * 1e4:.0f} bps", "red" if sd == 2 else "green"))
    else: L.add(("no live book yet", "dim"))
    secs["CROWDED ZONES"] = (L.text(), "red")

    # --- liquidations
    L = Lines()
    for lab, w in (("1m", 60), ("5m", 300), ("15m", 900)):
        lg, sh = liq_sum(x, now, w)
        L.add(f"{lab:<4}", (f"longs liquidated ${M(lg):>7}", "red"), "  ", (f"shorts liquidated ${M(sh):>7}", "green"))
    for t, sd, u, p in list(x.liq)[-5:][::-1]:
        L.add((f"{time.strftime('%H:%M:%S', time.localtime(t))} {'LONG ' if sd == 'L' else 'SHORT'} liquidated ${M(u)} @ {p:.8g}", "red" if sd == "L" else "green"))
    for sd, pc_, u, n in liq_clusters(x, now):
        L.add((f"cluster 1h: {'longs' if sd == 'L' else 'shorts'} near {pc_:.8g} ${M(u)} ({n} events)", "yellow"))
    L.add(("minimums: Binance sends at most 1 liquidation per coin per second", "dim"))
    secs["LIQUIDATIONS"] = (L.text(), "red")

    # --- walls, cancels, spoofs
    L = Lines()
    ws = sorted(x.walls.items(), key=lambda kv: -kv[1][1])[:5]
    if not ws: L.add(("no walls in the chosen band", "dim"))
    for (sd, p), w in ws:
        L.add((f"{'BID' if sd == 1 else 'ASK'} {p:.8g}  peak ${M(w[1])}  age {ago(now - w[0])}" + ("  (young, ignored in Clean)" if now - w[0] < YOUNG else ""),
               "green" if sd == 1 else "red"))
    if x.lc and now - x.lc[0] < 600:
        L.add(f"Last cancel: {'B' if x.lc[3] == 1 else 'A'} ${M(x.lc[1])} @ {x.lc[2]:.8g} {ago(now - x.lc[0])} ago")
    rec = [q for q in x.spoof if now - q < 600]
    L.add(f"Spoof flags last 10m: {len(rec)}" + (f" (latest ${M(x.sp[1])} @ {x.sp[2]:.8g})" if x.sp and rec else ""))
    secs["WALLS, CANCELS, SPOOF"] = (L.text(), "yellow")

    # --- data health
    L = Lines()
    L.add(f"Depth update age: {ago(now - x.lev) if x.lev else '-'}   levels: {len(x.bids)} bids / {len(x.asks)} asks")
    L.add(f"Candles: {len(x.kl)} loaded, fetched {ago(now - x.kl_t) + ' ago' if x.kl_t else 'never'}")
    L.add(f"Streams: pub {'UP' if W.get('pub') else 'DOWN'}  mkt {'UP' if W.get('mkt') else 'DOWN'}  drops {STAT['n']}")
    if x.gapinfo and now - x.gapinfo[0] < 900: L.add((x.gapinfo[1], "cyan"))
    secs["DATA HEALTH"] = (L.text(), "dim")

    left = ["PRICE vs 24H HIGH", "ORDER BOOK LADDER", "TRADE FLOW", "LIVE TAPE", "WALLS, CANCELS, SPOOF"]
    right = ["SETUP CHECKLIST", "IMBALANCE", "POSITIONING", "CROWDED ZONES", "LIQUIDATIONS", "DATA HEALTH"]
    grid = Table.grid(expand=True, padding=(0, 1)); grid.add_column(ratio=1); grid.add_column(ratio=1)
    grid.add_row(Group(*[Panel(secs[k][0], title=k, border_style=secs[k][1]) for k in left]),
                 Group(*[Panel(secs[k][0], title=k, border_style=secs[k][1]) for k in right]))
    plain = "\n".join([head.plain, summ.plain, ""] + [f"== {k} ==\n{secs[k][0].plain}\n" for k in left + right])
    return head, summ, grid, plain


class Pick(ModalScreen):
    DEFAULT_CSS = "Pick{align:center middle;} Pick OptionList{width:52;height:auto;max-height:20;border:round green;}"
    BINDINGS = [("escape", "close", "Cancel")]

    def __init__(self, opts): super().__init__(); self.opts = opts
    def compose(self): yield OptionList(*[Option(l, id=k) for k, l in self.opts])
    def on_mount(self): self.query_one(OptionList).focus()
    def on_option_list_option_selected(self, e): self.dismiss(e.option.id)
    def action_close(self): self.dismiss(None)


class Ask(ModalScreen):
    """Small entry box. kind 'int' accepts a whole number 1-10 (the band), kind 'text' returns what was typed."""
    DEFAULT_CSS = "Ask{align:center middle;} Ask Vertical{width:54;height:auto;border:round green;padding:1 2;background:$surface;}"
    BINDINGS = [("escape", "close", "Cancel")]

    def __init__(self, prompt, kind="int", value=""): super().__init__(); self.prompt = prompt; self.kind = kind; self.value = value

    def compose(self):
        with Vertical():
            yield Static(self.prompt); yield Input(value=self.value, id="ask")

    def on_mount(self):
        i = self.query_one("#ask", Input); i.focus(); i.select_all()      # typing replaces the pre-filled value

    def on_input_submitted(self, e):
        e.stop(); v = e.value.strip()
        if self.kind == "int":
            try: n = int(v)
            except ValueError: n = 0
            if 1 <= n <= 10: self.dismiss(n)
            else: self.notify("Enter a whole number from 1 to 10")
        else: self.dismiss(v)

    def action_close(self): self.dismiss(None)


class Detail(Screen):
    """Full-screen view of one coin, updated every second."""
    DEFAULT_CSS = "#dh{height:auto;padding:0 1;} #dsum{height:auto;padding:0 1;margin-bottom:1;} #dbody{width:100%;}"
    BINDINGS = [("escape", "close", "Back"), ("left", "step(-1)", "Prev coin"), ("right", "step(1)", "Next coin"),
                ("p", "pin", "Pin / unpin"), ("c", "copy", "Copy report")]

    def __init__(self, n, order): super().__init__(); self.n = n; self.order = order; self.plain = ""

    def compose(self):
        yield Static(id="dh"); yield Static(id="dsum")
        with VerticalScroll(): yield Static(id="dbody")
        yield Footer()

    def on_mount(self): self.set_interval(1, self.draw); self.draw()

    def goto(self, n): self.n = n; self.draw()

    def draw(self):
        x = SY.get(self.n); now = time.time(); DET[0] = self.n; KLREQ[self.n] = now
        if x is None:
            self.query_one("#dh", Static).update(f"{self.n} is not tracked"); return
        try:
            head, summ, grid, self.plain = detail_view(x, now, self.app.win, self.n in self.app.watch)
        except Exception:
            log("detail error: " + traceback.format_exc()[-500:]); return
        self.query_one("#dh", Static).update(head); self.query_one("#dsum", Static).update(summ); self.query_one("#dbody", Static).update(grid)

    def action_step(self, d):
        o = self.order
        if self.n in o: self.n = o[(o.index(self.n) + d) % len(o)]
        elif o: self.n = o[0]
        self.draw()

    def action_pin(self): self.app.pin(self.n); self.draw()

    def action_copy(self):
        ok = clip(self.plain)
        try: self.app.copy_to_clipboard(self.plain)
        except Exception: pass
        if not ok:
            with open("copied.txt", "a", encoding="utf-8") as f: f.write(self.plain + "\n")
        self.notify("Copied the report" if ok else "Clipboard failed, saved to copied.txt")

    def action_close(self): DET[0] = None; self.app.pop_screen()


class Fetcher(App):
    ENABLE_COMMAND_PALETTE = False      # hide the built-in Ctrl+P palette (it does nothing for the scanner)
    CSS = "#main{height:3fr;} #wl{height:2fr;} #tg{height:2fr;} .lbl{height:1;background:$boost;padding:0 1;} #search{height:3;}"
    BINDINGS = [("s", "win('s')", "CVD streak"), ("1", "win(60)", "CVD 1m"), ("5", "win(300)", "CVD 5m"),
                ("b", "band", "Band %"), ("o", "sort", "Sort menu"), ("r", "reverse", "Reverse"),
                ("d", "detail_coin", "Open coin"), ("x", "clear_trig", "Clear ended"), ("u", "reset_sort", "Reset sort"),
                ("slash", "search", "Search"), ("c", "copy", "Copy row"),
                Binding("left_square_bracket", "hs(-12)", "Scroll left", show=False),
                Binding("right_square_bracket", "hs(12)", "Scroll right", show=False),
                Binding("escape", "back", "Back", show=False), ("q", "quit", "Quit")]

    def __init__(self):
        super().__init__()
        self.win = "s"; self.sk = "ratio"; self.desc = True; self.bsig = None
        self.lsort = {"wl": None, "tg": None}               # per-table sort for the two lists: None = default, else (column key, high-to-low?)
        self.hist = collections.deque(maxlen=6); self.ht = 0; self.since = {}; self.t_start = time.time()
        self.tasks = []; self.k = 0; self.terr = 0; self.lt = 0.0; self.ft = time.time(); self.fm = self.fb = 0
        self.trig = {}; self.pend = []                       # Triggered session list {coin: record}; outcomes still to log
        self.names = {"main": [], "wl": [], "tg": []}        # coin behind each table row (for Enter / copy)
        self.plain = {"main": [], "wl": [], "tg": []}
        self.st = {k: {"n": 0, "sigs": []} for k in ("main", "wl", "tg")}
        try: self.watch = [n for n in json.load(open(PINS)) if isinstance(n, str)][:8]
        except Exception: self.watch = []

    def compose(self):
        yield Static(id="h"); yield DataTable(zebra_stripes=True, id="main")
        yield Static("", id="lw", classes="lbl"); yield DataTable(zebra_stripes=True, id="wl")
        yield Static("", id="lt", classes="lbl"); yield DataTable(zebra_stripes=True, id="tg")
        yield Input(placeholder="Search a coin (press /): Enter pins it to the top, Enter on empty clears pins", id="search")
        yield Footer()

    async def on_mount(self):
        global SQ, LK, HS
        SQ = asyncio.PriorityQueue(); LK = asyncio.Lock(); HS = aiohttp.ClientSession(connector=aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver()),
                                   timeout=aiohttp.ClientTimeout(total=20)); PRI.update(self.watch)
        # keep direct references: App.query_one only searches the active screen, which is the detail screen while it is open
        self.hd = self.query_one("#h", Static); self.tm = self.query_one("#main", DataTable); self.tw = self.query_one("#wl", DataTable)
        self.tt = self.query_one("#tg", DataTable); self.lw = self.query_one("#lw", Static); self.lt_ = self.query_one("#lt", Static)
        self.sb = self.query_one("#search", Input)
        for t in (self.tm, self.tw, self.tt): t.cursor_type = "row"
        self.rebuild()
        self.rebuild_list("wl"); self.rebuild_list("tg")
        self.tm.focus()
        for c in ((universe,), (lagmon,), (snap_worker,), (sweeper,), (live_manager,), (trade_backfill,), (oi_loop,), (kline_loop,),
                  (ws_loop, "pub", on_depth), (ws_loop, "mkt", on_trade)):
            self.tasks.append(asyncio.create_task(supervise(*c)))
        self.set_interval(1, self.tick)

    def rebuild(self):
        t = self.tm; t.clear(columns=True); self.st["main"] = {"n": 0, "sigs": []}
        for k, l, w in COLS:
            t.add_column(l + ((" ▼" if self.desc else " ▲") if k == self.sk else ""), width=w, key=k)

    def reset_view(self, rebuild=False):
        self.since = None; self.hist.clear(); self.ht = 0
        if rebuild: self.rebuild()

    def set_sort(self, k):
        if k == self.sk: self.desc = not self.desc
        else: self.sk, self.desc = k, True
        self.reset_view(True); self.sync_footer()

    def action_win(self, w): self.win = w; self.reset_view()

    def action_band(self):
        def done(v):
            if v:
                BAND[0] = v; BAND_GEN[0] += 1; self.reset_view()
                self.notify(f"Band set to ±{v}% (walls and streaks restarted)")
        self.push_screen(Ask("Band in whole percent of price, 1 to 10", "int", str(BAND[0])), done)

    def ftab(self):
        """Which table has keyboard focus: 'main' (top 20), 'wl' (Watchlist) or 'tg' (Triggered)."""
        f = self.focused
        if f is not None and f is getattr(self, "tw", None): return "wl"
        if f is not None and f is getattr(self, "tt", None): return "tg"
        return "main"

    def sort_active(self, t):
        if not hasattr(self, "lsort"): return False
        if t == "main": return self.sk != "ratio" or not self.desc
        return self.lsort[t] is not None

    def check_action(self, action, parameters):
        if action == "reset_sort": return self.sort_active(self.ftab())      # hidden from the footer unless a sort is active
        return True

    def sync_footer(self):
        t = self.ftab(); sig = (t, self.sort_active(t))
        if sig != self.bsig: self.bsig = sig; self.refresh_bindings()

    def rebuild_list(self, tab):
        t = self.tw if tab == "wl" else self.tt; ls = self.lsort[tab]
        t.clear(columns=True); self.st[tab] = {"n": 0, "sigs": []}
        for k, l, w in WCOLS:
            t.add_column(l + ((" ▼" if ls[1] else " ▲") if ls and ls[0] == k else ""), width=w, key=k)
        self.sync_footer()

    def set_lsort(self, tab, k):
        ls = self.lsort[tab]
        self.lsort[tab] = (k, not ls[1]) if ls and ls[0] == k else (k, LS_DESC[k])
        self.rebuild_list(tab)

    def action_reset_sort(self):
        t = self.ftab()
        if not self.sort_active(t): return
        if t == "main": self.sk, self.desc = "ratio", True; self.reset_view(True)
        else: self.lsort[t] = None; self.rebuild_list(t)
        self.sync_footer(); self.notify(f"{FOCUS[t]} sort reset to default")

    def action_reverse(self):
        t = self.ftab()
        if t == "main": self.desc = not self.desc; self.reset_view(True)
        else:
            ls = self.lsort[t]
            self.lsort[t] = ((("sp" if t == "wl" else "since"), False) if ls is None else (ls[0], not ls[1]))
            self.rebuild_list(t)
        self.sync_footer()

    def action_search(self):
        if self.screen is self.screen_stack[0]: self.sb.focus()

    def action_back(self): self.tm.focus()

    def action_hs(self, dx):
        for t in (self.tm, self.tw, self.tt): t.scroll_relative(x=dx, animate=False)

    def action_sort(self):
        t = self.ftab()
        if t != "main":
            def done_l(k):
                if k: self.lsort[t] = (k, LS_DESC[k]); self.rebuild_list(t)
            self.push_screen(Pick(list(LSORT_MENU.items())), done_l); return

        def done(k):
            if k: self.sk, self.desc = k, True; self.reset_view(True); self.sync_footer()
        self.push_screen(Pick([(k, v[0]) for k, v in SORTS.items()]), done)

    def action_detail_coin(self):
        def got(v):
            if not v: return
            c = find(v)
            if not c: self.notify(f"'{v}' not tracked (needs ${MIN_VOL / 1e6:.0f}M+ 24h volume)")
            elif len(c) == 1: self.open_detail(c[0])
            else: self.push_screen(Pick([(n, n[:-4]) for n in c]), lambda k: k and self.open_detail(k))
        self.push_screen(Ask("Open details for which coin? (e.g. aria)", "text"), got)

    def open_detail(self, n):
        if isinstance(self.screen, Detail): self.screen.goto(n); return
        DET[0] = n; self.push_screen(Detail(n, list(self.names["main"]) or [n]))

    def on_data_table_row_selected(self, e):
        names = self.names.get(e.data_table.id or "", [])
        if 0 <= e.cursor_row < len(names): self.open_detail(names[e.cursor_row])

    def on_data_table_header_selected(self, e):
        k = e.column_key.value; tid = e.data_table.id
        if tid == "main":
            if k in SORTS: self.set_sort(k)
        elif tid in ("wl", "tg") and k in LSORT_MENU: self.set_lsort(tid, k)

    def action_clear_trig(self):
        ended = [n for n, r in self.trig.items() if not r["active"]]
        for n in ended: del self.trig[n]
        self.notify(f"Cleared {len(ended)} ended Triggered rows" if ended else "No ended Triggered rows to clear")

    def pin(self, n):
        if n in self.watch: self.watch.remove(n); msg = f"Unpinned {n[:-4]}"
        elif len(self.watch) >= 8: self.notify("Max 8 pins - press Enter on an empty search to clear"); return
        else: self.watch.append(n); PRI.add(n); msg = f"Pinned {n[:-4]}"
        try: json.dump(self.watch, open(PINS, "w"))
        except Exception: pass
        self.notify(msg)

    def on_input_submitted(self, e):
        q = "".join(ch for ch in e.value.upper() if ch.isalnum()); e.input.value = ""
        if q.endswith("USDT") and len(q) > 4: q = q[:-4]
        self.tm.focus()
        if not q:
            self.watch = []; json.dump([], open(PINS, "w")); self.notify("Pins cleared"); return
        names = {n[:-4]: n for n in SY}
        if not names: self.notify("Still loading coin list, try again in a few seconds"); return
        if q in names: self.pin(names[q]); return
        c = sorted(b for b in names if b.startswith(q)) or sorted(b for b in names if q in b)
        if not c: self.notify(f"'{q}' not tracked (needs ${MIN_VOL/1e6:.0f}M+ 24h volume)")
        elif len(c) == 1: self.pin(names[c[0]])
        else: self.push_screen(Pick([(names[b], b) for b in c[:15]]), lambda k: k and self.pin(k))

    def action_copy(self):
        key = "wl" if self.focused is self.tw else "tg" if self.focused is self.tt else "main"
        t = {"main": self.tm, "wl": self.tw, "tg": self.tt}[key]; r = t.cursor_row; rows = self.plain[key]
        if not 0 <= r < len(rows): return
        txt = rows[r]; ok = clip(txt)
        try: self.copy_to_clipboard(txt)
        except Exception: pass
        if not ok:
            with open("copied.txt", "a", encoding="utf-8") as f: f.write(txt + "\n")
        self.notify(f"Copied row {r + 1}" if ok else f"Clipboard failed - saved row {r + 1} to copied.txt")

    def tick(self):
        gap = time.time() - self.lt if self.lt else 0; self.lt = time.time()
        if gap > 5: log(f"UI stalled {gap:.0f}s (program paused, e.g. console text selection, or PC overloaded/asleep)")
        try: self._tick()
        except Exception:
            self.terr += 1
            if self.terr <= 10: log("tick error: " + traceback.format_exc()[-600:])

    def holder(self, n, why):
        x = SY.get(n); dash = Text("-", style="dim")
        chg = Text(f"{x.chg:+.1f}%", style="green" if x.chg >= 0 else "red") if x else dash
        vol = Text(f"${M(x.vol)}") if x else dash
        return [dash, Text(""), Text("★" + n[:-4], style="bold yellow"), Text("-", style="dim"), chg, vol] + [dash] * 10 + \
               [Text(why, style="dim")] + [dash] * 5

    def paint(self, t, cols, rows, st):
        """Update a table in place (only changed cells) so cursor and scroll position stay put."""
        old = st["n"]; sigs = []
        with self.batch_update():
            for i, cells in enumerate(rows):
                sg = [(v.plain, str(v.style)) for v in cells]; sigs.append(sg)
                if i < old:
                    pv = st["sigs"][i] if i < len(st["sigs"]) else None
                    for n, (c, v) in enumerate(zip(cols, cells)):
                        if pv is None or pv[n] != sg[n]: t.update_cell(f"r{i}", c[0], v)
                else: t.add_row(*cells, key=f"r{i}")
            for i in range(len(rows), old): t.remove_row(f"r{i}")
        st["n"] = len(rows); st["sigs"] = sigs

    def log_event(self, x, st, now):
        ps = pstate(x, now)
        if not ps: return
        pk, pkt, drop = ps; fl = flow(x, weff(x, self.win), now); eid = f"{x.n}-{int(now)}-{KIND[st]}"
        v3 = tt(x, 3, drop, now, pkt); v5 = tt(x, 5, drop, now, pkt)
        b = s_ = cv = ""
        if fl: b, s_, cv = round(fl[0]), round(fl[1]), round(fl[0] - fl[1])
        csv_add(EV_LOG, EV_COLS, [eid, tstr(now), x.n, KIND[st], f"{x.mid:.10g}", f"{pk:.10g}", tstr(pkt), round(drop * 100, 3),
                                  round(now - pkt) if pkt else "", round(v3) if v3 is not None else "", round(v5) if v5 is not None else "",
                                  round(x.pgain * 100, 2), round(x.vol), BAND[0], WL[weff(x, self.win)], round(x.raw[0][0]), round(x.raw[1][0]),
                                  b, s_, cv, round(x.stall), ("up" if x.dir > 0 else "down") if x.dir else "", round(now - x.t0) if x.dir else "",
                                  round(oichg(x) * 100, 3) if oiref(x, 300, now) else ""])
        self.pend.append({"id": eid, "n": x.n, "kind": KIND[st], "t": now, "px": x.mid, "mx": x.mid, "mn": x.mid, "done": set()})

    def run_pending(self, now):
        keep = []
        for p in self.pend:
            x = SY.get(p["n"]); px = x.mid if x and x.live and x.ok and now - x.lev < STALE_S else None
            if px: p["mx"] = max(p["mx"], px); p["mn"] = min(p["mn"], px)
            for h in HORIZ:
                if h in p["done"] or now - p["t"] < h: continue
                p["done"].add(h)
                csv_add(OUT_LOG, OUT_COLS, [p["id"], p["n"], p["kind"], HL[h], tstr(now), f"{px:.10g}" if px else "",
                                            round((px / p["px"] - 1) * 100, 3) if px else "", round((p["mx"] / p["px"] - 1) * 100, 3),
                                            round((p["mn"] / p["px"] - 1) * 100, 3)])
            if len(p["done"]) < len(HORIZ): keep.append(p)
        self.pend = keep

    def track(self, now):
        """Update Watchlist / Triggered membership, write the CSV log, decide which coins must stay live."""
        for n in list(POOL) + [x.n for x in SY.values() if x.state and x.n not in POOLSET]:
            x = SY.get(n)
            if x is None: continue
            st = update_state(x, now, self.win)
            if st == x.state: continue
            prev = x.state; x.state = st; x.state_t = now
            if st == "trig":
                old = self.trig.get(n); self.trig[n] = {"t": now, "active": True, "end": None, "cnt": (old["cnt"] + 1) if old else 1}
            elif prev == "trig" and n in self.trig:
                self.trig[n]["active"] = False; self.trig[n]["end"] = now
            if st: self.log_event(x, st, now)
        keep = {x.n for x in SY.values() if x.state} | {p["n"] for p in self.pend} | {n for n, r in self.trig.items() if r["active"]}
        if DET[0]: keep.add(DET[0])
        KEEP.clear(); KEEP.update(keep); PRI.update(POOLSET); PRI.update(KEEP)
        self.run_pending(now)

    def wrow(self, x, why, since, now):
        ps = pstate(x, now); fresh = x.live and x.ok and now - x.lev < STALE_S
        part = BAND[0] / 100.0 > x.cov + 1e-9; dash = Text("-", style="dim")
        pk, pkt, drop = ps if ps else (0.0, 0.0, 0.0)
        fl = flow(x, weff(x, self.win), now); r = (x.cln[0][0] + 1) / (x.cln[1][0] + 1)
        v3 = tt(x, 3, drop, now, pkt) if ps else None; v5 = tt(x, 5, drop, now, pkt) if ps else None
        src = Text("LIVE", style="green") if fresh else Text(f"{int(now - x.lev)}s" if x.lev else "sync", style="yellow")
        if fl is None: buys = sells = cv = dash
        else:
            c = fl[0] - fl[1]; buys = Text(f"${M(fl[0])}", style="green"); sells = Text(f"${M(fl[1])}", style="red")
            cv = Text(f"{'+' if c >= 0 else '-'}${M(abs(c))}", style="green" if c >= 0 else "red")
        if x.dir:
            s = now - x.t0; stk = Text(f"{'▲' if x.dir > 0 else '▼'} {int(s // 60)}m{int(s % 60):02d}s", style="green" if x.dir > 0 else "red")
        else: stk = dash
        stall = dash
        if x.stall >= STALL_MIN and drop <= STALL_NEAR:
            wall = any(k[0] == 2 and w[3] <= 0.01 for k, w in list(x.walls.items()))
            stall = Text(f"STALL {ago(x.stall)}" + (" WALL" if wall else ""), style="bold yellow")
        sp = (Text(f"▼{drop * 100:.2f}%", style="bold red") if drop > 0 else Text("at high", style="green")) if ps else dash
        # how long ago price first fell 3% / 5% below the high = time since the peak minus the time it took to get there
        a3 = Text(fmt_t(max(0.0, now - pkt - v3))) if (v3 is not None and pkt) else dash
        a5 = Text(fmt_t(max(0.0, now - pkt - v5))) if (v5 is not None and pkt) else dash
        return [Text(x.n[:-4], style="bold"), why, src, Text(f"{x.mid:.8g}") if x.mid else dash, Text(f"{pk:.8g}") if ps else dash, sp,
                Text(ago(now - pkt)) if pkt else dash, Text(fmt_t(v3)) if v3 is not None else dash, Text(fmt_t(v5)) if v5 is not None else dash, a3, a5,
                Text(f"+{x.pgain * 100:.0f}%", style="green"), Text(f"${M(x.vol)}"),
                Text(f"${M(x.raw[0][0])}" + ("~" if part else "")), Text(f"${M(x.raw[1][0])}" + ("~" if part else "")), RT(r, part),
                buys, sells, cv, stk, stall, Text(time.strftime("%H:%M:%S", time.localtime(since))) if since else dash]

    def lkey(self, key, x, now, ent):
        """Value a list row is sorted on (None = no value, goes to the bottom)."""
        ps = pstate(x, now)
        if key == "since": return ent or None
        if not ps: return None
        if key == "sp": return ps[2]
        if key == "pa": return (now - ps[1]) if ps[1] else None
        return tt(x, 3 if key == "t3" else 5, ps[2], now, ps[1])

    def lorder(self, tab, items, now):
        """items = (coin, why, entered time, active). Applies this table's own sort."""
        ls = self.lsort[tab]
        if ls is None:
            if tab == "wl": return sorted(items, key=lambda it: drop_of(it[0], now), reverse=True)
            return sorted(items, key=lambda it: (not it[3], -it[2]))
        have = []; miss = []
        for it in items:
            v = self.lkey(ls[0], it[0], now, it[2])
            (miss if v is None else have).append((v, it))
        have.sort(key=lambda t: t[0], reverse=ls[1])
        return [it for _, it in have] + [it for _, it in miss]

    def lname(self, tab):
        ls = self.lsort[tab]
        return "default" if ls is None else f"{LSORT_MENU[ls[0]].split(' (')[0]} {'▼' if ls[1] else '▲'}"

    def show_small(self, now):
        wl = [(x, Text("DROP", style="red") if x.state == "drop" else Text("STALL", style="yellow"), x.state_t, True)
              for x in SY.values() if x.state in ("drop", "stall")]
        wl = self.lorder("wl", wl, now)
        rows = [self.wrow(x, why, since, now) for x, why, since, _ in wl]
        self.names["wl"] = [it[0].n for it in wl]
        self.plain["wl"] = [" | ".join(f"{c[1]}: {v.plain}" for c, v in zip(WCOLS, cells) if v.plain not in ("", "-")) for cells in rows]
        self.paint(self.tw, WCOLS, rows, self.st["wl"])
        tg = [(SY[n], Text("ACTIVE", style="bold red") if r["active"] else Text(f"ended {ago(now - r['end'])}", style="dim"), r["t"], r["active"])
              for n, r in self.trig.items() if n in SY]
        tg = self.lorder("tg", tg, now)
        rows = [self.wrow(x, why, since, now) for x, why, since, _ in tg]
        self.names["tg"] = [it[0].n for it in tg]
        self.plain["tg"] = [" | ".join(f"{c[1]}: {v.plain}" for c, v in zip(WCOLS, cells) if v.plain not in ("", "-")) for cells in rows]
        self.paint(self.tt, WCOLS, rows, self.st["tg"])
        act = sum(1 for it in tg if it[3]); ft = self.ftab()
        self.lw.update(f"{'▶ ' if ft == 'wl' else ''}WATCHLIST ({len(wl)}) | {DROP_WATCH * 100:g}% to {DROP_MAX * 100:g}% below the 24h high, or stalled at the high {STALL_MIN // 60}m+ "
                       f"| pool: top {POOL_N} by 24h peak gain | sort: {self.lname('wl')}")
        self.lt_.update(f"{'▶ ' if ft == 'tg' else ''}TRIGGERED ({act} active, {len(tg)} this session) | {DROP_TRIG * 100:g}%+ below high + asks > bids + sells > buys "
                        f"(band ±{BAND[0]}%, CVD {WL[self.win]}) | sort: {self.lname('tg')} | x clears ended | logs: {EV_LOG}, {OUT_LOG}, {DB_FILE}")

    def _tick(self):
        now = time.time(); xs = list(SY.values()); self.k += 1
        for x in xs:   # watched/visible coins every second, the rest every 3rd second (saves CPU)
            if x.mode == "live" and (x.n in PRI or not x.live or (x.id + self.k) % 3 == 0): metrics(x, now)
        self.track(now); self.sync_footer()
        dt = max(now - self.ft, 0.001); fm = (TOT["msgs"] - self.fm) / dt; fb = (TOT["bytes"] - self.fb) / dt
        self.ft, self.fm, self.fb = now, TOT["msgs"], TOT["bytes"]
        lv = [x for x in xs if x.mode == "live"]; sc = [x for x in xs if x.mode == "scan"]
        lok = sum(x.ok for x in lv); sok = sum(1 for x in sc if x.snap_t and x.ok)
        stale = sum(1 for x in lv if x.ok and now - x.lev > 60)
        warn = "" if stale < max(3, lok * 0.2) else f" | !! DEPTH STREAM NOT UPDATING ({stale} live books stale)"
        dn = lambda k: "UP" if W.get(k) else f"DOWN {int(now - STAT['down'].get(k, now))}s"
        rc = f" | pub {dn('pub')} mkt {dn('mkt')} | drops {STAT['n']} (failed connects {STAT['fail']})"
        if STAT["last"]: rc += f" | last: {STAT['last']}"
        rc += f" | dns: {'DoH ok' if DNS['ok'] else 'DoH FAILED (system dns)' if DNS['ok'] is False else '-'}"
        if STAT["rec"]: rc += f" | recovered {STAT['rec']} trades on {STAT['rcoins']} coins"
        pins = f" | pinned: {','.join(n[:-4] for n in self.watch)}" if self.watch else ""
        self.hd.update(
            f"Binance USDT-M perps ≥${MIN_VOL/1e6:.0f}M | tracking {len(xs)} | live {lok}/{len(lv)} | scanned {sok}/{len(sc)} | "
            f"CVD: {WL[self.win]} | band ±{BAND[0]}% | sort: {SORTS[self.sk][0]} {'▼' if self.desc else '▲'}"
            f" | pool {len(POOL)} | focus: {FOCUS[self.ftab()]} (Tab switches) | feed {fm:.0f} msg/s {fb/1024:.0f} KB/s{pins}{rc}{warn}{' | first scan in progress...' if sok < len(sc) else ''}")
        j = 0; allit = []
        for x in xs:
            if not x.live: continue
            r = (x.cln[0][j] + 1) / (x.cln[1][j] + 1); rr = (x.raw[0][j] + 1) / (x.raw[1][j] + 1)
            fl = flow(x, self.win, now); cvd = None if fl is None else fl[0] - fl[1]
            allit.append((x, r, rr, fl, cvd, j))
        by = {it[0].n: it for it in allit}
        _, sf, flt = SORTS[self.sk]
        items = [it for it in allit if flt is None or flt(it)]
        items.sort(key=lambda it: sf(it, now), reverse=self.desc)
        rank_all = {it[0].n: i + 1 for i, it in enumerate(items)}
        top = [it for it in items if it[0].n not in self.watch][:TOP_N]
        if now - self.ht > 5: self.hist.append(rank_all); self.ht = now
        ref = self.hist[0] if self.hist else {}
        names = {it[0].n for it in top}
        if self.since is None: self.since = {n: now - 999 for n in names}
        for n in names: self.since.setdefault(n, now)
        for n in [n for n in self.since if n not in names]: del self.since[n]
        disp = [(by[n], True) if n in by else (n, True) for n in self.watch] + [(it, False) for it in top]
        WANT.clear(); WANT.update(it[0].n for it in items[:TOP_N + LIVE_EXTRA]); WANT.update(self.watch)
        WANT.update(it[0].n for it in sorted(allit, key=lambda it: max(it[1], 1 / it[1]), reverse=True)[:TOP_N + LIVE_EXTRA])
        PRI.clear(); PRI.update(self.watch); PRI.update(it[0].n for it, p in disp if not isinstance(it, str)); PRI.update(POOLSET); PRI.update(KEEP)
        rows = []; plain = []; order = []
        for it, pinned in disp:
            if isinstance(it, str):
                cells = self.holder(it, "syncing..." if it in SY else "not tracked"); order.append(it)
            else:
                (x, r, rr, fl, cvd, _) = it; order.append(x.n)
                part = BAND[0] / 100.0 > x.cov + 1e-9
                rk = rank_all.get(x.n); d = (ref.get(x.n, rk) - rk) if rk else 0
                arrow = Text(f"↑{d}", style="green") if d > 0 else Text(f"↓{-d}", style="red") if d < 0 else Text("")
                new = (not pinned) and now - self.since.get(x.n, now - 999) < 30 and now - self.t_start > WARM
                coin = Text(("★" if pinned else "") + x.n[:-4] + (" NEW" if new else ""),
                            style="bold yellow" if (new or pinned) else "bold")
                age = now - x.snap_t
                src = Text("sync", style="yellow") if x.mode == "live" and not x.ok else Text("fill", style="cyan") if x.mode == "live" and x.gapfill \
                    else Text("LIVE", style="green") if x.mode == "live" \
                    else Text(f"{int(age)}s" if age < 120 else f"{int(age//60)}m", style="dim")
                ch = Text(f"{x.chg:+.1f}%", style="green" if x.chg >= 0 else "red")
                if x.dir:
                    s = now - x.t0; stk = Text(f"{'▲' if x.dir > 0 else '▼'} {int(s//60)}m{int(s%60):02d}s" + ("~" if x.stale or x.gapfill else ""),
                                               style="green" if x.dir > 0 else "red")
                else: stk = Text("-", style="dim")
                if fl is None: buys = sells = cv = Text("-", style="dim")
                else:
                    buys = Text(f"${M(fl[0])}", style="green"); sells = Text(f"${M(fl[1])}", style="red")
                    ok = "" if not x.dir else (" AGREE" if (cvd > 0) == (x.dir > 0) else " CONFLICT")
                    cv = Text(f"{'+' if cvd >= 0 else '-'}${M(abs(cvd))}{ok}", style="green" if cvd >= 0 else "red")
                if x.oih:
                    oi = Text(f"${M(x.oih[-1][1] * x.mid)}"); od = oichg(x)
                    oc = Text(f"{od*100:+.1f}%", style="green" if od >= 0 else "red") if oiref(x, 300, now) else Text("-", style="dim")
                else: oi = oc = Text("-", style="dim")
                pos = pos_label(x, now) if x.oih else Text("warming up", style="dim")
                bw = Text(f"${M(x.bm[0][0])} @ {x.bm[0][1]:.8g}", style="green")
                aw = Text(f"${M(x.bm[1][0])} @ {x.bm[1][1]:.8g}", style="red")
                lc = Text("-", style="dim")
                if x.lc and now - x.lc[0] < 180:
                    lc = Text(f"{'B' if x.lc[3] == 1 else 'A'} ${M(x.lc[1])}@{x.lc[2]:.8g} {int(now-x.lc[0])}s ago")
                rec = [q for q in x.spoof if now - q < 600]
                sp = Text(f"SPOOF {len(rec)}x ${M(x.sp[1])}@{x.sp[2]:.8g}", style="bold yellow") if rec else Text("")
                gp = Text("")
                if x.dead or x.gapfill or x.tinfo or x.binfo: gp = Text("recovering missed data...", style="cyan")
                elif x.gapinfo and now - x.gapinfo[0] < 900: gp = Text(x.gapinfo[1])
                cells = [Text(str(rk) if rk else "-"), arrow, coin, src, ch, Text(f"${M(x.vol)}"),
                         Text(f"${M(x.raw[0][j])}" + ("~" if part else "")), Text(f"${M(x.raw[1][j])}" + ("~" if part else "")),
                         RT(rr, part), RT(r, part), stk, buys, sells, cv, oi, oc, pos, bw, aw, lc, sp, gp]
            rows.append(cells)
            plain.append(" | ".join(f"{c[1]}: {v.plain}" for c, v in zip(COLS, cells) if v.plain not in ("", "-")))
        self.names["main"] = order; self.plain["main"] = plain
        self.paint(self.tm, COLS, rows, self.st["main"])
        self.show_small(now)


if __name__ == "__main__":
    Fetcher().run()
