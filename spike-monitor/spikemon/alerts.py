"""Alert formatting and Telegram delivery.

Style is quant, not chart: plain figures only (interval return, signed flow,
volume versus the coin's own pace, z-score). No candles or sparklines.
Telegram allows about 1 message per second per chat, so queued alerts are
bundled: a few alerts go out as one message, a storm becomes one digest.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import aiohttp

NO_PRIORITY = 1e9


def usd(x: float) -> str:
    a = abs(x)
    sign = "-" if x < 0 else ""
    if a >= 1e9:
        return f"{sign}${a / 1e9:.2f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.1f}M"
    if a >= 1e3:
        return f"{sign}${a / 1e3:.0f}K"
    return f"{sign}${a:.0f}"


def sgn_usd(x: float) -> str:
    return ("+" if x >= 0 else "-") + usd(abs(x))


def pct(x: float, d: int = 1) -> str:
    return f"{x * 100:+.{d}f}%"


def px(p: float) -> str:
    return f"{p:.5g}"


def hhmm(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M")


def format_alert(ev: dict) -> str:
    start = ev["interval_start_ms"]
    mins = ev["interval_ms"] // 60_000
    win = f"{mins}m interval {hhmm(start)}-{hhmm(start + ev['interval_ms'])} UTC, minute {ev['minute']:.1f}"
    tier = ">=$1B" if ev["tier"] == "ge1b" else "<$1B"
    sym = ev["symbol"]
    lines = []
    if ev["kind"] == "VOLATILE":
        lines.append(f"⚠️ VERY VOLATILE {sym} | {win}")
        lines.append(
            f"Range vs open: {pct(ev['high_ret'])} high / {pct(ev['low_ret'])} low | now {pct(ev['ret'])}"
        )
    else:
        lines.append(f"⚡ {sym} {pct(ev['ret'])} | {win}")
    lines.append(f"Tier {tier} (trigger {ev['threshold'] * 100:g}%) | 24h vol {usd(ev['vol24h'])}")
    if ev.get("z") is not None and ev.get("sigma"):
        zt = f"z {ev['z']:+.1f} (typical {mins}m move {ev['sigma'] * 100:.2f}%)"
    else:
        zt = "z n/a (short history)"
    lines.append(f"Open {px(ev['open'])} → {px(ev['price'])} | {zt}")
    net = ev["buy_notional"] - ev["sell_notional"]
    lines.append(
        f"Net flow {sgn_usd(net)} (buy {usd(ev['buy_notional'])} / sell {usd(ev['sell_notional'])})"
    )
    if ev.get("vol_mult") is not None:
        lines.append(f"Interval volume {usd(ev['interval_volume'])} | {ev['vol_mult']:.1f}x normal pace")
    else:
        lines.append(f"Interval volume {usd(ev['interval_volume'])}")
    orders = ev.get("top_orders", [])[: ev.get("n_top_shown", 3)]
    if orders:
        parts = []
        for o in orders:
            s = f"{o['side']} {usd(o['notional'])} ({o['pct_of_volume']:.0f}% of vol)"
            if o["flagged"]:
                s += " 🏷️"
            parts.append(s)
        extra = ev.get("flagged_5pct", 0) - sum(o["flagged"] for o in orders)
        tail = f" (+{extra} more 5%+)" if extra > 0 else ""
        lines.append("Top orders: " + " | ".join(parts) + tail)
    liq = []
    if ev.get("liq_short", 0) > 0:
        liq.append(f"{usd(ev['liq_short'])} shorts")
    if ev.get("liq_long", 0) > 0:
        liq.append(f"{usd(ev['liq_long'])} longs")
    if liq:
        lines.append("Liquidations: " + ", ".join(liq))
    walls = ev.get("walls") or []
    if walls:
        lines.append(
            "Walls: "
            + "; ".join(
                f"{w['side']} {usd(w['notional'])} @ {px(w['price'])} {w['kind']}" for w in walls
            )
        )
    if ev.get("partial"):
        lines.append("Note: partial interval (monitor started or reconnected mid-interval)")
    return "\n".join(lines)


def priority_key(ev: dict):
    z = ev.get("z")
    strength = abs(z) if z is not None else abs(ev["ret"]) * 100
    return (0 if ev["kind"] == "VOLATILE" else 1, -strength)


def format_digest(events: list[dict], max_lines: int = 8) -> str:
    ordered = sorted(events, key=priority_key)
    head = f"📊 {len(events)} spike alerts queued (top {min(max_lines, len(ordered))} by strength)"
    rows = []
    for ev in ordered[:max_lines]:
        z = f"z{ev['z']:+.1f}" if ev.get("z") is not None else "z n/a"
        net = ev["buy_notional"] - ev["sell_notional"]
        tag = "VOLATILE " if ev["kind"] == "VOLATILE" else ""
        rows.append(f"{tag}{ev['symbol']} {pct(ev['ret'])} {z} flow {sgn_usd(net)} min {ev['minute']:.1f}")
    rest = len(ordered) - len(rows)
    if rest > 0:
        rows.append(f"+{rest} more (all stored in the database)")
    return head + "\n" + "\n".join(rows)


class Alerter:
    def __init__(self, cfg, on_sent, live: bool = False, post=None, clock=time.time):
        a = cfg.alerts
        self.live = live
        self.min_interval = float(a.min_interval_s)
        self.digest_threshold = int(a.digest_threshold)
        self.digest_max = int(a.digest_max_lines)
        self.token = a.telegram_token
        self.chat = a.telegram_chat_id
        self.on_sent = on_sent
        self.clock = clock
        self._post = post or self._telegram_post
        self._pending: list[dict] = []
        self._wake = asyncio.Event()
        self._next_ok = 0.0
        self._session: aiohttp.ClientSession | None = None
        self.sent_msgs = 0
        self.sent_events = 0
        self.digests = 0
        self.failed = 0
        self.retries = 0
        self.last_error: str | None = None

    def submit(self, ev: dict) -> None:
        self._pending.append(ev)
        self._wake.set()

    @property
    def backlog(self) -> int:
        return len(self._pending)

    async def run(self) -> None:
        while True:
            await self._wake.wait()
            if self.live:
                wait = self._next_ok - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
            self._wake.clear()
            batch, self._pending = self._pending, []
            if batch:
                await self._deliver(batch)

    async def drain(self, timeout: float = 3.0) -> None:
        """Best-effort flush on shutdown."""
        if self._pending:
            batch, self._pending = self._pending, []
            try:
                await asyncio.wait_for(self._deliver(batch), timeout)
            except Exception:
                pass

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def _deliver(self, batch: list[dict]) -> None:
        batch.sort(key=priority_key)
        if len(batch) >= self.digest_threshold:
            text, kind = format_digest(batch, self.digest_max), "digest"
            self.digests += 1
        else:
            text = "\n\n".join(format_alert(e) for e in batch)[:4000]
            kind = "alert"
        if not self.live:
            self.sent_msgs += 1
            self.sent_events += len(batch)
            self.on_sent(batch, int(self.clock() * 1000), "practice")
            return
        ok = await self._send_with_retry(text)
        self._next_ok = time.monotonic() + self.min_interval
        self.sent_msgs += 1 if ok else 0
        self.sent_events += len(batch) if ok else 0
        if not ok:
            self.failed += 1
        self.on_sent(batch, int(self.clock() * 1000), kind if ok else "failed")

    async def _send_with_retry(self, text: str) -> bool:
        for _ in range(3):
            ok, retry_after, err = await self._post(text)
            if ok:
                return True
            self.last_error = err
            if retry_after:
                self.retries += 1
                await asyncio.sleep(min(float(retry_after), 30))
                continue
            await asyncio.sleep(0.5)
        return False

    async def _telegram_post(self, text: str):
        if self._session is None:
            from .binance import make_session
            self._session = make_session(10)
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with self._session.post(
                url,
                json={"chat_id": self.chat, "text": text, "disable_web_page_preview": True},
            ) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200 and data.get("ok"):
                    return True, None, None
                retry = (data.get("parameters") or {}).get("retry_after") if resp.status == 429 else None
                return False, retry, f"HTTP {resp.status}: {str(data)[:150]}"
        except Exception as exc:
            return False, None, repr(exc)
