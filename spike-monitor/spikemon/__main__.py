"""Command line entry point: python -m spikemon [options]"""

from __future__ import annotations

import argparse
import asyncio
import sys

from .config import load_config


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="spikemon",
        description="Binance USDT-M perpetual 15m spike monitor (practice mode unless --live).",
    )
    p.add_argument("--config", default="config.yaml", help="path to config.yaml")
    p.add_argument("--db", help="override SQLite path")
    p.add_argument("--live", action="store_true", help="send Telegram alerts (needs token and chat id)")
    p.add_argument("--no-ui", action="store_true", help="plain scrolling output instead of the dashboard")
    p.add_argument("--follow", action="store_true",
                   help="alerts-only live view: tail alerts.log (run in a second window)")
    p.add_argument("--check", action="store_true", help="test Binance and Telegram connectivity, then exit")
    p.add_argument("--duration", type=float, help="stop automatically after N seconds")
    p.add_argument("--max-symbols", type=int, help="development: only the N highest-volume coins")
    args = p.parse_args(argv)

    overrides = {"storage": {"db_path": args.db}} if args.db else None
    cfg = load_config(args.config, overrides)

    if args.follow:
        from .follow import follow
        return follow(cfg.storage.alert_log_path)

    if args.check:
        from .check import run_check
        return 1 if asyncio.run(run_check(cfg)) else 0

    if args.live and not (cfg.alerts.telegram_token and cfg.alerts.telegram_chat_id):
        print("--live needs a Telegram token and chat id (config.yaml or the "
              "TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID environment variables). "
              "See the README, or run without --live for practice mode.", file=sys.stderr)
        return 2

    from .app import App

    async def _go() -> None:
        app = App(cfg, live=args.live, ui=not args.no_ui, duration=args.duration,
                  max_symbols=args.max_symbols)
        await app.run()

    try:
        asyncio.run(_go())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
