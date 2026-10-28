"""Configuration: defaults, YAML overlay, environment overrides for Telegram."""

from __future__ import annotations

import copy
import os
from types import SimpleNamespace

import yaml

DEFAULTS: dict = {
    "exchange": {
        "rest_base": "https://fapi.binance.com",
        # Binance split the USDT-M futures websockets into /public, /market and
        # /private paths (legacy URLs retired 2026-04-23). aggTrade, ticker and
        # forceOrder live on /market; depth lives on /public.
        "ws_market": "wss://fstream.binance.com/market",
        "ws_public": "wss://fstream.binance.com/public",
    },
    "universe": {
        "quote": "USDT",
        # Binance's own classification. Crypto only.
        "underlying_types": ["COIN"],
        # Sub-types that mark non-crypto listings (stocks, ETFs, commodities, indices).
        "exclude_subtypes": ["TRADFI", "STOCK", "EQUITY", "ETF", "COMMODITY", "INDEX", "FX"],
        "exclude": [],
        "btc_symbol": "BTCUSDT",
    },
    "spike": {
        "interval_minutes": 15,
        "big_tier_volume_usd": 1_000_000_000,
        "big_tier_threshold_pct": 3.0,
        "small_tier_threshold_pct": 5.0,
        "history_intervals": 96,
        "min_baseline_intervals": 20,
        "order_flag_pct_of_volume": 5.0,
        "top_orders": 3,
        "volatile_upgrade": True,
        "roll_grace_s": 2.0,
    },
    "watchlist": {
        "top_volume": 20,
        "top_gainers": 25,
        "top_losers": 25,
        "min_volume_usd_movers": 20_000_000,
        "hysteresis_extra": 5,
        "refresh_seconds": 5,
        "forced_hold_minutes": 60,
    },
    "walls": {
        "enabled": True,
        "levels": 20,
        "update_ms": 500,
        "min_usd_big": 1_000_000,
        "min_usd_small": 150_000,
        "median_multiple": 10,
        "confirm_snapshots": 2,
        "eaten_fraction": 0.5,
    },
    "followups": {"horizons_s": [1, 5, 30, 60, 300, 900, 1800]},
    "alerts": {
        "telegram_token": "",
        "telegram_chat_id": "",
        "min_interval_s": 1.1,
        "digest_threshold": 4,
        "digest_max_lines": 8,
    },
    "storage": {"db_path": "spikemon.sqlite3", "batch_seconds": 0.5, "retention_days": 30,
                "alert_log_path": "alerts.log"},
    "ui": {"refresh_hz": 2, "feed_lines": 12, "watch_rows": 18, "leader_rows": 8},
    "runtime": {
        "history_concurrency": 8,
        "subscribe_chunk": 100,
        "subscribe_pause_s": 0.12,
        "stale_stream_s": 30,
        "reconnect_max_s": 30,
        "max_connection_age_s": 82800,
        "rest_weight_soft_limit": 1800,
    },
}


class Cfg(SimpleNamespace):
    """Attribute-style config node."""


def _merge(base: dict, over: dict) -> dict:
    for key, val in over.items():
        if isinstance(val, dict) and isinstance(base.get(key), dict):
            _merge(base[key], val)
        else:
            base[key] = val
    return base


def _ns(d: dict) -> Cfg:
    return Cfg(**{k: _ns(v) if isinstance(v, dict) else v for k, v in d.items()})


def to_dict(node) -> dict:
    if isinstance(node, SimpleNamespace):
        return {k: to_dict(v) for k, v in vars(node).items()}
    return node


def load_config(path: str | None = None, overrides: dict | None = None) -> Cfg:
    data = copy.deepcopy(DEFAULTS)
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            _merge(data, yaml.safe_load(fh) or {})
    if overrides:
        _merge(data, overrides)
    env_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    env_chat = os.environ.get("TELEGRAM_CHAT_ID")
    if env_token and not data["alerts"]["telegram_token"]:
        data["alerts"]["telegram_token"] = env_token
    if env_chat and not data["alerts"]["telegram_chat_id"]:
        data["alerts"]["telegram_chat_id"] = env_chat
    return _ns(data)
