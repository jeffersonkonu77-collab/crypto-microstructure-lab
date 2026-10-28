"""spikemon: Binance USDT-M perpetual futures 15-minute spike monitor.

Streams trades for every crypto perpetual, detects interval-return spikes
(3% for coins with 24h volume >= $1B, 5% otherwise), tags them with order flow,
liquidations and order book walls, stores everything in SQLite, and alerts via
Telegram. A separate analysis script runs an event study on the stored data.
"""

__version__ = "1.0.0"
