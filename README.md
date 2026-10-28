# Crypto Volatility Lab

Crypto futures research in Python: a mean-reversion watchlist that tracks 1m to 60m outcomes after every signal, and a real-time spike monitor that uses order flow, liquidations and order book walls to separate real spikes from liquidation-driven moves and flag ultra-volatile coins. Includes an event-study script and a test suite.

Built on Binance USDT-M perpetuals using public market data (no API keys needed).

## Projects

### [Mean-Reversion Watchlist](mean-reversion-watchlist/)
Scans every coin above $3M daily volume, tracks live order books and trade flow, and flags coins that spiked and have started to fall back. Every flagged event is logged with its 1m, 5m, 15m and 60m outcome so the idea can be tested rather than assumed.

### [Spike Monitor](spike-monitor/)
Streams all Binance perpetuals and alerts on 3% (large coins) or 5% (smaller coins) moves inside 15-minute intervals. Each alert includes buy/sell flow, volume vs. normal pace, a robust z-score, largest orders, liquidations and order book walls. An event-study script adds fees, slippage and cluster-bootstrap confidence intervals.

## Skills shown
Python (asyncio, websockets, pandas, SQLite), order book and order flow analysis, statistical testing, automated testing.

For more information on how to use this, read the README files within the files.

## Contact
Jefferson Konu · linkedin.com/in/ebunoluwa-konu-aat-308306244 · williamkonu23@gmail.com
