# spikemon

Streams every Binance USDT-M crypto perpetual and alerts when a coin moves 3% (24h volume of $1B or more) or 5% (below $1B) inside a clock-aligned 15 minute interval. Each alert carries the interval return, buy/sell flow, volume against the coin's own pace, a robust z-score, the largest orders, liquidations and order book walls. Everything is stored in SQLite, and a separate script runs an event study on the stored spikes.

No API key, no account, no cost. Market data is public.

## Run it

```
pip install -r requirements.txt
python -m spikemon --check          # tests Binance (REST and both websockets) from your machine
python -m spikemon                  # practice mode: detects and stores, sends nothing
python -m spikemon --no-ui          # plain scrolling output instead of the dashboard
python -m spikemon --live           # also sends Telegram alerts
```

It runs until you press Ctrl+C, which flushes the database before exiting. Closing the window or sleeping the laptop stops it. On reconnect, the downtime is written to the `gaps` table and affected intervals are marked `partial`.

## Telegram (after you are happy in practice mode)

1. In Telegram, message `@BotFather`, send `/newbot`, copy the token.
2. Send any message to your new bot, then open `https://api.telegram.org/bot<TOKEN>/getUpdates` and copy `chat.id`.
3. Put both in `config.yaml` (or `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`), run `python -m spikemon --check`, then `--live`.

Telegram allows about 1 message per second per chat. A few queued alerts go out as one message and a storm becomes one digest of the strongest, ranked by z-score. Every alert is still stored.

## Alert log

Every alert's full text is appended to `alerts.log` (path set in `config.yaml`) the moment it fires, in all modes including practice. Open it while the monitor runs. To follow it live in cmd: `powershell -command "Get-Content alerts.log -Wait -Tail 30"`.

Two windows work well together: run `python -m spikemon` (dashboard) in one and `python -m spikemon --follow` in the other. `--follow` prints the newest alerts from `alerts.log` and then each new one as it fires. The dashboard also has a "Latest alert" panel showing the full text of the newest alert.

## Definitions

- **Interval**: UTC clock boundaries, like any 15m bar. Move = live price versus the interval's first price.
- **Trigger**: fires the moment the threshold is crossed, not at interval end. One alert per coin per interval. If both directions cross the trigger in the same interval, one **VERY VOLATILE** alert follows.
- **Tier**: from live 24h quote volume, so a coin moves tier automatically.
- **z-score**: move divided by `1.4826 * median(|15m open-to-close return|)` over the last 96 intervals, excluding the current one. A median is used so earlier spikes cannot inflate the baseline, and 1.4826 makes it equal the standard deviation for normal data. Crypto returns are fat-tailed, so treat z as a ranking, not a probability. Coins with under 20 intervals of history show `z n/a`. Known limitation: intraday volatility seasonality is not modelled. Per-interval data is stored in `intervals` to add it later.
- **Volume pace**: interval volume so far divided by the coin's median interval volume, scaled for time elapsed.
- **Top orders / 5% flag**: largest aggregated trades in the interval. Flagged if 5% or more of the interval's volume. These are aggTrade prints, not resting orders.
- **Walls**: watchlist coins only, from the top 20 book levels. A wall is a level at least 10x the median level and above a dollar floor. It must persist two snapshots. Vanishing walls are labelled `eaten` (traded through) or `pulled` (cancelled).
- **Liquidations**: `forceOrder` stream, split into longs and shorts liquidated.

## Scale and limits

All crypto perpetuals get a trade subscription, and one all-coin ticker stream feeds prices. Subscriptions are paced under Binance's 10 incoming messages per second per connection, and the REST client tracks `X-MBX-USED-WEIGHT-1M` and backs off on 429/418. Startup pulls about 24h of 15m klines per coin, a minute or two of requests. Spike alerts begin after history loads. Binance routes futures streams over separate `/market` and `/public` endpoints since 2026-03-06, and the legacy URLs were retired on 2026-04-23, so the monitor uses one connection for each.

I could not test against live Binance from the build environment. Run `--check` first. Parts that depend on exact exchange behaviour (for example the all-coin ticker stream) have fallbacks: if the ticker stream is silent, the monitor polls REST instead.

## Event study

```
python analysis/event_study.py --db spikemon.sqlite3
```

Entry is the price 5 seconds after the alert, so the triggering move is never counted. Exits at +1m, +5m, interval close and +30m. Returns are direction-adjusted (positive means continuation), BTC-adjusted, and net of configurable fees and slippage (`--fee-bps`, `--slip-bps-*`). Confidence intervals come from a cluster bootstrap that resamples whole 15m intervals, since spikes arrive together. Slices under 30 events say "not enough data". The number of slices tested is printed, because about 5% will look significant by chance. A first-half versus second-half check shows whether a pattern persists. Output is the terminal, `analysis_output/slices.csv` and `analysis_output/summary.md`, including latency percentiles and gap counts. Expect to need 2 to 4 weeks of data.

`analysis/simulate_db.py` generates **synthetic** data to test the pipeline. Its output is labelled as synthetic and none of it is a finding.

## Tests

```
python -m pytest tests
```

Unit tests cover the engine, walls, alerts, storage, ranker and analysis. An end-to-end test runs the full app against a scripted fake exchange.

## Database tables

`spikes`, `big_orders`, `followups`, `intervals`, `wall_events`, `gaps`, `watchlist_changes`, `health`, `runs`. Every spike stores receive, detect and sent timestamps, so the 2 second alert claim can be measured from your own run.
