# Crypto Spike Monitor

A tool that watches the crypto futures market and tells you when a coin makes a sudden big move.

## What it does

It watches about 520 coins on Binance (USDT perpetual futures) around the clock. When a coin jumps or drops fast, you get an alert.

- Big coins (over $1 billion traded per day) trigger an alert at a 3% move.
- Smaller coins trigger an alert at a 5% move.
- Moves are measured inside standard 15-minute windows.

## What an alert tells you

- How big the move is and how unusual it is for that coin (z-score)
- Who was buying and who was selling (order flow)
- Any very large orders that pushed the price
- Forced sell-offs (liquidations) and large orders sitting in the market (order book walls)

## What happens to the data

Every alert is saved to a database, along with the price in the minutes that followed. A separate analysis tool then checks one simple question: after a spike, does the price keep going or reverse, and is it still worth trading after fees?

## How to run it

1. Install Python, then run: pip install -r requirements.txt
2. Check your connection: python -m spikemon --check
3. Start it: python -m spikemon

Press Ctrl+C to stop. Alerts are also saved to a file called alerts.log. Open a second window and run python -m spikemon --follow to watch them arrive live.

No account or paid service is needed. The market data is free and public.

## Results

[Add your own numbers here once you have them, for example: "Ran for X days, recorded Y alerts. After fees, spikes showed no reliable edge." Only write what you measured.]

## Good to know

- This is a research tool. It does not place trades.
- Nothing here is financial advice.
- Moves like these are rare, so the tool can go quiet for a while. That's normal.
- Telegram phone alerts are optional and need a free bot set up first.