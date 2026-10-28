MUST READ: STRATEGY README
==========================

This file explains the idea behind the Watchlist and Triggered tables and how
I use them. It is short on purpose. For every key and column, see README.txt.


THE IDEA
--------
It is hard to find an edge in an open market. Most ideas stop working once you
count fees and slippage, so I do not trust any idea until the data shows it.

Here is the idea I am testing. On crypto, coins that spike hard in one day
often give a big part of that move back soon after. Yesterday's top gainers
are often among the top losers a day or a week later. This is called mean
reversion: price tends to come back toward where it came from.

This program watches for it. It does not just track the market. It keeps a
Watchlist of coins that spiked and have started to fall back, and a Triggered
table for the ones that are already doing it with sellers in control.


WHAT IS PROVEN AND WHAT IS NOT
------------------------------
- I have seen this pattern many times while watching the market.
- I have NOT yet measured how often it happens. I have not run a backtest, so
  I do not claim a win rate or a percentage.
- Some spikes keep going up. That is why the program waits for the drop to
  start before it shows a coin.
- The program saves every coin that enters the Watchlist or Triggered table,
  and what the price did 1, 5, 15 and 60 minutes later (files:
  watchlist_events.csv, watchlist_outcomes.csv and watchlist.db). That is the
  data needed to test the idea honestly.

RESULTS SO FAR: (add your own numbers here after you analyze the saved files:
how many events, how often price was lower after 15 and 60 minutes, and the
average and worst move. Leave nothing here that you did not measure.)


WHAT THE PROGRAM DOES NOT DO
----------------------------
It does not place trades. It has no trading code. It only watches, shows and
saves notes. Every decision and every order is made by the person using it.


HOW THE TWO TABLES DECIDE
-------------------------
Pool: the 30 coins (above $3M daily volume) that rose the most in 24 hours.

WATCHLIST: a pool coin that is 3% to 15% below its 24h high, or that has sat
at its high for an hour or more (a "stall").

TRIGGERED: a pool coin that is 5% or more below its 24h high AND has more
sell orders than buy orders waiting AND more market selling than buying.


HOW TO USE IT
-------------
Step 1. Run the program. Press b, type 8, 9 or 10, press Enter. Wait 15 to 20
        minutes. The program needs this time to build its numbers (open
        interest, walls, streaks, flow). A wide band sees more of the book.
        On very large coins the book may not reach that far. Those numbers
        show a "~" and are partial.

Step 2. Look at the Watchlist and the Triggered table. Press Tab to move
        between the tables.

Step 3. Sort by the most recent peak. Press Tab to pick a table, press o, then
        choose "Peak ago (most recent first)". The newest spikes are on top.

Step 4. Look at "To -3%" and "To -5%". These show how long after the peak the
        coin took to fall 3% and then 5%. The gap between them is To -5%
        minus To -3%. My rule: I want at least 5 minutes between them. If the
        two times are very close together, it is not a good one for me.

Step 5. Check "-3% ago" and "-5% ago" to see how long ago those drops
        happened. Also check "Since peak" to see how far from the peak the
        coin is now. My sweet spot is between 7% and 10% below the peak, in
        either table.

Step 6. Open the coin (press Enter on its row) and then look at the real
        chart on the exchange. Do not trust the numbers alone:
        - The percentages and times come from 1-minute candles. They are
          accurate to about a minute.
        - Order book data is open data from Binance, and it only shows total
          size at each price. It cannot show who is behind an order.
        - CVD only counts Binance futures trades that the program saw. On
          very busy, high-volume coins it can be off or miss part of the
          flow. Use it as a guide, then confirm on the chart.

Step 7. Use risk management. See below.


RISK MANAGEMENT (PLAIN VERSION)
-------------------------------
- Use small size. Never risk money you cannot afford to lose.
- Decide where you are wrong BEFORE you enter, and place a stop there.
- A coin that spiked can spike again. Shorting a spike can lose a lot very
  fast if price squeezes upward. Leverage makes this worse.
- Do not add to a losing trade.
- Count fees and slippage. A small win can disappear after costs.


NOTES ABOUT MY RULES
--------------------
The numbers in the steps (8 to 10% band, 5 minute gap, 7 to 10% sweet spot) are
my own rules of thumb. They are not proven. The program saves its data so that
they can be tested and changed.

This is not financial advice. It is a tool for looking at the market.
