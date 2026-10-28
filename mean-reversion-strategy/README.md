FETCHER: BINANCE FUTURES ORDER BOOK SCANNER
============================================

WHAT IT IS
----------
This program watches every Binance USDT futures coin that trades more than
$3 million a day. It shows:
  - who is waiting to buy and who is waiting to sell (the order book)
  - who is buying and selling right now (the trades)
  - which coins are one-sided, and for how long

It ranks the 20 most one-sided coins in the top table. Under that it has two
extra tables, Watchlist and Triggered, that follow coins which spiked and are
now falling back from their daily high.

IMPORTANT: this program does NOT place trades. It only watches, shows and
saves notes. There is no trading code in it.

Read MUST_READ_STRATEGY_README.txt to see how the Watchlist idea is used.


WORDS USED IN THIS FILE
-----------------------
Order book  The list of orders waiting to be filled.
Bids        Orders waiting to BUY (below the price).
Asks        Orders waiting to SELL (above the price).
Band        How far from the price we look, in percent. Band 1 means we only
            count orders within 1% above and below the price.
Ratio       Bids compared with asks. "2.0:1 up" means twice as many dollars
            waiting to buy as to sell.
Wall        One very big order sitting in the book.
Spoof       A big order that gets pulled right before the price reaches it.
            It looks like support or resistance, but it was fake.
Buys/Sells  Market orders that hit the book right now (people who want in or
            out immediately).
CVD         Buys minus Sells. Positive = more buying. Negative = more selling.
Streak      How long the book has stayed one-sided.
Open interest (OI)  The total size of open futures trades.
24h high    The highest price in the last 24 hours. Also called the peak.
Liquidation A forced close of a leveraged trade that ran out of margin.
Pin         Keep a coin at the top of the table so you can follow it.


HOW TO RUN
----------
1. Install Python 3.10 or newer.
2. Install the packages:      pip install textual websockets aiohttp
3. Start the program:         python fetcher.py
   (On Windows, use "py" if "python" or "pip" is not recognized.)
4. Wait 2 to 3 minutes while the order books load. Use a wide or maximized
   window, because the tables have many columns.


WHAT YOU SEE ON THE SCREEN
--------------------------
Top line (header)   Status: how many coins are tracked, CVD window, band,
                    sort, which table has focus, connection health.
Top table           The 20 coins ranked by the current sort.
WATCHLIST table     Coins falling back from their daily high (see below).
TRIGGERED table     Coins that fit the stricter rules (see below).
Search box          Type a coin name to pin it.
Bottom line         The keys you can press right now.


COMMANDS
--------
s            Show Buys, Sells and CVD since the current streak started
             (this is the default).
1            Show Buys, Sells and CVD for the last 1 minute.
5            Show Buys, Sells and CVD for the last 5 minutes.
b            Set the band. A small box opens. Type a whole number from 1 to
             10 (percent of price) and press Enter. This changes Bids$, Asks$,
             Ratio, Clean, walls and the streak. Changing it restarts walls
             and streaks, so give it a few minutes to rebuild.
Tab          Move to the next table (top 20, Watchlist, Triggered).
Shift+Tab    Move to the previous table.
o            Open the sort menu for the table that has focus. Use the up and
             down arrows, then Enter. Esc cancels.
r            Reverse the sort of the focused table (high to low becomes low
             to high). Clicking the same column title again does the same.
u            Put the focused table's sort back to its default. This key only
             shows at the bottom when that table's sort is not the default.
Enter        Open the full detail screen for the highlighted row.
d            Open the detail screen for any coin by name. Type for example
             "aria" and press Enter. The coin does not need to be pinned.
x            Clear the "ended" rows from the Triggered table (coins that
             qualified earlier but no longer fit). Active rows stay.
/            Search for a coin. Type a name and press Enter to pin it (a star
             shows next to it). Search the same coin again to unpin it. Press
             Enter on an empty search to remove all pins. You can pin up to 8.
             Pins are saved for next time. Esc returns to the table.
c            Copy the highlighted row as text. If the clipboard fails, it is
             saved to copied.txt.
Up / Down    Move the highlight.
Left / Right Scroll the table sideways. The [ and ] keys do the same.
Mouse click  Click a column title to sort by it. Click again to reverse. An
             arrow in the title shows the direction (down arrow = high first,
             up arrow = low first).
q            Quit.

Inside the detail screen:
Left / Right Go to the previous or next coin.
p            Pin or unpin this coin.
c            Copy the whole detail report as text.
Esc          Go back.


THE TOP TABLE: COLUMNS
----------------------
#           Rank in the current sort.
Delta       (The column title is a small triangle symbol.) Rank change compared
            with about 30 seconds ago, up or down.
Data        LIVE = streaming right now. sync = just going live. A time like
            45s or 2m = how old the last check is (coins not on screen are
            refreshed about every 1.5 to 2 minutes).
Coin        The coin. NEW = it just entered the top 20 (shown after the first
            4 minutes).
24h%        Price change over the last 24 hours.
Vol24h      Money traded in the last 24 hours.
Bids$       Dollars waiting to buy inside your band.
Asks$       Dollars waiting to sell inside your band.
Ratio       Bids against asks. Up arrow = more bids. Down arrow = more asks.
Clean       The same ratio, but it ignores walls that are less than 60
            seconds old (new walls are often fake). The ranking uses this.
Streak      How long the ratio has stayed at 1.5 to 1 or more. It resets if
            it fades for more than 10 seconds or flips side.
Buys$       Dollars of market buying in the CVD window.
Sells$      Dollars of market selling in the CVD window.
CVD         Buys minus Sells. AGREE = it points the same way as the streak.
            CONFLICT = it points the other way.
OI$         Open interest in dollars.
OI change   How open interest changed over the last 5 minutes. (The column
            title is "OI" and the triangle symbol.)
Positioning A short label from price and open interest over 5 minutes:
              NEW LONGS        price up, open interest up (new buyers)
              SHORTS COVERING  price up, open interest down (sellers leaving)
              NEW SHORTS       price down, open interest up (new sellers)
              LONGS CLOSING    price down, open interest down (buyers leaving)
              flat             not much moved
              warming up       still collecting readings
Bid wall    The biggest single buy order in your band, and its price.
Ask wall    The biggest single sell order in your band, and its price.
Last cancel The latest order over $25K that looks cancelled (not filled).
Spoof       A big order was pulled just as price came close. Shows how many
            times in the last 10 minutes.
Gap         What was recovered after a connection drop (see below).

A "~" next to a number means it is partial (see LIMITS).

How to read it:
  Stronger signal: Ratio and Clean agree, long streak, CVD says AGREE.
  Weaker signal:   CVD says CONFLICT, short streak, or one order makes up
                   most of the Bids$ or Asks$.
  Be careful:      Ratio is much bigger than Clean, or there is a Spoof flag.


THE WATCHLIST AND TRIGGERED TABLES
----------------------------------
These two tables look for coins that spiked up and have started to fall back.

The pool (which coins are checked)
  Every 30 seconds the program takes all coins above $3M daily volume and
  ranks them by how much they rose in 24 hours (the 24h high compared with the
  price 24 hours ago). The top 30 are the pool. Only the pool is checked.
  Ranking this way keeps a coin in the pool even after it has started to drop.

How the peak is found
  For each pool coin the program loads the last 24 hours of 1-minute candles.
  From these it knows the 24h high, when it happened, and how long the fall
  took. This works even if the drop happened before you started the program.
  Times are accurate to about one minute.

WATCHLIST: a coin is here when either is true
  DROP   It is 3% to 15% below its 24h high. (Deeper than 15% is hidden.)
  STALL  It has sat at its 24h high for 60 minutes or more. Price is within
         0.5% of the high and stayed inside a 1% range for the whole hour.
         WALL is added if there is a big ask order close above.

TRIGGERED: a coin is here when ALL are true
  - It is 5% or more below its 24h high
  - Asks are bigger than bids (in your band)
  - Sells are bigger than buys (in your CVD window: s, 1 or 5. If you chose s
    and there is no streak, 5 minutes is used)
  Triggered keeps every coin that qualified since you started. ACTIVE means it
  fits right now. "ended 4m02s" means it stopped fitting 4 minutes 2 seconds
  ago. Press x to clear the ended ones.

A coin is in one table or the other, never both. A coin only leaves a table
after its rule has been broken for 10 seconds, so rows do not flicker. Old or
frozen data never counts as Triggered.

Columns (both tables use the same columns, lined up):
Coin         The coin.
Why          DROP or STALL (Watchlist). ACTIVE or ended (Triggered).
Data         LIVE means fresh data. A number means seconds since last update.
Price        Current price.
24h high     The peak price in the last 24 hours.
Since peak   How far below the peak the price is now.
Peak ago     How long ago the peak happened.
To -3%       How long it took to fall 3% from the peak (time AFTER the peak).
To -5%       How long it took to fall 5% from the peak.
-3% ago      How long ago price first fell 3% below the peak.
-5% ago      How long ago price first fell 5% below the peak.
Peak gain    How much the coin rose from 24 hours ago up to its peak.
Vol24h       Money traded in the last 24 hours.
Bids$ Asks$  Dollars waiting to buy / sell in your band.
Clean        The ratio of bids to asks.
Buys$ Sells$ CVD   Market buying, selling and the difference.
Streak       How long the book has been one-sided.
Stall        Shown when the coin is stalled at the high (and WALL if relevant).
Entered      The clock time the coin joined this table.

Example: Peak ago 10h00m, To -3% 1h23m, -3% ago 8h37m. The peak was 10 hours
ago. Price first fell 3% 1 hour 23 minutes after the peak. That happened 8
hours 37 minutes ago.

Sorting the two tables
  Each table has its own sort. Press Tab to move to it, then o. Menu:
    Peak ago (most recent first)   To -3% (fastest first)
    To -5% (fastest first)         Since peak (deepest first)
    Entered (newest first)
  Press r to flip it (for example furthest peak first). Coins with no value in
  that column (like one that has not fallen 5% yet) always go to the bottom.
  Default: Watchlist = deepest drop first. Triggered = active first, then
  newest. Press u to go back to the default.


THE DETAIL SCREEN (Enter or d)
------------------------------
A live page for one coin. It updates every second. It shows:
  - Price and 24h high, with small charts for the last hour and 24 hours and
    a mark where the peak was
  - A checklist of every rule, with a tick or a cross and the live value
  - The order book ladder: the closest sell and buy orders with bars, and
    wall ages
  - Imbalance for your band and for fixed bands (0.10%, 0.25%, 0.50%), and the
    first 5 and 10 price levels
  - Buys, sells and CVD for 10 seconds, 1 minute, 5 minutes and the streak
  - A live list of recent trades (big ones are highlighted)
  - Open interest changes, funding rate, mark price against index price
  - Crowded zones: the 3 price slices on each side with the most money
  - Liquidations (see below)
  - Walls, cancels and spoof flags
  - Data health (how fresh everything is)

Liquidations come from Binance's liquidation feed. Binance sends at most ONE
per coin per second, so in a crash the totals are a minimum, not the full
amount. The "clusters" are prices where liquidations really happened in the
last hour. The program does not guess where other positions might be.


FILES THE PROGRAM CREATES
-------------------------
watchlist_events.csv     One row each time a coin enters the Watchlist or
                         Triggered: time, price, peak, drop, timing, and the
                         numbers on screen at that moment.
watchlist_outcomes.csv   For each of those, the price change 1 minute, 5
                         minutes, 15 minutes and 60 minutes later, plus the
                         best and worst move so far.
watchlist.db             The same two tables in SQLite (tables "events" and
                         "outcomes", joined on the column "id"). Example:
                         sqlite3 watchlist.db "select e.coin, e.event,
                         o.after, o.change_pct from events e join outcomes o
                         on o.id = e.id"
fetcher.log              Errors and disconnects.
fetcher_pins.json        Your pinned coins.
copied.txt               Only if the clipboard copy fails.

These are notes, not trades. The program cannot know when you would enter, so
it does not calculate profit. Only the Watchlist and Triggered tables are
saved. Nothing else is recorded.


HOW IT WORKS INSIDE
-------------------
- The program loads a picture of each coin's order book from Binance, then
  keeps it up to date with Binance's live feed. It checks that no updates were
  missed, and reloads the book if any were.
- Coins on screen, in the pool, on a list, pinned or open in the detail screen
  get the live feed. All other coins are checked by a fresh picture every 1.5
  to 2 minutes (the Data column shows how old).
- Trades come from the live feed too. That is where Buys, Sells, CVD, cancels
  and spoof checks come from, so only live coins have them.
- If the connection drops, the program reloads the books and fetches the
  trades it missed, so Buys, Sells and CVD are not lost. The Gap column says
  what was recovered. Nothing can be recovered while your internet is fully
  down.
- The 24h peak, "To -3%" and "To -5%" come from 1-minute candles.
- If your network blocks Binance's address lookup ("getaddrinfo failed"), the
  program looks the address up in a private way (DNS over HTTPS). The header
  shows "dns: DoH ok". If connections still get reset, use a VPN or a server.


LIMITS (READ THIS)
------------------
- Each order book picture holds only the 500 closest price levels on each
  side. On big coins like BTC, 500 levels may only reach a fraction of 1% from
  the price. If your band is wider than the book reaches, the number shows a
  "~" (partial) and the detail screen shows how far the book really reaches.
- The book shows total size at each price, not single orders. Cancels are
  estimated by comparing book drops with trades. They are a good guess, not a
  fact.
- Long or short positions are guessed from price and open interest. They are
  not read directly.
- Buys, Sells and CVD only count Binance futures trades. Other exchanges are
  not seen.
- Liquidation totals are minimums (one per coin per second).
- The Watchlist and Triggered rules (3%, 5%, 15%, 60 minute stall, 10 second
  hold) are starting values that I have not tested. Use the saved files to
  check whether they work.
- This tool shows information. It is not trading advice.


TROUBLESHOOTING
---------------
"!! DEPTH STREAM NOT UPDATING" in the header
    Order book updates stopped for many coins, so the numbers would be frozen.
    Restart the program. If it keeps happening, check your network.
A coin shows "sync" for a long time
    Its book is reloading. It should finish in a few seconds.
Table looks empty at first
    Wait 2 to 3 minutes. The Watchlist and Triggered tables also need the
    candles to load.
Errors
    Look at fetcher.log next to the program.


SETTINGS
--------
Open fetcher.py in a text editor. The settings are at the top:
  MIN_VOL      Smallest daily volume to track ($3,000,000).
  TOP_N        How many coins in the top table (20).
  DEPTH_MS     How often the book updates: 100, 250 or 500.
  THRESH       The ratio that counts as one-sided for a streak (1.5).
  GRACE        Seconds a streak can fade before it resets (10).
  WALL_MIN, WALL_X     What counts as a wall ($250,000 and 8 times average).
  CANCEL_MIN   Smallest order shown as a cancel ($25,000).
  PX_MIN, OI_MIN       How much price and open interest must move before a
                       Positioning label shows.
  POOL_N       How many coins are in the pool (30).
  DROP_WATCH   Drop to join the Watchlist (3%).
  DROP_MAX     Deepest drop shown in the Watchlist (15%).
  DROP_TRIG    Drop to be Triggered (5%).
  HOLD         Seconds a rule must stay broken before a coin leaves (10).
  STALL_MIN, STALL_NEAR, STALL_RANGE   The stall rule (60 minutes, 0.5%, 1%).
  HORIZ        The follow-up times that are saved (1, 5, 15, 60 minutes).
