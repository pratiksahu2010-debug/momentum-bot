# Momentum Explosion Bot — Binance Spot WebSocket

Signal-only Telegram bot. It scans the same fixed universe of 120 Binance Spot USDT pairs and preserves the existing momentum strategy. It does not place orders.

## Market-data architecture
- **Live market data uses one Binance combined WebSocket connection** for all 120 `<symbol>@kline_5m` streams (or the interval selected by `INTERVAL`).
- At startup, the bot makes a **one-time historical bootstrap** using Binance Spot `GET /api/v3/klines`: it requests enough recent candles for each symbol to seed the existing indicator history. This is a finite startup snapshot, not polling.
- Only fully closed candles are accepted from REST or WebSocket. Historical candles seed buffers only; they are never evaluated or sent as backdated alerts.
- After bootstrap, all live updates and strategy evaluations come from the WebSocket. The bot does not run periodic REST market-data polling.
- If a REST request fails or a symbol has insufficient contiguous history, that symbol stays in WebSocket warm-up. Bootstrap failures do not stop the bot.
- The combined connection is rotated before Binance's documented 24-hour connection lifetime. Automatic reconnect uses exponential backoff (1–60 seconds), and aiohttp handles server ping/pong frames.
- Duplicate candle-open timestamps are ignored. Candle gaps clear that symbol's buffer rather than calculating indicators across missing data.

## Startup history bootstrap
- Default: `HISTORY_BOOTSTRAP=true`; set it to `false` to return to WebSocket-only warm-up.
- REST base URL: `BINANCE_REST_URL` (default `https://api.binance.com`).
- The bot requests up to 60 recent klines per symbol from Binance public market-data REST (`https://data-api.binance.vision`) with two in-flight requests and a 250 ms global request-start spacing. If Binance returns HTTP 418/429, it stops the current batch, honors `Retry-After`, and retries only the symbols still missing after the cooldown. It does not hammer the endpoint while the IP is restricted. The live WebSocket continues independently during the wait.
- Binance's public ZIP archive (`data.binance.vision/data/spot/daily/...`) is useful for historical research, but daily files are published the next day; they cannot supply the current session's missing candles. The bot therefore does not use stale archive candles to bridge a live-data gap or create misleading indicator history.
- Each symbol needs `max(50, LOOKBACK + 16)` contiguous closed candles. With the default 5-minute interval, a successful bootstrap normally makes all 120 symbols strategy-ready shortly after the WebSocket begins delivering fresh closed candles.
- Health reports bootstrap loaded/failed counts separately from live WebSocket candle timestamps.

## Balanced four-gate momentum strategy
Exactly **four setup conditions** are mandatory. LONG and SHORT use mirrored breakout logic.

| Priority | Mandatory setup gate | Default |
|---|---|---|
| 1 | Average quote-volume liquidity | >= 12,000 |
| 2 | Quote-volume explosion | >= 2.0x lookback average |
| 3 | Candle body strength | >= 55% of candle range |
| 4 | Donchian breakout | Close above prior 20-candle high (LONG) or below prior 20-candle low (SHORT) |

The following are deliberately **not additional setup gates**:
- Range expansion (1.5x reference) contributes to score, not setup eligibility.
- Close location (70% reference) and RSI(14) (58–82) contribute to the quality score.
- EMA 9/21 and VWAP are not mandatory confirmations.

Independent safety controls remain active after the four gates: maximum candle range (5 ATR), reject a breakout already confirmed on the previous candle, maximum breakout extension (3.5 ATR), and structural stop validation (stop must remain beyond the signal candle and within the 1.8 ATR risk ceiling). These controls can still reject an unsafe or late setup.

The minimum score remains 48/100. Cooldown is 0 minutes. Hourly and per-scan alert-count caps are removed; every qualified candidate can be sent, subject to the strategy gates, safety checks, score threshold, and per-symbol cooldown. This is a signal-only system and does not execute orders.

## Health endpoints
- `/ping`: empty-body HTTP 200 for an external cron/uptime monitor.
- `/`: plain `ok`.
- `/health`: WebSocket connection, reconnect, warm-up, one-time bootstrap, strategy and alert status.
- `/health/qualified`, `/health/rejections`, `/health/symbol/BTCUSDT`: diagnostics.

For Render Free, configure an external cron monitor to GET `https://momentum-bot-fwdp.onrender.com/ping` every 5 minutes. Do not rely on an internal self-ping task.

## Render deployment
- Build command: `pip install -r requirements.txt`
- Start command: `python main.py`
- Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
- Optional: `HISTORY_BOOTSTRAP=true`, `BINANCE_REST_URL=https://data-api.binance.vision`.
- Python 3.11.9 is specified in `render.yaml`.

## Tests
```bash
python -m pytest -q
```

Tests cover strategy gates, closed-candle processing, duplicate suppression, WebSocket open-candle rejection, candle-gap resets, and REST history seeding (including ignoring the current unfinished candle).

## Operational notes
- Binance Spot market-data stream docs: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- Binance Spot kline REST docs: https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data
- Combined streams support up to 1024 streams per connection; this bot uses 120.
- A Binance Spot WebSocket connection is valid for 24 hours; the bot rotates proactively.
