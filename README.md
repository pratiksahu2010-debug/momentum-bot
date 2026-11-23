# Momentum Explosion Bot — Binance Spot WebSocket

Signal-only Telegram bot. It scans the same fixed universe of 120 Binance Spot USDT pairs and preserves the existing momentum strategy. It does not place orders.

## Market-data architecture
- **WebSocket only for market data.** One Binance combined-stream connection subscribes to all 120 `<symbol>@kline_5m` streams (or the interval selected by `INTERVAL`).
- Only candles with Binance's `k.x == true` (closed candle) are evaluated. Partial candles are ignored.
- The bot makes **no Binance REST market-data requests**. The earlier REST polling path that triggered HTTP 418/429 is removed.
- The combined connection is rotated before Binance's documented 24-hour connection lifetime. Automatic reconnect uses exponential backoff (1–60 seconds), and aiohttp handles server ping/pong frames.
- Duplicate candle-open timestamps are ignored. A single in-memory rolling history is maintained per symbol.

## Startup warm-up
WebSocket-only means there is no REST candle-history download. Each symbol needs `max(50, LOOKBACK + 16)` closed candles before its existing indicators can be evaluated. With the default 5-minute interval and 50-candle minimum, allow approximately **4 hours 10 minutes** after a fresh process start before all symbols are ready. A symbol is evaluated after its history is ready; no strategy thresholds or indicator calculations are changed. The first qualified candidate for each symbol during warm-up is suppressed, matching the existing warm-up safety behavior.

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

The minimum score remains 48/100. Cooldown is 0 minutes; alert caps remain six per hour and three per scan. This is a signal-only system and does not execute orders.

## Health endpoints
- `/ping`: empty-body HTTP 200 for an external cron/uptime monitor.
- `/`: plain `ok`.
- `/health`: WebSocket connection, reconnect, warm-up, strategy and alert status.
- `/health/qualified`, `/health/rejections`, `/health/symbol/BTCUSDT`: diagnostics.

For Render Free, configure an external cron monitor to GET `https://momentum-bot-fwdp.onrender.com/ping` every 5 minutes. Do not rely on an internal self-ping task.

## Render deployment
- Build command: `pip install -r requirements.txt`
- Start command: `python main.py`
- Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
- Python 3.11.9 is specified in `render.yaml`.

## Tests
```bash
python tests/test_strategy.py
python tests/test_engine.py
```

The strategy tests exercise the original evaluator. Engine tests cover closed-candle processing, duplicate suppression and ignoring open candles.

## Operational notes
- Binance Spot market-data stream docs: https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- Combined streams support up to 1024 streams per connection; this bot uses 120.
- A Binance Spot WebSocket connection is valid for 24 hours; the bot rotates proactively.
- WebSocket-only startup trades the convenience of immediate history for eliminating REST market-data polling. If the process restarts, in-memory candle history must be rebuilt.
