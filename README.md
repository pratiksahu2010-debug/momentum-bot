# Momentum Explosion Bot (Render + Telegram)

One signal module, 120 fixed Binance Spot USDT pairs, pure Python, one dependency (`aiohttp`).

## Data feed and rate-limit protection
- Uses the official Binance Spot REST host `https://api.binance.com` by default. Set `BASE_URL` only if you intentionally need another official compatible host.
- Requests are globally paced (one every 120 ms) with at most 3 concurrent workers.
- On HTTP 429 or 418 the bot stops sending market-data requests and honours Binance's `Retry-After` header. It does not retry a rate-limited request or continue hammering the API.
- HTTP 418 means Binance has auto-banned the Render outbound IP after earlier rate-limit violations; changing API keys does not fix an IP ban. Wait until `Retry-After` expires. Check `/health` for `last_rate_limit` and `rate_limit_pause_remaining_s`.

## How it detects an explosion (last closed candle only)
| # | Check | Default |
|---|---|---|
| 1 | Quote-volume spike vs 20-candle avg | >= 3.0x |
| 2 | Range vs ATR(14) | >= 1.8 ATR, body >= 60%, close in top/bottom 25% |
| 3 | Close breaks prior 20-candle high/low | yes |
| 4 | RSI band + EMA20 distance | RSI 58-82 (mirrored for short), <= 4 ATR from EMA20 |

Also: liquidity floor and late-entry gate. Signal includes Entry / SL / TP1 / TP2.

## Anti-spam
- Only closed candles, each processed once, scan runs just after candle close.
- Per-symbol cooldown 90 min, max 6 signals/hour, max 3 per scan (best score first), min score 55.
- First scan after boot is warm-up (never sends) so wake-ups don't fire stale alerts.
- Telegram sends are serialized with 429 retry handling.

## Health endpoints
- `/ping`: empty-body HTTP 200 for external cron/uptime monitors.
- `/`: plain `ok`.
- `/health`: status, last scan, totals, rejected gates, suppressed alerts, fetch errors and rate-limit state.
- `/health/qualified`, `/health/rejections`, `/health/symbol/BTCUSDT`: diagnostics.

## Deploy
Render build: `pip install -r requirements.txt`; start: `python main.py`.
Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Optional `SEND_STARTUP=true`.
For a Free web service, configure an external cron monitor to GET `https://momentum-bot-fwdp.onrender.com/ping` every 5 minutes. The response body is intentionally empty.

## Tests
`python tests/test_strategy.py && python tests/test_engine.py`

Signal-only: the bot does not place orders. Backtest/paper-trade before using alerts for decisions.
