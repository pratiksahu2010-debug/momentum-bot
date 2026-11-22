# Momentum Explosion Bot (Render free + Telegram)

One signal module, 120 fixed USDT pairs, pure Python, one dependency (`aiohttp`).

## How it detects an explosion (last closed candle only)
| # | Check | Default |
|---|-------|---------|
| 1 | Quote-volume spike vs 20-candle avg | >= 3.0x |
| 2 | Range vs ATR(14) (ATR excludes signal candle) | >= 1.8 ATR, body >= 60%, close in top/bottom 25% |
| 3 | Close breaks prior 20-candle high (long) / low (short) | yes |
| 4 | RSI band + EMA20 distance (anti-chase) | RSI 58-82 (mirrored for short), <= 4 ATR from EMA20 |

Also: liquidity floor, and "late entry" gate (rejects if the previous candle had already broken out) so you get the **first** explosion candle, not the 3rd.
Signal includes Entry / SL (tighter of candle extreme or 1.8 ATR) / TP1 1.5R / TP2 3R.

## Anti-spam
- Only closed candles, each processed once, scan runs right after candle close.
- Per-symbol cooldown 90 min, max 6 signals/hour, max 3 per scan (best score first), min score 55.
- First scan after boot is warm-up (never sends) so wake-ups don't fire stale alerts.
- Telegram sends are serialized with 429 retry handling.

## Health endpoints
- `/` plain `ok` (for pingers)
- `/health` status, last scan, totals, **rejected_by_gate**, suppressed, fetch errors, rules. Returns 503 if scans stall (Render restarts it).
- `/health/qualified` recent qualified signals and whether each was `sent` or `suppressed:<reason>`
- `/health/rejections` every symbol grouped by rejection reason + top near-misses
- `/health/symbol/BTCUSDT` last metrics and verdict for one symbol

## Deploy
1. Create bot with @BotFather, get chat id (message the bot, then open `https://api.telegram.org/bot<TOKEN>/getUpdates`).
2. Push this folder to GitHub.
3. Render -> New -> Blueprint (uses `render.yaml`) or Web Service: build `pip install -r requirements.txt`, start `python main.py`, plan Free.
4. Set env `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`. Optional `SEND_STARTUP=true` for a first test.
5. **Keep awake:** Render free sleeps after ~15 min without inbound traffic. Add a free UptimeRobot / cron-job.org monitor hitting `https://<your-app>.onrender.com/health` every 5 min. (The built-in self-ping helps but an external pinger is more reliable.)

## Notes
- Data: `data-api.binance.vision` (public, works from US-hosted Render). Override with `BASE_URL`.
- Symbols delisted by the exchange are auto-skipped and listed under `invalid_symbols` in `/health`.
- State is in memory; a restart resets cooldowns.
- Tests: `python tests/test_strategy.py && python tests/test_engine.py`
- Not financial advice. Backtest/paper-trade before using real money.
