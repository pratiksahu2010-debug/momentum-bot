import asyncio
import logging
import time
from datetime import datetime, timezone

import aiohttp

log = logging.getLogger("telegram")


def fmt(p):
    d = 2 if p >= 100 else 4 if p >= 1 else 6 if p >= 0.01 else 8
    return f"{p:.{d}f}"


def format_signal(ev, cfg):
    m, lv = ev.m, ev.levels
    icon = "🟢🚀" if ev.side == "LONG" else "🔴💥"
    base = ev.symbol[:-4]
    t = datetime.fromtimestamp(ev.candle_open / 1000, timezone.utc).strftime("%H:%M UTC")
    return (
        f"{icon} <b>MOMENTUM EXPLOSION · {ev.side}</b>\n"
        f"<b>{ev.symbol}</b> · {cfg.interval} · score <b>{ev.score:.0f}</b>\n\n"
        f"Entry: <code>{fmt(lv['entry'])}</code>\n"
        f"SL: <code>{fmt(lv['sl'])}</code> ({lv['risk_pct']:.2f}%)\n"
        f"TP1: <code>{fmt(lv['tp1'])}</code> · TP2: <code>{fmt(lv['tp2'])}</code>\n\n"
        f"Vol ×{m['vol_x']} · Range {m['range_atr']} ATR · RSI {m['rsi']}\n"
        f"Candle {t}\n"
        f"https://www.binance.com/en/trade/{base}_USDT"
    )


class Notifier:
    def __init__(self, session, token, chat_id):
        self.session, self.token, self.chat_id = session, token, chat_id
        self._lock = asyncio.Lock()
        self._last = 0.0

    @property
    def enabled(self):
        return bool(self.token and self.chat_id)

    async def send(self, text):
        if not self.enabled:
            log.info("DRY-RUN (no Telegram creds):\n%s", text)
            return True
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id, "text": text, "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        async with self._lock:
            for attempt in range(3):
                wait = 1.1 - (time.time() - self._last)
                if wait > 0:
                    await asyncio.sleep(wait)
                try:
                    async with self.session.post(
                        url, json=payload, timeout=aiohttp.ClientTimeout(total=15)
                    ) as response:
                        self._last = time.time()
                        try:
                            data = await response.json(content_type=None)
                        except (ValueError, aiohttp.ContentTypeError):
                            data = {}
                        if response.status == 200 and data.get("ok") is True:
                            return True
                        if response.status == 429:
                            retry_after = data.get("parameters", {}).get("retry_after", 3)
                            try:
                                retry_after = max(1, min(60, int(retry_after)))
                            except (TypeError, ValueError):
                                retry_after = 3
                            log.warning("Telegram rate limited; retrying after %ss", retry_after)
                            await asyncio.sleep(retry_after + 1)
                            continue
                        detail = data.get("description") or str(data) or (await response.text())[:200]
                        log.error("Telegram delivery failed HTTP %s: %s", response.status, detail)
                        if response.status >= 500 and attempt < 2:
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
                        return False
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    log.warning("Telegram network error (attempt %d/3): %s", attempt + 1, exc)
                    if attempt < 2:
                        await asyncio.sleep(2 * (attempt + 1))
            return False
