import asyncio
import logging

import aiohttp
from aiohttp import web

from config import load
from engine import Engine
from health import make_app
from notifier import Notifier
from symbols import SYMBOLS

log = logging.getLogger("main")


async def main():
    cfg = load()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # No REST market-data calls are made. Keep the session open for the WS feed and Telegram.
    timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=None)
    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=aiohttp.TCPConnector(limit=20, keepalive_timeout=60),
    ) as session:
        notifier = Notifier(session, cfg.token, cfg.chat_id)
        if not notifier.enabled:
            log.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing -> DRY-RUN mode")
        engine = Engine(cfg, session, notifier, SYMBOLS)

        runner = web.AppRunner(make_app(engine))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", cfg.port)
        await site.start()
        log.info("health server on :%d | %d Binance WS streams | %s",
                 cfg.port, len(SYMBOLS), cfg.interval)

        if cfg.send_startup:
            await notifier.send(
                f"✅ Momentum Explosion bot online · Binance Spot · {len(SYMBOLS)} symbols · {cfg.interval} · WebSocket"
            )

        try:
            await engine.run()
        finally:
            await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
