import asyncio
import logging
import os

import aiohttp
from aiohttp import web

from config import load
from engine import Engine
from health import make_app
from notifier import Notifier
from symbols import SYMBOLS

log = logging.getLogger("main")


async def keepalive(session, url, every=600):
    """Pings our own public URL so Render's free tier is less likely to idle-sleep."""
    while True:
        await asyncio.sleep(every)
        try:
            async with session.get(url + "/") as r:
                await r.read()
        except Exception:
            pass


async def main():
    cfg = load()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(limit=20)) as session:
        notifier = Notifier(session, cfg.token, cfg.chat_id)
        if not notifier.enabled:
            log.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing -> DRY-RUN mode")
        engine = Engine(cfg, session, notifier, SYMBOLS)

        runner = web.AppRunner(make_app(engine))
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", cfg.port).start()
        log.info("health server on :%d | %d symbols | %s", cfg.port, len(SYMBOLS), cfg.interval)

        if cfg.send_startup:
            await notifier.send(f"✅ Momentum Explosion bot online · {len(SYMBOLS)} symbols · {cfg.interval}")

        tasks = [asyncio.create_task(engine.run())]
        ext = os.getenv("RENDER_EXTERNAL_URL")
        if ext:
            tasks.append(asyncio.create_task(keepalive(session, ext.rstrip("/"))))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
