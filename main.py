import asyncio
import logging

import aiohttp
from aiohttp import web

from config import load
from engine import Engine
from health import make_app
from notifier import Notifier
from symbols import SYMBOLS

# One exchange metadata snapshot at startup; never poll per symbol or per scan.
STABLE_BASES = {
    "USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI",
    "EUR", "TRY", "BRL", "BIDR", "RUB", "UAH", "AUD", "GBP", "JPY",
}


async def resolve_symbols(session, configured):
    """Keep configured active USDT spot pairs and fill removed slots by 24h quote volume."""
    result = {
        "validation_ok": False,
        "configured": len(configured),
        "active_configured": [],
        "rejected_configured": [],
        "replacements": [],
        "validation_error": None,
    }
    base_url = "https://api.binance.com"
    request_timeout = aiohttp.ClientTimeout(total=20, connect=8, sock_read=12)
    try:
        async with session.get(
            f"{base_url}/api/v3/exchangeInfo",
            params={"permissions": '["SPOT"]'},
            timeout=request_timeout,
        ) as response:
            if response.status != 200:
                raise RuntimeError(f"exchangeInfo HTTP {response.status}")
            exchange = await response.json()
        rows = exchange.get("symbols") if isinstance(exchange, dict) else None
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("exchangeInfo returned no symbol metadata")

        active = {
            row.get("symbol")
            for row in rows
            if isinstance(row, dict)
            and row.get("status") == "TRADING"
            and row.get("quoteAsset") == "USDT"
            and row.get("isSpotTradingAllowed", True) is not False
        }
        kept = [symbol for symbol in configured if symbol in active]
        rejected = [symbol for symbol in configured if symbol not in active]
        result["validation_ok"] = True
        result["active_configured"] = kept
        result["rejected_configured"] = rejected

        # If the fixed universe has gaps, use one all-symbol 24h ticker snapshot
        # to fill them with the most liquid active USDT spot pairs.
        missing = max(0, len(configured) - len(kept))
        replacements = []
        if missing:
            async with session.get(
                f"{base_url}/api/v3/ticker/24hr",
                timeout=request_timeout,
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"24hr ticker HTTP {response.status}")
                tickers = await response.json()
            candidates = []
            for ticker in tickers if isinstance(tickers, list) else []:
                symbol = ticker.get("symbol", "")
                if symbol not in active or symbol in kept:
                    continue
                base = symbol[:-4] if symbol.endswith("USDT") else ""
                if not base or base in STABLE_BASES or base.endswith(("UP", "DOWN", "BULL", "BEAR")):
                    continue
                try:
                    quote_volume = float(ticker.get("quoteVolume", 0))
                except (TypeError, ValueError):
                    continue
                if quote_volume > 0:
                    candidates.append((quote_volume, symbol))
            candidates.sort(reverse=True)
            replacements = [symbol for _, symbol in candidates[:missing]]
            kept.extend(replacements)

        if not kept:
            raise RuntimeError("No configured symbols are currently active Binance Spot USDT markets")
        result["replacements"] = replacements
        result["active_total"] = len(kept)
        result["target_total"] = len(configured)
        log.info(
            "Binance universe validated: configured=%d active=%d rejected=%s replacements=%s",
            len(configured), len(kept), rejected, replacements,
        )
        return kept, result
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as exc:
        # The six known delisted pairs are removed from symbols.py, so fallback
        # remains a safe 114-symbol core rather than reconnecting stale markets.
        result["validation_error"] = str(exc)[:240]
        result["active_total"] = len(configured)
        result["target_total"] = len(configured)
        log.warning("Binance live symbol validation unavailable; using cleaned static universe: %s", exc)
        return list(configured), result

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
        symbols, universe_status = await resolve_symbols(session, SYMBOLS)
        engine = Engine(cfg, session, notifier, symbols)
        engine.symbol_validation = universe_status

        runner = web.AppRunner(make_app(engine))
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", cfg.port)
        await site.start()
        log.info("health server on :%d | %d validated Binance WS streams | %s",
                 cfg.port, len(symbols), cfg.interval)

        if cfg.send_startup:
            await notifier.send(
                f"✅ Momentum Explosion bot online · Binance Spot · {len(symbols)} symbols · {cfg.interval} · WebSocket"
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
