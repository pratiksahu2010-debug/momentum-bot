from aiohttp import web

HEALTH_STARTUP_GRACE_SECONDS = 120


def make_app(engine):
    async def root(_):
        return web.Response(text="ok")

    async def ping(_):
        # Liveness/cron endpoint. Feed readiness is reported separately by /health.
        return web.Response(status=200)

    async def health(_):
        s = engine.summary()
        # Allow the initial WebSocket handshake/warm-up, then make disconnected,
        # reconnecting and stale feeds visible to Render's readiness check.
        unhealthy = s["status"] != "ok" and s["uptime_s"] >= HEALTH_STARTUP_GRACE_SECONDS
        return web.json_response(s, status=503 if unhealthy else 200)

    async def qualified(_):
        return web.json_response(engine.qualified_view())

    async def rejections(_):
        return web.json_response(engine.rejections_view())

    async def symbol(req):
        sym = req.match_info["sym"].upper()
        result = engine.last_eval.get(sym)
        return web.json_response(
            result or {"error": "unknown symbol or not scanned yet"},
            status=200 if result else 404,
        )

    app = web.Application()
    app.add_routes([
        web.get("/", root), web.get("/ping", ping), web.get("/health", health),
        web.get("/health/qualified", qualified), web.get("/health/rejections", rejections),
        web.get("/health/symbol/{sym}", symbol),
    ])
    return app
