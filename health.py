from aiohttp import web


def make_app(engine):
    async def root(_):
        return web.Response(text="ok")

    async def ping(_):
        # Cron keep-alive endpoint: HTTP 200 with a deliberately empty body.
        return web.Response(status=200)

    async def health(_):
        s = engine.summary()
        return web.json_response(s, status=503 if s["status"] == "stale" else 200)

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
