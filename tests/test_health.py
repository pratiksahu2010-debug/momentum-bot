import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from aiohttp.test_utils import TestClient, TestServer
from health import make_app


class FakeEngine:
    def __init__(self, status, uptime):
        self._status, self._uptime = status, uptime
        self.last_eval = {}

    def summary(self):
        return {"status": self._status, "uptime_s": self._uptime}

    def qualified_view(self):
        return {}

    def rejections_view(self):
        return {}


def test_health_allows_startup_grace_but_fails_disconnected_feed():
    async def run():
        client = TestClient(TestServer(make_app(FakeEngine("starting", 20))))
        await client.start_server()
        try:
            response = await client.get("/health")
            assert response.status == 200
        finally:
            await client.close()

        client = TestClient(TestServer(make_app(FakeEngine("reconnecting", 180))))
        await client.start_server()
        try:
            response = await client.get("/health")
            assert response.status == 503
        finally:
            await client.close()
    asyncio.run(run())
