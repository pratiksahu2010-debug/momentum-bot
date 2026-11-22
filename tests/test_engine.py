import asyncio, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))
from config import load
from engine import Engine
import test_strategy as ts


class FakeNotifier:
    enabled = True
    def __init__(self): self.msgs = []
    async def send(self, t): self.msgs.append(t); return True


class Resp:
    def __init__(self, data): self.data, self.status, self.headers = data, 200, {}
    async def __aenter__(self): return self
    async def __aexit__(self, *a): pass
    async def json(self): return self.data


class FakeSession:
    def __init__(self, boom): self.boom, self.n = boom, 0
    def get(self, url, params=None):
        k = ts.base(); p = float(k[-1][4])
        sym = params["symbol"]
        # make candles "closed" in the past, new open time each scan so dedupe passes
        off = self.n_off
        k = [[x[0] + off, *x[1:6], x[6] + off - 10**9, x[7]] for x in k]
        if sym in self.boom:
            k.append([79 * 300000 + off, str(p), str(p + .5), str(p - .02), str(p + .46), "0", 80 * 300000 + off - 10**9, "250000"])
        else:
            k.append([79 * 300000 + off, str(p), str(p + .1), str(p - .02), str(p + .08), "0", 80 * 300000 + off - 10**9, "50000"])
        return Resp(k)


async def main():
    cfg = load()
    syms = [f"S{i}USDT" for i in range(8)]
    sess = FakeSession({"S1USDT", "S2USDT", "S3USDT", "S4USDT", "S5USDT"}); sess.n_off = 0
    n = FakeNotifier()
    e = Engine(cfg, sess, n, syms)
    await e.scan(); assert not n.msgs and e.suppressed["warmup"] == 5        # warm-up sends nothing
    sess.n_off = 300000 * 1000; await e.scan()
    assert len(n.msgs) == cfg.max_per_scan, len(n.msgs)                       # scan cap respected
    sess.n_off = 600000 * 1000; await e.scan()
    assert len(n.msgs) == cfg.max_per_scan + 2 or len(n.msgs) <= 5            # cooldown blocks repeats
    print("msgs", len(n.msgs), "suppressed", dict(e.suppressed))
    print(e.summary()["rejected_by_gate"], e.status())
    print(e.rejections_view()["by_reason"].keys())

asyncio.run(main())
