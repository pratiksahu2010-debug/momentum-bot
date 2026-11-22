import asyncio
import logging
import time
from collections import Counter, deque
from datetime import datetime, timezone

from momentum import GATES, evaluate
from notifier import format_signal

log = logging.getLogger("engine")


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class Engine:
    def __init__(self, cfg, session, notifier, symbols):
        self.cfg, self.session, self.notifier, self.symbols = cfg, session, notifier, symbols
        self.sem = asyncio.Semaphore(cfg.concurrency)
        self.started = time.time()
        self.pause_until = 0.0
        self.warmup = True
        self.dead = set()
        self.last_seen = {}
        self.last_signal = {}
        self.sent_times = deque()
        self.last_eval = {}
        self.recent = deque(maxlen=30)
        self.totals, self.rejected = Counter(), Counter()
        self.suppressed, self.errors = Counter(), Counter()
        self.last_scan = {}

    # ---------- data ----------
    async def _fetch(self, sym):
        url = f"{self.cfg.base_url}/api/v3/klines"
        params = {"symbol": sym, "interval": self.cfg.interval, "limit": 80}
        async with self.sem:
            for _ in range(2):
                try:
                    async with self.session.get(url, params=params) as r:
                        if r.status in (418, 429):
                            self.pause_until = time.time() + int(r.headers.get("Retry-After", 30))
                            return None, f"http_{r.status}"
                        if r.status == 400:
                            return None, "invalid_symbol"
                        if r.status != 200:
                            raise RuntimeError(f"http {r.status}")
                        return await r.json(), None
                except Exception:
                    await asyncio.sleep(0.5)
        return None, "fetch_error"

    async def _one(self, sym):
        data, err = await self._fetch(sym)
        if err:
            return sym, None, err
        now_ms = int(time.time() * 1000)
        if data and int(data[-1][6]) >= now_ms:  # drop still-forming candle
            data = data[:-1]
        if not data:
            return sym, None, "no_data"
        if self.last_seen.get(sym) == data[-1][0]:
            return sym, None, "duplicate"
        self.last_seen[sym] = data[-1][0]
        return sym, evaluate(sym, data, self.cfg), None

    # ---------- scan ----------
    async def scan(self):
        t0 = time.time()
        if t0 < self.pause_until:
            self.totals["scans_skipped_rate_limit"] += 1
            return
        syms = [s for s in self.symbols if s not in self.dead]
        results = await asyncio.gather(*(self._one(s) for s in syms))

        cands, rej, err = [], Counter(), Counter()
        evaluated = 0
        for sym, ev, e in results:
            if e == "duplicate":
                continue
            if e:
                err[e] += 1
                if e == "invalid_symbol":
                    self.dead.add(sym)
                self.last_eval[sym] = {"at": iso(t0), "passed": False, "reason": e, "depth": -1}
                continue
            evaluated += 1
            self.last_eval[sym] = ev.as_dict(iso(t0))
            if ev.passed:
                cands.append(ev)
            else:
                rej[ev.reason] += 1

        sent, supp = await self._dispatch(cands, t0)

        self.totals["scans"] += 1
        self.totals["evaluated"] += evaluated
        self.totals["qualified"] += len(cands)
        self.totals["sent"] += sent
        self.rejected.update(rej)
        self.suppressed.update(supp)
        self.errors.update(err)
        dur = time.time() - t0
        self.last_scan = {"at": iso(t0), "ts": t0, "duration_s": round(dur, 1), "evaluated": evaluated,
                          "qualified": len(cands), "sent": sent, "rejected": dict(rej),
                          "suppressed": dict(supp), "errors": dict(err)}
        log.info("scan %.1fs eval=%d qualified=%d sent=%d rejected=%s errors=%s",
                 dur, evaluated, len(cands), sent, dict(rej), dict(err))
        self.warmup = False

    async def _dispatch(self, cands, now):
        cfg = self.cfg
        while self.sent_times and now - self.sent_times[0] > 3600:
            self.sent_times.popleft()
        cands.sort(key=lambda e: e.score, reverse=True)  # best first, caps apply to the rest
        sent, supp = 0, Counter()
        for ev in cands:
            reason = None
            if self.warmup:
                reason = "warmup"
            elif ev.score < cfg.min_score:
                reason = "low_score"
            elif now - self.last_signal.get(ev.symbol, 0) < cfg.cooldown_min * 60:
                reason = "cooldown"
            elif len(self.sent_times) >= cfg.max_per_hour:
                reason = "hourly_cap"
            elif sent >= cfg.max_per_scan:
                reason = "scan_cap"

            status = f"suppressed:{reason}" if reason else None
            if not reason:
                ok = await self.notifier.send(format_signal(ev, cfg))
                if ok:
                    status = "sent" if self.notifier.enabled else "dry_run"
                    self.last_signal[ev.symbol] = now
                    self.sent_times.append(now)
                    sent += 1
                else:
                    status = "send_failed"
                    supp["send_failed"] += 1
            else:
                supp[reason] += 1
            self.recent.appendleft({"at": iso(now), "symbol": ev.symbol, "side": ev.side,
                                    "score": round(ev.score, 1), "status": status,
                                    "metrics": ev.m, "levels": {k: round(v, 8) for k, v in ev.levels.items()}})
        return sent, supp

    async def run(self):
        iv = self.cfg.interval_seconds
        try:
            await self.scan()  # warm-up: evaluates, never sends (avoids stale/duplicate alerts on wake)
        except Exception:
            log.exception("warmup scan failed")
        while True:
            now = time.time()
            nxt = (now // iv + 1) * iv + self.cfg.scan_delay  # just after candle close
            await asyncio.sleep(max(0.0, nxt - now))
            try:
                await self.scan()
            except Exception:
                log.exception("scan failed")

    # ---------- health views ----------
    def status(self):
        iv = self.cfg.interval_seconds
        up = time.time() - self.started
        if not self.last_scan:
            return "starting" if up < iv * 2 + 60 else "stale"
        return "ok" if time.time() - self.last_scan["ts"] <= iv * 3 else "stale"

    def summary(self):
        c = self.cfg
        return {
            "status": self.status(),
            "uptime_s": int(time.time() - self.started),
            "telegram_configured": self.notifier.enabled,
            "interval": c.interval,
            "symbols_total": len(self.symbols),
            "symbols_active": len(self.symbols) - len(self.dead),
            "invalid_symbols": sorted(self.dead),
            "warmup_done": not self.warmup,
            "last_scan": {k: v for k, v in self.last_scan.items() if k != "ts"},
            "totals": dict(self.totals),
            "rejected_by_gate": {g: self.rejected.get(g, 0) for g in GATES} | {
                k: v for k, v in self.rejected.items() if k not in GATES},
            "suppressed": dict(self.suppressed),
            "fetch_errors": dict(self.errors),
            "signals_last_hour": len(self.sent_times),
            "rules": {"vol_x>=": c.vol_mult, "range_atr>=": c.range_mult, "body>=": c.min_body,
                      "close_loc>=": c.close_loc, "rsi": [c.rsi_min, c.rsi_max],
                      "max_ext_atr": c.max_ext_atr, "cooldown_min": c.cooldown_min,
                      "max_per_hour": c.max_per_hour, "max_per_scan": c.max_per_scan,
                      "min_score": c.min_score},
            "endpoints": ["/health/qualified", "/health/rejections", "/health/symbol/BTCUSDT"],
        }

    def qualified_view(self):
        return {"total_qualified": self.totals.get("qualified", 0), "total_sent": self.totals.get("sent", 0),
                "recent": list(self.recent)}

    def rejections_view(self):
        by_reason = {}
        for sym, e in self.last_eval.items():
            by_reason.setdefault(e["reason"], []).append(sym)
        near = [(s, e) for s, e in self.last_eval.items() if not e["passed"] and e.get("depth", -1) >= 4]
        near.sort(key=lambda x: (-x[1]["depth"], -x[1]["metrics"].get("vol_x", 0)))
        return {"last_scan": self.last_scan.get("at"),
                "by_reason": {k: {"count": len(v), "symbols": sorted(v)} for k, v in by_reason.items()},
                "near_misses": [{"symbol": s, "failed_gate": e["reason"], "gates_passed": e["depth"],
                                 "side": e["side"], "metrics": e["metrics"]} for s, e in near[:15]]}
