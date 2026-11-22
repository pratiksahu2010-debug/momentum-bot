import asyncio
import json
import logging
import time
from collections import Counter, deque
from datetime import datetime, timezone

import aiohttp

from momentum import GATES, evaluate
from notifier import format_signal

log = logging.getLogger("engine")
RECONNECT_MAX_SECONDS = 60
CONNECTION_ROTATE_SECONDS = 23 * 60 * 60 + 50 * 60
BATCH_WINDOW_SECONDS = 5


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class Engine:
    """Binance Spot combined-WebSocket consumer; strategy evaluation is unchanged."""

    def __init__(self, cfg, session, notifier, symbols):
        self.cfg, self.session, self.notifier, self.symbols = cfg, session, notifier, list(symbols)
        self.buffers = {s: deque(maxlen=80) for s in self.symbols}
        self.required_history = max(50, cfg.lookback + 16)
        self.warmup_symbols = set(self.symbols)
        self.last_seen = {}
        self.last_signal = {}
        self.sent_times = deque()
        self.last_eval = {}
        self.recent = deque(maxlen=30)
        self.totals, self.rejected = Counter(), Counter()
        self.suppressed, self.errors = Counter(), Counter()
        self.pending_batches = {}
        self.batch_tasks = {}
        self.started = time.time()
        self.connected = False
        self.connection_count = 0
        self.reconnects = 0
        self.last_message_at = None
        self.last_kline_at = None
        self.last_scan = {}
        self.last_disconnect = None
        self.stream_url = self._build_stream_url()

    def _build_stream_url(self):
        streams = "/".join(f"{symbol.lower()}@kline_{self.cfg.interval}" for symbol in self.symbols)
        return f"{self.cfg.ws_url.rstrip('/')}/stream?streams={streams}"

    async def on_closed_kline(self, symbol, row):
        """Accept one Binance-format, CLOSED kline row; safe to call repeatedly."""
        if symbol not in self.buffers:
            return
        open_time = int(row[0])
        if open_time <= self.last_seen.get(symbol, -1):
            self.totals["duplicate_candles"] += 1
            return
        self.last_seen[symbol] = open_time
        candles = self.buffers[symbol]
        candles.append(row)
        self.last_kline_at = time.time()

        if len(candles) < self.required_history:
            self.totals["warmup_candles"] += 1
            self.last_eval[symbol] = {
                "at": iso(time.time()), "passed": False, "reason": "insufficient_data",
                "depth": 0, "side": "-", "score": 0, "metrics": {},
            }
            return

        warming = symbol in self.warmup_symbols
        if warming:
            self.warmup_symbols.discard(symbol)
            self.totals["symbols_warmed"] += 1

        ev = evaluate(symbol, list(candles), self.cfg)
        self.last_eval[symbol] = ev.as_dict(iso(time.time()))

        # Batch the symbols closing the same 5-minute candle. This preserves the
        # original max_per_scan cap and best-score-first ordering with streaming data.
        batch = self.pending_batches.get(open_time)
        if batch is None:
            batch = {
                "started": time.time(), "processed": 0, "evaluated": 0,
                "candidates": [], "rejected": Counter(), "warmup_symbols": set(),
            }
            self.pending_batches[open_time] = batch
            self.batch_tasks[open_time] = asyncio.create_task(self._flush_after(open_time))
        batch["processed"] += 1
        batch["evaluated"] += 1
        if ev.passed:
            batch["candidates"].append(ev)
            if warming:
                batch["warmup_symbols"].add(symbol)
        else:
            batch["rejected"][ev.reason] += 1

        if batch["processed"] >= len(self.symbols):
            await self._flush_batch(open_time)

    async def _flush_after(self, open_time):
        try:
            await asyncio.sleep(BATCH_WINDOW_SECONDS)
            await self._flush_batch(open_time)
        except asyncio.CancelledError:
            return

    async def flush_pending(self):
        """Flush any incomplete candle batches; also useful for deterministic tests."""
        for open_time in list(self.pending_batches):
            await self._flush_batch(open_time)

    async def _flush_batch(self, open_time):
        batch = self.pending_batches.pop(open_time, None)
        task = self.batch_tasks.pop(open_time, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        if batch is None:
            return

        now = time.time()
        candidates = batch["candidates"]
        sent, suppressed = await self._dispatch(candidates, now, batch["warmup_symbols"])
        rejected = batch["rejected"]
        self.totals["scans"] += 1
        self.totals["evaluated"] += batch["evaluated"]
        self.totals["qualified"] += len(candidates)
        self.totals["sent"] += sent
        self.rejected.update(rejected)
        self.suppressed.update(suppressed)
        self.last_scan = {
            "at": iso(now), "ts": now, "candle_open": open_time,
            "duration_s": round(now - batch["started"], 2),
            "evaluated": batch["evaluated"], "qualified": len(candidates),
            "sent": sent, "rejected": dict(rejected), "suppressed": dict(suppressed), "errors": {},
        }
        log.info("closed-candle batch eval=%d qualified=%d sent=%d rejected=%s suppressed=%s",
                 batch["evaluated"], len(candidates), sent, dict(rejected), dict(suppressed))

    async def _dispatch(self, candidates, now, warmup_symbols):
        cfg = self.cfg
        while self.sent_times and now - self.sent_times[0] > 3600:
            self.sent_times.popleft()
        candidates.sort(key=lambda e: e.score, reverse=True)
        sent, suppressed = 0, Counter()
        for ev in candidates:
            reason = None
            if ev.symbol in warmup_symbols:
                reason = "warmup"
            elif ev.score < cfg.min_score:
                reason = "low_score"
            elif now - self.last_signal.get(ev.symbol, 0) < cfg.cooldown_min * 60:
                reason = "cooldown"
            elif len(self.sent_times) >= cfg.max_per_hour:
                reason = "hourly_cap"
            elif sent >= cfg.max_per_scan:
                reason = "scan_cap"

            if reason:
                status = f"suppressed:{reason}"
                suppressed[reason] += 1
            else:
                ok = await self.notifier.send(format_signal(ev, cfg))
                if ok:
                    status = "sent" if self.notifier.enabled else "dry_run"
                    self.last_signal[ev.symbol] = now
                    self.sent_times.append(now)
                    sent += 1
                else:
                    status = "send_failed"
                    suppressed["send_failed"] += 1
            self.recent.appendleft({
                "at": iso(now), "symbol": ev.symbol, "side": ev.side,
                "score": round(ev.score, 1), "status": status,
                "metrics": ev.m, "levels": {k: round(v, 8) for k, v in ev.levels.items()},
            })
        return sent, suppressed

    async def _handle_message(self, raw):
        self.last_message_at = time.time()
        try:
            envelope = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            self.errors["invalid_json"] += 1
            return
        data = envelope.get("data", envelope)
        if data.get("e") == "serverShutdown":
            raise ConnectionError("Binance announced WebSocket server shutdown")
        if data.get("e") != "kline":
            return
        k = data.get("k") or {}
        if not k.get("x"):  # strategy always evaluates completed candles only
            return
        symbol = str(k.get("s") or data.get("s") or "").upper()
        if symbol not in self.buffers:
            self.errors["unknown_symbol"] += 1
            return
        row = [
            int(k["t"]), str(k["o"]), str(k["h"]), str(k["l"]),
            str(k["c"]), str(k["v"]), int(k["T"]), str(k["q"]),
        ]
        await self.on_closed_kline(symbol, row)

    async def _consume_connection(self):
        connected_at = time.monotonic()
        async with self.session.ws_connect(
            self.stream_url, heartbeat=30, autoping=True, autoclose=True,
            timeout=20, receive_timeout=75, max_msg_size=4 * 1024 * 1024,
        ) as ws:
            self.connected = True
            self.connection_count += 1
            log.info("Binance WebSocket connected: %d kline streams (%s)", len(self.symbols), self.cfg.interval)
            while time.monotonic() - connected_at < CONNECTION_ROTATE_SECONDS:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                    raise ConnectionError("Binance WebSocket closed")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise ConnectionError(f"Binance WebSocket error: {ws.exception()}")
            log.info("Rotating Binance WebSocket connection before its 24-hour limit")

    async def run(self):
        delay = 1
        while True:
            try:
                await self._consume_connection()
                delay = 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.errors["websocket_disconnects"] += 1
                self.last_disconnect = {"at": iso(time.time()), "reason": str(exc)[:240]}
                log.warning("Binance WebSocket disconnected: %s", exc)
            finally:
                self.connected = False
            self.reconnects += 1
            log.info("Reconnecting to Binance WebSocket in %ss", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_SECONDS)

    def status(self):
        if not self.connected:
            return "reconnecting" if self.connection_count else "starting"
        if self.last_message_at is None:
            return "starting"
        return "ok" if time.time() - self.last_message_at <= 90 else "stale"

    def summary(self):
        c = self.cfg
        return {
            "status": self.status(),
            "uptime_s": int(time.time() - self.started),
            "telegram_configured": self.notifier.enabled,
            "market_data": "binance_spot_websocket",
            "websocket_connected": self.connected,
            "websocket_connections": self.connection_count,
            "websocket_reconnects": self.reconnects,
            "websocket_streams": len(self.symbols),
            "websocket_interval": c.interval,
            "last_message_at": iso(self.last_message_at) if self.last_message_at else None,
            "last_kline_at": iso(self.last_kline_at) if self.last_kline_at else None,
            "last_disconnect": self.last_disconnect,
            "symbols_total": len(self.symbols),
            "symbols_ready": len(self.symbols) - len(self.warmup_symbols),
            "symbols_warming": sorted(self.warmup_symbols),
            "warmup_done": not self.warmup_symbols,
            "last_scan": {k: v for k, v in self.last_scan.items() if k != "ts"},
            "totals": dict(self.totals),
            "rejected_by_gate": {g: self.rejected.get(g, 0) for g in GATES} | {
                k: v for k, v in self.rejected.items() if k not in GATES},
            "suppressed": dict(self.suppressed),
            "stream_errors": dict(self.errors),
            "signals_last_hour": len(self.sent_times),
            "rules": {"vol_x>=": c.vol_mult, "range_atr>=": c.range_mult, "body>=": c.min_body,
                      "close_loc>=": c.close_loc, "rsi": [c.rsi_min, c.rsi_max],
                      "max_ext_atr": c.max_ext_atr, "cooldown_min": c.cooldown_min,
                      "max_per_hour": c.max_per_hour, "max_per_scan": c.max_per_scan,
                      "min_score": c.min_score},
            "endpoints": ["/ping", "/health", "/health/qualified", "/health/rejections", "/health/symbol/BTCUSDT"],
        }

    def qualified_view(self):
        return {"total_qualified": self.totals.get("qualified", 0), "total_sent": self.totals.get("sent", 0),
                "recent": list(self.recent)}

    def rejections_view(self):
        by_reason = {}
        for symbol, result in self.last_eval.items():
            by_reason.setdefault(result["reason"], []).append(symbol)
        near = [(symbol, result) for symbol, result in self.last_eval.items()
                if not result["passed"] and result.get("depth", -1) >= 4]
        near.sort(key=lambda item: (-item[1]["depth"], -item[1].get("metrics", {}).get("vol_x", 0)))
        return {
            "last_scan": self.last_scan.get("at"),
            "by_reason": {reason: {"count": len(symbols), "symbols": sorted(symbols)}
                          for reason, symbols in by_reason.items()},
            "near_misses": [{"symbol": symbol, "failed_gate": result["reason"],
                             "gates_passed": result["depth"], "side": result["side"],
                             "metrics": result.get("metrics", {})} for symbol, result in near[:15]],
        }
