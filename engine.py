import asyncio
import json
import logging
import re
import time
from collections import Counter, deque
from datetime import datetime, timezone

import aiohttp

from momentum import GATES, evaluate
from notifier import format_signal

log = logging.getLogger("engine")
RECONNECT_MAX_SECONDS = 60
CONNECTION_ROTATE_SECONDS = 23 * 60 * 60 + 50 * 60
BATCH_WINDOW_SECONDS = 10
HISTORY_REQUEST_LIMIT = 100
HISTORY_CONCURRENCY = 2
HISTORY_RETRIES = 2
HISTORY_REQUEST_SPACING_SECONDS = 0.25


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class Engine:
    """Binance Spot combined-WebSocket consumer with one-time REST history bootstrap."""

    def __init__(self, cfg, session, notifier, symbols):
        self.cfg, self.session, self.notifier, self.symbols = cfg, session, notifier, list(symbols)
        if not self.symbols or len(self.symbols) > 1024:
            raise ValueError("Symbol universe must contain 1..1024 symbols")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("Symbol universe contains duplicates")
        invalid = [s for s in self.symbols if not re.fullmatch(r"[A-Z0-9]{1,20}USDT", s)]
        if invalid:
            raise ValueError(f"Invalid Binance USDT symbol format: {invalid[:10]}")
        self.required_history = max(50, cfg.lookback + 16)
        self.history_size = max(80, self.required_history)
        self.buffers = {s: deque(maxlen=self.history_size) for s in self.symbols}
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
        self.flushed_batch_order = deque()
        self.flushed_batch_set = set()
        self.started = time.time()
        self.connected = False
        self.connection_count = 0
        self.reconnects = 0
        self.last_message_at = None
        self.last_kline_at = None
        self.last_disconnect = None
        self.bootstrap_started_at = None
        self.bootstrap_completed_at = None
        self.bootstrap_loaded_symbols = 0
        self.bootstrap_failed_symbols = 0
        self.bootstrap_enabled = cfg.history_bootstrap
        self.history_rate_limited = False
        self.history_retry_after_at = None
        self.history_bootstrap_attempts = 0
        self.history_request_lock = asyncio.Lock()
        self.history_last_request_at = 0.0
        self.history_last_candle_at = None
        self.last_scan = {}
        self.symbol_validation = {"validation_ok": None, "configured": len(self.symbols), "active_total": None, "selected_total": len(self.symbols), "rejected_configured": [], "replacements": []}
        self.stream_url = self._build_stream_url()

    def _build_stream_url(self):
        streams = "/".join(f"{symbol.lower()}@kline_{self.cfg.interval}" for symbol in self.symbols)
        return f"{self.cfg.ws_url.rstrip('/')}/stream?streams={streams}"

    def _seed_history(self, symbol, rows, now_ms=None):
        """Seed only the latest contiguous, fully closed REST candles. Never evaluates them."""
        if symbol not in self.buffers or not isinstance(rows, list):
            return False
        now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        interval_ms = self.cfg.interval_seconds * 1000
        clean = []
        seen = set()
        for row in rows:
            try:
                if not isinstance(row, (list, tuple)) or len(row) < 8:
                    continue
                open_time = int(row[0])
                close_time = int(row[6])
                values = [float(row[i]) for i in (1, 2, 3, 4, 7)]
                if close_time >= now_ms or open_time in seen:
                    continue
                o, h, low, c, q = values
                if not all(map(lambda v: v >= 0 and v < float("inf"), values)):
                    continue
                if min(o, h, low, c) <= 0 or h < max(o, c, low) or low > min(o, c):
                    continue
                seen.add(open_time)
                clean.append([
                    open_time, str(row[1]), str(row[2]), str(row[3]),
                    str(row[4]), str(row[5]), close_time, str(row[7]),
                ])
            except (TypeError, ValueError, IndexError, OverflowError):
                continue

        clean.sort(key=lambda candle: candle[0])
        # Retain only the latest uninterrupted segment; indicator windows must not
        # bridge a missing market-data interval.
        contiguous = []
        for candle in clean:
            if contiguous and candle[0] != contiguous[-1][0] + interval_ms:
                contiguous = []
            contiguous.append(candle)
        if len(contiguous) < self.required_history:
            return False

        selected = contiguous[-self.history_size:]
        # Bootstrap may finish after the live socket has already delivered candles.
        # Merge those newer live candles instead of overwriting them with older REST data.
        existing = list(self.buffers[symbol])
        if existing:
            merged = {int(c[0]): c for c in selected}
            for candle in existing:
                merged[int(candle[0])] = candle
            ordered = [merged[k] for k in sorted(merged)]
            latest_segment = []
            for candle in ordered:
                if latest_segment and int(candle[0]) != int(latest_segment[-1][0]) + interval_ms:
                    latest_segment = []
                latest_segment.append(candle)
            selected = latest_segment[-self.history_size:]
        if len(selected) < self.required_history:
            return False
        self.buffers[symbol].clear()
        self.buffers[symbol].extend(selected)
        self.last_seen[symbol] = int(selected[-1][0])
        self.warmup_symbols.discard(symbol)
        # Do not overwrite a more recent live evaluation with a historical marker.
        if not existing:
            self.last_eval[symbol] = {
                "at": iso(time.time()), "passed": False, "reason": "history_seeded_waiting_for_live_candle",
                "depth": 0, "side": "-", "score": 0, "metrics": {},
            }
        self.history_last_candle_at = max(
            self.history_last_candle_at or 0, selected[-1][6] / 1000
        )
        self.totals["history_candles_loaded"] += len(selected)
        self.totals["symbols_bootstrapped"] += 1
        return True

    async def _fetch_symbol_history(self, symbol, semaphore):
        if self.session is None:
            return False
        url = f"{self.cfg.rest_url.rstrip('/')}/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": self.cfg.interval,
            "limit": min(HISTORY_REQUEST_LIMIT, self.required_history + 10),
        }
        async with semaphore:
            for attempt in range(HISTORY_RETRIES):
                if self.history_rate_limited:
                    return False
                try:
                    # Pace request starts across all symbols. Two in flight is enough
                    # to keep startup moving without a burst against Binance's shared IP limits.
                    async with self.history_request_lock:
                        wait = HISTORY_REQUEST_SPACING_SECONDS - (time.monotonic() - self.history_last_request_at)
                        if wait > 0:
                            await asyncio.sleep(wait)
                        self.history_last_request_at = time.monotonic()
                    timeout = aiohttp.ClientTimeout(total=15, connect=8, sock_read=10)
                    async with self.session.get(url, params=params, timeout=timeout) as response:
                        if response.status == 200:
                            rows = await response.json()
                            loaded = self._seed_history(symbol, rows)
                            if not loaded:
                                self.errors["history_insufficient_or_invalid"] += 1
                            return loaded
                        if response.status in (418, 429):
                            self.errors[f"history_http_{response.status}"] += 1
                            # Stop the whole bootstrap on a rate-limit response. Retrying
                            # every queued symbol can extend an IP ban; live WebSocket data
                            # must not wait for REST recovery.
                            self.history_rate_limited = True
                            retry_after = response.headers.get("Retry-After")
                            try:
                                retry_seconds = max(1, int(float(retry_after))) if retry_after else 60
                            except (TypeError, ValueError, OverflowError):
                                retry_seconds = 60
                            retry_at = time.time() + retry_seconds
                            self.history_retry_after_at = max(self.history_retry_after_at or 0, retry_at)
                            log.warning(
                                "Binance REST history rate-limited (%s, Retry-After=%s); "
                                "aborting remaining bootstrap requests and continuing WebSocket-only",
                                response.status, retry_after or "not supplied",
                            )
                            return False
                        self.errors[f"history_http_{response.status}"] += 1
                        if response.status >= 500 and attempt + 1 < HISTORY_RETRIES:
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            continue
                        return False
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                    self.errors["history_request_errors"] += 1
                    if attempt + 1 < HISTORY_RETRIES:
                        await asyncio.sleep(0.5 * (2 ** attempt))
                    else:
                        log.warning("History bootstrap failed for %s: %s", symbol, str(exc)[:180])
                        return False
        return False

    async def bootstrap_history(self):
        """Continuously and safely seed missing symbols; never blocks the live WebSocket."""
        if not self.bootstrap_enabled:
            log.info("Historical bootstrap disabled; using WebSocket warm-up")
            return

        semaphore = asyncio.Semaphore(HISTORY_CONCURRENCY)
        loaded_symbols = set()
        consecutive_failures = 0
        retry_base = 30
        retry_cap = 900

        while len(loaded_symbols) < len(self.symbols):
            self.bootstrap_started_at = self.bootstrap_started_at or time.time()
            self.history_bootstrap_attempts += 1
            pending_symbols = [s for s in self.symbols if s not in loaded_symbols]
            log.info(
                "Starting Binance history bootstrap attempt %d: %d pending symbols, %d closed candles each",
                self.history_bootstrap_attempts, len(pending_symbols), self.required_history,
            )

            results = await asyncio.gather(
                *(self._fetch_symbol_history(symbol, semaphore) for symbol in pending_symbols),
                return_exceptions=True,
            )
            newly_loaded = 0
            for symbol, result in zip(pending_symbols, results):
                if result is True:
                    loaded_symbols.add(symbol)
                    newly_loaded += 1
                elif isinstance(result, Exception):
                    self.errors["history_unhandled_errors"] += 1
                    log.warning("History bootstrap exception for %s: %s", symbol, str(result)[:180])

            self.bootstrap_loaded_symbols = len(loaded_symbols)
            self.bootstrap_failed_symbols = len(self.symbols) - len(loaded_symbols)
            self.totals["history_bootstrap_loaded_symbols"] = self.bootstrap_loaded_symbols
            self.totals["history_bootstrap_failed_symbols"] = self.bootstrap_failed_symbols

            if not pending_symbols or self.bootstrap_failed_symbols == 0:
                break

            if newly_loaded:
                consecutive_failures = 0
            else:
                consecutive_failures += 1

            # Honour an exchange ban exactly; otherwise back off progressively for
            # transient network/5xx/invalid-history failures. Retry only missing pairs.
            if self.history_rate_limited:
                wait_seconds = max(
                    1, int((self.history_retry_after_at or (time.time() + retry_base)) - time.time())
                )
                log.warning(
                    "Binance history rate-limited; pausing remaining %d symbols for %ds",
                    self.bootstrap_failed_symbols, wait_seconds,
                )
            else:
                wait_seconds = min(retry_cap, retry_base * (2 ** min(consecutive_failures - 1, 5)))
                log.warning(
                    "History bootstrap incomplete: %d/%d loaded; retrying only missing symbols in %ds",
                    self.bootstrap_loaded_symbols, len(self.symbols), wait_seconds,
                )

            await asyncio.sleep(wait_seconds)
            self.history_rate_limited = False
            self.history_retry_after_at = None
            self.history_last_request_at = 0.0

        if self.bootstrap_failed_symbols == 0:
            self.bootstrap_completed_at = time.time()
            log.info(
                "History bootstrap completed: loaded=%d/%d attempts=%d; WebSocket remains live source",
                self.bootstrap_loaded_symbols, len(self.symbols), self.history_bootstrap_attempts,
            )
        else:
            # Defensive fallback; the loop normally continues until every symbol is seeded.
            log.warning("History bootstrap stopped with %d symbols still warming", self.bootstrap_failed_symbols)

    async def on_closed_kline(self, symbol, row):
        """Accept one Binance-format, CLOSED kline row; safe to call repeatedly."""
        if symbol not in self.buffers:
            return
        open_time = int(row[0])
        previous_open = self.last_seen.get(symbol)
        if previous_open is not None and open_time <= previous_open:
            self.totals["duplicate_candles"] += 1
            return
        candles = self.buffers[symbol]
        if previous_open is not None:
            interval_ms = self.cfg.interval_seconds * 1000
            expected_open = previous_open + interval_ms
            if open_time != expected_open:
                self.totals["candle_gaps"] += 1
                self.totals["missed_intervals"] += max(1, (open_time - expected_open) // interval_ms)
                candles.clear()
                self.warmup_symbols.add(symbol)
                self.last_eval[symbol] = {
                    "at": iso(time.time()), "passed": False, "reason": "candle_gap_warmup",
                    "depth": 0, "side": "-", "score": 0, "metrics": {},
                }
        self.last_seen[symbol] = open_time
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

        # If this symbol's candle arrives after its batch was finalized, still
        # evaluate and dispatch it independently. Never create a second scan
        # for the same candle or silently drop a late qualified candidate.
        if open_time in self.flushed_batch_set:
            self.totals["late_candle_arrivals"] += 1
            self.totals["evaluated"] += 1
            if ev.passed:
                self.totals["qualified"] += 1
                sent, suppressed = await self._dispatch([ev], time.time(), {symbol} if warming else set())
                self.totals["sent"] += sent
                self.suppressed.update(suppressed)
            else:
                self.rejected[ev.reason] += 1
            self.last_scan["late_arrivals"] = self.last_scan.get("late_arrivals", 0) + 1
            return

        batch = self.pending_batches.get(open_time)
        if batch is None:
            batch = {
                "started": time.time(), "processed": 0, "evaluated": 0,
                "candidates": [], "rejected": Counter(), "warmup_symbols": set(),
                "symbols_seen": set(),
            }
            self.pending_batches[open_time] = batch
            self.batch_tasks[open_time] = asyncio.create_task(self._flush_after(open_time))
        batch["symbols_seen"].add(symbol)
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
        missing_symbols = sorted(set(self.symbols) - batch["symbols_seen"])
        self.flushed_batch_order.append(open_time)
        self.flushed_batch_set.add(open_time)
        while len(self.flushed_batch_order) > 32:
            oldest = self.flushed_batch_order.popleft()
            self.flushed_batch_set.discard(oldest)
        coverage = 100.0 * batch["evaluated"] / len(self.symbols)
        self.last_scan = {
            "at": iso(now), "ts": now, "candle_open": open_time,
            "duration_s": round(now - batch["started"], 2),
            "evaluated": batch["evaluated"], "expected": len(self.symbols),
            "coverage_pct": round(coverage, 1),
            "missing_count": len(missing_symbols), "missing_symbols": missing_symbols,
            "late_arrivals": 0,
            "qualified": len(candidates), "sent": sent,
            "rejected": dict(rejected), "suppressed": dict(suppressed), "errors": {},
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
        if isinstance(envelope, dict) and "code" in envelope and "msg" in envelope:
            self.errors["binance_stream_errors"] += 1
            raise ConnectionError(f"Binance stream error {envelope.get('code')}: {envelope.get('msg')}")
        data = envelope.get("data", envelope)
        if not isinstance(data, dict):
            self.errors["invalid_message"] += 1
            return
        if data.get("e") == "serverShutdown":
            raise ConnectionError("Binance announced WebSocket server shutdown")
        if data.get("e") != "kline":
            return
        k = data.get("k") or {}
        if not k.get("x"):
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
        # Start the live socket immediately. Historical REST seeding runs in the
        # background and must never delay WebSocket connection or health readiness.
        bootstrap_task = asyncio.create_task(self.bootstrap_history())
        delay = 1
        try:
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
        finally:
            if not bootstrap_task.done():
                bootstrap_task.cancel()
                await asyncio.gather(bootstrap_task, return_exceptions=True)

    def status(self):
        if not self.connected:
            return "reconnecting" if self.connection_count else "starting"
        if self.last_message_at is None:
            return "starting"
        return "ok" if time.time() - self.last_message_at <= 90 else "stale"

    def summary(self):
        c = self.cfg
        ready = len(self.symbols) - len(self.warmup_symbols)
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
            "symbol_validation": self.symbol_validation,
            "symbols_ready": ready,
            "symbols_warming": sorted(self.warmup_symbols),
            "warmup_done": not self.warmup_symbols,
            "strategy_ready": not self.warmup_symbols and self.connected and self.status() == "ok",
            "history_bootstrap_enabled": self.bootstrap_enabled,
            "history_rate_limited": self.history_rate_limited,
            "history_retry_after_at": iso(self.history_retry_after_at) if self.history_retry_after_at else None,
            "history_bootstrap_attempts": self.history_bootstrap_attempts,
            "history_bootstrap_started_at": iso(self.bootstrap_started_at) if self.bootstrap_started_at else None,
            "history_bootstrap_completed_at": iso(self.bootstrap_completed_at) if self.bootstrap_completed_at else None,
            "history_bootstrap_loaded_symbols": self.bootstrap_loaded_symbols,
            "history_bootstrap_failed_symbols": self.bootstrap_failed_symbols,
            "history_last_candle_at": iso(self.history_last_candle_at) if self.history_last_candle_at else None,
            "last_scan": {k: v for k, v in self.last_scan.items() if k != "ts"},
            "totals": dict(self.totals),
            "rejected_by_gate": {g: self.rejected.get(g, 0) for g in GATES} | {
                k: v for k, v in self.rejected.items() if k not in GATES},
            "suppressed": dict(self.suppressed),
            "stream_errors": dict(self.errors),
            "signals_last_hour": len(self.sent_times),
            "rules": {"mandatory_gates": ["liquidity", "volume", "body", "donchian_breakout"],
                      "avg_quote_volume>=": c.min_avg_qvol, "vol_x>=": c.vol_mult,
                      "body>=": c.min_body, "range_atr_soft_score_from": c.range_mult,
                      "range_atr_safety_max": c.max_range_atr,
                      "close_location_soft_score_only": True,
                      "close_loc_score_reference": c.close_loc,
                      "rsi_soft_score_band": [c.rsi_min, c.rsi_max],
                      "breakout_extension_atr<=": c.max_ext_atr, "stop_atr<=": c.sl_atr,
                      "cooldown_min": c.cooldown_min,
                      "alert_count_caps": "disabled", "min_score": c.min_score},
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
            "scan_coverage": {
                "evaluated": self.last_scan.get("evaluated", 0),
                "expected": self.last_scan.get("expected", len(self.symbols)),
                "coverage_pct": self.last_scan.get("coverage_pct", 0),
                "missing_symbols": self.last_scan.get("missing_symbols", []),
                "late_arrivals": self.last_scan.get("late_arrivals", 0),
                "batch_window_seconds": BATCH_WINDOW_SECONDS,
            },
            "by_reason": {reason: {"count": len(symbols), "symbols": sorted(symbols)}
                          for reason, symbols in by_reason.items()},
            "near_misses": [{"symbol": symbol, "failed_gate": result["reason"],
                             "gates_passed": result["depth"], "side": result["side"],
                             "metrics": result.get("metrics", {})} for symbol, result in near[:15]],
        }
