import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from config import load
from engine import Engine
import test_strategy as ts


class FakeNotifier:
    enabled = True

    def __init__(self):
        self.msgs = []

    async def send(self, message):
        self.msgs.append(message)
        return True


def test_closed_candle_signal_and_duplicate_guard():
    async def run():
        cfg = load()
        notifier = FakeNotifier()
        engine = Engine(cfg, session=None, notifier=notifier, symbols=["TESTUSDT"])
        for candle in ts.base():
            await engine.on_closed_kline("TESTUSDT", candle)
        assert engine.summary()["symbols_ready"] == 1
        p = float(ts.base()[-1][4])
        breakout = ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 300000, 79)
        await engine.on_closed_kline("TESTUSDT", breakout)
        assert len(notifier.msgs) == 1, engine.last_eval["TESTUSDT"]
        assert "MOMENTUM EXPLOSION" in notifier.msgs[0]
        await engine.on_closed_kline("TESTUSDT", breakout)
        assert len(notifier.msgs) == 1
        assert engine.totals["duplicate_candles"] == 1
    asyncio.run(run())


def test_per_candle_has_no_alert_cap():
    async def run():
        cfg = load()
        symbols = ["AUSDT", "BUSDT", "CUSDT", "DUSDT"]
        notifier = FakeNotifier()
        engine = Engine(cfg, session=None, notifier=notifier, symbols=symbols)
        history = ts.base()
        for candle in history:
            for symbol in symbols:
                await engine.on_closed_kline(symbol, candle)
        p = float(history[-1][4])
        breakout = ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 300000, 79)
        for symbol in symbols:
            await engine.on_closed_kline(symbol, breakout)
        assert len(notifier.msgs) == len(symbols)
        assert engine.suppressed["scan_cap"] == 0
        assert engine.suppressed["hourly_cap"] == 0
    asyncio.run(run())


def test_incomplete_batch_reports_missing_and_late_symbol_is_still_evaluated():
    async def run():
        cfg = load()
        notifier = FakeNotifier()
        engine = Engine(cfg, session=None, notifier=notifier, symbols=["AUSDT", "BUSDT"])
        history = ts.base()
        now_ms = int(history[-1][6]) + 1
        assert engine._seed_history("AUSDT", history, now_ms=now_ms)
        assert engine._seed_history("BUSDT", history, now_ms=now_ms)

        p = float(history[-1][4])
        late_candle = ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79)
        await engine.on_closed_kline("AUSDT", late_candle)
        await engine.flush_pending()

        assert engine.last_scan["evaluated"] == 1
        assert engine.last_scan["missing_symbols"] == ["BUSDT"]
        assert engine.last_scan["coverage_pct"] == 50.0

        # A delayed symbol must not create a second scan or lose its candidate.
        await engine.on_closed_kline("BUSDT", late_candle)
        assert engine.totals["scans"] == 1
        assert engine.totals["late_candle_arrivals"] == 1
        assert engine.last_scan["late_arrivals"] == 1
        assert engine.totals["evaluated"] == 2
        assert len(notifier.msgs) == 2

    asyncio.run(run())


def test_stream_message_ignores_open_candles():
    async def run():
        engine = Engine(load(), session=None, notifier=FakeNotifier(), symbols=["TESTUSDT"])
        open_event = {
            "stream": "testusdt@kline_5m",
            "data": {"e": "kline", "s": "TESTUSDT", "k": {
                "t": 0, "T": 299999, "o": "100", "h": "101", "l": "99",
                "c": "100.5", "v": "10", "q": "1000", "x": False,
            }},
        }
        await engine._handle_message(json.dumps(open_event))
        assert len(engine.buffers["TESTUSDT"]) == 0
    asyncio.run(run())


def test_gap_clears_history_and_restarts_warmup():
    async def run():
        engine = Engine(load(), session=None, notifier=FakeNotifier(), symbols=["TESTUSDT"])
        for candle in ts.base():
            await engine.on_closed_kline("TESTUSDT", candle)
        assert len(engine.buffers["TESTUSDT"]) == len(ts.base())
        p = float(ts.base()[-1][4])
        gap_candle = ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 80)
        await engine.on_closed_kline("TESTUSDT", gap_candle)
        assert len(engine.buffers["TESTUSDT"]) == 1
        assert "TESTUSDT" in engine.warmup_symbols
        assert engine.totals["candle_gaps"] == 1
        assert engine.totals["missed_intervals"] == 1
    asyncio.run(run())


def test_symbol_universe_validation():
    engine = Engine(load(), session=None, notifier=FakeNotifier(), symbols=["BTCUSDT"])
    assert engine.symbols == ["BTCUSDT"]
    for invalid in ([], ["BTCUSDT", "BTCUSDT"], ["BTC/USDT"], ["BTCUSDT"] * 1025):
        try:
            Engine(load(), session=None, notifier=FakeNotifier(), symbols=invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid symbol universe accepted: {invalid[:3]}")


def test_rest_history_seeds_only_closed_contiguous_candles_without_signals():
    engine = Engine(load(), session=None, notifier=FakeNotifier(), symbols=["TESTUSDT"])
    rows = ts.base()[-50:]
    now_ms = int(rows[-1][6]) + 1
    assert engine._seed_history("TESTUSDT", rows, now_ms=now_ms)
    assert len(engine.buffers["TESTUSDT"]) == 50
    assert engine.summary()["symbols_ready"] == 1
    assert engine.notifier.msgs == []
    assert engine.last_seen["TESTUSDT"] == int(rows[-1][0])


def test_rest_history_ignores_unclosed_candle_and_requires_full_warmup():
    engine = Engine(load(), session=None, notifier=FakeNotifier(), symbols=["TESTUSDT"])
    rows = ts.base()[-50:]
    # The last row is still forming at this point; only 49 closed candles remain.
    now_ms = int(rows[-1][6])
    assert not engine._seed_history("TESTUSDT", rows, now_ms=now_ms)
    assert len(engine.buffers["TESTUSDT"]) == 0
    assert engine.summary()["symbols_ready"] == 0


def test_rest_history_discards_gap_before_seeding():
    engine = Engine(load(), session=None, notifier=FakeNotifier(), symbols=["TESTUSDT"])
    rows = ts.base()[-51:]
    rows = rows[:20] + rows[21:]
    now_ms = int(rows[-1][6]) + 1
    # The most recent uninterrupted tail is shorter than the 50-candle requirement.
    assert not engine._seed_history("TESTUSDT", rows, now_ms=now_ms)
    assert len(engine.buffers["TESTUSDT"]) == 0
