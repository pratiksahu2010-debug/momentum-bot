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
        breakout = ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79)
        await engine.on_closed_kline("TESTUSDT", breakout)
        assert len(notifier.msgs) == 1, engine.last_eval["TESTUSDT"]
        assert "MOMENTUM EXPLOSION" in notifier.msgs[0]
        await engine.on_closed_kline("TESTUSDT", breakout)
        assert len(notifier.msgs) == 1
        assert engine.totals["duplicate_candles"] == 1
    asyncio.run(run())


def test_per_candle_batch_cap():
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
        breakout = ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79)
        for symbol in symbols:
            await engine.on_closed_kline(symbol, breakout)
        assert len(notifier.msgs) == cfg.max_per_scan == 3
        assert engine.suppressed["scan_cap"] == 1
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
        # Skip one full 5-minute interval (expected index 79, received index 80).
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
