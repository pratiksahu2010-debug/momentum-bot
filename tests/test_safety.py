import os
import sys
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from config import load
from momentum import evaluate
import test_strategy as ts


def test_maximum_range_atr_is_enforced():
    candles = ts.base()
    p = float(candles[-1][4])
    candles.append(ts.kl(p, p + 2.0, p - 0.02, p + 1.9, 250000, 79))
    ev = evaluate("TESTUSDT", candles, load())
    assert not ev.passed
    assert ev.reason == "max_range"


def test_breakout_extension_cap_is_enforced():
    candles = ts.base()
    p = float(candles[-1][4])
    candles.append(ts.kl(p, p + 1.2, p - 0.01, p + 1.1, 250000, 79))
    cfg = replace(load(), max_ext_atr=3.5)
    ev = evaluate("TESTUSDT", candles, cfg)
    assert not ev.passed
    assert ev.reason == "extension", (ev.reason, ev.m)


def test_structural_stop_is_never_capped_inside_signal_candle():
    candles = ts.base()
    p = float(candles[-1][4])
    candles.append(ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79))
    cfg = replace(load(), sl_atr=0.5)
    ev = evaluate("TESTUSDT", candles, cfg)
    assert not ev.passed
    assert ev.reason == "stop_too_wide"


def test_qualified_stop_is_outside_signal_candle():
    candles = ts.base()
    p = float(candles[-1][4])
    candles.append(ts.kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79))
    ev = evaluate("TESTUSDT", candles, load())
    assert ev.passed, (ev.reason, ev.m)
    assert ev.levels["sl"] < float(candles[-1][3])
    assert ev.levels["sl"] < ev.levels["entry"] < ev.levels["tp1"] < ev.levels["tp2"]


def test_invalid_numeric_configuration_is_rejected():
    old = os.environ.get("VOL_MULT")
    try:
        os.environ["VOL_MULT"] = "nan"
        try:
            load()
        except ValueError:
            pass
        else:
            raise AssertionError("NaN VOL_MULT was accepted")
    finally:
        if old is None:
            os.environ.pop("VOL_MULT", None)
        else:
            os.environ["VOL_MULT"] = old
