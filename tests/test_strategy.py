import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import load
from momentum import evaluate

cfg = load()


def kl(o, h, l, c, qv, i):
    return [i * 300000, str(o), str(h), str(l), str(c), "0", i * 300000 + 299999, str(qv)]


def base(n=79):
    out, p = [], 100.0
    for i in range(n):  # calm, slowly drifting, small ranges
        o = p; c = p + (0.05 if i % 2 else -0.04); out.append(kl(o, max(o, c) + 0.1, min(o, c) - 0.1, c, 50000, i)); p = c
    return out


def test_explosion_long():
    k = base()
    p = float(k[-1][4])
    k.append(kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79))
    ev = evaluate("TESTUSDT", k, cfg)
    assert ev.passed and ev.side == "LONG", (ev.reason, ev.m)
    assert ev.levels["sl"] < ev.levels["entry"] < ev.levels["tp1"] < ev.levels["tp2"]


def test_explosion_short():
    k = base()
    p = float(k[-1][4])
    k.append(kl(p, p + 0.104, p - 0.416, p - 0.312, 250000, 79))
    ev = evaluate("TESTUSDT", k, cfg)
    assert ev.passed and ev.side == "SHORT", (ev.reason, ev.m)
    assert ev.levels["tp2"] < ev.levels["tp1"] < ev.levels["entry"] < ev.levels["sl"]


def test_rejects_low_volume():
    k = base(); p = float(k[-1][4])
    k.append(kl(p, p + 0.416, p - 0.104, p + 0.312, 60000, 79))
    assert evaluate("T", k, cfg).reason == "volume"


def test_rejects_quiet():
    k = base(); p = float(k[-1][4])
    k.append(kl(p, p + 0.12, p - 0.02, p + 0.1, 50000, 79))
    assert not evaluate("T", k, cfg).passed


def test_rejects_late_entry():
    k = base(); p = float(k[-1][4])
    k.append(kl(p, p + 0.416, p - 0.104, p + 0.312, 250000, 79))
    p2 = p + 0.416
    k.append(kl(p2, p2 + 0.416, p2 - 0.104, p2 + 0.416, 250000, 80))
    ev = evaluate("T", k, cfg)
    assert not ev.passed and ev.reason in ("late_entry", "overextended", "rsi"), ev.reason


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f(); print("ok", n)


def test_close_location_and_range_are_not_mandatory_setup_gates():
    from dataclasses import replace
    candles = base()
    p = float(candles[-1][4])
    # Lower close location and range expansion must not become hard setup gates.
    candles.append(kl(p - 0.07, p + 0.50, p - 0.10, p + 0.26, 250000, 79))
    cfg_soft = replace(cfg, close_loc=0.70, range_mult=99.0)
    ev = evaluate("TESTUSDT", candles, cfg_soft)
    assert ev.passed, (ev.reason, ev.m)
    assert ev.depth == 4
