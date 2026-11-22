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
    k.append(kl(p, p + 0.5, p - 0.02, p + 0.46, 250000, 79))
    ev = evaluate("TESTUSDT", k, cfg)
    assert ev.passed and ev.side == "LONG", (ev.reason, ev.m)
    assert ev.levels["sl"] < ev.levels["entry"] < ev.levels["tp1"] < ev.levels["tp2"]


def test_explosion_short():
    k = base()
    p = float(k[-1][4])
    k.append(kl(p, p + 0.02, p - 0.5, p - 0.46, 250000, 79))
    ev = evaluate("TESTUSDT", k, cfg)
    assert ev.passed and ev.side == "SHORT", (ev.reason, ev.m)
    assert ev.levels["tp2"] < ev.levels["tp1"] < ev.levels["entry"] < ev.levels["sl"]


def test_rejects_low_volume():
    k = base(); p = float(k[-1][4])
    k.append(kl(p, p + 0.5, p - 0.02, p + 0.46, 60000, 79))
    assert evaluate("T", k, cfg).reason == "volume"


def test_rejects_quiet():
    k = base(); p = float(k[-1][4])
    k.append(kl(p, p + 0.12, p - 0.02, p + 0.1, 50000, 79))
    assert not evaluate("T", k, cfg).passed


def test_rejects_late_entry():
    k = base(); p = float(k[-1][4])
    k.append(kl(p, p + 0.5, p - 0.02, p + 0.46, 250000, 79))
    p2 = p + 0.46
    k.append(kl(p2, p2 + 0.5, p2 - 0.02, p2 + 0.46, 250000, 80))
    ev = evaluate("T", k, cfg)
    assert not ev.passed and ev.reason in ("late_entry", "overextended", "rsi"), ev.reason


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f(); print("ok", n)
