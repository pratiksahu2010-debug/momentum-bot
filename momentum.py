"""
Momentum Explosion detector.

Keeps the breakout strategy and soft RSI scoring. Safety guards:
- liquidity, volume and minimum/maximum range
- strong body/close and Donchian breakout
- previous-candle late-entry and breakout-extension checks
- structural stop must remain beyond the signal candle extreme
"""
from dataclasses import dataclass, field
import math

from indicators import atr, rsi

GATES = (
    "liquidity", "volume", "range", "max_range", "body",
    "breakout", "late_entry", "extension",
)


@dataclass
class Eval:
    symbol: str
    candle_open: int
    passed: bool
    reason: str
    depth: int
    side: str = "-"
    score: float = 0.0
    m: dict = field(default_factory=dict)
    levels: dict = field(default_factory=dict)

    def as_dict(self, at=""):
        return {
            "at": at, "passed": self.passed, "reason": self.reason,
            "depth": self.depth, "side": self.side,
            "score": round(self.score, 1), "metrics": self.m,
        }


def _clip(x):
    return max(0.0, min(1.0, x))


def evaluate(symbol, k, cfg) -> Eval:
    """Evaluate CLOSED Binance-format klines only."""
    L = cfg.lookback
    if len(k) < max(50, L + 16):
        return Eval(symbol, 0, False, "insufficient_data", 0)

    try:
        o = [float(x[1]) for x in k]
        h = [float(x[2]) for x in k]
        low = [float(x[3]) for x in k]
        c = [float(x[4]) for x in k]
        q = [float(x[7]) for x in k]
        t = int(k[-1][0])
    except (IndexError, TypeError, ValueError, OverflowError):
        return Eval(symbol, 0, False, "invalid_candle", 0)

    if not all(math.isfinite(v) for series in (o, h, low, c, q) for v in series):
        return Eval(symbol, t, False, "invalid_candle", 0)
    if any(v <= 0 for v in o + h + low + c) or any(v < 0 for v in q):
        return Eval(symbol, t, False, "invalid_candle", 0)
    if any(h[i] < max(o[i], c[i], low[i]) or low[i] > min(o[i], c[i]) for i in range(len(k))):
        return Eval(symbol, t, False, "invalid_candle", 0)

    long = c[-1] >= o[-1]
    side = "LONG" if long else "SHORT"
    ev = Eval(symbol, t, False, "", 0, side)
    rng = h[-1] - low[-1]
    a = atr(h[:-1], low[:-1], c[:-1], 14)
    if rng <= 0 or not math.isfinite(a) or a <= 0:
        ev.reason = "flat"
        return ev
    if not long and not cfg.allow_short:
        ev.reason = "short_disabled"
        return ev

    avg_q = sum(q[-L - 1:-1]) / L
    vol_x = q[-1] / avg_q if avg_q > 0 else 0.0
    range_atr = rng / a
    body = abs(c[-1] - o[-1]) / rng
    loc = (c[-1] - low[-1]) / rng if long else (h[-1] - c[-1]) / rng
    hh, ll = max(h[-L - 1:-1]), min(low[-L - 1:-1])
    broke = c[-1] > hh if long else c[-1] < ll
    prev_broke = c[-2] > max(h[-L - 2:-2]) if long else c[-2] < min(low[-L - 2:-2])
    breakout_level = hh if long else ll
    extension_atr = max(0.0, (c[-1] - breakout_level) / a if long
                        else (breakout_level - c[-1]) / a)
    r = rsi(c, 14)
    rsi_dir = r if long else 100 - r

    ev.m = {
        "price": c[-1], "vol_x": round(vol_x, 2),
        "range_atr": round(range_atr, 2), "body": round(body, 2),
        "close_loc": round(loc, 2), "rsi": round(r, 1),
        "avg_qvol": round(avg_q), "broke": bool(broke),
        "extension_atr": round(extension_atr, 2),
    }

    checks = (
        avg_q >= cfg.min_avg_qvol,
        vol_x >= cfg.vol_mult,
        range_atr >= cfg.range_mult,
        range_atr <= cfg.max_range_atr,
        body >= cfg.min_body and loc >= cfg.close_loc,
        broke,
        not prev_broke,
        extension_atr <= cfg.max_ext_atr,
    )
    for i, ok in enumerate(checks):
        if not ok:
            ev.reason, ev.depth = GATES[i], i
            return ev

    # Structural stop: must be outside the signal candle. If that stop is
    # wider than the configured ATR risk ceiling, reject instead of placing
    # a capped stop inside a price range already traded.
    structural_risk = (c[-1] - low[-1] if long else h[-1] - c[-1]) + 0.1 * a
    max_risk = cfg.sl_atr * a
    if not math.isfinite(structural_risk) or structural_risk <= 0 or structural_risk > max_risk:
        ev.reason, ev.depth = "stop_too_wide", len(GATES)
        return ev

    risk = structural_risk
    s = 1 if long else -1
    sl = c[-1] - s * risk
    if (long and sl >= low[-1]) or (not long and sl <= h[-1]):
        ev.reason, ev.depth = "invalid_stop", len(GATES)
        return ev

    ev.passed, ev.reason, ev.depth = True, "qualified", len(GATES)
    ev.score = (
        40 * _clip(vol_x / (cfg.vol_mult * 2))
        + 30 * _clip(range_atr / (cfg.range_mult * 2))
        + 15 * _clip((loc - cfg.close_loc) / (1 - cfg.close_loc))
        + 15 * _clip(1 - abs(rsi_dir - 70) / 15)
    )
    ev.levels = {
        "entry": c[-1], "sl": sl,
        "tp1": c[-1] + s * cfg.rr1 * risk,
        "tp2": c[-1] + s * cfg.rr2 * risk,
        "risk_pct": risk / c[-1] * 100,
    }
    return ev
