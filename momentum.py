"""
MOMENTUM EXPLOSION detector (single module).

Hard gates are deliberately minimal:
  1. Liquidity floor
  2. Quote-volume spike vs the prior LOOKBACK candles
  3. Range expansion vs ATR(14)
  4. Strong candle body and close location
  5. Donchian breakout in the candle direction
  6. Reject a breakout that already happened on the previous candle

RSI is retained as a soft scoring factor, not a mandatory gate.
EMA extension is not a hard rejection filter.
"""
from dataclasses import dataclass, field

from indicators import atr, rsi

GATES = ("liquidity", "volume", "range", "body", "breakout", "late_entry")


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
        return {"at": at, "passed": self.passed, "reason": self.reason, "depth": self.depth,
                "side": self.side, "score": round(self.score, 1), "metrics": self.m}


def _clip(x):
    return max(0.0, min(1.0, x))


def evaluate(symbol, k, cfg) -> Eval:
    """k = list of CLOSED klines (Binance format). Returns an Eval."""
    L = cfg.lookback
    if len(k) < max(50, L + 16):
        return Eval(symbol, 0, False, "insufficient_data", 0)

    o = [float(x[1]) for x in k]
    h = [float(x[2]) for x in k]
    l = [float(x[3]) for x in k]
    c = [float(x[4]) for x in k]
    q = [float(x[7]) for x in k]
    t = int(k[-1][0])

    long = c[-1] >= o[-1]
    side = "LONG" if long else "SHORT"
    ev = Eval(symbol, t, False, "", 0, side)

    rng = h[-1] - l[-1]
    a = atr(h[:-1], l[:-1], c[:-1], 14)
    if rng <= 0 or a <= 0:
        ev.reason = "flat"
        return ev
    if not long and not cfg.allow_short:
        ev.reason = "short_disabled"
        return ev

    avg_q = sum(q[-L - 1:-1]) / L
    vol_x = q[-1] / avg_q if avg_q > 0 else 0.0
    range_atr = rng / a
    body = abs(c[-1] - o[-1]) / rng
    loc = (c[-1] - l[-1]) / rng if long else (h[-1] - c[-1]) / rng
    hh, ll = max(h[-L - 1:-1]), min(l[-L - 1:-1])
    broke = c[-1] > hh if long else c[-1] < ll
    prev_broke = c[-2] > max(h[-L - 2:-2]) if long else c[-2] < min(l[-L - 2:-2])
    r = rsi(c, 14)
    rsi_dir = r if long else 100 - r

    ev.m = {"price": c[-1], "vol_x": round(vol_x, 2), "range_atr": round(range_atr, 2),
            "body": round(body, 2), "close_loc": round(loc, 2), "rsi": round(r, 1),
            "avg_qvol": round(avg_q), "broke": bool(broke)}

    checks = (
        avg_q >= cfg.min_avg_qvol,
        vol_x >= cfg.vol_mult,
        range_atr >= cfg.range_mult,
        body >= cfg.min_body and loc >= cfg.close_loc,
        broke,
        not prev_broke,
    )
    for i, ok in enumerate(checks):
        if not ok:
            ev.reason, ev.depth = GATES[i], i
            return ev

    ev.passed, ev.reason, ev.depth = True, "qualified", len(GATES)
    ev.score = (40 * _clip(vol_x / (cfg.vol_mult * 2))
                + 30 * _clip(range_atr / (cfg.range_mult * 2))
                + 15 * _clip((loc - cfg.close_loc) / (1 - cfg.close_loc))
                + 15 * _clip(1 - abs(rsi_dir - 70) / 15))

    low_dist = (c[-1] - l[-1] if long else h[-1] - c[-1]) + 0.1 * a
    risk = min(low_dist, cfg.sl_atr * a)
    s = 1 if long else -1
    ev.levels = {"entry": c[-1], "sl": c[-1] - s * risk,
                 "tp1": c[-1] + s * cfg.rr1 * risk, "tp2": c[-1] + s * cfg.rr2 * risk,
                 "risk_pct": risk / c[-1] * 100}
    return ev
