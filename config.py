import math
import os
from dataclasses import dataclass

INTERVAL_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}


def _f(name, default):
    value = float(os.getenv(name, default))
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    return value


def _i(name, default):
    return int(os.getenv(name, default))


def _b(name, default):
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    token: str
    chat_id: str
    interval: str
    interval_seconds: int
    ws_url: str
    port: int
    log_level: str
    send_startup: bool
    lookback: int
    vol_mult: float
    range_mult: float
    min_body: float
    close_loc: float
    rsi_min: float
    rsi_max: float
    max_ext_atr: float
    max_range_atr: float
    min_avg_qvol: float
    allow_short: bool
    sl_atr: float
    rr1: float
    rr2: float
    cooldown_min: int
    max_per_hour: int
    max_per_scan: int
    min_score: float


def load() -> Config:
    interval = os.getenv("INTERVAL", "5m")
    if interval not in INTERVAL_SECONDS:
        raise ValueError(f"INTERVAL must be one of {list(INTERVAL_SECONDS)}")
    cfg = Config(
        token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        interval=interval,
        interval_seconds=INTERVAL_SECONDS[interval],
        # WebSocket market data only; no REST market-data polling.
        ws_url=os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:443").rstrip("/"),
        port=_i("PORT", 10000),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        send_startup=_b("SEND_STARTUP", False),
        lookback=_i("LOOKBACK", 20),
        vol_mult=_f("VOL_MULT", 3.0),
        range_mult=_f("RANGE_MULT", 1.8),
        min_body=_f("MIN_BODY", 0.6),
        close_loc=_f("CLOSE_LOC", 0.75),
        rsi_min=_f("RSI_MIN", 58),
        rsi_max=_f("RSI_MAX", 82),
        max_ext_atr=_f("MAX_EXT_ATR", 4.0),
        max_range_atr=_f("MAX_RANGE_ATR", 6.0),
        min_avg_qvol=_f("MIN_AVG_QVOL", 15000),
        allow_short=_b("ALLOW_SHORT", True),
        sl_atr=_f("SL_ATR", 1.8),
        rr1=_f("RR1", 1.5),
        rr2=_f("RR2", 3.0),
        cooldown_min=_i("COOLDOWN_MIN", 90),
        max_per_hour=_i("MAX_PER_HOUR", 6),
        max_per_scan=_i("MAX_PER_SCAN", 3),
        min_score=_f("MIN_SCORE", 55),
    )
    if cfg.port < 1 or cfg.port > 65535:
        raise ValueError("PORT must be between 1 and 65535")
    if cfg.lookback < 5:
        raise ValueError("LOOKBACK must be at least 5")
    if cfg.vol_mult <= 0 or cfg.range_mult <= 0:
        raise ValueError("VOL_MULT and RANGE_MULT must be greater than zero")
    if not 0 <= cfg.min_body <= 1 or not 0 < cfg.close_loc <= 1:
        raise ValueError("MIN_BODY must be 0..1 and CLOSE_LOC must be >0..1")
    if not 0 <= cfg.rsi_min < cfg.rsi_max <= 100:
        raise ValueError("RSI_MIN and RSI_MAX must satisfy 0 <= min < max <= 100")
    if cfg.max_ext_atr <= 0 or cfg.max_range_atr < cfg.range_mult:
        raise ValueError("MAX_EXT_ATR must be positive and MAX_RANGE_ATR >= RANGE_MULT")
    if cfg.min_avg_qvol < 0:
        raise ValueError("MIN_AVG_QVOL cannot be negative")
    if cfg.sl_atr <= 0 or cfg.rr1 <= 0 or cfg.rr2 <= cfg.rr1:
        raise ValueError("SL_ATR and RR1 must be positive and RR2 must be greater than RR1")
    if cfg.cooldown_min < 0 or cfg.max_per_hour < 1 or cfg.max_per_scan < 1:
        raise ValueError("COOLDOWN_MIN cannot be negative and signal caps must be positive")
    if not 0 <= cfg.min_score <= 100:
        raise ValueError("MIN_SCORE must be between 0 and 100")
    return cfg
