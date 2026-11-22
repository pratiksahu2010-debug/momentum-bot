import os
from dataclasses import dataclass

INTERVAL_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}


def _f(name, default):
    return float(os.getenv(name, default))


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
    base_url: str
    port: int
    scan_delay: int
    concurrency: int
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
    return Config(
        token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        interval=interval,
        interval_seconds=INTERVAL_SECONDS[interval],
        # Official Binance Spot REST host; can be overridden in Render if needed.
        base_url=os.getenv("BASE_URL", "https://api.binance.com").rstrip("/"),
        port=_i("PORT", 10000),
        scan_delay=_i("SCAN_DELAY", 4),
        # Keep the burst small; Engine also applies a process-wide request pacer.
        concurrency=_i("CONCURRENCY", 3),
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
