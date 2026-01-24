# QD_BOOKMAP_STREAM_FIX02_WS_INGEST_P02__20260124.py
# QuantDesk Bookmap Service — FIX02 (STREAM) — REST Ingest Bootstrap (no extra deps)
#
# Purpose:
# - Establish a reliable "data ingest heartbeat" using only stdlib + FastAPI + Uvicorn.
# - Avoid websocket dependencies at this stage (keeps requirements.txt unchanged).
# - Provide deterministic telemetry endpoints for QA gating.
#
# Endpoints:
# - GET /              : status page (human)
# - GET /telemetry.json: telemetry (machine)
# - GET /healthz       : simple health response
#
# Environment:
# - PORT                  : server port (default 8000)
# - QD_SYMBOL             : default "BTC_USDT"
# - QD_POLL_SEC           : polling interval seconds (default 1.0)
# - QD_MEXC_BASE          : default "https://api.mexc.com"
# - QD_DEPTH_LIMIT        : default 50
# - QD_TRADES_LIMIT       : default 50
#
# Health logic (FIX02 expectation):
# - GREEN  : depth_ok AND trades_ok AND freshness_ok
# - YELLOW : process alive but missing any ingest leg or stale
# - RED    : repeated HTTP errors above threshold (still serves status)
#
# Notes:
# - This is a STREAM ingest milestone only. No book reconstruction yet.

from __future__ import annotations

import json
import os
import threading
import time
import traceback
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# -----------------------------
# Version / identity
# -----------------------------
FIX_NN = "02"
PATCH_PP = "02"
SUBSYSTEM = "STREAM"
INTENT = "WS_INGEST"  # kept for naming parity; implementation is REST-poll at this stage
BUILD_DATE = "20260124"

SERVICE_NAME = "qd-bookmap"
ARTIFACT = f"QD_BOOKMAP_{SUBSYSTEM}_FIX{FIX_NN}_{INTENT}_P{PATCH_PP}__{BUILD_DATE}.py"

# -----------------------------
# Config
# -----------------------------
def _env_str(key: str, default: str) -> str:
    v = os.environ.get(key)
    return v if v is not None and str(v).strip() != "" else default

def _env_int(key: str, default: int) -> int:
    v = os.environ.get(key)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _env_float(key: str, default: float) -> float:
    v = os.environ.get(key)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

QD_SYMBOL = _env_str("QD_SYMBOL", "BTC_USDT")  # internal symbol format (underscore)
POLL_SEC = max(0.25, _env_float("QD_POLL_SEC", 1.0))
MEXC_BASE = _env_str("QD_MEXC_BASE", "https://api.mexc.com").rstrip("/")
DEPTH_LIMIT = max(5, min(200, _env_int("QD_DEPTH_LIMIT", 50)))
TRADES_LIMIT = max(5, min(200, _env_int("QD_TRADES_LIMIT", 50)))

# MEXC spot public endpoints (stable REST)
# Depth:  /api/v3/depth?symbol=BTCUSDT&limit=50
# Trades: /api/v3/trades?symbol=BTCUSDT&limit=50
DEPTH_PATH = "/api/v3/depth"
TRADES_PATH = "/api/v3/trades"

# Freshness thresholds
FRESH_DEPTH_SEC = 5.0
FRESH_TRADES_SEC = 5.0

# Error thresholds
ERR_RED_THRESHOLD = 10  # cumulative HTTP errors before RED
ERR_WINDOW_SEC = 60.0   # window length used only for reporting


def mexc_symbol_from_internal(sym: str) -> str:
    # "BTC_USDT" -> "BTCUSDT"
    return sym.replace("_", "").upper().strip()


# -----------------------------
# Telemetry model
# -----------------------------
@dataclass
class Telemetry:
    service: str
    artifact: str
    fix: str
    patch: str
    subsystem: str
    intent: str
    symbol: str
    mexc_symbol: str
    mexc_base: str
    poll_sec: float
    started_at_unix: float

    # ingest counters
    depth_ok: bool = False
    trades_ok: bool = False
    depth_http_ok: int = 0
    depth_http_err: int = 0
    trades_http_ok: int = 0
    trades_http_err: int = 0

    depth_last_ok_unix: float = 0.0
    trades_last_ok_unix: float = 0.0
    depth_last_err_unix: float = 0.0
    trades_last_err_unix: float = 0.0

    # last payload summaries
    depth_levels_bid: int = 0
    depth_levels_ask: int = 0
    trades_count: int = 0
    last_trade_price: Optional[float] = None
    last_trade_qty: Optional[float] = None
    last_trade_is_buyer_maker: Optional[bool] = None

    # rolling error info
    http_err_total: int = 0
    last_error: str = ""
    last_error_unix: float = 0.0

    # derived
    health: str = "YELLOW"
    note: str = "BOOTSTRAP"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


telemetry = Telemetry(
    service=SERVICE_NAME,
    artifact=ARTIFACT,
    fix=FIX_NN,
    patch=PATCH_PP,
    subsystem=SUBSYSTEM,
    intent=INTENT,
    symbol=QD_SYMBOL,
    mexc_symbol=mexc_symbol_from_internal(QD_SYMBOL),
    mexc_base=MEXC_BASE,
    poll_sec=POLL_SEC,
    started_at_unix=time.time(),
)

_lock = threading.Lock()
_stop = threading.Event()

# -----------------------------
# HTTP client (stdlib)
# -----------------------------
def http_get_json(url: str, timeout: float = 10.0) -> Tuple[bool, Any, str]:
    """
    Returns: (ok, parsed_json_or_none, err_str)
    """
    req = urllib.request.Request(
        url=url,
        headers={
            "User-Agent": "qd-bookmap-fix02/1.0",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            raw = resp.read()
        if status < 200 or status >= 300:
            return (False, None, f"HTTP {status}")
        try:
            parsed = json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as e:
            return (False, None, f"JSON decode error: {e}")
        return (True, parsed, "")
    except Exception as e:
        return (False, None, str(e))


# -----------------------------
# Ingest logic
# -----------------------------
def _update_health(now: float) -> None:
    # Called under lock
    depth_fresh = telemetry.depth_last_ok_unix > 0 and (now - telemetry.depth_last_ok_unix) <= FRESH_DEPTH_SEC
    trades_fresh = telemetry.trades_last_ok_unix > 0 and (now - telemetry.trades_last_ok_unix) <= FRESH_TRADES_SEC
    telemetry.depth_ok = depth_fresh
    telemetry.trades_ok = trades_fresh

    if telemetry.http_err_total >= ERR_RED_THRESHOLD:
        telemetry.health = "RED"
        telemetry.note = "HTTP_ERR_THRESHOLD"
        return

    if depth_fresh and trades_fresh:
        telemetry.health = "GREEN"
        telemetry.note = "INGEST_OK"
    else:
        telemetry.health = "YELLOW"
        if telemetry.depth_last_ok_unix == 0 and telemetry.trades_last_ok_unix == 0:
            telemetry.note = "WAITING_FIRST_INGEST"
        elif not depth_fresh and not trades_fresh:
            telemetry.note = "STALE_BOTH"
        elif not depth_fresh:
            telemetry.note = "STALE_DEPTH"
        else:
            telemetry.note = "STALE_TRADES"


def poll_depth_once(now: float) -> None:
    mexc_sym = telemetry.mexc_symbol
    qs = urllib.parse.urlencode({"symbol": mexc_sym, "limit": str(DEPTH_LIMIT)})
    url = f"{MEXC_BASE}{DEPTH_PATH}?{qs}"

    ok, data, err = http_get_json(url, timeout=10.0)
    with _lock:
        if ok and isinstance(data, dict):
            bids = data.get("bids", []) or []
            asks = data.get("asks", []) or []
            telemetry.depth_http_ok += 1
            telemetry.depth_last_ok_unix = now
            telemetry.depth_levels_bid = len(bids)
            telemetry.depth_levels_ask = len(asks)
        else:
            telemetry.depth_http_err += 1
            telemetry.http_err_total += 1
            telemetry.depth_last_err_unix = now
            telemetry.last_error = f"depth: {err}"
            telemetry.last_error_unix = now


def poll_trades_once(now: float) -> None:
    mexc_sym = telemetry.mexc_symbol
    qs = urllib.parse.urlencode({"symbol": mexc_sym, "limit": str(TRADES_LIMIT)})
    url = f"{MEXC_BASE}{TRADES_PATH}?{qs}"

    ok, data, err = http_get_json(url, timeout=10.0)
    with _lock:
        if ok and isinstance(data, list):
            telemetry.trades_http_ok += 1
            telemetry.trades_last_ok_unix = now
            telemetry.trades_count = len(data)

            # MEXC spot trades entries usually contain: price, qty, time, isBuyerMaker
            # We only extract a best-effort last trade snapshot.
            if len(data) > 0 and isinstance(data[0], dict):
                t0 = data[0]
                try:
                    telemetry.last_trade_price = float(t0.get("price")) if t0.get("price") is not None else telemetry.last_trade_price
                except Exception:
                    pass
                try:
                    telemetry.last_trade_qty = float(t0.get("qty")) if t0.get("qty") is not None else telemetry.last_trade_qty
                except Exception:
                    pass
                v = t0.get("isBuyerMaker")
                if isinstance(v, bool):
                    telemetry.last_trade_is_buyer_maker = v
        else:
            telemetry.trades_http_err += 1
            telemetry.http_err_total += 1
            telemetry.trades_last_err_unix = now
            telemetry.last_error = f"trades: {err}"
            telemetry.last_error_unix = now


def ingest_loop() -> None:
    # Staggered polling so we do not always hit endpoints at exact same instant
    phase = 0
    while not _stop.is_set():
        start = time.time()
        try:
            now = start
            if phase % 2 == 0:
                poll_depth_once(now)
            else:
                poll_trades_once(now)

            with _lock:
                _update_health(now)
        except Exception:
            now = time.time()
            with _lock:
                telemetry.http_err_total += 1
                telemetry.last_error = "ingest_loop: " + traceback.format_exc(limit=2)
                telemetry.last_error_unix = now
                _update_health(now)

        phase += 1
        elapsed = time.time() - start
        sleep_for = max(0.05, POLL_SEC - elapsed)
        _stop.wait(sleep_for)


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="QuantDesk Bookmap Service", version=f"FIX{FIX_NN}-P{PATCH_PP}")

def _fmt_ts(ts: float) -> str:
    if ts <= 0:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

def _age(now: float, ts: float) -> str:
    if ts <= 0:
        return "—"
    a = now - ts
    if a < 0:
        a = 0.0
    return f"{a:.2f}s"

@app.get("/telemetry.json")
def telemetry_json() -> JSONResponse:
    with _lock:
        now = time.time()
        _update_health(now)
        payload = telemetry.to_dict()
        payload["now_unix"] = now
        payload["depth_last_ok_human"] = _fmt_ts(telemetry.depth_last_ok_unix)
        payload["trades_last_ok_human"] = _fmt_ts(telemetry.trades_last_ok_unix)
        payload["depth_age_sec"] = (now - telemetry.depth_last_ok_unix) if telemetry.depth_last_ok_unix > 0 else None
        payload["trades_age_sec"] = (now - telemetry.trades_last_ok_unix) if telemetry.trades_last_ok_unix > 0 else None
    return JSONResponse(payload)

@app.get("/healthz")
def healthz() -> PlainTextResponse:
    with _lock:
        now = time.time()
        _update_health(now)
        h = telemetry.health
    code = 200 if h in ("GREEN", "YELLOW") else 503
    return PlainTextResponse(f"{h}\n", status_code=code)

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    with _lock:
        now = time.time()
        _update_health(now)
        t = telemetry

        depth_age = _age(now, t.depth_last_ok_unix)
        trades_age = _age(now, t.trades_last_ok_unix)

        html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QD Bookmap — FIX{FIX_NN} P{PATCH_PP} — STREAM</title>
  <style>
    body {{ margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; background: #0b0f14; color: #e6edf3; }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 18px 16px 40px; }}
    .card {{ background: #111827; border: 1px solid #1f2937; border-radius: 14px; padding: 14px 14px; margin-top: 12px; }}
    .row {{ display: flex; gap: 12px; flex-wrap: wrap; }}
    .k {{ color: #93c5fd; font-size: 12px; text-transform: uppercase; letter-spacing: .06em; }}
    .v {{ font-size: 14px; margin-top: 4px; }}
    .pill {{ display: inline-block; padding: 4px 10px; border-radius: 999px; font-weight: 700; font-size: 12px; }}
    .GREEN {{ background: #064e3b; color: #a7f3d0; border: 1px solid #065f46; }}
    .YELLOW {{ background: #3f2d00; color: #fde68a; border: 1px solid #92400e; }}
    .RED {{ background: #450a0a; color: #fecaca; border: 1px solid #7f1d1d; }}
    a {{ color: #93c5fd; text-decoration: none; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 12px; color: #cbd5e1; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
    @media (max-width: 820px) {{ .grid {{ grid-template-columns: 1fr; }} }}
    .small {{ color: #94a3b8; font-size: 12px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="row" style="align-items:center; justify-content: space-between;">
      <div>
        <div class="k">QuantDesk Bookmap Service</div>
        <div style="font-size:18px; font-weight:800;">FIX{FIX_NN} / P{PATCH_PP} — {SUBSYSTEM} / REST Ingest</div>
        <div class="small">Artifact: <span class="mono">{t.artifact}</span></div>
      </div>
      <div class="pill {t.health}">HEALTH: {t.health}</div>
    </div>

    <div class="card">
      <div class="grid">
        <div>
          <div class="k">Symbol</div>
          <div class="v">{t.symbol} (<span class="mono">{t.mexc_symbol}</span>)</div>
        </div>
        <div>
          <div class="k">Note</div>
          <div class="v">{t.note}</div>
        </div>
        <div>
          <div class="k">Polling</div>
          <div class="v">{t.poll_sec:.2f}s</div>
        </div>
        <div>
          <div class="k">MEXC Base</div>
          <div class="v"><span class="mono">{t.mexc_base}</span></div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="k">Ingest status</div>
      <div class="grid" style="margin-top:10px;">
        <div>
          <div class="k">Depth</div>
          <div class="v">ok: <b>{str(t.depth_ok).lower()}</b> — bids: {t.depth_levels_bid}, asks: {t.depth_levels_ask}</div>
          <div class="small">last ok: <span class="mono">{_fmt_ts(t.depth_last_ok_unix)}</span> — age: <span class="mono">{depth_age}</span></div>
          <div class="small">http ok: <span class="mono">{t.depth_http_ok}</span> — http err: <span class="mono">{t.depth_http_err}</span></div>
        </div>
        <div>
          <div class="k">Trades</div>
          <div class="v">ok: <b>{str(t.trades_ok).lower()}</b> — count: {t.trades_count}</div>
          <div class="small">last ok: <span class="mono">{_fmt_ts(t.trades_last_ok_unix)}</span> — age: <span class="mono">{trades_age}</span></div>
          <div class="small">http ok: <span class="mono">{t.trades_http_ok}</span> — http err: <span class="mono">{t.trades_http_err}</span></div>
        </div>
      </div>

      <div style="margin-top:12px;" class="small">
        Last trade snapshot: price=<span class="mono">{t.last_trade_price}</span>,
        qty=<span class="mono">{t.last_trade_qty}</span>,
        isBuyerMaker=<span class="mono">{t.last_trade_is_buyer_maker}</span>
      </div>

      <div style="margin-top:10px;" class="small">
        Errors total: <span class="mono">{t.http_err_total}</span>
        — last error: <span class="mono">{(t.last_error[:160] + '…') if len(t.last_error) > 160 else t.last_error}</span>
      </div>
    </div>

    <div class="card">
      <div class="k">Links</div>
      <div class="v">
        <a href="/telemetry.json">/telemetry.json</a>
        &nbsp;|&nbsp;
        <a href="/healthz">/healthz</a>
      </div>
      <div class="small" style="margin-top:8px;">
        This is FIX02 ingest bootstrap. Book reconstruction and rendering occur in later FIXes.
      </div>
    </div>
  </div>
</body>
</html>"""
    return HTMLResponse(html)

# -----------------------------
# Startup / shutdown
# -----------------------------
_ingest_thread: Optional[threading.Thread] = None

@app.on_event("startup")
def _on_startup() -> None:
    global _ingest_thread
    if _ingest_thread is None or not _ingest_thread.is_alive():
        _stop.clear()
        _ingest_thread = threading.Thread(target=ingest_loop, name="qd_ingest_loop", daemon=True)
        _ingest_thread.start()

@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop.set()
    t = _ingest_thread
    if t is not None and t.is_alive():
        t.join(timeout=2.0)

# -----------------------------
# Entrypoint
# -----------------------------
def main() -> None:
    # Avoid extra dependency (click) — minimal args.
    import uvicorn
    port = _env_int("PORT", 8000)
    # Replit expects 0.0.0.0
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

if __name__ == "__main__":
    main()
