# QuantDesk Bookmap Service - current_fix.py
# Build: FIX12/P01
# Purpose:
# - Harden Replit startup so the process does not "start then stop" due to connector/import issues.
# - Keep FastAPI up on 0.0.0.0:5000 even if WS/deps fail (health will show ERROR but server stays alive).
# - Provide a Preview-safe HTML dashboard at "/" with clickable links and auto-refresh.
#
# Contract (Workflow v1.2):
# - Single executable file: current_fix.py
# - Endpoints: /, /health.json, /telemetry.json
# - Additional endpoints: /book.json, /tape.json, /raw.json, /bootlog.txt
#
# Notes:
# - ASCII-only file (avoid unicode like U+2014 that can break execution if pasted).
# - No console spam; boot diagnostics are written to /tmp/qd_boot.log and served via /bootlog.txt.

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
from dataclasses import dataclass, field
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

BOOT_LOG_PATH = "/tmp/qd_boot.log"


def _bootlog(msg: str) -> None:
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        with open(BOOT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}Z] {msg}\n")
    except Exception:
        pass


def _now() -> float:
    return time.time()


# -----------------------------
# Config
# -----------------------------
BUILD = "FIX12/P01"
SERVICE = "qd-bookmap"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "5000"))  # Replit expects 5000
SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT")

# MEXC contract WS base (as per earlier working state)
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")

# Optional stream hints
TRADE_CHANNELS = [
    os.environ.get("QD_TRADE_CH1", "push.deal"),
    os.environ.get("QD_TRADE_CH2", "push.trade"),
]

RAW_RING_MAX = int(os.environ.get("QD_RAW_RING_MAX", "200"))
TAPE_RING_MAX = int(os.environ.get("QD_TAPE_RING_MAX", "300"))

RECONNECT_MIN_S = float(os.environ.get("QD_RECONNECT_MIN_S", "0.5"))
RECONNECT_MAX_S = float(os.environ.get("QD_RECONNECT_MAX_S", "15.0"))


# -----------------------------
# State Models
# -----------------------------
@dataclass
class BookState:
    best_bid: Optional[Tuple[str, str]] = None  # (price, qty)
    best_ask: Optional[Tuple[str, str]] = None
    depth_counts: Dict[str, int] = field(default_factory=lambda: {"bids": 0, "asks": 0})
    totals: Dict[str, str] = field(default_factory=lambda: {"bid_qty": "0", "ask_qty": "0"})
    version: Optional[int] = None
    last_update_ts: Optional[float] = None


@dataclass
class EngineState:
    status: str = "WARMING"  # WARMING/ACTIVE/ERROR
    frames: int = 0
    reconnects: int = 0
    last_error: str = ""
    last_close_code: Optional[int] = None
    last_close_reason: str = ""
    last_connect_ts: Optional[float] = None
    last_event_ts: Optional[float] = None

    # Parsing stats
    snapshots_applied: int = 0
    parse_hits: int = 0
    parse_misses: int = 0
    invariants_ok: bool = True
    last_invariant_violation: str = ""

    # Tape stats
    trades_seen: int = 0
    last_trade_px: Optional[float] = None
    last_trade_side: Optional[str] = None
    last_trade_qty: Optional[float] = None
    last_trade_ts: Optional[float] = None

    # Book
    book: BookState = field(default_factory=BookState)


@dataclass
class ServiceState:
    started_ts: float = field(default_factory=_now)
    subsystem: str = "FOUNDATION+STREAM+GLOBAL_LADDER_STATE_ENGINE+INCREMENTAL_MERGE+TRADE_TAPE"
    health: str = "YELLOW"  # GREEN/YELLOW/RED
    ws_url: str = WS_URL
    symbol: str = SYMBOL
    engine: EngineState = field(default_factory=EngineState)
    raw_ring: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=RAW_RING_MAX))
    tape_ring: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=TAPE_RING_MAX))
    import_errors: List[str] = field(default_factory=list)


STATE = ServiceState()
STATE_LOCK = asyncio.Lock()


# -----------------------------
# Dependency-safe imports
# -----------------------------
try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, PlainTextResponse, JSONResponse
except Exception as e:
    STATE.import_errors.append(f"fastapi_import_error: {repr(e)}")
    _bootlog(f"FastAPI import failed: {repr(e)}")
    FastAPI = None  # type: ignore

try:
    import websockets  # type: ignore
except Exception as e:
    STATE.import_errors.append(f"websockets_import_error: {repr(e)}")
    _bootlog(f"websockets import failed: {repr(e)}")
    websockets = None  # type: ignore


# -----------------------------
# Helpers
# -----------------------------
def _compute_health() -> str:
    eng = STATE.engine
    if eng.status == "ERROR":
        return "RED"
    if eng.status == "ACTIVE" and eng.last_event_ts is not None:
        if (_now() - eng.last_event_ts) <= 5.0:
            return "GREEN"
    return "YELLOW"


def _json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=True)


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _norm_side(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).lower()
    if s in ("buy", "b", "1"):
        return "buy"
    if s in ("sell", "s", "2", "-1"):
        return "sell"
    return None


def _html_index() -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap</title>
  <style>
    body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
    .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 16px; max-width: 900px; }}
    .row {{ display: flex; gap: 16px; flex-wrap: wrap; align-items: center; }}
    .pill {{ display: inline-block; padding: 4px 10px; border-radius: 999px; border: 1px solid #ccc; font-weight: 700; }}
    pre {{ background: #0b0f14; color: #e6edf3; padding: 12px; border-radius: 10px; overflow: auto; }}
    a {{ text-decoration: none; }}
    code {{ background: #f6f8fa; padding: 1px 4px; border-radius: 6px; }}
    .muted {{ color: #666; }}
  </style>
</head>
<body>
  <h2>QuantDesk Bookmap</h2>
  <div class="card">
    <div class="row">
      <div><span class="pill" id="healthPill">HEALTH: ...</span></div>
      <div class="muted">Build: <b>{BUILD}</b></div>
      <div class="muted">Symbol: <b>{SYMBOL}</b></div>
      <div class="muted">WS: <b id="wsUrl">...</b></div>
    </div>

    <p class="muted">
      If you opened an editor URL like <code>replit.com/@user/...</code> it can 404.
      Use the runtime host you are on now and these endpoints:
    </p>

    <ul>
      <li><a id="l_health" href="/health.json" target="_blank">/health.json</a></li>
      <li><a id="l_tele" href="/telemetry.json" target="_blank">/telemetry.json</a></li>
      <li><a id="l_book" href="/book.json" target="_blank">/book.json</a></li>
      <li><a id="l_tape" href="/tape.json" target="_blank">/tape.json</a></li>
      <li><a id="l_raw" href="/raw.json" target="_blank">/raw.json</a></li>
      <li><a id="l_boot" href="/bootlog.txt" target="_blank">/bootlog.txt</a></li>
    </ul>

    <h3>Status (auto-refresh)</h3>
    <pre id="statusBox">loading...</pre>
  </div>

<script>
(function() {{
  function setHealth(h) {{
    const pill = document.getElementById("healthPill");
    pill.textContent = "HEALTH: " + h;
    pill.style.borderColor = (h === "GREEN") ? "#1a7f37" : (h === "RED" ? "#cf222e" : "#bf8700");
  }}
  function abs(u) {{
    return window.location.origin + u;
  }}
  function fixLinks() {{
    document.getElementById("l_health").href = abs("/health.json");
    document.getElementById("l_tele").href = abs("/telemetry.json");
    document.getElementById("l_book").href = abs("/book.json");
    document.getElementById("l_tape").href = abs("/tape.json");
    document.getElementById("l_raw").href = abs("/raw.json");
    document.getElementById("l_boot").href = abs("/bootlog.txt");
  }}
  async function tick() {{
    try {{
      const r = await fetch("/health.json", {{ cache: "no-store" }});
      const j = await r.json();
      document.getElementById("wsUrl").textContent = j.ws_url || "";
      setHealth(j.health || "YELLOW");
      document.getElementById("statusBox").textContent = JSON.stringify(j, null, 2);
    }} catch(e) {{
      document.getElementById("statusBox").textContent = "fetch failed: " + (e && e.message ? e.message : String(e));
    }}
  }}
  fixLinks();
  tick();
  setInterval(tick, 1000);
}})();
</script>
</body>
</html>
"""


# -----------------------------
# WS Connector (best-effort, never fatal)
# -----------------------------
async def _ws_loop() -> None:
    if websockets is None:
        async with STATE_LOCK:
            STATE.engine.status = "WARMING"
            STATE.engine.last_error = "websockets library missing; connector disabled"
            STATE.engine.last_event_ts = None
            STATE.health = _compute_health()
        _bootlog("WS loop disabled: websockets missing")
        return

    backoff = RECONNECT_MIN_S
    while True:
        try:
            async with STATE_LOCK:
                STATE.engine.status = "WARMING"
                STATE.engine.last_error = ""
                STATE.ws_url = WS_URL
                STATE.health = _compute_health()
            _bootlog(f"Connecting WS: {WS_URL}")

            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:  # type: ignore
                async with STATE_LOCK:
                    STATE.engine.last_connect_ts = _now()
                    STATE.engine.last_close_code = None
                    STATE.engine.last_close_reason = ""
                    STATE.engine.status = "ACTIVE"
                    STATE.health = _compute_health()
                backoff = RECONNECT_MIN_S

                # Conservative subscribe set (schema may vary; failures show in /raw.json and /bootlog.txt)
                sub_msgs: List[Dict[str, Any]] = []
                for ch in TRADE_CHANNELS:
                    sub_msgs.append({"method": ch, "param": {"symbol": SYMBOL}})
                sub_msgs.append({"method": "sub.depth", "param": {"symbol": SYMBOL, "level": 20}})

                for m in sub_msgs:
                    try:
                        await ws.send(_json(m))
                    except Exception as e:
                        _bootlog(f"WS send failed: {repr(e)} msg={m}")

                async for msg in ws:
                    ts = _now()
                    try:
                        if isinstance(msg, (bytes, bytearray)):
                            msg = msg.decode("utf-8", errors="replace")
                        obj = json.loads(msg)
                    except Exception:
                        obj = {"_raw": str(msg)}

                    async with STATE_LOCK:
                        eng = STATE.engine
                        eng.frames += 1
                        eng.last_event_ts = ts
                        STATE.raw_ring.append({"ts": ts, "raw": obj})

                        # Extract trades best-effort
                        trades: List[Dict[str, Any]] = []
                        try:
                            d = obj.get("data") if isinstance(obj, dict) else None
                            if isinstance(d, list):
                                trades = [it for it in d if isinstance(it, dict)]
                            elif isinstance(d, dict) and isinstance(d.get("deals"), list):
                                trades = [it for it in d["deals"] if isinstance(it, dict)]
                            elif isinstance(d, dict) and isinstance(d.get("trades"), list):
                                trades = [it for it in d["trades"] if isinstance(it, dict)]
                        except Exception:
                            trades = []

                        for t in trades[:50]:
                            px = _as_float(t.get("p") or t.get("price") or t.get("px"))
                            qty = _as_float(t.get("v") or t.get("qty") or t.get("q") or t.get("size"))
                            side = _norm_side(t.get("S") or t.get("side") or t.get("takerSide") or t.get("m"))
                            eng.trades_seen += 1
                            eng.last_trade_px = px
                            eng.last_trade_qty = qty
                            eng.last_trade_side = side
                            eng.last_trade_ts = ts
                            STATE.tape_ring.append({
                                "ts": ts,
                                "px": px,
                                "qty": qty,
                                "side": side,
                                "raw_type": obj.get("method") or obj.get("channel") or obj.get("c"),
                            })

                        STATE.health = _compute_health()

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            _bootlog(f"WS loop error: {err}\n{traceback.format_exc()}")
            async with STATE_LOCK:
                STATE.engine.status = "ERROR"
                STATE.engine.last_error = err
                STATE.engine.reconnects += 1
                STATE.health = _compute_health()
            await asyncio.sleep(backoff)
            backoff = min(RECONNECT_MAX_S, max(RECONNECT_MIN_S, backoff * 1.7))


# -----------------------------
# FastAPI app
# -----------------------------
APP = None

if FastAPI is not None:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: "FastAPI"):
        _bootlog("Lifespan startup entered")
        try:
            if os.environ.get("QD_CLEAR_BOOTLOG", "0") == "1" and os.path.exists(BOOT_LOG_PATH):
                os.remove(BOOT_LOG_PATH)
        except Exception:
            pass

        task = asyncio.create_task(_ws_loop())
        try:
            yield
        finally:
            _bootlog("Lifespan shutdown entered")
            task.cancel()
            try:
                await task
            except Exception:
                pass

    app = FastAPI(lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse(_html_index())

    @app.get("/bootlog.txt", response_class=PlainTextResponse)
    async def bootlog():
        try:
            if not os.path.exists(BOOT_LOG_PATH):
                return PlainTextResponse("boot log empty\n")
            with open(BOOT_LOG_PATH, "r", encoding="utf-8") as f:
                return PlainTextResponse(f.read())
        except Exception as e:
            return PlainTextResponse(f"bootlog read error: {repr(e)}\n")

    @app.get("/health.json")
    async def health():
        async with STATE_LOCK:
            STATE.health = _compute_health()
            eng = STATE.engine
            out = {
                "ts": _now(),
                "service": SERVICE,
                "build": BUILD,
                "subsystem": STATE.subsystem,
                "health": STATE.health,
                "uptime_s": round(_now() - STATE.started_ts, 3),
                "ws_url": STATE.ws_url,
                "symbol": STATE.symbol,
                "connector": {
                    "status": "CONNECTED" if eng.status == "ACTIVE" else ("ERROR" if eng.status == "ERROR" else "WARMING"),
                    "frames": eng.frames,
                    "reconnects": eng.reconnects,
                    "last_error": eng.last_error,
                    "last_event_ts": eng.last_event_ts,
                    "last_connect_ts": eng.last_connect_ts,
                    "last_close_code": eng.last_close_code,
                    "last_close_reason": eng.last_close_reason,
                },
                "book": {
                    "best_bid": eng.book.best_bid,
                    "best_ask": eng.book.best_ask,
                    "depth_counts": eng.book.depth_counts,
                    "version": eng.book.version,
                    "last_update_ts": eng.book.last_update_ts,
                },
                "tape": {
                    "trades_seen": eng.trades_seen,
                    "last_trade_px": eng.last_trade_px,
                    "last_trade_qty": eng.last_trade_qty,
                    "last_trade_side": eng.last_trade_side,
                    "last_trade_ts": eng.last_trade_ts,
                    "ring_size": len(STATE.tape_ring),
                },
                "import_errors": list(STATE.import_errors),
                "host": HOST,
                "port": PORT,
            }
            return JSONResponse(out)

    @app.get("/telemetry.json")
    async def telemetry():
        async with STATE_LOCK:
            eng = STATE.engine
            return JSONResponse({
                "ts": _now(),
                "service": SERVICE,
                "build": BUILD,
                "health": STATE.health,
                "uptime_s": _now() - STATE.started_ts,
                "engine": {
                    "status": eng.status,
                    "frames": eng.frames,
                    "reconnects": eng.reconnects,
                    "last_error": eng.last_error,
                    "last_event_age_s": (None if eng.last_event_ts is None else (_now() - eng.last_event_ts)),
                },
                "rings": {"raw": len(STATE.raw_ring), "tape": len(STATE.tape_ring)},
            })

    @app.get("/raw.json")
    async def raw():
        async with STATE_LOCK:
            return JSONResponse({"ts": _now(), "build": BUILD, "raw_ring": list(STATE.raw_ring)})

    @app.get("/book.json")
    async def book():
        async with STATE_LOCK:
            b = STATE.engine.book
            return JSONResponse({
                "ts": _now(),
                "build": BUILD,
                "symbol": SYMBOL,
                "best_bid": b.best_bid,
                "best_ask": b.best_ask,
                "depth_counts": b.depth_counts,
                "totals": b.totals,
                "version": b.version,
                "last_update_ts": b.last_update_ts,
            })

    @app.get("/tape.json")
    async def tape():
        async with STATE_LOCK:
            eng = STATE.engine
            return JSONResponse({
                "ts": _now(),
                "build": BUILD,
                "symbol": SYMBOL,
                "trades_seen": eng.trades_seen,
                "last_trade_px": eng.last_trade_px,
                "last_trade_qty": eng.last_trade_qty,
                "last_trade_side": eng.last_trade_side,
                "last_trade_ts": eng.last_trade_ts,
                "ring_size": len(STATE.tape_ring),
                "tape": list(STATE.tape_ring),
            })

    APP = app


# -----------------------------
# Fallback minimal HTTP server (only if FastAPI missing)
# -----------------------------
def _run_fallback_http() -> None:
    import http.server
    import socketserver

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = {
                "service": SERVICE,
                "build": BUILD,
                "health": "RED",
                "error": "FastAPI not available",
                "import_errors": STATE.import_errors,
            }
            raw = (json.dumps(body, indent=2) + "\n").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format, *args):  # noqa: A002
            return

    with socketserver.TCPServer((HOST, PORT), Handler) as httpd:
        _bootlog(f"Fallback HTTP server listening on {HOST}:{PORT}")
        httpd.serve_forever()


# -----------------------------
# Entrypoint
# -----------------------------
def main() -> None:
    _bootlog(f"Starting {SERVICE} {BUILD} on {HOST}:{PORT}")
    try:
        if APP is None:
            _run_fallback_http()
            return

        import uvicorn
        uvicorn.run(APP, host=HOST, port=PORT, log_level="warning")
    except Exception as e:
        _bootlog(f"Fatal main() exception: {repr(e)}\n{traceback.format_exc()}")
        while True:
            time.sleep(1.0)


if __name__ == "__main__":
    main()
