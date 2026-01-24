# QD_BOOKMAP_STREAM_FIX02_WS_INGEST_P01__20260124.py
# QuantDesk Bookmap — FIX02 STREAM: WS ingestion framework + status/telemetry (graceful degradation)
#
# Workflow v1 compliance:
# - Single file
# - Copy/paste into GitHub repo
# - Replit: git pull -> Run
#
# Notes:
# - Uses FastAPI + uvicorn (already in requirements.txt per FIX01).
# - Attempts to use "websockets" library for WS client if available.
#   If not available, it runs with HEALTH=RED and an explicit message on the status page.
#
# Environment variables (optional, but recommended for real ingestion):
# - PORT: server port (default 8000)
# - QD_WS_URL: WebSocket URL to connect to (default empty -> ingestion disabled)
# - QD_WS_SUBSCRIBE_JSON: JSON string sent after connect (default empty)
# - QD_SYMBOL: display label only (default "BTC_USDT")
# - QD_ENV: display label only (default "replit")
#
# Acceptance (FIX02):
# - Service stays running (no crashes)
# - Status page renders, shows ingestion state, last message time, msg counters, ringbuffer samples
# - If websockets lib missing or QD_WS_URL unset: HEALTH=RED with explicit cause
# - If WS connects and messages arrive: HEALTH=YELLOW (still early stage), msg counters increment

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple
from collections import deque

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# Optional WS client dependency.
# We do NOT hard-require it so the service remains runnable under minimal requirements.txt.
try:
    import websockets  # type: ignore
    WEBSOCKETS_AVAILABLE = True
except Exception:
    websockets = None  # type: ignore
    WEBSOCKETS_AVAILABLE = False


# -----------------------------
# Config
# -----------------------------
FIX = "FIX02"
PATCH = "P01"
DATE = "20260124"
SUBSYSTEM = "STREAM"
INTENT = "WS_INGEST"

SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT")
ENV = os.environ.get("QD_ENV", "replit")

WS_URL = os.environ.get("QD_WS_URL", "").strip()
WS_SUBSCRIBE_JSON = os.environ.get("QD_WS_SUBSCRIBE_JSON", "").strip()

PORT = int(os.environ.get("PORT", "8000"))

# Ring buffers for observability (keep small to avoid memory issues)
RAW_MSG_RING_MAX = 20
PARSE_ERR_RING_MAX = 10

# Stall thresholds
STALL_WARN_SEC = 5.0
STALL_RED_SEC = 15.0

# Reconnect backoff
BACKOFF_MIN = 0.25
BACKOFF_MAX = 8.0


# -----------------------------
# Data model
# -----------------------------
@dataclass
class StreamTelemetry:
    started_unix: float = field(default_factory=lambda: time.time())
    ws_attempts: int = 0
    ws_connects: int = 0
    ws_disconnects: int = 0
    ws_rx_messages: int = 0
    ws_rx_bytes: int = 0
    ws_last_rx_unix: float = 0.0
    ws_last_connect_unix: float = 0.0
    ws_last_disconnect_unix: float = 0.0
    ws_last_error: str = ""
    ws_state: str = "INIT"  # INIT | DISABLED | NO_CLIENT | CONNECTING | CONNECTED | ERROR | BACKOFF
    ws_backoff_sec: float = 0.0

    parse_ok: int = 0
    parse_fail: int = 0

    # rings
    last_raw_messages: Deque[str] = field(default_factory=lambda: deque(maxlen=RAW_MSG_RING_MAX))
    last_parse_errors: Deque[str] = field(default_factory=lambda: deque(maxlen=PARSE_ERR_RING_MAX))


TEL = StreamTelemetry()


def now() -> float:
    return time.time()


def iso(ts: float) -> str:
    if ts <= 0:
        return "-"
    # ISO-ish without timezone libraries (Replit-safe)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)) + "Z"


def health_state() -> Tuple[str, str]:
    """
    Returns (health_color, reason)
    Health semantics for FIX02:
      GREEN: not used yet in early pipeline phases
      YELLOW: WS connected and receiving messages recently
      RED: ingestion not operational (missing ws lib, URL missing, long stall, error loop)
    """
    if not WEBSOCKETS_AVAILABLE:
        return ("RED", "WS client library 'websockets' not available in environment.")
    if not WS_URL:
        return ("RED", "QD_WS_URL not set; ingestion disabled by configuration.")
    if TEL.ws_state in ("CONNECTED",):
        # If connected but stalled, degrade
        age = now() - TEL.ws_last_rx_unix if TEL.ws_last_rx_unix > 0 else 1e9
        if age >= STALL_RED_SEC:
            return ("RED", f"Connected but no messages received for {age:.1f}s (stall).")
        if age >= STALL_WARN_SEC:
            return ("YELLOW", f"Connected; messages stale by {age:.1f}s (warn).")
        return ("YELLOW", "Connected and receiving messages (early-stage).")
    if TEL.ws_state in ("CONNECTING", "BACKOFF"):
        return ("RED", f"Not connected yet ({TEL.ws_state}).")
    if TEL.ws_state in ("ERROR",):
        return ("RED", "WS in ERROR state.")
    if TEL.ws_state in ("DISABLED", "NO_CLIENT"):
        return ("RED", "Ingestion disabled.")
    return ("RED", "Ingestion not operational.")


# -----------------------------
# WS ingestion loop
# -----------------------------
async def ws_ingest_loop(stop_event: asyncio.Event) -> None:
    """
    Connect -> optional subscribe -> receive -> update telemetry -> reconnect on failure.
    """
    if not WEBSOCKETS_AVAILABLE:
        TEL.ws_state = "NO_CLIENT"
        return
    if not WS_URL:
        TEL.ws_state = "DISABLED"
        return

    backoff = BACKOFF_MIN
    TEL.ws_state = "CONNECTING"

    while not stop_event.is_set():
        TEL.ws_attempts += 1
        TEL.ws_state = "CONNECTING"
        try:
            # websockets.connect supports ping/pong internally; keep small timeouts
            async with websockets.connect(  # type: ignore
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_size=2_000_000,
            ) as ws:
                TEL.ws_connects += 1
                TEL.ws_last_connect_unix = now()
                TEL.ws_state = "CONNECTED"
                backoff = BACKOFF_MIN
                TEL.ws_backoff_sec = 0.0

                # Subscribe if provided
                if WS_SUBSCRIBE_JSON:
                    try:
                        await ws.send(WS_SUBSCRIBE_JSON)
                    except Exception as e:
                        TEL.ws_last_error = f"subscribe_send_failed: {type(e).__name__}: {e}"
                        TEL.last_parse_errors.append(TEL.ws_last_error)

                # Receive loop
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    TEL.ws_rx_messages += 1
                    if isinstance(msg, (bytes, bytearray)):
                        TEL.ws_rx_bytes += len(msg)
                        try:
                            msg_str = msg.decode("utf-8", errors="replace")
                        except Exception:
                            msg_str = repr(msg[:200])
                    else:
                        # str
                        TEL.ws_rx_bytes += len(msg.encode("utf-8", errors="ignore"))
                        msg_str = msg

                    TEL.ws_last_rx_unix = now()
                    TEL.last_raw_messages.append(msg_str[:2000])

                    # Parse JSON best-effort (we do not assume exchange schema yet)
                    try:
                        _ = json.loads(msg_str)
                        TEL.parse_ok += 1
                    except Exception as e:
                        TEL.parse_fail += 1
                        TEL.last_parse_errors.append(f"json_parse_fail: {type(e).__name__}: {str(e)[:160]}")

        except Exception as e:
            TEL.ws_disconnects += 1
            TEL.ws_last_disconnect_unix = now()
            TEL.ws_state = "ERROR"
            TEL.ws_last_error = f"{type(e).__name__}: {str(e)[:240]}"
            TEL.last_parse_errors.append("ws_error: " + TEL.ws_last_error)

            # Backoff before reconnect
            TEL.ws_state = "BACKOFF"
            TEL.ws_backoff_sec = backoff
            # Sleep in small increments to allow stop_event responsiveness
            sleep_left = backoff
            while sleep_left > 0 and not stop_event.is_set():
                step = min(0.25, sleep_left)
                await asyncio.sleep(step)
                sleep_left -= step
            backoff = min(BACKOFF_MAX, backoff * 2.0)

    # exit
    if TEL.ws_state not in ("DISABLED", "NO_CLIENT"):
        TEL.ws_state = "DISABLED"


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="QuantDesk Bookmap Service", version=f"{FIX}-{PATCH}", docs_url=None, redoc_url=None)

STOP_EVENT = asyncio.Event()
BG_TASK: Optional[asyncio.Task] = None


@app.on_event("startup")
async def on_startup() -> None:
    global BG_TASK
    STOP_EVENT.clear()
    BG_TASK = asyncio.create_task(ws_ingest_loop(STOP_EVENT))


@app.on_event("shutdown")
async def on_shutdown() -> None:
    STOP_EVENT.set()
    try:
        if BG_TASK:
            BG_TASK.cancel()
    except Exception:
        pass


def status_payload() -> Dict[str, Any]:
    health, reason = health_state()
    return {
        "service": "quantdesk-bookmap-service",
        "fix": FIX,
        "patch": PATCH,
        "date": DATE,
        "subsystem": SUBSYSTEM,
        "intent": INTENT,
        "symbol": SYMBOL,
        "env": ENV,
        "health": health,
        "health_reason": reason,
        "ws": {
            "available_client_lib": WEBSOCKETS_AVAILABLE,
            "url_configured": bool(WS_URL),
            "state": TEL.ws_state,
            "attempts": TEL.ws_attempts,
            "connects": TEL.ws_connects,
            "disconnects": TEL.ws_disconnects,
            "rx_messages": TEL.ws_rx_messages,
            "rx_bytes": TEL.ws_rx_bytes,
            "last_rx": iso(TEL.ws_last_rx_unix),
            "last_connect": iso(TEL.ws_last_connect_unix),
            "last_disconnect": iso(TEL.ws_last_disconnect_unix),
            "backoff_sec": TEL.ws_backoff_sec,
            "last_error": TEL.ws_last_error,
        },
        "parse": {
            "ok": TEL.parse_ok,
            "fail": TEL.parse_fail,
        },
        "rings": {
            "raw_messages": list(TEL.last_raw_messages),
            "parse_errors": list(TEL.last_parse_errors),
        },
        "uptime_sec": round(now() - TEL.started_unix, 3),
        "server_time_utc": iso(now()),
    }


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def render_status_html() -> str:
    st = status_payload()
    health = st["health"]
    # no CSS dependencies; keep simple and readable in Replit preview and iPad
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QuantDesk Bookmap — {FIX} {PATCH} — {SUBSYSTEM}</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji"; margin: 18px; }}
    .card {{ border: 1px solid #3333; border-radius: 10px; padding: 14px; margin-bottom: 12px; }}
    .row {{ display: flex; flex-wrap: wrap; gap: 10px; }}
    .kv {{ min-width: 240px; }}
    .k {{ color: #666; font-size: 12px; }}
    .v {{ font-size: 14px; white-space: pre-wrap; word-break: break-word; }}
    .health {{ font-weight: 700; }}
    .RED {{ color: #b00020; }}
    .YELLOW {{ color: #a36a00; }}
    .GREEN {{ color: #0a7a0a; }}
    code, pre {{ background: #00000010; padding: 8px; border-radius: 8px; overflow-x: auto; }}
    .small {{ font-size: 12px; color: #666; }}
  </style>
</head>
<body>
  <h2>QuantDesk Bookmap Service — {FIX} / {PATCH}</h2>
  <div class="card">
    <div class="row">
      <div class="kv"><div class="k">Subsystem</div><div class="v">{SUBSYSTEM}</div></div>
      <div class="kv"><div class="k">Intent</div><div class="v">{INTENT}</div></div>
      <div class="kv"><div class="k">Symbol</div><div class="v">{html_escape(SYMBOL)}</div></div>
      <div class="kv"><div class="k">Env</div><div class="v">{html_escape(ENV)}</div></div>
      <div class="kv"><div class="k">Uptime</div><div class="v">{st["uptime_sec"]}s</div></div>
      <div class="kv"><div class="k">Server time (UTC)</div><div class="v">{st["server_time_utc"]}</div></div>
    </div>
    <div style="margin-top:10px;">
      <div class="k">Health</div>
      <div class="v health {health}">{health}</div>
      <div class="small">{html_escape(st["health_reason"])}</div>
    </div>
  </div>

  <div class="card">
    <h3>WS Ingestion</h3>
    <div class="row">
      <div class="kv"><div class="k">Client lib available</div><div class="v">{st["ws"]["available_client_lib"]}</div></div>
      <div class="kv"><div class="k">QD_WS_URL configured</div><div class="v">{st["ws"]["url_configured"]}</div></div>
      <div class="kv"><div class="k">State</div><div class="v">{html_escape(st["ws"]["state"])}</div></div>
      <div class="kv"><div class="k">Attempts</div><div class="v">{st["ws"]["attempts"]}</div></div>
      <div class="kv"><div class="k">Connects</div><div class="v">{st["ws"]["connects"]}</div></div>
      <div class="kv"><div class="k">Disconnects</div><div class="v">{st["ws"]["disconnects"]}</div></div>
      <div class="kv"><div class="k">RX messages</div><div class="v">{st["ws"]["rx_messages"]}</div></div>
      <div class="kv"><div class="k">RX bytes</div><div class="v">{st["ws"]["rx_bytes"]}</div></div>
      <div class="kv"><div class="k">Last RX</div><div class="v">{st["ws"]["last_rx"]}</div></div>
      <div class="kv"><div class="k">Last connect</div><div class="v">{st["ws"]["last_connect"]}</div></div>
      <div class="kv"><div class="k">Last disconnect</div><div class="v">{st["ws"]["last_disconnect"]}</div></div>
      <div class="kv"><div class="k">Backoff (sec)</div><div class="v">{st["ws"]["backoff_sec"]}</div></div>
    </div>
    <div style="margin-top:10px;">
      <div class="k">Last error</div>
      <div class="v">{html_escape(st["ws"]["last_error"] or "-")}</div>
    </div>
  </div>

  <div class="card">
    <h3>Parsing (best-effort)</h3>
    <div class="row">
      <div class="kv"><div class="k">JSON parse ok</div><div class="v">{st["parse"]["ok"]}</div></div>
      <div class="kv"><div class="k">JSON parse fail</div><div class="v">{st["parse"]["fail"]}</div></div>
    </div>
    <p class="small">FIX02 does not assume exchange schema; it only checks that inbound messages are valid JSON.</p>
  </div>

  <div class="card">
    <h3>Recent raw messages (truncated)</h3>
    <pre>{html_escape("\\n\\n".join(st["rings"]["raw_messages"]) or "-")}</pre>
  </div>

  <div class="card">
    <h3>Recent parse / WS errors</h3>
    <pre>{html_escape("\\n".join(st["rings"]["parse_errors"]) or "-")}</pre>
  </div>

  <div class="card">
    <h3>Release Gate (FIX02)</h3>
    <ul>
      <li><b>GREEN</b>: Not applicable in FIX02 (reserved for later phases).</li>
      <li><b>YELLOW</b>: WS connected and receiving messages recently; counters increment.</li>
      <li><b>RED</b>: Missing WS client library, missing URL config, or stalled/disconnected/error loop.</li>
    </ul>
    <p class="small">If RED due to missing WS client library, FIX03 will formalize dependency handling (or switch to an alternate client approach) under a new FIX.</p>
  </div>

  <div class="small">
    Endpoints: <a href="/health.json">/health.json</a> · <a href="/telemetry.json">/telemetry.json</a> · <a href="/raw_sample.txt">/raw_sample.txt</a>
  </div>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    return render_status_html()


@app.get("/health.json")
def health_json() -> JSONResponse:
    st = status_payload()
    # minimal payload for uptime monitors
    return JSONResponse(
        {
            "service": st["service"],
            "fix": st["fix"],
            "patch": st["patch"],
            "subsystem": st["subsystem"],
            "intent": st["intent"],
            "symbol": st["symbol"],
            "health": st["health"],
            "health_reason": st["health_reason"],
            "uptime_sec": st["uptime_sec"],
            "ws_state": st["ws"]["state"],
            "ws_rx_messages": st["ws"]["rx_messages"],
            "ws_last_rx": st["ws"]["last_rx"],
        }
    )


@app.get("/telemetry.json")
def telemetry_json() -> JSONResponse:
    return JSONResponse(status_payload())


@app.get("/raw_sample.txt", response_class=PlainTextResponse)
def raw_sample() -> str:
    st = status_payload()
    msgs = st["rings"]["raw_messages"]
    if not msgs:
        return "No messages captured.\n"
    return "\n\n---\n\n".join(msgs) + "\n"


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    # No console spam; single startup line is acceptable for operational sanity.
    # Replit expects a long-running server; uvicorn handles that.
    import uvicorn  # in requirements.txt

    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
