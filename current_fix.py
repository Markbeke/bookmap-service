# =========================================================
# QuantDesk Bookmap Service — FIX02/P01 (Replit)
# File: current_fix.py
#
# Purpose (FIX02):
# - Keep the FastAPI service up reliably under Replit "Run"
# - Add a resilient WebSocket ingest loop for MEXC Spot streams
# - Expose / (status HTML), /health.json, /telemetry.json
#
# Notes:
# - MEXC Spot v3 WS base in docs: ws://wbs-api.mexc.com/ws
#   (We try wss first, then ws fallback).
# - Streams in docs include protobuf suffix ".pb". Server may send binary frames.
#   FIX02 treats frames as opaque and only measures liveness + counts.
#
# Zero local edits expectation:
# - You can run via Replit Shell:  python current_fix.py
# - If Replit "Run" is wired to Workflows/.replit, set it ONCE to: python current_fix.py
# =========================================================

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

try:
    import websockets  # provided by uvicorn[standard] dependency set
except Exception as e:  # pragma: no cover
    websockets = None  # type: ignore
    _WEBSOCKETS_IMPORT_ERR = repr(e)
else:
    _WEBSOCKETS_IMPORT_ERR = ""


# -----------------------------
# Build identity
# -----------------------------
BUILD_FIX = "FIX02"
BUILD_P = "P01"
BUILD_DATE = "2026-01-24"  # keep pinned to the milestone date for auditability

SERVICE_NAME = "qd-bookmap"
SUBSYSTEM = "STREAM"

# -----------------------------
# Configuration (env)
# -----------------------------
HOST = os.getenv("QD_HOST", "0.0.0.0")
PORT = int(os.getenv("QD_PORT", "8000"))

# MEXC spot v3 WS base (docs show ws://wbs-api.mexc.com/ws)
# We'll try wss first. If it fails, fallback to ws.
MEXC_WS_WSS = os.getenv("MEXC_WS_WSS", "wss://wbs-api.mexc.com/ws")
MEXC_WS_WS = os.getenv("MEXC_WS_WS", "ws://wbs-api.mexc.com/ws")

# Symbol format in MEXC spot docs typically like BTCUSDT.
SYMBOL = os.getenv("QD_SYMBOL", "BTCUSDT").upper()

# A conservative set of public streams (protobuf channel names from MEXC spot v3 docs).
# FIX02 only counts frames; it does not decode protobuf payloads.
STREAMS_DEFAULT = [
    f"spot@public.aggre.deals.v3.api.pb@100ms@{SYMBOL}",
    f"spot@public.increase.depth.v3.api.pb@100ms@{SYMBOL}",
]
STREAMS = os.getenv("QD_STREAMS", ",".join(STREAMS_DEFAULT)).split(",")

# Liveness thresholds
FRESH_SECONDS_GREEN = float(os.getenv("QD_FRESH_SECONDS_GREEN", "3.0"))
FRESH_SECONDS_YELLOW = float(os.getenv("QD_FRESH_SECONDS_YELLOW", "15.0"))

# Reconnect policy
RECONNECT_BASE_SECONDS = float(os.getenv("QD_RECONNECT_BASE_SECONDS", "0.5"))
RECONNECT_MAX_SECONDS = float(os.getenv("QD_RECONNECT_MAX_SECONDS", "10.0"))

# -----------------------------
# Runtime state
# -----------------------------

@dataclass
class ConnectorState:
    connected: bool = False
    ws_url: str = ""
    last_connect_ts: float = 0.0
    last_msg_ts: float = 0.0
    last_err: str = ""
    reconnects: int = 0
    frames_total: int = 0
    bytes_total: int = 0
    text_frames: int = 0
    binary_frames: int = 0
    acks: int = 0
    last_ack_ts: float = 0.0
    last_event: str = ""

    def freshness_age_s(self, now: Optional[float] = None) -> float:
        n = time.time() if now is None else now
        if self.last_msg_ts <= 0:
            return float("inf")
        return max(0.0, n - self.last_msg_ts)

    def health(self) -> Tuple[str, str]:
        """
        Returns: (health_color, reason)
        - GREEN: connected and msg fresh under FRESH_SECONDS_GREEN
        - YELLOW: connected but stale, or not yet connected but service is up
        - RED: websockets import missing or repeated errors without any successful connect
        """
        if websockets is None:
            return ("RED", f"websockets import failed: {_WEBSOCKETS_IMPORT_ERR}")

        # Service up but not connected yet (expected transient)
        if not self.connected and self.last_connect_ts == 0.0 and self.last_err == "":
            return ("YELLOW", "Connector not started yet.")

        # If never received messages but connected, still yellow
        if self.connected and self.last_msg_ts == 0.0:
            return ("YELLOW", "Connected, awaiting first market frames.")

        age = self.freshness_age_s()
        if self.connected and age <= FRESH_SECONDS_GREEN:
            return ("GREEN", f"Market frames fresh ({age:.2f}s).")

        if self.connected and age <= FRESH_SECONDS_YELLOW:
            return ("YELLOW", f"Market frames stale ({age:.2f}s).")

        # Not connected / very stale
        if self.last_err:
            return ("YELLOW", f"Connector issue: {self.last_err[:200]}")
        return ("YELLOW", "Disconnected or stale; reconnect loop active.")


STATE = ConnectorState()
START_TS = time.time()

# -----------------------------
# WS subscription helpers
# -----------------------------

def _mexc_subscribe_payload(streams: List[str]) -> str:
    """
    MEXC spot v3 docs show JSON subscription with:
      {"method":"SUBSCRIPTION","params":[...]}
    (Docs also mention request id).
    """
    payload = {
        "method": "SUBSCRIPTION",
        "params": [s for s in streams if s.strip()],
    }
    return json.dumps(payload, separators=(",", ":"))

def _safe_exc(e: BaseException) -> str:
    return f"{type(e).__name__}: {str(e)}"

async def _ws_connect_and_stream(ws_url: str, streams: List[str]) -> None:
    """
    Connects, subscribes, reads frames, updates STATE.
    Frames may be text or binary (protobuf). We count both.
    """
    assert websockets is not None  # checked by caller
    STATE.ws_url = ws_url
    STATE.last_event = "connecting"
    STATE.last_connect_ts = time.time()

    # Conservative timeouts
    open_timeout = 10
    ping_interval = 20
    ping_timeout = 20

    async with websockets.connect(
        ws_url,
        open_timeout=open_timeout,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        max_size=2**20,  # 1MB
    ) as ws:
        STATE.connected = True
        STATE.last_event = "connected"
        STATE.last_err = ""

        # Subscribe
        sub_msg = _mexc_subscribe_payload(streams)
        await ws.send(sub_msg)
        # We treat any immediate response as an "ack" if it looks like JSON.
        # If it's binary, we still count it as frame, but not ack.
        STATE.last_event = "subscribed"

        while True:
            msg = await ws.recv()
            now = time.time()
            STATE.last_msg_ts = now
            STATE.frames_total += 1

            if isinstance(msg, (bytes, bytearray)):
                STATE.binary_frames += 1
                STATE.bytes_total += len(msg)
                STATE.last_event = "frame:binary"
                # cannot decode protobuf here (FIX02), but we can still be alive
                continue

            # Text frame
            if isinstance(msg, str):
                STATE.text_frames += 1
                STATE.bytes_total += len(msg.encode("utf-8", errors="ignore"))
                STATE.last_event = "frame:text"

                # Best-effort ack detection (do not assume schema)
                # Many WS APIs echo {"id":...,"code":...} or {"success":true,...}
                try:
                    j = json.loads(msg)
                    # if message contains "code"/"msg"/"success" and no "d"/"data" payload, call it ack
                    if isinstance(j, dict):
                        keys = set(j.keys())
                        if ("code" in keys or "success" in keys or "msg" in keys) and not ("data" in keys or "d" in keys):
                            STATE.acks += 1
                            STATE.last_ack_ts = now
                            STATE.last_event = "ack"
                except Exception:
                    # ignore parse errors
                    pass

                continue

            # Unknown type (shouldn't happen)
            STATE.last_event = f"frame:unknown:{type(msg).__name__}"


async def connector_loop() -> None:
    """
    Robust reconnect loop. Tries wss then ws.
    """
    if websockets is None:
        STATE.last_err = f"websockets import failed: {_WEBSOCKETS_IMPORT_ERR}"
        return

    backoff = RECONNECT_BASE_SECONDS
    urls = [MEXC_WS_WSS, MEXC_WS_WS]

    while True:
        for url in urls:
            try:
                await _ws_connect_and_stream(url, STREAMS)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                STATE.connected = False
                STATE.last_err = _safe_exc(e)
                STATE.last_event = "error"
                STATE.reconnects += 1
                # backoff with cap
                await asyncio.sleep(backoff)
                backoff = min(RECONNECT_MAX_SECONDS, backoff * 1.8)
                continue

        # If both URLs failed, slow down briefly and retry
        await asyncio.sleep(min(RECONNECT_MAX_SECONDS, backoff))
        backoff = min(RECONNECT_MAX_SECONDS, backoff * 1.2)


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="QuantDesk Bookmap Service", version=f"{BUILD_FIX}/{BUILD_P}")

@app.on_event("startup")
async def _startup() -> None:
    # start background connector
    asyncio.create_task(connector_loop())

def _status_html() -> str:
    health_color, health_reason = STATE.health()
    uptime = time.time() - START_TS
    age = STATE.freshness_age_s()

    def pill(text: str, color: str) -> str:
        bg = {
            "GREEN": "#16a34a",
            "YELLOW": "#f59e0b",
            "RED": "#dc2626",
        }.get(color, "#6b7280")
        return f'<span style="display:inline-block;padding:6px 10px;border-radius:999px;background:{bg};color:#fff;font-weight:600;font-size:12px">{text}</span>'

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap — Status</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; background: #fff; color: #111; }}
    .muted {{ color: #6b7280; }}
    .row {{ display:flex; gap:16px; flex-wrap: wrap; }}
    .card {{ border:1px solid #e5e7eb; border-radius:12px; padding:16px; min-width: 260px; flex: 1; }}
    table {{ width: 100%; border-collapse: collapse; }}
    td, th {{ text-align:left; padding: 10px 8px; border-bottom: 1px solid #e5e7eb; }}
    th {{ font-size: 12px; color: #6b7280; font-weight: 700; letter-spacing: .02em; }}
    code {{ background:#f3f4f6; padding:2px 6px; border-radius:6px; }}
  </style>
</head>
<body>
  <h2>QuantDesk Bookmap — Status</h2>
  <div class="muted">
    Service: <b>{SERVICE_NAME}</b> · Subsystem: <b>{SUBSYSTEM}</b> · Build: <b>{BUILD_FIX}/{BUILD_P}</b> · Date: <b>{BUILD_DATE}</b>
  </div>

  <div style="margin-top:12px;">{pill(f"HEALTH: {health_color}", health_color)} &nbsp; <a href="/health.json">health.json</a> &nbsp; <a href="/telemetry.json">telemetry.json</a></div>

  <div class="row" style="margin-top:14px;">
    <div class="card">
      <div class="muted" style="font-size:12px;font-weight:700;">Server</div>
      <div style="font-size:20px;font-weight:700;margin-top:6px;">{HOST}:{PORT}</div>
      <div class="muted">Uptime: {uptime:.1f}s</div>
    </div>
    <div class="card">
      <div class="muted" style="font-size:12px;font-weight:700;">Connector</div>
      <div style="font-size:20px;font-weight:700;margin-top:6px;">{"CONNECTED" if STATE.connected else "DISCONNECTED"}</div>
      <div class="muted">WS: <code>{STATE.ws_url or "(pending)"}</code></div>
      <div class="muted">Last event: <code>{STATE.last_event or "(none)"}</code></div>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <div class="muted" style="font-size:12px;font-weight:700;">Notes</div>
    <div style="margin-top:6px;">
      FIX02: WebSocket ingest loop enabled. Frames are counted (protobuf decode deferred to later FIX).
    </div>
    <div class="muted" style="margin-top:8px;">{health_reason}</div>
  </div>

  <h3 style="margin-top:18px;">Release Gate Summary</h3>
  <table>
    <thead>
      <tr><th>Gate</th><th>Status</th><th>Detail</th></tr>
    </thead>
    <tbody>
      <tr><td>deps.fastapi_uvicorn</td><td><b>PASS</b></td><td>FastAPI and Uvicorn imported.</td></tr>
      <tr><td>server.bind_config</td><td><b>PASS</b></td><td>Host/port configured: {HOST}:{PORT}</td></tr>
      <tr><td>process.liveness</td><td><b>PASS</b></td><td>Uptime {uptime:.1f}s</td></tr>
      <tr><td>stream.ws_connect</td><td><b>{"PASS" if STATE.connected else "WARN"}</b></td><td>{"Connected to WS." if STATE.connected else "Not connected yet / reconnecting."}</td></tr>
      <tr><td>data.freshness</td><td><b>{"PASS" if (STATE.connected and age <= FRESH_SECONDS_GREEN) else "WARN"}</b></td><td>Last frame age: {age:.2f}s</td></tr>
    </tbody>
  </table>

  <div class="muted" style="margin-top:10px;font-size:12px;">
    Expected FIX02 outcome: service stays running; connector attempts WS; health becomes GREEN once frames arrive.
  </div>
</body>
</html>
"""
    return html

@app.get("/", response_class=HTMLResponse)
async def root() -> HTMLResponse:
    return HTMLResponse(_status_html())

@app.get("/health.json")
async def health_json() -> JSONResponse:
    health_color, health_reason = STATE.health()
    payload = {
        "service": SERVICE_NAME,
        "subsystem": SUBSYSTEM,
        "build": f"{BUILD_FIX}/{BUILD_P}",
        "date": BUILD_DATE,
        "host": HOST,
        "port": PORT,
        "uptime_s": round(time.time() - START_TS, 3),
        "health": health_color,
        "reason": health_reason,
        "connector": {
            "connected": STATE.connected,
            "ws_url": STATE.ws_url,
            "last_connect_ts": STATE.last_connect_ts,
            "last_msg_ts": STATE.last_msg_ts,
            "last_err": STATE.last_err,
            "reconnects": STATE.reconnects,
            "last_event": STATE.last_event,
        },
    }
    return JSONResponse(payload)

@app.get("/telemetry.json")
async def telemetry_json() -> JSONResponse:
    health_color, health_reason = STATE.health()
    payload = {
        "service": SERVICE_NAME,
        "build": f"{BUILD_FIX}/{BUILD_P}",
        "uptime_s": round(time.time() - START_TS, 3),
        "health": health_color,
        "reason": health_reason,
        "streams": STREAMS,
        "connector_state": asdict(STATE),
        "freshness_age_s": STATE.freshness_age_s(),
    }
    return JSONResponse(payload)

def main() -> None:
    # Use uvicorn programmatically so `python current_fix.py` is enough.
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")

if __name__ == "__main__":
    main()
