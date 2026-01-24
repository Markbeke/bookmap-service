"""
QuantDesk Bookmap Service — current_fix.py
Build: FIX02/P03
Subsystem: STREAM (WS ingest)
Date: 2026-01-24

Goal of FIX02/P03
- Stabilize WS connectivity in Replit by using the WS endpoint observed in user screenshots as default.
- Add explicit, user-visible connector error diagnostics in /telemetry.json and on the status page.
- Add robust reconnect with backoff + jitter and clear counters (reconnects, last_error, last_close_code/reason).

Notes / limitations (truthful):
- I cannot confirm MEXC's correct websocket base URL for your exact market stream without their live docs.
  Therefore:
    1) We default to the URL shown in your screenshots: wss://wss-api.mexc.com/ws
    2) You can override via env QD_MEXC_WS_URL in Replit Secrets if needed.
- FIX02/P03 still "counts frames" and does not decode protobuf/binary payloads yet (explicitly deferred).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None


# -----------------------------
# Config
# -----------------------------
SERVICE_NAME = "qd-bookmap"
BUILD = "FIX02/P03"
SUBSYSTEM = "STREAM"

# Symbol/streams are kept minimal and "best-effort".
# If the exchange requires different stream names, you can override streams via env.
DEFAULT_STREAMS = [
    # These are placeholders aligned with earlier FIX02 behavior.
    # If MEXC expects different identifiers, set QD_MEXC_STREAMS to a JSON list.
    "spot@public.deals.v3.api@BTCUSDT",
    "spot@public.depth.v3.api@BTCUSDT@20",
]

# IMPORTANT: Default to what your screenshot shows.
DEFAULT_WS_URL = "wss://wss-api.mexc.com/ws"

WS_URL = os.environ.get("QD_MEXC_WS_URL", DEFAULT_WS_URL).strip()

def _load_streams() -> List[str]:
    raw = os.environ.get("QD_MEXC_STREAMS", "").strip()
    if not raw:
        return DEFAULT_STREAMS[:]
    try:
        v = json.loads(raw)
        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            return [x.strip() for x in v if x.strip()]
    except Exception:
        pass
    # Fallback: comma-separated
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]

STREAMS = _load_streams()

# Replit typically routes external :80 -> internal :8000 or :5000 depending on config.
# We bind to 0.0.0.0 and PORT env if present.
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", os.environ.get("QD_PORT", "5000")))

START_TS = time.time()


# -----------------------------
# State / Telemetry
# -----------------------------
@dataclass
class ConnectorState:
    status: str = "DISCONNECTED"      # DISCONNECTED | CONNECTED | ERROR
    frames_total: int = 0
    last_event_ts: float = 0.0
    last_ack_ts: float = 0.0
    reconnects: int = 0

    ws_url: str = WS_URL
    streams: List[str] = None  # type: ignore

    last_error: str = ""
    last_close_code: Optional[int] = None
    last_close_reason: str = ""
    last_connect_ts: float = 0.0

    def to_public(self) -> Dict[str, Any]:
        d = asdict(self)
        d["streams"] = self.streams or []
        return d


STATE = ConnectorState(streams=STREAMS[:])
STATE_LOCK = threading.Lock()


def _now_ts() -> float:
    return time.time()


def _safe_exc(e: BaseException) -> str:
    # keep it short but useful
    s = f"{type(e).__name__}: {str(e)}".strip()
    return s[:500]


def _mexc_subscribe_payload(streams: List[str]) -> str:
    """
    Earlier FIX02 assumed MEXC spot v3 uses:
      {"method":"SUBSCRIPTION","params":[...]}
    If this turns out to be incorrect for your account/market, override streams/url.
    """
    payload = {
        "method": "SUBSCRIPTION",
        "params": [s for s in streams if s.strip()],
        "id": int(_now_ts() * 1000) % 1_000_000_000,
    }
    return json.dumps(payload, separators=(",", ":"))


# -----------------------------
# WS loop
# -----------------------------
async def _ws_session(ws_url: str, streams: List[str]) -> None:
    assert websockets is not None

    # websockets.connect kwargs kept conservative for Replit stability
    async with websockets.connect(
        ws_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_queue=64,
    ) as ws:
        with STATE_LOCK:
            STATE.status = "CONNECTED"
            STATE.last_error = ""
            STATE.last_close_code = None
            STATE.last_close_reason = ""
            STATE.last_connect_ts = _now_ts()

        # Subscribe
        sub = _mexc_subscribe_payload(streams)
        await ws.send(sub)

        # Read loop
        while True:
            msg = await ws.recv()  # may be str or bytes
            ts = _now_ts()
            with STATE_LOCK:
                STATE.frames_total += 1
                STATE.last_event_ts = ts

            # Best-effort "ack" detection (not guaranteed)
            if isinstance(msg, str):
                if '"code"' in msg or '"success"' in msg or '"ack"' in msg:
                    with STATE_LOCK:
                        STATE.last_ack_ts = ts


async def _ws_reconnect_forever() -> None:
    if websockets is None:
        with STATE_LOCK:
            STATE.status = "ERROR"
            STATE.last_error = "Missing dependency: websockets"
        return

    # Exponential backoff with jitter
    backoff = 0.5
    backoff_max = 20.0

    while True:
        try:
            await _ws_session(STATE.ws_url, STATE.streams or [])
            # If session exits cleanly (rare), reset backoff
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Capture close codes if present (websockets raises ConnectionClosed*)
            close_code = getattr(e, "code", None)
            close_reason = getattr(e, "reason", "")
            with STATE_LOCK:
                STATE.status = "ERROR"
                STATE.last_error = _safe_exc(e)
                STATE.last_close_code = int(close_code) if isinstance(close_code, int) else None
                STATE.last_close_reason = str(close_reason)[:300]
                STATE.reconnects += 1

            # Sleep with jitter then retry
            jitter = random.uniform(0.0, 0.25)
            await asyncio.sleep(min(backoff + jitter, backoff_max))
            backoff = min(backoff * 1.7, backoff_max)


def _start_ws_thread() -> None:
    """
    Run asyncio WS reconnect loop in a dedicated daemon thread,
    so FastAPI/Uvicorn can run normally.
    """
    def runner() -> None:
        try:
            asyncio.run(_ws_reconnect_forever())
        except Exception as e:
            with STATE_LOCK:
                STATE.status = "ERROR"
                STATE.last_error = f"WS thread crashed: {_safe_exc(e)}"

    t = threading.Thread(target=runner, name="qd_ws_thread", daemon=True)
    t.start()


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="QuantDesk Bookmap Service", version=BUILD)

@app.on_event("startup")
async def _startup() -> None:
    _start_ws_thread()


def _health() -> Dict[str, Any]:
    with STATE_LOCK:
        st = STATE.to_public()

    uptime = _now_ts() - START_TS

    # Health logic:
    # - GREEN if connected and fresh
    # - YELLOW if disconnected but process alive (startup / transient)
    # - RED if persistent error (ERROR status or staleness)
    fresh_s = _now_ts() - (st.get("last_event_ts") or 0.0) if st.get("last_event_ts") else 1e9
    if st["status"] == "CONNECTED" and fresh_s < 3.0:
        health = "GREEN"
    elif st["status"] == "ERROR":
        health = "RED"
    else:
        # DISCONNECTED or stale CONNECTED -> YELLOW then RED if too stale
        health = "YELLOW" if fresh_s < 30.0 else "RED"

    return {
        "ts": _now_ts(),
        "service": SERVICE_NAME,
        "build": BUILD,
        "subsystem": SUBSYSTEM,
        "health": health,
        "uptime_s": round(uptime, 3),
        "connector": st,
        "freshness_s": round(fresh_s, 3) if fresh_s < 1e8 else None,
    }


@app.get("/health.json")
async def health_json() -> JSONResponse:
    return JSONResponse(_health())


@app.get("/telemetry.json")
async def telemetry_json() -> JSONResponse:
    # Alias to health for now; we keep separate endpoint for future expansion
    return JSONResponse(_health())


@app.get("/", response_class=HTMLResponse)
async def status_page() -> HTMLResponse:
    h = _health()
    conn = h["connector"]

    health = h["health"]
    health_color = {"GREEN": "#16a34a", "YELLOW": "#f59e0b", "RED": "#dc2626"}.get(health, "#6b7280")

    def esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>QuantDesk Bookmap — Status</title>
  <style>
    body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
    .row {{ display: grid; grid-template-columns: 1fr; gap: 12px; max-width: 820px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 14px; padding: 14px 16px; }}
    .h {{ display: flex; align-items: center; gap: 12px; }}
    .pill {{ padding: 6px 10px; border-radius: 999px; color: white; font-weight: 700; background: {health_color}; }}
    .muted {{ color: #6b7280; }}
    .kv {{ display: grid; grid-template-columns: 140px 1fr; gap: 6px 12px; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 8px; }}
    a {{ color: #2563eb; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="row">
    <div class="h">
      <h2 style="margin:0;">QuantDesk Bookmap — Status</h2>
      <span class="pill">HEALTH: {esc(health)}</span>
      <span class="muted">Service: {esc(SERVICE_NAME)} · Subsystem: {esc(SUBSYSTEM)} · Build: {esc(BUILD)} · Date: 2026-01-24</span>
    </div>

    <div class="card">
      <div style="display:flex; gap:16px; flex-wrap:wrap;">
        <a href="/health.json">health.json</a>
        <a href="/telemetry.json">telemetry.json</a>
      </div>
    </div>

    <div class="card">
      <div class="kv">
        <div class="muted">Server</div><div><code>{esc(HOST)}:{esc(PORT)}</code> · Uptime: <code>{esc(round(h["uptime_s"],1))}s</code></div>
        <div class="muted">Connector</div><div><b>{esc(conn.get("status",""))}</b></div>
        <div class="muted">WS URL</div><div><code>{esc(conn.get("ws_url",""))}</code></div>
        <div class="muted">Streams</div><div><code>{esc(", ".join(conn.get("streams",[])[:3]))}{esc("..." if len(conn.get("streams",[]))>3 else "")}</code></div>
        <div class="muted">Frames</div><div><code>{esc(conn.get("frames_total",0))}</code></div>
        <div class="muted">Freshness</div><div><code>{esc(h.get("freshness_s"))}s</code></div>
        <div class="muted">Reconnects</div><div><code>{esc(conn.get("reconnects",0))}</code></div>
        <div class="muted">Last error</div><div><code>{esc(conn.get("last_error","") or "—")}</code></div>
        <div class="muted">Last close</div><div><code>{esc(conn.get("last_close_code"))}:{esc(conn.get("last_close_reason") or "")}</code></div>
      </div>
    </div>

    <div class="card">
      <div class="muted">Notes</div>
      <div>
        FIX02/P03: WS ingest loop enabled with explicit diagnostics.<br/>
        Frames counted; decoding deferred to later FIX.<br/>
        If connector stays RED with non-empty <code>Last error</code>, paste that string back to ChatGPT.
      </div>
    </div>
  </div>
</body>
</html>
"""
    return HTMLResponse(html)


@app.get("/robots.txt")
async def robots() -> PlainTextResponse:
    return PlainTextResponse("User-agent: *\nDisallow: /\n")


def main() -> None:
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
