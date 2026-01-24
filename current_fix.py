""" 
QuantDesk Bookmap — CURRENT FIX ENTRYPOINT

Build: FIX02 / P02
Subsystem: STREAM (WS ingest loop)

What this FIX guarantees
- FastAPI server stays up.
- Status page renders (/) plus /health.json and /telemetry.json.
- Background WS ingest loop connects to MEXC, subscribes to configured channels,
  and updates telemetry/health continuously.

Notes
- This is intentionally a *minimal* ingest: it counts frames and preserves last
  activity timestamps. Book reconstruction, protobuf decoding, and order book
  semantics are later FIX milestones.
- If WS subscription format changes, adjust WS_CHANNELS env without editing code.

Env vars
- PORT (default 5000)
- HOST (default 0.0.0.0)
- QD_SERVICE (default qd-bookmap)
- QD_SYMBOL (default BTCUSDT)
- MEXC_WS_URL (default wss://wss-api.mexc.com/ws)
- WS_CHANNELS (default derived from symbol; comma-separated)
- WS_CONNECT_TIMEOUT (seconds, default 10)
- WS_PING_INTERVAL (seconds, default 20)
- WS_PING_TIMEOUT (seconds, default 20)

Run (manual):
  python current_fix.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import traceback
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# websockets is expected via requirements.txt
try:
    import websockets
    from websockets.client import WebSocketClientProtocol
except Exception:  # pragma: no cover
    websockets = None
    WebSocketClientProtocol = Any  # type: ignore


# ---------------------------
# Config
# ---------------------------

def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v in (None, ""):
        return default
    try:
        return int(v)
    except Exception:
        return default


HOST = _env_str("HOST", "0.0.0.0")
PORT = _env_int("PORT", 5000)
SERVICE = _env_str("QD_SERVICE", "qd-bookmap")
SYMBOL = _env_str("QD_SYMBOL", "BTCUSDT")
MEXC_WS_URL = _env_str("MEXC_WS_URL", "wss://wss-api.mexc.com/ws")

WS_CONNECT_TIMEOUT = float(_env_int("WS_CONNECT_TIMEOUT", 10))
WS_PING_INTERVAL = float(_env_int("WS_PING_INTERVAL", 20))
WS_PING_TIMEOUT = float(_env_int("WS_PING_TIMEOUT", 20))

# Default channel set: use MEXC public v3 API naming convention (spot).
# If you need futures/perp channels, override WS_CHANNELS with a comma-separated list.
DEFAULT_CHANNELS = [
    f"spot@public.deals.v3.api@{SYMBOL}",
    f"spot@public.increase.depth.v3.api@{SYMBOL}",
]
WS_CHANNELS = [c.strip() for c in _env_str("WS_CHANNELS", ",".join(DEFAULT_CHANNELS)).split(",") if c.strip()]


# ---------------------------
# Telemetry model
# ---------------------------

@dataclass
class ConnectorState:
    status: str = "DISCONNECTED"  # DISCONNECTED|CONNECTING|CONNECTED|ERROR
    ws_url: str = MEXC_WS_URL
    channels: List[str] = None  # type: ignore
    last_event_ts: float = 0.0
    last_ack_ts: float = 0.0
    frames_total: int = 0
    reconnects: int = 0
    last_error: str = ""

    def __post_init__(self) -> None:
        if self.channels is None:
            self.channels = list(WS_CHANNELS)


@dataclass
class ServiceState:
    service: str = SERVICE
    subsystem: str = "STREAM"
    build: str = "FIX02/P02"
    date: str = "2026-01-24"
    server_started_ts: float = 0.0
    server_host: str = HOST
    server_port: int = PORT
    health: str = "YELLOW"  # GREEN|YELLOW|RED
    notes: str = "FIX02/P02: WebSocket ingest loop enabled. Frames counted; decoding deferred to later FIX."


STATE = ServiceState(server_started_ts=time.time())
CONNECTOR = ConnectorState()


# ---------------------------
# Helpers
# ---------------------------


def _now() -> float:
    return time.time()


def _age(ts: float) -> Optional[float]:
    if not ts:
        return None
    return max(0.0, _now() - ts)


def _set_health_from_state() -> None:
    """Health policy for FIX02: strict but simple."""
    # If websockets library missing: hard RED.
    if websockets is None:
        STATE.health = "RED"
        return

    if CONNECTOR.status == "CONNECTED":
        # Freshness window: 3 seconds to stay GREEN.
        age = _age(CONNECTOR.last_event_ts)
        if age is not None and age <= 3.0:
            STATE.health = "GREEN"
        else:
            # Connected but stale.
            STATE.health = "YELLOW"
    elif CONNECTOR.status in ("CONNECTING", "DISCONNECTED"):
        STATE.health = "YELLOW"
    else:
        STATE.health = "RED"


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


# ---------------------------
# WS ingest
# ---------------------------


def _build_subscribe_payload(channels: List[str], req_id: int) -> Dict[str, Any]:
    """MEXC v3 public subscribe shape.

    Common forms observed in MEXC documentation and examples:
      {"method":"SUBSCRIPTION","params":["channel@..."],"id":1}

    If the venue expects a different method name in your market (swap/perp),
    override WS_CHANNELS with the correct channel strings; method name remains
    SUBSCRIPTION for this FIX.
    """
    return {"method": "SUBSCRIPTION", "params": channels, "id": req_id}


async def _ws_connect_and_stream() -> None:
    if websockets is None:
        CONNECTOR.status = "ERROR"
        CONNECTOR.last_error = "Missing dependency: websockets"
        _set_health_from_state()
        return

    backoff = 1.0
    req_id = 1

    while True:
        try:
            CONNECTOR.status = "CONNECTING"
            CONNECTOR.last_error = ""
            _set_health_from_state()

            async with websockets.connect(
                MEXC_WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                close_timeout=5,
                open_timeout=WS_CONNECT_TIMEOUT,
                max_size=None,
            ) as ws:
                await _ws_on_open(ws, req_id)
                req_id += 1
                backoff = 1.0

                CONNECTOR.status = "CONNECTED"
                _set_health_from_state()

                async for msg in ws:
                    CONNECTOR.frames_total += 1
                    CONNECTOR.last_event_ts = _now()

                    # MEXC sometimes sends pings as JSON or raw strings; handle both.
                    await _ws_handle_message(ws, msg)

                    _set_health_from_state()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            CONNECTOR.reconnects += 1
            CONNECTOR.status = "ERROR"
            CONNECTOR.last_error = f"{type(e).__name__}: {e}"
            _set_health_from_state()

            # bounded exponential backoff with jitter
            await asyncio.sleep(min(15.0, backoff) + (0.1 * (CONNECTOR.reconnects % 7)))
            backoff = min(15.0, backoff * 1.8)


async def _ws_on_open(ws: WebSocketClientProtocol, req_id: int) -> None:
    # Subscribe
    payload = _build_subscribe_payload(CONNECTOR.channels, req_id=req_id)
    await ws.send(_safe_json(payload))


async def _ws_handle_message(ws: WebSocketClientProtocol, msg: Any) -> None:
    # msg can be str or bytes
    if isinstance(msg, (bytes, bytearray)):
        # For FIX02 we do not decode binary frames; just count them.
        return

    if not isinstance(msg, str):
        return

    s = msg.strip()

    # Some endpoints send literal "pong" / "ping".
    if s.lower() == "ping":
        try:
            await ws.send("pong")
        except Exception:
            pass
        return

    # JSON payload handling
    try:
        j = json.loads(s)
    except Exception:
        return

    # Common ack patterns: {"code":0,"msg":"..."} or {"id":1,"code":0,...}
    if isinstance(j, dict):
        # MEXC v3 examples: {"id":1,"code":0,"msg":"success"}
        code = j.get("code")
        if code == 0 and ("id" in j or j.get("msg") in ("success", "Success", "SUCCESS")):
            CONNECTOR.last_ack_ts = _now()

        # Some ping formats: {"ping": 123456}
        if "ping" in j:
            try:
                await ws.send(_safe_json({"pong": j["ping"]}))
            except Exception:
                pass


# ---------------------------
# FastAPI
# ---------------------------

app = FastAPI(title="QuantDesk Bookmap", version=STATE.build)


@app.on_event("startup")
async def _startup() -> None:
    # Ensure loop starts and doesn't crash the server.
    asyncio.create_task(_ws_connect_and_stream())


@app.get("/health.json")
async def health_json() -> JSONResponse:
    _set_health_from_state()
    payload = {
        "service": STATE.service,
        "subsystem": STATE.subsystem,
        "build": STATE.build,
        "date": STATE.date,
        "health": STATE.health,
        "server": {
            "host": STATE.server_host,
            "port": STATE.server_port,
            "uptime_s": round(_now() - STATE.server_started_ts, 3),
        },
        "connector": {
            "status": CONNECTOR.status,
            "ws_url": CONNECTOR.ws_url,
            "channels": CONNECTOR.channels,
            "last_event_age_s": None if _age(CONNECTOR.last_event_ts) is None else round(_age(CONNECTOR.last_event_ts), 3),
            "last_ack_age_s": None if _age(CONNECTOR.last_ack_ts) is None else round(_age(CONNECTOR.last_ack_ts), 3),
            "frames_total": CONNECTOR.frames_total,
            "reconnects": CONNECTOR.reconnects,
            "last_error": CONNECTOR.last_error,
        },
        "notes": STATE.notes,
        "release_gate": {
            "deps.fastapi_uvicorn": "PASS",
            "server.bind_config": "PASS" if STATE.server_port in (5000, 8000) else "WARN",
            "process.liveness": "PASS",
            "data.freshness": "PASS" if STATE.health == "GREEN" else ("WARN" if STATE.health == "YELLOW" else "FAIL"),
        },
    }
    return JSONResponse(payload)


@app.get("/telemetry.json")
async def telemetry_json() -> JSONResponse:
    _set_health_from_state()
    payload = {
        "ts": _now(),
        "service": STATE.service,
        "build": STATE.build,
        "health": STATE.health,
        "connector": {
            "status": CONNECTOR.status,
            "frames_total": CONNECTOR.frames_total,
            "last_event_ts": CONNECTOR.last_event_ts,
            "last_ack_ts": CONNECTOR.last_ack_ts,
            "reconnects": CONNECTOR.reconnects,
        },
    }
    return JSONResponse(payload)


@app.get("/")
async def root() -> HTMLResponse:
    _set_health_from_state()

    health_color = {
        "GREEN": "#22c55e",
        "YELLOW": "#f59e0b",
        "RED": "#ef4444",
    }.get(STATE.health, "#f59e0b")

    uptime = round(_now() - STATE.server_started_ts, 1)
    last_event_age = _age(CONNECTOR.last_event_ts)
    last_event_age_txt = "—" if last_event_age is None else f"{last_event_age:.2f}s"

    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>QuantDesk Bookmap — Status</title>
  <style>
    :root {{ --fg:#111827; --muted:#6b7280; --card:#ffffff; --bg:#f3f4f6; }}
    body {{ margin:0; font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif; background:var(--bg); color:var(--fg); }}
    .wrap {{ max-width: 920px; margin: 28px auto; padding: 0 14px; }}
    .title {{ font-size: 24px; font-weight: 700; margin-bottom: 6px; }}
    .subtitle {{ color: var(--muted); margin-bottom: 14px; }}
    .row {{ display:flex; gap:14px; flex-wrap:wrap; }}
    .badge {{ display:inline-block; padding:6px 10px; border-radius: 999px; font-weight:700; color:white; background:{health_color}; }}
    .card {{ background:var(--card); border-radius: 14px; padding: 14px; box-shadow: 0 1px 8px rgba(0,0,0,.06); min-width: 240px; flex:1; }}
    .k {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
    .v {{ font-size: 18px; font-weight: 700; margin-top: 3px; }}
    .links a {{ margin-right: 10px; }}
    table {{ width:100%; border-collapse:collapse; margin-top: 10px; }}
    th, td {{ text-align:left; padding: 10px 8px; border-bottom: 1px solid #e5e7eb; }}
    th {{ color: var(--muted); font-weight:600; font-size: 13px; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"title\">QuantDesk Bookmap — Status</div>
    <div class=\"subtitle\">Service: {STATE.service} · Subsystem: {STATE.subsystem} · Build: {STATE.build} · Date: {STATE.date}</div>

    <div class=\"row\" style=\"align-items:center; margin-bottom: 10px;\">
      <span class=\"badge\">HEALTH: {STATE.health}</span>
      <span class=\"links\" style=\"margin-left:10px\">
        <a href=\"/health.json\" target=\"_blank\">health.json</a>
        <a href=\"/telemetry.json\" target=\"_blank\">telemetry.json</a>
      </span>
    </div>

    <div class=\"row\">
      <div class=\"card\">
        <div class=\"k\">Server</div>
        <div class=\"v\">{STATE.server_host}:{STATE.server_port}</div>
        <div class=\"k\" style=\"margin-top:10px\">Uptime</div>
        <div class=\"v\">{uptime}s</div>
      </div>
      <div class=\"card\">
        <div class=\"k\">Connector</div>
        <div class=\"v\">{CONNECTOR.status}</div>
        <div class=\"k\" style=\"margin-top:10px\">WS</div>
        <div class=\"v\" style=\"font-size:13px; font-weight:600\">{CONNECTOR.ws_url}</div>
        <div class=\"k\" style=\"margin-top:10px\">Last event</div>
        <div class=\"v\">{last_event_age_txt}</div>
      </div>
    </div>

    <div class=\"card\" style=\"margin-top:14px\">
      <div class=\"k\">Notes</div>
      <div style=\"margin-top:6px\">{STATE.notes}</div>
    </div>

    <div class=\"card\" style=\"margin-top:14px\">
      <div class=\"k\">Release Gate Summary</div>
      <table>
        <thead><tr><th>Gate</th><th>Status</th><th>Detail</th></tr></thead>
        <tbody>
          <tr><td>deps.fastapi_uvicorn</td><td><b>PASS</b></td><td>FastAPI and Uvicorn imported.</td></tr>
          <tr><td>server.bind_config</td><td><b>PASS</b></td><td>Host/port configured: {STATE.server_host}:{STATE.server_port}</td></tr>
          <tr><td>process.liveness</td><td><b>PASS</b></td><td>Uptime {uptime}s</td></tr>
          <tr><td>data.freshness</td><td><b>{'PASS' if STATE.health=='GREEN' else 'WARN' if STATE.health=='YELLOW' else 'FAIL'}</b></td>
              <td>Connector={CONNECTOR.status}. Frames={CONNECTOR.frames_total}. Reconnects={CONNECTOR.reconnects}.</td></tr>
        </tbody>
      </table>
    </div>

  </div>

  <script>
    // Auto-refresh minimal status indicators without noisy logging.
    setInterval(async () => {{
      try {{
        const r = await fetch('/health.json', {{cache:'no-store'}});
        const j = await r.json();
        const badge = document.querySelector('.badge');
        if (!badge) return;
        badge.textContent = 'HEALTH: ' + j.health;
      }} catch (e) {{ /* ignore */ }}
    }}, 1000);
  </script>
</body>
</html>"""

    return HTMLResponse(html)


# ---------------------------
# Entrypoint
# ---------------------------


def main() -> None:
    import uvicorn

    # Running via python current_fix.py should always work.
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
