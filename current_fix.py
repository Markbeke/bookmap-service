"""
QuantDesk Bookmap Service — current_fix.py
Build: FIX04/P01
Subsystem: FOUNDATION+STREAM
Date: 2026-01-24

Purpose of FIX04/P01 (Exchange WS Endpoint Correction)
- Fix DNS failure: "gaierror: [Errno -5] No address associated with hostname"
- Replace invalid WS base host used previously with a resolvable MEXC Spot WS base.
- No other behavioral changes.

Notes / Limits (Truth Protocol)
- This FIX assumes you are using SPOT public streams (your current params: spot@public.*).
- If you later move to PERPETUALS/CONTRACT streams, the WS base will differ and must be a new FIX.
- WS base can always be overridden via env: QD_MEXC_WS_URL
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

# -----------------------------
# Config
# -----------------------------
SERVICE_NAME = "qd-bookmap"
BUILD = "FIX04/P01"
SUBSYSTEM = "FOUNDATION+STREAM"

# FIX04: corrected MEXC Spot WS base (resolvable host)
DEFAULT_WS_URL = "wss://wbs.mexc.com/ws"

# Override allowed per workflow
WS_URL = os.environ.get("QD_MEXC_WS_URL", DEFAULT_WS_URL).strip()

DEFAULT_STREAMS = [
    # Placeholders aligned with prior runs.
    # Override with env QD_MEXC_STREAMS as JSON list or comma-separated.
    "spot@public.deals.v3.api@BTCUSDT",
    "spot@public.depth.v3.api@BTCUSDT@20",
]

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", os.environ.get("QD_PORT", "5000")))

START_TS = time.time()


def _now_ts() -> float:
    return time.time()


def _load_streams() -> List[str]:
    raw = os.environ.get("QD_MEXC_STREAMS", "").strip()
    if not raw:
        return DEFAULT_STREAMS[:]
    try:
        v = json.loads(raw)
        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            out = [x.strip() for x in v if x.strip()]
            return out or DEFAULT_STREAMS[:]
    except Exception:
        pass
    parts = [p.strip() for p in raw.split(",")]
    out = [p for p in parts if p]
    return out or DEFAULT_STREAMS[:]


STREAMS = _load_streams()


def _safe_exc(e: BaseException) -> str:
    s = f"{type(e).__name__}: {str(e)}".strip()
    return s[:700]


# -----------------------------
# Dependency probes (do not crash)
# -----------------------------
def _try_imports() -> Tuple[Optional[Any], Optional[Any], Optional[Any], List[str]]:
    """
    Returns (FastAPI, responses_module, websockets_module, errors)
    Where responses_module provides HTMLResponse/JSONResponse/PlainTextResponse.
    """
    errors: List[str] = []

    fastapi_mod = None
    responses_mod = None
    websockets_mod = None

    try:
        import fastapi  # type: ignore
        from fastapi import FastAPI  # type: ignore
        from fastapi import responses as _responses  # type: ignore

        fastapi_mod = FastAPI
        responses_mod = _responses
    except Exception as e:
        errors.append(f"fastapi import failed: {_safe_exc(e)}")

    try:
        import websockets  # type: ignore

        websockets_mod = websockets
    except Exception as e:
        errors.append(f"websockets import failed: {_safe_exc(e)}")

    # uvicorn is only required for the FastAPI path; fallback server does not require it.
    try:
        import uvicorn  # type: ignore
        _ = uvicorn  # suppress unused
    except Exception as e:
        errors.append(f"uvicorn import failed: {_safe_exc(e)}")

    return fastapi_mod, responses_mod, websockets_mod, errors


FastAPI_cls, fastapi_responses, websockets, IMPORT_ERRORS = _try_imports()


# -----------------------------
# State / Telemetry
# -----------------------------
@dataclass
class ConnectorState:
    status: str = "DISCONNECTED"  # DISCONNECTED | CONNECTED | ERROR
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

    # foundation diagnostics
    import_errors: List[str] = None  # type: ignore

    def to_public(self) -> Dict[str, Any]:
        d = asdict(self)
        d["streams"] = self.streams or []
        d["import_errors"] = self.import_errors or []
        return d


STATE = ConnectorState(streams=STREAMS[:], import_errors=IMPORT_ERRORS[:])
STATE_LOCK = threading.Lock()


def _mexc_subscribe_payload(streams: List[str]) -> str:
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

        sub = _mexc_subscribe_payload(streams)
        await ws.send(sub)

        while True:
            msg = await ws.recv()
            ts = _now_ts()
            with STATE_LOCK:
                STATE.frames_total += 1
                STATE.last_event_ts = ts

            # best-effort ack signal
            if isinstance(msg, str) and (
                '"code"' in msg or '"success"' in msg or '"ack"' in msg
            ):
                with STATE_LOCK:
                    STATE.last_ack_ts = ts


async def _ws_reconnect_forever() -> None:
    if websockets is None:
        with STATE_LOCK:
            STATE.status = "ERROR"
            STATE.last_error = "Missing dependency: websockets (see import_errors)"
        return

    backoff = 0.5
    backoff_max = 20.0

    while True:
        try:
            await _ws_session(STATE.ws_url, STATE.streams or [])
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except Exception as e:
            close_code = getattr(e, "code", None)
            close_reason = getattr(e, "reason", "")
            with STATE_LOCK:
                STATE.status = "ERROR"
                STATE.last_error = _safe_exc(e)
                STATE.last_close_code = int(close_code) if isinstance(close_code, int) else None
                STATE.last_close_reason = str(close_reason)[:300]
                STATE.reconnects += 1

            jitter = random.uniform(0.0, 0.25)
            await asyncio.sleep(min(backoff + jitter, backoff_max))
            backoff = min(backoff * 1.7, backoff_max)


def _start_ws_thread() -> None:
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
# Health logic (shared)
# -----------------------------
def _health_payload() -> Dict[str, Any]:
    with STATE_LOCK:
        st = STATE.to_public()

    uptime = _now_ts() - START_TS
    last_event = st.get("last_event_ts") or 0.0
    fresh_s = _now_ts() - last_event if last_event else 1e9

    # Health rules:
    # GREEN: connected + fresh frames
    # YELLOW: server running but warming/disconnected (acceptable early)
    # RED: connector error OR foundation import errors that prevent intended operation
    import_errors = st.get("import_errors") or []

    if st["status"] == "CONNECTED" and fresh_s < 3.0 and not import_errors:
        health = "GREEN"
    elif st["status"] == "ERROR":
        health = "RED"
    elif import_errors:
        health = "RED"
    else:
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
        "host": HOST,
        "port": PORT,
    }


# -----------------------------
# FastAPI path (preferred)
# -----------------------------
def _run_fastapi() -> None:
    """
    Runs FastAPI via Uvicorn. Only called if FastAPI + Uvicorn are importable.
    """
    assert FastAPI_cls is not None
    assert fastapi_responses is not None

    HTMLResponse = fastapi_responses.HTMLResponse
    JSONResponse = fastapi_responses.JSONResponse
    PlainTextResponse = fastapi_responses.PlainTextResponse

    app = FastAPI_cls(title="QuantDesk Bookmap Service", version=BUILD)

    @app.on_event("startup")
    async def _startup() -> None:
        _start_ws_thread()

    @app.get("/health.json")
    async def health_json() -> Any:
        return JSONResponse(_health_payload())

    @app.get("/telemetry.json")
    async def telemetry_json() -> Any:
        return JSONResponse(_health_payload())

    @app.get("/", response_class=HTMLResponse)
    async def status_page() -> Any:
        h = _health_payload()
        conn = h["connector"]
        health = h["health"]
        health_color = {"GREEN": "#16a34a", "YELLOW": "#f59e0b", "RED": "#dc2626"}.get(
            health, "#6b7280"
        )

        def esc(s: Any) -> str:
            return (
                str(s)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )

        import_errs = conn.get("import_errors") or []
        import_block = ""
        if import_errs:
            items = "".join(f"<li><code>{esc(x)}</code></li>" for x in import_errs[:10])
            import_block = f"""
            <div class="card">
              <div class="muted"><b>Foundation import errors (action required)</b></div>
              <ul>{items}</ul>
              <div class="muted">If Replit previously started then stopped, these errors are the most likely root cause.</div>
            </div>
            """

        html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>QuantDesk Bookmap — Status</title>
  <style>
    body {{ font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
    .row {{ display: grid; grid-template-columns: 1fr; gap: 12px; max-width: 920px; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 14px; padding: 14px 16px; }}
    .h {{ display: flex; align-items: center; gap: 12px; flex-wrap:wrap; }}
    .pill {{ padding: 6px 10px; border-radius: 999px; color: white; font-weight: 700; background: {health_color}; }}
    .muted {{ color: #6b7280; }}
    .kv {{ display: grid; grid-template-columns: 160px 1fr; gap: 6px 12px; }}
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

    {import_block}

    <div class="card">
      <div class="kv">
        <div class="muted">Server</div><div><code>{esc(h["host"])}:{esc(h["port"])}</code> · Uptime: <code>{esc(round(h["uptime_s"],1))}s</code></div>
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
        FIX04/P01: Corrected MEXC WS base URL. If health remains RED with gaierror, it indicates runtime DNS/network restrictions, not application logic.<br/>
        WS ingest counts frames; decoding/state-engine/visualization are deferred to later FIX.<br/>
        If health is RED, open <code>/</code> and copy <code>Last error</code>.
      </div>
    </div>
  </div>
</body>
</html>
"""
        return HTMLResponse(html)

    @app.get("/robots.txt")
    async def robots() -> Any:
        return PlainTextResponse("User-agent: *\nDisallow: /\n")

    import uvicorn  # type: ignore
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


# -----------------------------
# Fallback HTTP server (keeps Replit alive)
# -----------------------------
def _run_fallback_http() -> None:
    """
    If FastAPI/Uvicorn are unavailable, keep the process alive and serve diagnostics.
    """
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: str, ctype: str = "text/plain; charset=utf-8") -> None:
            b = body.encode("utf-8", errors="replace")
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self) -> None:  # noqa: N802
            if self.path in ("/health.json", "/telemetry.json"):
                payload = _health_payload()
                self._send(200, json.dumps(payload, indent=2), "application/json; charset=utf-8")
                return

            if self.path == "/" or self.path.startswith("/?"):
                payload = _health_payload()
                errs = payload["connector"].get("import_errors") or []
                msg = [
                    "QuantDesk Bookmap — FALLBACK SERVER (FastAPI/Uvicorn not available)",
                    f"Build: {BUILD}",
                    f"Bind: {HOST}:{PORT}",
                    "",
                    "Import errors:",
                    *(f"- {e}" for e in errs),
                    "",
                    "Action:",
                    "- Ensure fastapi + uvicorn are installed in the environment.",
                    "- Then re-run with the normal path (Replit: delete workspace → re-import → Run) per workflow v1.2.",
                    "",
                    "Endpoints available in fallback:",
                    "- /health.json",
                    "- /telemetry.json",
                ]
                self._send(200, "\n".join(msg))
                return

            self._send(404, "Not found")

        def log_message(self, format: str, *args: Any) -> None:
            return

    httpd = HTTPServer((HOST, PORT), Handler)
    _start_ws_thread()
    httpd.serve_forever()


def main() -> None:
    can_fastapi = FastAPI_cls is not None and fastapi_responses is not None
    has_uvicorn_error = any(e.startswith("uvicorn import failed") for e in IMPORT_ERRORS)
    has_fastapi_error = any(e.startswith("fastapi import failed") for e in IMPORT_ERRORS)

    if can_fastapi and not has_uvicorn_error and not has_fastapi_error:
        _run_fastapi()
    else:
        with STATE_LOCK:
            STATE.status = "ERROR"
            STATE.last_error = "Foundation fallback active (missing FastAPI/Uvicorn). See import_errors."
        _run_fallback_http()


if __name__ == "__main__":
    main()
