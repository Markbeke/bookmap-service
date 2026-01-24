"""
QuantDesk Bookmap — CURRENT FIX ENTRYPOINT
CURRENT: FIX02 / STREAM / WS_INGEST (P01)
Date: 2026-01-24

Purpose (FIX02/P01):
- Provide a single stable entrypoint file for Replit (Procfile/.replit can point here forever).
- Surface an unambiguous build identifier in the UI so we can prove what is running.
- Keep connector stubbed (no real WS yet). Real market data starts in FIX02/P02+.

Workflow note:
- Keep archived, immutable FIX files (e.g., QD_BOOKMAP_*_FIX02_*.py) for rollback evidence.
- This file ("current_fix.py") may be overwritten each FIX as the stable entrypoint.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn


# ---------------------------
# Build / runtime metadata
# ---------------------------

APP_START_TS = time.time()

@dataclass(frozen=True)
class FixMeta:
    service: str = "qd-bookmap"
    fix: str = "FIX02"
    phase: str = "P01"
    subsystem: str = "STREAM / WS_INGEST"
    date: str = "2026-01-24"

    # Health semantics for FIX02/P01:
    # - YELLOW is expected because we have no live feed yet.
    health: str = "YELLOW"
    connector: str = "DISCONNECTED"
    notes: str = "FIX02/P01: connector stubbed. Live market data begins in FIX02/P02+."


META = FixMeta()

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))  # Replit commonly injects PORT


def _uptime_sec() -> float:
    return round(time.time() - APP_START_TS, 1)


def _release_gate_summary() -> Dict[str, Dict[str, str]]:
    """
    Minimal gating surface (kept intentionally small in FIX02/P01).
    """
    return {
        "deps.fastapi_uvicorn": {
            "status": "PASS",
            "detail": "FastAPI and Uvicorn import OK.",
        },
        "server.bind_config": {
            "status": "PASS",
            "detail": f"Host/port configured: {HOST}:{PORT}",
        },
        "process.liveness": {
            "status": "PASS",
            "detail": f"Uptime {_uptime_sec()}s",
        },
        "data.freshness": {
            "status": "WARN",
            "detail": "Connector=DISCONNECTED. Real feed begins in FIX02/P02+.",
        },
    }


# ---------------------------
# FastAPI app
# ---------------------------

app = FastAPI(title="QuantDesk Bookmap", version=f"{META.fix}/{META.phase}")


@app.get("/", response_class=HTMLResponse)
def status_page() -> str:
    # Simple HTML (no external assets) to keep deployment friction minimal.
    gates = _release_gate_summary()
    rows = "\n".join(
        f"<tr><td>{k}</td><td><b>{v['status']}</b></td><td>{v['detail']}</td></tr>"
        for k, v in gates.items()
    )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap — Status</title>
  <style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin: 24px; }}
    .pill {{ display:inline-block; padding:6px 10px; border-radius:999px; font-weight:700; background:#f2c94c; }}
    .grid {{ display:grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 14px; }}
    .card {{ border:1px solid #e5e7eb; border-radius: 12px; padding: 14px; }}
    table {{ width:100%; border-collapse: collapse; margin-top: 10px; }}
    th, td {{ border-top:1px solid #e5e7eb; padding:10px; text-align:left; }}
    th {{ background:#f9fafb; }}
    .muted {{ color:#6b7280; }}
    code {{ background:#f3f4f6; padding:2px 6px; border-radius:6px; }}
  </style>
</head>
<body>
  <h1>QuantDesk Bookmap — Status</h1>
  <div class="muted">Service: <code>{META.service}</code> · Subsystem: <code>{META.subsystem}</code> · Build: <code>{META.fix}/{META.phase}</code> · Date: <code>{META.date}</code></div>

  <div style="margin-top: 12px;">
    <span class="pill">HEALTH: {META.health}</span>
    <a style="margin-left:10px;" href="/health.json">health.json</a>
    <a style="margin-left:10px;" href="/telemetry.json">telemetry.json</a>
  </div>

  <div class="grid">
    <div class="card">
      <div class="muted">Server</div>
      <div style="font-size:20px; font-weight:700;">{HOST}:{PORT}</div>
      <div class="muted">Uptime: {_uptime_sec()}s</div>
    </div>

    <div class="card">
      <div class="muted">Connector</div>
      <div style="font-size:20px; font-weight:700;">{META.connector}</div>
      <div class="muted">Last internal event: 1.0s ago</div>
    </div>
  </div>

  <div class="card" style="margin-top: 14px;">
    <div class="muted">Notes</div>
    <div>{META.notes}</div>
  </div>

  <h2 style="margin-top: 18px;">Release Gate Summary</h2>
  <table>
    <thead><tr><th>Gate</th><th>Status</th><th>Detail</th></tr></thead>
    <tbody>
      {rows}
    </tbody>
  </table>

  <div class="muted" style="margin-top: 10px;">
    Expected FIX02/P01 outcome: process stays running, status page loads, health remains YELLOW (by design) until FIX02/P02+ starts live market data.
  </div>
</body>
</html>
"""


@app.get("/health.json")
def health_json() -> JSONResponse:
    return JSONResponse(
        {
            "service": META.service,
            "fix": META.fix,
            "phase": META.phase,
            "subsystem": META.subsystem,
            "date": META.date,
            "health": META.health,
            "connector": META.connector,
            "uptime_sec": _uptime_sec(),
            "notes": META.notes,
            "release_gate_summary": _release_gate_summary(),
        }
    )


@app.get("/telemetry.json")
def telemetry_json() -> JSONResponse:
    # FIX02/P01 telemetry is intentionally minimal; it becomes meaningful once WS ingestion exists.
    return JSONResponse(
        {
            "ts_unix": time.time(),
            "uptime_sec": _uptime_sec(),
            "connector": {
                "state": META.connector,
                "msg_rate_per_sec": 0.0,
                "last_msg_age_sec": None,
                "gaps": 0,
                "reconnects": 0,
            },
            "pipeline": {
                "queue_depth": 0,
                "drops": 0,
            },
        }
    )


def main() -> None:
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
