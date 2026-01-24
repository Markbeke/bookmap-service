# QD_BOOKMAP_CONNECT_FIX01_BOOTSTRAP_P01__20260124.py
# ===================================================
# QuantDesk Bookmap — FIX01 (CONNECT / BOOTSTRAP)
#
# PURPOSE (FIX01):
# - Runs reliably on Replit via GitHub -> git pull -> Run (no extra steps).
# - Starts a web server and serves a single status page.
# - Displays Release Gate Summary (PASS/FAIL) and Health (green/yellow/red).
# - Provides a connector skeleton with staleness metrics (no exchange dependency yet).
#
# WORKFLOW V1 COMPLIANCE:
# - Single .py file.
# - No multi-file patches.
# - No local install steps.
#
# NOTE:
# - This FIX does NOT implement real exchange connectivity yet.
# - It establishes stable runtime, UI endpoint, and gating scaffolding.
#
# ---------------------------------------------------

from __future__ import annotations

import os
import time
import json
import socket
import threading
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Tuple

# FastAPI / Uvicorn are commonly available on Replit templates.
# If your Replit environment does not have them, the app will show a clear red gate on stderr.
try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, PlainTextResponse
    import uvicorn
    _FASTAPI_OK = True
except Exception as e:
    FastAPI = None  # type: ignore
    HTMLResponse = None  # type: ignore
    PlainTextResponse = None  # type: ignore
    uvicorn = None  # type: ignore
    _FASTAPI_OK = False
    _FASTAPI_ERR = repr(e)

BUILD = {
    "service": "qd-bookmap",
    "fix": "FIX01",
    "patch": "P01",
    "subsystem": "CONNECT",
    "intent": "BOOTSTRAP",
    "date": "2026-01-24",
}

# --------------------------
# Health & Release Gate Model
# --------------------------

@dataclass
class GateResult:
    name: str
    status: str  # PASS/FAIL/WARN
    detail: str

@dataclass
class HealthSnapshot:
    health: str  # green/yellow/red
    since_epoch: float
    now_epoch: float
    uptime_s: float
    last_event_epoch: float
    staleness_s: float
    connector_state: str
    gates: Tuple[GateResult, ...]
    build: Dict[str, str]
    host: str
    port: int
    notes: str

class HealthEngine:
    """
    FIX01 health engine:
    - Reports liveness, server readiness, and data staleness (simulated).
    - Health colors:
        green  => all required gates PASS
        yellow => WARN present, no FAIL
        red    => any FAIL
    """
    def __init__(self) -> None:
        self.start_epoch = time.time()
        self._lock = threading.Lock()
        self._last_event_epoch = 0.0
        self._connector_state = "DISCONNECTED"
        self._notes = "FIX01 bootstrap: connectivity is stubbed; staleness is expected until FIX02/03."

    def mark_event(self) -> None:
        with self._lock:
            self._last_event_epoch = time.time()

    def set_connector_state(self, s: str) -> None:
        with self._lock:
            self._connector_state = s

    def set_notes(self, s: str) -> None:
        with self._lock:
            self._notes = s

    def snapshot(self, host: str, port: int) -> HealthSnapshot:
        now = time.time()
        with self._lock:
            last_ev = self._last_event_epoch
            conn_state = self._connector_state
            notes = self._notes

        uptime = now - self.start_epoch
        staleness = (now - last_ev) if last_ev > 0 else float("inf")

        gates = []

        # Gate 1: Runtime dependencies present
        if _FASTAPI_OK:
            gates.append(GateResult("deps.fastapi_uvicorn", "PASS", "FastAPI and Uvicorn imported."))
        else:
            gates.append(GateResult("deps.fastapi_uvicorn", "FAIL", f"FastAPI/Uvicorn import failed: {_FASTAPI_ERR}"))

        # Gate 2: Server binding readiness (host/port resolved)
        if host and isinstance(port, int) and (0 < port < 65536):
            gates.append(GateResult("server.bind_config", "PASS", f"Host/port configured: {host}:{port}"))
        else:
            gates.append(GateResult("server.bind_config", "FAIL", f"Invalid host/port: {host}:{port}"))

        # Gate 3: Liveness (uptime > 2s after start)
        if uptime >= 2.0:
            gates.append(GateResult("process.liveness", "PASS", f"Uptime {uptime:.1f}s"))
        else:
            gates.append(GateResult("process.liveness", "WARN", f"Uptime {uptime:.1f}s (warming up)"))

        # Gate 4: Data freshness (stubbed)
        # FIX01 expected state is DISCONNECTED + staleness inf => WARN, not FAIL.
        if conn_state != "CONNECTED":
            gates.append(GateResult("data.freshness", "WARN", f"Connector={conn_state}. Real feed begins in FIX02+."))
        else:
            # If connected, enforce a basic staleness threshold.
            if staleness <= 2.5:
                gates.append(GateResult("data.freshness", "PASS", f"Last event {staleness:.2f}s ago"))
            elif staleness <= 10.0:
                gates.append(GateResult("data.freshness", "WARN", f"Stale: {staleness:.2f}s"))
            else:
                gates.append(GateResult("data.freshness", "FAIL", f"Too stale: {staleness:.2f}s"))

        # Overall health color
        any_fail = any(g.status == "FAIL" for g in gates)
        any_warn = any(g.status == "WARN" for g in gates)
        if any_fail:
            health = "red"
        elif any_warn:
            health = "yellow"
        else:
            health = "green"

        return HealthSnapshot(
            health=health,
            since_epoch=self.start_epoch,
            now_epoch=now,
            uptime_s=uptime,
            last_event_epoch=last_ev,
            staleness_s=staleness,
            connector_state=conn_state,
            gates=tuple(gates),
            build={k: str(v) for k, v in BUILD.items()},
            host=host,
            port=port,
            notes=notes,
        )

# --------------------------
# Connector Skeleton (FIX01)
# --------------------------

class ConnectorStub(threading.Thread):
    """
    FIX01: stub connector to prove thread lifecycle + telemetry.
    It does NOT connect to any exchange yet.
    It flips states to show the UI updating.
    """
    def __init__(self, health: HealthEngine) -> None:
        super().__init__(daemon=True)
        self.health = health
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        # Simulate lifecycle
        self.health.set_connector_state("CONNECTING")
        time.sleep(0.8)
        self.health.set_connector_state("DISCONNECTED")
        # Periodically emit a "heartbeat event" so the UI is alive, but keep freshness WARN
        # until real connectivity arrives.
        while not self._stop.is_set():
            # Mark an internal event every 5 seconds (not "market data").
            self.health.mark_event()
            time.sleep(5.0)

# --------------------------
# App / UI
# --------------------------

def _html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def render_status_page(h: HealthSnapshot) -> str:
    # Minimal, dependency-free HTML. No external JS/CSS.
    gates_rows = []
    for g in h.gates:
        gates_rows.append(
            f"<tr>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #eee'>{_html_escape(g.name)}</td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #eee'><b>{_html_escape(g.status)}</b></td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #eee'>{_html_escape(g.detail)}</td>"
            f"</tr>"
        )
    gates_html = "\n".join(gates_rows)

    # Health badge
    if h.health == "green":
        badge_bg = "#1f9d55"
    elif h.health == "yellow":
        badge_bg = "#d69e2e"
    else:
        badge_bg = "#e53e3e"

    staleness_display = "∞" if h.staleness_s == float("inf") else f"{h.staleness_s:.2f}"

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>QuantDesk Bookmap — {h.build.get("fix","")} {h.build.get("patch","")}</title>
</head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; margin:0; background:#fafafa;">
  <div style="padding:18px 18px 10px 18px; background:white; border-bottom:1px solid #eaeaea;">
    <div style="display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap;">
      <div>
        <div style="font-size:18px; font-weight:700;">QuantDesk Bookmap — Status</div>
        <div style="font-size:12px; color:#666; margin-top:2px;">
          Service: {_html_escape(h.build.get("service",""))} • Subsystem: {_html_escape(h.build.get("subsystem",""))} •
          Build: {_html_escape(h.build.get("fix",""))}/{_html_escape(h.build.get("patch",""))} • Date: {_html_escape(h.build.get("date",""))}
        </div>
      </div>
      <div style="display:flex; align-items:center; gap:10px;">
        <div style="padding:6px 10px; border-radius:999px; background:{badge_bg}; color:white; font-weight:700; font-size:12px;">
          HEALTH: {h.health.upper()}
        </div>
        <a href="/health.json" style="font-size:12px; color:#2463eb; text-decoration:none;">health.json</a>
      </div>
    </div>

    <div style="margin-top:12px; display:flex; gap:12px; flex-wrap:wrap;">
      <div style="background:#f6f6f6; border:1px solid #ededed; border-radius:10px; padding:10px 12px; min-width:240px;">
        <div style="font-size:12px; color:#666;">Server</div>
        <div style="font-size:14px; font-weight:700; margin-top:2px;">{_html_escape(h.host)}:{h.port}</div>
        <div style="font-size:12px; color:#666; margin-top:4px;">Uptime: {h.uptime_s:.1f}s</div>
      </div>

      <div style="background:#f6f6f6; border:1px solid #ededed; border-radius:10px; padding:10px 12px; min-width:240px;">
        <div style="font-size:12px; color:#666;">Connector</div>
        <div style="font-size:14px; font-weight:700; margin-top:2px;">{_html_escape(h.connector_state)}</div>
        <div style="font-size:12px; color:#666; margin-top:4px;">Last internal event: {staleness_display}s ago</div>
      </div>

      <div style="background:#f6f6f6; border:1px solid #ededed; border-radius:10px; padding:10px 12px; min-width:240px; flex:1;">
        <div style="font-size:12px; color:#666;">Notes</div>
        <div style="font-size:13px; margin-top:4px; color:#222;">{_html_escape(h.notes)}</div>
      </div>
    </div>
  </div>

  <div style="padding:16px 18px;">
    <div style="font-size:14px; font-weight:800; margin-bottom:8px;">Release Gate Summary</div>
    <div style="background:white; border:1px solid #eaeaea; border-radius:12px; overflow:hidden;">
      <table style="border-collapse:collapse; width:100%;">
        <thead>
          <tr style="background:#fcfcfc;">
            <th style="text-align:left; padding:8px; border-bottom:1px solid #eee; font-size:12px; color:#444;">Gate</th>
            <th style="text-align:left; padding:8px; border-bottom:1px solid #eee; font-size:12px; color:#444;">Status</th>
            <th style="text-align:left; padding:8px; border-bottom:1px solid #eee; font-size:12px; color:#444;">Detail</th>
          </tr>
        </thead>
        <tbody>
          {gates_html}
        </tbody>
      </table>
    </div>

    <div style="margin-top:14px; font-size:12px; color:#666;">
      FIX01 expected outcome: server stays running, status page loads, health is typically YELLOW (freshness WARN) until real market data starts in FIX02+.
    </div>
  </div>

  <script>
    // Simple auto-refresh every 1s without external dependencies.
    setTimeout(function(){{ window.location.reload(); }}, 1000);
  </script>
</body>
</html>
"""

# --------------------------
# Server bootstrap
# --------------------------

def pick_host_port() -> Tuple[str, int]:
    # Replit often provides PORT. Default to 8000.
    # Bind host 0.0.0.0 for external access.
    host = "0.0.0.0"
    port = 8000
    try:
        env_port = os.environ.get("PORT", "").strip()
        if env_port:
            port = int(env_port)
    except Exception:
        port = 8000
    return host, port

def main() -> None:
    host, port = pick_host_port()
    health = HealthEngine()

    if not _FASTAPI_OK:
        # Minimal fallback: print an actionable stderr line and exit non-zero.
        # (User will see "stops immediately"; the reason is explicit.)
        raise SystemExit(f"[FIX01] FastAPI/Uvicorn not available: {_FASTAPI_ERR}")

    app = FastAPI()

    connector = ConnectorStub(health)
    connector.start()

    @app.get("/", response_class=HTMLResponse)
    def root() -> str:
        snap = health.snapshot(host, port)
        return render_status_page(snap)

    @app.get("/health.json")
    def health_json() -> Dict:
        snap = health.snapshot(host, port)
        # Make JSON fully serializable
        d = asdict(snap)
        d["gates"] = [asdict(g) for g in snap.gates]
        # Handle inf
        if d.get("staleness_s") == float("inf"):
            d["staleness_s"] = None
            d["staleness_is_inf"] = True
        else:
            d["staleness_is_inf"] = False
        return d

    @app.get("/robots.txt", response_class=PlainTextResponse)
    def robots() -> str:
        return "User-agent: *\nDisallow: /\n"

    # Run server
    uvicorn.run(app, host=host, port=port, log_level="warning")

if __name__ == "__main__":
    main()
