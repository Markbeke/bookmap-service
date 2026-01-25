#!/usr/bin/env python3
# =========================================================
# QuantDesk Bookmap Service — FIX16_INTERACTIVE_SCROLL_ZOOM
#
# Builds on FIX14 (parity-safe incremental book):
# - Maintains canonical CLOB via incremental merge (no side wipeouts)
# - Adds Price Travel engine: moving anchored price window (mid/last trade)
# - Corrects price-axis orientation: higher prices rendered at the TOP
# - Adds minimal time-window overlay (anchor history) to make motion visible
#
# Still NOT included (intentional):
# - Heatmap bands, zoom/pan UX, replay determinism, footprints
#
# Replit runtime contract:
# - Single entrypoint: current_fix.py
# - FastAPI on 0.0.0.0:5000
# - Always stays up even if WS feed fails
# - Endpoints:
#     /              (UI: render-bridge demo)
#     /health.json   (health summary)
#     /telemetry.json(telemetry snapshot)
#     /book.json     (canonical book summary)
#     /tape.json     (recent trades)
#     /render.json   (single render frame)
#     /render.ws     (continuous render frames)
#     /raw.json      (last raw WS message excerpt)
#     /bootlog.txt   (boot log)
# =========================================================

import asyncio
import json
import math
import os
import time
from dataclasses import dataclass, field
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# NOTE: We rely on `websockets` for WS client.
import websockets  # type: ignore

SERVICE = "quantdesk-bookmap-ui"
BUILD = "FIX16/P02"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "5000"))

SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT")
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")

# Render controls (minimal)
RENDER_FPS = float(os.environ.get("QD_RENDER_FPS", "8"))
RENDER_LEVELS = int(os.environ.get("QD_LEVELS", "140"))   # visible rows
RENDER_STEP = float(os.environ.get("QD_STEP", "0.1"))      # price tick for ladder rows
ANCHOR_ALPHA = float(os.environ.get("QD_ANCHOR_ALPHA", "0.25"))  # smoothing factor
ANCHOR_HISTORY_SEC = float(os.environ.get("QD_ANCHOR_HISTORY_SEC", "90"))  # overlay window seconds

# Book bounds (avoid unbounded growth)
MAX_LEVELS_PER_SIDE = int(os.environ.get("QD_MAX_LEVELS_PER_SIDE", "1200"))

BOOTLOG_PATH = "/tmp/qd_bootlog.txt"


def _now() -> float:
    return time.time()


def bootlog(msg: str) -> None:
    try:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        line = f"[{ts}Z] {msg}"
        with open(BOOTLOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        # Never crash because of logging
        pass


def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _as_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


# ----------------------------
# Canonical Book + Tape
# ----------------------------

@dataclass
class Book:
    bids: Dict[float, float] = field(default_factory=dict)  # price -> qty
    asks: Dict[float, float] = field(default_factory=dict)  # price -> qty
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_qty_total: float = 0.0
    ask_qty_total: float = 0.0
    version: int = 0
    last_update_ts: float = 0.0

    def recompute(self) -> None:
        self.best_bid = max(self.bids) if self.bids else None
        self.best_ask = min(self.asks) if self.asks else None
        self.bid_qty_total = float(sum(self.bids.values())) if self.bids else 0.0
        self.ask_qty_total = float(sum(self.asks.values())) if self.asks else 0.0
        # version is used as a monotonic local counter (not exchange seq)
        self.version += 1
        self.last_update_ts = _now()


@dataclass
class Trade:
    ts: float
    px: float
    qty: float
    side: str  # "buy" | "sell" | "unknown"


@dataclass
class State:
    status: str = "INIT"  # INIT | CONNECTING | CONNECTED | ERROR
    last_error: Optional[str] = None
    reconnects: int = 0
    frames: int = 0
    last_event_ts: Optional[float] = None

    # Canonical state
    book: Book = field(default_factory=Book)
    trades: Deque[Trade] = field(default_factory=lambda: deque(maxlen=500))

    # Render anchor + history (time window)
    anchor_px: Optional[float] = None
    anchor_hist: Deque[Tuple[float, float]] = field(default_factory=lambda: deque(maxlen=5000))

    # Debug
    last_raw: Optional[Dict[str, Any]] = None

    # Counters
    depth_msgs: int = 0
    trade_msgs: int = 0
    depth_updates_applied: int = 0
    snapshots_seen: int = 0


STATE = State()


def _mid_px() -> Optional[float]:
    bb = STATE.book.best_bid
    ba = STATE.book.best_ask
    if bb is not None and ba is not None:
        return (bb + ba) / 2.0
    # fallback to last trade
    if STATE.trades:
        return STATE.trades[-1].px
    return bb or ba


def _update_anchor() -> Optional[float]:
    """Price-travel anchor: smoothed mid (or last trade)."""
    target = _mid_px()
    if target is None:
        return STATE.anchor_px
    if STATE.anchor_px is None:
        STATE.anchor_px = float(target)
    else:
        # Exponential smoothing to reduce jitter while still allowing travel
        a = max(0.02, min(0.95, ANCHOR_ALPHA))
        STATE.anchor_px = (1.0 - a) * STATE.anchor_px + a * float(target)

    # Maintain history (time window)
    ts = _now()
    STATE.anchor_hist.append((ts, float(STATE.anchor_px)))
    # Trim old points (by time)
    cutoff = ts - max(10.0, ANCHOR_HISTORY_SEC)
    while STATE.anchor_hist and STATE.anchor_hist[0][0] < cutoff:
        STATE.anchor_hist.popleft()
    return STATE.anchor_px


def _apply_side_updates(side: Dict[float, float], raw_levels: Any) -> int:
    """Apply incremental updates to a side dict.

    For each level:
      - qty <= 0 => delete level
      - qty > 0  => set level
    Returns number of levels touched.
    """
    if raw_levels is None:
        return 0
    touched = 0

    # normalize to iterable
    if isinstance(raw_levels, dict):
        # sometimes exchanges send {price: qty, ...}
        items = list(raw_levels.items())
    else:
        items = raw_levels

    if not isinstance(items, (list, tuple)):
        return 0

    for lvl in items:
        px = None
        qty = None
        if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            px = _as_float(lvl[0])
            qty = _as_float(lvl[1])
        elif isinstance(lvl, dict):
            px = _as_float(lvl.get("price") or lvl.get("p"))
            qty = _as_float(lvl.get("quantity") or lvl.get("q"))
        if px is None or qty is None:
            continue
        touched += 1
        if qty <= 0:
            side.pop(px, None)
        else:
            side[px] = qty
    return touched


def _trim_book() -> None:
    """Bound book size to avoid unbounded growth if we only merge updates."""
    n = max(200, MAX_LEVELS_PER_SIDE)
    if len(STATE.book.bids) > n:
        # keep top-n highest bids
        keep = dict(sorted(STATE.book.bids.items(), key=lambda kv: kv[0], reverse=True)[:n])
        STATE.book.bids = keep
    if len(STATE.book.asks) > n:
        # keep top-n lowest asks
        keep = dict(sorted(STATE.book.asks.items(), key=lambda kv: kv[0])[:n])
        STATE.book.asks = keep


# ----------------------------
# MEXC WS parsing
# ----------------------------

def _handle_depth(obj: Dict[str, Any], ts: float) -> bool:
    """Parity-safe incremental depth merge.

    FIX14 failure cause was side wipeout when messages were partial.
    FIX15 keeps the FIX14 rules:
      - If a message omits bids or asks, that side is NOT cleared.
      - qty <= 0 deletes
      - qty > 0 sets
    """
    try:
        data = obj.get("data", obj)
        bids_raw = data.get("bids") if isinstance(data, dict) else None
        asks_raw = data.get("asks") if isinstance(data, dict) else None

        touched = 0
        if bids_raw is not None:
            touched += _apply_side_updates(STATE.book.bids, bids_raw)
        if asks_raw is not None:
            touched += _apply_side_updates(STATE.book.asks, asks_raw)

        if touched <= 0:
            return False

        STATE.depth_updates_applied += touched
        STATE.depth_msgs += 1
        _trim_book()
        STATE.book.recompute()
        STATE.last_event_ts = ts
        return True
    except Exception as e:
        STATE.last_error = f"depth_parse_error: {e}"
        return False


def _extract_trade_side(d: Dict[str, Any]) -> str:
    # Known variants:
    # - "S": 1 buy, 2 sell (common)
    # - "side": "buy"/"sell"
    # - "T"/"m" maker-taker flags; we avoid guessing if unclear
    s = d.get("S") if isinstance(d, dict) else None
    if s in (1, "1", "buy", "BUY"):
        return "buy"
    if s in (2, "2", "sell", "SELL"):
        return "sell"
    side = d.get("side") if isinstance(d, dict) else None
    if isinstance(side, str):
        side_l = side.lower()
        if "buy" in side_l:
            return "buy"
        if "sell" in side_l:
            return "sell"
    return "unknown"


def _handle_trades(obj: Dict[str, Any], ts: float) -> int:
    """Parse trade push messages; store in tape."""
    try:
        data = obj.get("data")
        if data is None:
            return 0

        events: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            events = [data]
        elif isinstance(data, list):
            events = [x for x in data if isinstance(x, dict)]
        else:
            return 0

        added = 0
        for d in events:
            px = _as_float(d.get("p") or d.get("price"))
            qty = _as_float(d.get("v") or d.get("qty") or d.get("volume"))
            if px is None or qty is None:
                continue
            side = _extract_trade_side(d)
            STATE.trades.append(Trade(ts=ts, px=float(px), qty=float(qty), side=side))
            added += 1

        if added:
            STATE.trade_msgs += 1
            STATE.last_event_ts = ts
        return added
    except Exception as e:
        STATE.last_error = f"trade_parse_error: {e}"
        return 0


def _route_message(obj: Dict[str, Any]) -> None:
    # Persist last raw for debugging (small excerpt)
    try:
        STATE.last_raw = {"ts": _now(), "obj": obj}
    except Exception:
        STATE.last_raw = None

    ts = _now()
    method = obj.get("channel") or obj.get("c") or obj.get("method") or obj.get("event")

    # MEXC contract WS often uses:
    # - "push.depth" or "depth" style channels
    # - "push.deal"  for trades
    if isinstance(method, str):
        m = method.lower()
    else:
        m = ""

    if "depth" in m:
        _handle_depth(obj, ts)
        return
    if "deal" in m or "trade" in m:
        _handle_trades(obj, ts)
        return

    # Some messages may carry data without channel hints; attempt depth heuristic
    data = obj.get("data")
    if isinstance(data, dict) and ("bids" in data or "asks" in data):
        _handle_depth(obj, ts)
        return


# ----------------------------
# Connector loop (resilient)
# ----------------------------

async def _connector_loop() -> None:
    backoff = 1.0
    while True:
        try:
            STATE.status = "CONNECTING"
            STATE.last_error = None
            bootlog(f"CONNECTING ws={WS_URL} symbol={SYMBOL}")

            async with websockets.connect(WS_URL, ping_interval=15, ping_timeout=15, close_timeout=5) as ws:
                # Subscribe to depth + trades
                sub_depth = {
                    "method": "sub.depth",
                    "param": {"symbol": SYMBOL, "depth": 200},
                }
                sub_trade = {
                    "method": "sub.deal",
                    "param": {"symbol": SYMBOL},
                }
                await ws.send(json.dumps(sub_depth))
                await ws.send(json.dumps(sub_trade))

                STATE.status = "CONNECTED"
                backoff = 1.0
                bootlog("CONNECTED")

                async for msg in ws:
                    try:
                        obj = json.loads(msg)
                    except Exception:
                        continue
                    _route_message(obj)

        except Exception as e:
            STATE.status = "ERROR"
            STATE.last_error = str(e)
            STATE.reconnects += 1
            bootlog(f"WS ERROR: {e} (reconnects={STATE.reconnects})")

            # bounded exponential backoff with jitter
            await asyncio.sleep(backoff + (0.1 * backoff * (0.5 - (time.time() % 1.0))))
            backoff = min(backoff * 1.8, 20.0)


# ----------------------------
# Render frame (bridge contract)
# ----------------------------

def _render_frame(levels: int, step: float, pan_ticks: float = 0.0) -> Dict[str, Any]:
    ts = _now()
    anchor = _update_anchor()
    view_anchor = None
    if anchor is not None:
        # pan_ticks shifts the view window in units of current step
        try:
            view_anchor = float(anchor) + float(pan_ticks) * float(step)
        except Exception:
            view_anchor = float(anchor)
    bb = STATE.book.best_bid
    ba = STATE.book.best_ask

    # Determine ladder around anchor
    prices: List[float] = []
    bid_qty: List[float] = []
    ask_qty: List[float] = []

    if view_anchor is not None and step > 0 and levels > 0:
        center = round(view_anchor / step) * step
        half = levels // 2
        start = center - half * step

        # NOTE: prices are ascending low->high; UI will map high->top (axis fix)
        for i in range(levels):
            px = round(start + i * step, 10)
            prices.append(px)
            bid_qty.append(float(STATE.book.bids.get(px, 0.0)))
            ask_qty.append(float(STATE.book.asks.get(px, 0.0)))

    # Trades (last N)
    tape = [{"ts": t.ts, "px": t.px, "qty": t.qty, "side": t.side} for t in list(STATE.trades)[-60:]]

    # Anchor history for time window overlay
    hist = [{"ts": hts, "px": hpx} for (hts, hpx) in list(STATE.anchor_hist)[-900:]]

    last_event_age = None
    if STATE.last_event_ts is not None:
        last_event_age = ts - STATE.last_event_ts

    # Health gating:
    # - GREEN only if connected AND fresh data AND parity ok
    # - YELLOW if warming / no frames yet / not connected
    # - RED if connector ERROR
    parity_ok = (bb is not None) and (ba is not None) and (len(STATE.book.bids) > 0) and (len(STATE.book.asks) > 0)
    fresh = (STATE.last_event_ts is not None) and ((ts - STATE.last_event_ts) <= 5.0)
    if STATE.status == "ERROR":
        health = "RED"
    elif STATE.status == "CONNECTED" and fresh and parity_ok:
        health = "GREEN"
    else:
        health = "YELLOW"

    return {
        "ts": ts,
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "ws_url": WS_URL,
        "health": health,
        "connector": {
            "status": STATE.status,
            "frames": STATE.frames,
            "reconnects": STATE.reconnects,
            "last_error": STATE.last_error,
            "last_event_age_s": last_event_age,
            "depth_msgs": STATE.depth_msgs,
            "trade_msgs": STATE.trade_msgs,
            "depth_updates_applied": STATE.depth_updates_applied,
        },
        "book": {
            "best_bid": bb,
            "best_ask": ba,
            "version": STATE.book.version,
            "depth_counts": {"bids": len(STATE.book.bids), "asks": len(STATE.book.asks)},
            "totals": {"bid_qty": STATE.book.bid_qty_total, "ask_qty": STATE.book.ask_qty_total},
            "last_update_ts": STATE.book.last_update_ts,
        },
        "tape": {"items": tape},
        "render": {
            "levels": levels,
            "step": step,
            "anchor_px": view_anchor,
            "base_step": RENDER_STEP,
            "pan_ticks": pan_ticks,
            "prices": prices,
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
            "anchor_hist": hist,
        },
    }


# ----------------------------
# FastAPI app
# ----------------------------

app = FastAPI(title="QuantDesk Bookmap", version=BUILD)


@app.on_event("startup")
async def _startup() -> None:
    bootlog(f"Starting {SERVICE} {BUILD} on {HOST}:{PORT}")
    asyncio.create_task(_connector_loop())


@app.get("/bootlog.txt")
async def bootlog_txt() -> PlainTextResponse:
    try:
        with open(BOOTLOG_PATH, "r", encoding="utf-8") as f:
            return PlainTextResponse(f.read())
    except Exception:
        return PlainTextResponse("bootlog unavailable", status_code=404)


@app.get("/health.json")
async def health_json() -> JSONResponse:
    frame = _render_frame(RENDER_LEVELS, RENDER_STEP, pan_ticks=0.0)
    return JSONResponse({"service": SERVICE, "build": BUILD, "health": frame["health"], "connector": frame["connector"], "book": frame["book"]})


@app.get("/telemetry.json")
async def telemetry_json() -> JSONResponse:
    frame = _render_frame(RENDER_LEVELS, RENDER_STEP, pan_ticks=0.0)
    return JSONResponse(frame)


@app.get("/book.json")
async def book_json() -> JSONResponse:
    b = STATE.book
    return JSONResponse({
        "ts": _now(),
        "build": BUILD,
        "symbol": SYMBOL,
        "best_bid": b.best_bid,
        "best_ask": b.best_ask,
        "depth_counts": {"bids": len(b.bids), "asks": len(b.asks)},
        "totals": {"bid_qty": b.bid_qty_total, "ask_qty": b.ask_qty_total},
        "version": b.version,
        "last_update_ts": b.last_update_ts,
    })


@app.get("/tape.json")
async def tape_json() -> JSONResponse:
    return JSONResponse({
        "ts": _now(),
        "build": BUILD,
        "symbol": SYMBOL,
        "trades_seen": len(STATE.trades),
        "items": [{"ts": t.ts, "px": t.px, "qty": t.qty, "side": t.side} for t in list(STATE.trades)[-120:]],
    })


@app.get("/raw.json")
async def raw_json() -> JSONResponse:
    return JSONResponse({"ts": _now(), "build": BUILD, "last_raw": STATE.last_raw})


@app.get("/render.json")
async def render_json() -> JSONResponse:
    frame = _render_frame(RENDER_LEVELS, RENDER_STEP, pan_ticks=0.0)
    return JSONResponse(frame["render"])


@app.websocket("/render.ws")
async def render_ws(ws: WebSocket) -> None:
    await ws.accept()
    # Per-connection interactive view state
    view_levels = int(RENDER_LEVELS)
    view_step = float(RENDER_STEP)
    pan_ticks = 0.0
    try:
        interval = 1.0 / max(1.0, float(RENDER_FPS))
        while True:
            # Non-blocking receive: apply client view updates if present
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=0.0)
            except asyncio.TimeoutError:
                msg = None
            except Exception:
                return

            if msg:
                try:
                    payload = json.loads(msg)
                    if isinstance(payload, dict) and payload.get("cmd") == "set_view":
                        if "levels" in payload:
                            lv = int(payload["levels"])
                            if 40 <= lv <= 800:
                                view_levels = lv
                        if "step" in payload:
                            st = float(payload["step"])
                            # Clamp to avoid degenerate windows
                            if st > 0:
                                view_step = max(1e-6, min(st, 1e6))
                        if "pan_ticks" in payload:
                            pt = float(payload["pan_ticks"])
                            # Clamp panning to a bounded range to prevent runaway
                            pan_ticks = max(-2000.0, min(pt, 2000.0))
                except Exception:
                    # ignore malformed control messages
                    pass

            frame = _render_frame(view_levels, view_step, pan_ticks=pan_ticks)
            STATE.frames += 1
            await ws.send_text(json.dumps(frame))
            await asyncio.sleep(interval)
    except Exception:
        # client disconnected or send failure
        return


def _ui_html() -> str:
    # Minimal bridge demo:
    # - Canvas ladder view
    # - Price axis corrected: higher prices at top
    # - Time-window overlay: anchor history line
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }}
    .top {{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
    .pill {{ padding:6px 10px; border-radius:999px; border:1px solid #ddd; font-weight:600; }}
    .pill.green {{ border-color:#2e7d32; color:#2e7d32; }}
    .pill.yellow {{ border-color:#f9a825; color:#8a6d00; }}
    .pill.red {{ border-color:#c62828; color:#c62828; }}
    .muted {{ color:#666; }}
    .grid {{ display:grid; grid-template-columns: 1fr; gap:14px; margin-top:16px; max-width: 1100px; }}
    @media (min-width: 980px) {{
      .grid {{ grid-template-columns: 1.2fr 0.8fr; }}
    }}
    .card {{ border:1px solid #eee; border-radius:12px; padding:12px; box-shadow: 0 1px 8px rgba(0,0,0,0.04); }}
    canvas {{ width:100%; height:520px; border:1px solid #eee; border-radius:12px; background:#fff; }}
    .small {{ font-size: 12px; }}
    a {{ color: inherit; }}
    .endpoints a {{ display:inline-block; margin-right:12px; }}
    pre {{ white-space: pre-wrap; word-break: break-word; }}
  </style>
</head>
<body>
  <h1>QuantDesk Bookmap</h1>
  <div class="top">
    <div id="health" class="pill yellow">HEALTH: YELLOW</div>
    <div><b>Build:</b> {BUILD}</div>
    <div><b>Symbol:</b> {SYMBOL}</div>
    <div class="muted small">WS: {WS_URL}</div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="muted">This page is a <b>render bridge contract demo</b>. It streams <code>/render.ws</code> frames and draws a moving anchored ladder (price travel) with interactive pan/zoom (FIX16). It is not a full Bookmap renderer yet.</div>
      <div style="height:10px"></div>
      <canvas id="cv" width="980" height="520"></canvas>
      <div class="muted small" style="margin-top:8px;">Ladder: green = bids, red = asks. Axis: higher prices at top. Mid line shows anchor. Wheel/pinch = zoom, drag = pan.</div>
    </div>

    <div class="card">
      <div><b>Endpoints</b></div>
      <div class="endpoints small" style="margin-top:6px;">
        <a href="/health.json">/health.json</a>
        <a href="/telemetry.json">/telemetry.json</a>
        <a href="/book.json">/book.json</a>
        <a href="/tape.json">/tape.json</a>
        <a href="/render.json">/render.json</a>
        <a href="/raw.json">/raw.json</a>
        <a href="/bootlog.txt">/bootlog.txt</a>
      </div>
      <div style="height:10px"></div>
      <div class="muted small"><b>Status</b> (auto-refresh)</div>
      <pre id="status" class="small"></pre>
    </div>
  </div>

<script>
(() => {{
  const cv = document.getElementById('cv');
  const ctx = cv.getContext('2d');
  const healthEl = document.getElementById('health');
  const statusEl = document.getElementById('status');

  // Interactive view state (FIX16):
  // - wheel / pinch zoom changes step
  // - drag pans ladder in price ticks
  let view = {{ levels: null, baseStep: null, step: null, zoom: 1.0, panTicks: 0.0 }};
  let wsRef = null;

  function clamp(x, a, b) {{ return Math.max(a, Math.min(b, x)); }}

  function sendView() {{
    if (!wsRef || wsRef.readyState !== 1) return;
    const payload = {{
      cmd: "set_view",
      levels: view.levels,
      step: view.step,
      pan_ticks: view.panTicks
    }};
    try {{ wsRef.send(JSON.stringify(payload)); }} catch(e) {{}}
  }}


  function setHealth(h) {{
    healthEl.classList.remove('green','yellow','red');
    if (h === 'GREEN') healthEl.classList.add('green');
    else if (h === 'RED') healthEl.classList.add('red');
    else healthEl.classList.add('yellow');
    healthEl.textContent = 'HEALTH: ' + h;
  }}

  function fmt(x) {{
    if (x === null || x === undefined) return 'null';
    if (typeof x === 'number') {{
      if (Math.abs(x) >= 1000) return x.toFixed(1);
      return x.toFixed(2);
    }}
    return String(x);
  }}

  function draw(frame) {{
    const W = cv.width, H = cv.height;
    ctx.clearRect(0,0,W,H);

    const r = frame.render;
    // Update client-side view defaults from server frame
    if (view.levels === null) view.levels = r.levels || 220;
    if (view.baseStep === null) view.baseStep = r.base_step || r.step || 0.1;
    if (view.step === null) view.step = r.step || view.baseStep;
    const prices = r.prices || [];
    const bids = r.bid_qty || [];
    const asks = r.ask_qty || [];
    const levels = prices.length;

    // Layout
    const padL = 84;
    const padR = 18;
    const axisX = padL;
    const midX = Math.floor(W*0.52);
    const bidX0 = padL + 10;
    const bidX1 = midX - 8;
    const askX0 = midX + 8;
    const askX1 = W - padR;

    // Scaling for bars
    let maxQty = 1.0;
    for (let i=0;i<levels;i++) {{
      maxQty = Math.max(maxQty, bids[i]||0, asks[i]||0);
    }}
    const bidW = Math.max(10, bidX1 - bidX0);
    const askW = Math.max(10, askX1 - askX0);

    // Row height (fit canvas)
    const rowH = Math.max(3, Math.floor((H-20) / Math.max(1, levels)));
    const top = 10;

    // Helper: map index to Y with correct price orientation:
    // prices array is low->high, but screen Y increases downward.
    // So high price should be near top => invert index.
    function yForIndex(i) {{
      const inv = (levels - 1 - i);
      return top + inv * rowH;
    }}

    // Price labels + grid
    ctx.font = '12px system-ui, -apple-system, Segoe UI, Roboto, sans-serif';
    ctx.fillStyle = '#444';
    ctx.strokeStyle = '#eee';

    for (let i=0;i<levels;i++) {{
      const y = yForIndex(i);
      if (i % 10 === 0) {{
        // horizontal grid line
        ctx.beginPath();
        ctx.moveTo(padL, y+0.5);
        ctx.lineTo(W-padR, y+0.5);
        ctx.stroke();

        // price label
        ctx.fillText(fmt(prices[i]), 8, y + 10);
      }}
    }}

    // Draw bids/asks
    for (let i=0;i<levels;i++) {{
      const y = yForIndex(i);
      const bq = bids[i] || 0;
      const aq = asks[i] || 0;

      // bid bar
      const bw = Math.min(bidW, (bq / maxQty) * bidW);
      if (bw > 0.5) {{
        ctx.fillStyle = 'rgba(46,125,50,0.35)';
        ctx.fillRect(bidX1 - bw, y, bw, rowH-1);
      }}

      // ask bar
      const aw = Math.min(askW, (aq / maxQty) * askW);
      if (aw > 0.5) {{
        ctx.fillStyle = 'rgba(198,40,40,0.35)';
        ctx.fillRect(askX0, y, aw, rowH-1);
      }}
    }}

    // Mid/anchor line (center of ladder)
    if (r.anchor_px !== null && r.anchor_px !== undefined) {{
      // anchor index nearest to anchor_px
      const step = r.step || 0.1;
      const anchor = r.anchor_px;
      // find closest index by rounding
      const center = Math.round(anchor / step) * step;
      // find index of center in array (prices[i] == center)
      let idx = -1;
      for (let i=0;i<levels;i++) {{
        if (Math.abs((prices[i]||0) - center) < (step/2 + 1e-9)) {{ idx = i; break; }}
      }}
      if (idx >= 0) {{
        const y = yForIndex(idx) + Math.floor(rowH/2);
        ctx.strokeStyle = 'rgba(0,0,0,0.35)';
        ctx.beginPath();
        ctx.moveTo(padL, y+0.5);
        ctx.lineTo(W-padR, y+0.5);
        ctx.stroke();
        ctx.fillStyle = '#111';
        ctx.fillText('anchor ' + fmt(anchor), padL+6, y-4);
      }}
    }}

    // Time-window overlay (anchor history as a line on the far right)
    const hist = r.anchor_hist || [];
    if (hist.length >= 2 && levels > 0) {{
      // map px -> y using ladder window
      const pMin = prices[0];
      const pMax = prices[levels-1];
      const tMin = hist[0].ts;
      const tMax = hist[hist.length-1].ts;
      const x0 = W - 180;
      const x1 = W - 20;

      ctx.strokeStyle = 'rgba(0,0,0,0.25)';
      ctx.beginPath();
      for (let k=0;k<hist.length;k++) {{
        const t = hist[k].ts;
        const px = hist[k].px;
        const x = x0 + (t - tMin) / Math.max(1e-9, (tMax - tMin)) * (x1 - x0);
        // price to y: higher price -> smaller y
        const y = top + (pMax - px) / Math.max(1e-9, (pMax - pMin)) * (levels*rowH);
        if (k===0) ctx.moveTo(x,y);
        else ctx.lineTo(x,y);
      }}
      ctx.stroke();

      ctx.fillStyle = '#666';
      ctx.fillText('anchor history', x0, 18);
    }}

    // Status panel
    try {{
      statusEl.textContent = JSON.stringify({{
        service: frame.service,
        build: frame.build,
        symbol: frame.symbol,
        connector: frame.connector,
        book: frame.book,
        render: {{
          levels: r.levels,
          step: r.step,
          anchor_px: r.anchor_px
        }}
      }}, null, 2);
    }} catch(e) {{}}
  }}

  function connect() {{
    const proto = (location.protocol === 'https:') ? 'wss' : 'ws';
    const ws = new WebSocket(proto + '://' + location.host + '/render.ws');
    wsRef = ws;
    ws.onmessage = (ev) => {{
      try {{
        const frame = JSON.parse(ev.data);
        setHealth(frame.health || 'YELLOW');
        draw(frame);
      }} catch(e) {{}}
    }};

    ws.onopen = () => {{
      // Push current view to server
      if (view.baseStep !== null) {{
        view.step = view.baseStep * view.zoom;
      }}
      sendView();
    }};

    ws.onclose = () => setTimeout(connect, 600);
  }}


  // ----------------------------
  // Interaction handlers (FIX16)
  // ----------------------------
  let dragging = false;
  let lastY = 0;

  cv.addEventListener('wheel', (ev) => {{
    ev.preventDefault();
    if (view.baseStep === null) return;
    const delta = ev.deltaY;
    // Zoom: wheel up = zoom in (smaller step)
    const factor = (delta > 0) ? 1.15 : 0.87;
    view.zoom = clamp(view.zoom * factor, 0.25, 8.0);
    view.step = view.baseStep * view.zoom;
    sendView();
  }}, {{ passive: false }});

    // Pointer pan (mouse + touch) — works reliably on iPad
  let pointerActive = false;
  let pointerId = null;

  cv.addEventListener('pointerdown', (ev) => {{
    // Ensure the page does not scroll/zoom on touch interactions
    ev.preventDefault();
    pointerActive = true;
    pointerId = ev.pointerId;
    lastY = ev.clientY;
    try {{ cv.setPointerCapture(pointerId); }} catch (e) {{}}
  }}, {{ passive: false }});

  cv.addEventListener('pointermove', (ev) => {{
    if (!pointerActive) return;
    if (pointerId !== null && ev.pointerId !== pointerId) return;
    ev.preventDefault();
    const dy = ev.clientY - lastY;
    lastY = ev.clientY;
    const approxRowH = 6;
    view.panTicks = clamp(view.panTicks + (-dy / approxRowH), -2000.0, 2000.0);
    sendView();
  }}, {{ passive: false }});

  function endPointer(ev) {{
    if (pointerId !== null && ev.pointerId !== pointerId) return;
    pointerActive = false;
    pointerId = null;
    try {{ cv.releasePointerCapture(ev.pointerId); }} catch (e) {{}}
  }}

  cv.addEventListener('pointerup', endPointer, {{ passive: true }});
  cv.addEventListener('pointercancel', endPointer, {{ passive: true }});


  // Touch support (iPad): one-finger drag = pan, two-finger pinch = zoom
  let touchMode = null;
  let pinchStartDist = 0;
  let pinchStartZoom = 1.0;

  function dist(t1, t2) {{
    const dx = t1.clientX - t2.clientX;
    const dy = t1.clientY - t2.clientY;
    return Math.sqrt(dx*dx + dy*dy);
  }}

  cv.addEventListener('touchstart', (ev) => {{
    ev.preventDefault();
    if (ev.touches.length === 1) {{
      touchMode = 'pan';
      lastY = ev.touches[0].clientY;
    }} else if (ev.touches.length === 2) {{
      touchMode = 'pinch';
      pinchStartDist = dist(ev.touches[0], ev.touches[1]);
      pinchStartZoom = view.zoom;
    }}
  }}, {{ passive: false }});

  cv.addEventListener('touchmove', (ev) => {{
    if (view.baseStep === null) return;
    if (touchMode === 'pan' && ev.touches.length === 1) {{
      const y = ev.touches[0].clientY;
      const dy = y - lastY;
      lastY = y;
      const approxRowH = 6;
      view.panTicks = clamp(view.panTicks + (-dy / approxRowH), -2000.0, 2000.0);
      sendView();
    }} else if (touchMode === 'pinch' && ev.touches.length === 2) {{
      const d = dist(ev.touches[0], ev.touches[1]);
      if (pinchStartDist > 1) {{
        const ratio = pinchStartDist / d; // closer fingers => zoom in
        view.zoom = clamp(pinchStartZoom * ratio, 0.25, 8.0);
        view.step = view.baseStep * view.zoom;
        sendView();
      }}
    }}
  }}, {{ passive: true }});

  cv.addEventListener('touchend', () => {{ touchMode = null; }}, {{ passive: true }});

  connect();
}})();
</script>
</body>
</html>"""


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_ui_html())


# ----------------------------
# Entrypoint
# ----------------------------
if __name__ == "__main__":
    try:
        import uvicorn  # type: ignore
    except Exception as e:
        bootlog(f"FATAL: uvicorn import failed: {e}")
        raise

    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
