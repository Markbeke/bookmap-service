#!/usr/bin/env python3
# =========================================================
# QuantDesk Bookmap Service — FIX17_P04_BOOKMAP_COORDINATE_PIVOT
#
# STRICT BOOKMAP PARITY PIVOT:
# - Absolute price axis (no price-centered/anchor-relative heatmap sliding)
# - Time scrolls left→right (newest at right)
# - Camera/viewport moves over a price-anchored heatmap world
# - iPad gestures:
#     * 1-finger drag: price pan (camera)
#     * pinch: price zoom (span)
#     * 2-finger horizontal drag: time scrub (history)
#     * 2-finger vertical drag: time scale (seconds per column)
#
# Backend remains parity-safe:
# - Incremental depth merge (no side wipeouts)
# - qty<=0 removes level
# - best_bid<best_ask invariant
#
# Replit runtime contract:
# - Single entrypoint: current_fix.py
# - FastAPI on 0.0.0.0:5000
# - Endpoints: /, /health.json, /telemetry.json, /book.json, /tape.json, /render.ws, /raw.json, /bootlog.txt
# =========================================================

import asyncio
import json
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

import websockets  # type: ignore

SERVICE = "quantdesk-bookmap-ui"
BUILD = "FIX17/P04"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "5000"))

SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT")
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")

# Feed config
DEPTH_N = int(os.environ.get("QD_DEPTH_N", "200"))  # per side
TAPE_MAX = int(os.environ.get("QD_TAPE_MAX", "200"))

# Render / stream config
STREAM_FPS = float(os.environ.get("QD_STREAM_FPS", "12"))  # ws snapshots to UI
RAW_EXCERPT_MAX = 1200

# Health freshness
STALE_S = float(os.environ.get("QD_STALE_S", "5.0"))

# ----------------------------
# Utilities
# ----------------------------

BOOTLOG: Deque[str] = deque(maxlen=400)

def bootlog(s: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    BOOTLOG.append(f"[{ts}Z] {s}")

def now() -> float:
    return time.time()

def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

# ----------------------------
# Canonical state
# ----------------------------

@dataclass
class Trade:
    ts: float
    price: float
    qty: float
    side: str  # "buy" or "sell"

@dataclass
class AppState:
    status: str = "BOOTING"
    started_ts: float = field(default_factory=now)

    # Canonical book: price->qty (float). We store full levels we see; UI uses top N.
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

    best_bid: float = 0.0
    best_ask: float = 0.0
    last_update_ts: float = 0.0

    # Tape
    tape: Deque[Trade] = field(default_factory=lambda: deque(maxlen=TAPE_MAX))

    # Telemetry
    ws_msgs: int = 0
    ws_reconnects: int = 0
    frames: int = 0
    errors: int = 0

    # Last raw excerpt for debugging
    last_raw: str = ""

STATE = AppState()

# ----------------------------
# Book merge (parity-safe)
# ----------------------------

def _apply_side(side: Dict[float, float], updates: List[List[Any]]) -> None:
    """
    MEXC contract depth format observed in prior FIX:
      depth updates contain lists of [price, qty]
    Rules:
      qty <= 0 -> delete
      qty > 0  -> set
    """
    for lvl in updates:
        if not isinstance(lvl, list) or len(lvl) < 2:
            continue
        p = _safe_float(lvl[0]); q = _safe_float(lvl[1])
        if p is None or q is None:
            continue
        if q <= 0:
            side.pop(p, None)
        else:
            side[p] = q

def _recompute_best() -> None:
    if STATE.bids:
        STATE.best_bid = max(STATE.bids.keys())
    else:
        STATE.best_bid = 0.0
    if STATE.asks:
        STATE.best_ask = min(STATE.asks.keys())
    else:
        STATE.best_ask = 0.0

def _book_ok() -> bool:
    if STATE.best_bid <= 0 or STATE.best_ask <= 0:
        return False
    return STATE.best_bid < STATE.best_ask

def _book_snapshot(top_n: int) -> Dict[str, Any]:
    bids = sorted(STATE.bids.items(), key=lambda x: x[0], reverse=True)[:top_n]
    asks = sorted(STATE.asks.items(), key=lambda x: x[0])[:top_n]
    bid_qty = sum(q for _, q in bids)
    ask_qty = sum(q for _, q in asks)
    return {
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "best_bid": STATE.best_bid,
        "best_ask": STATE.best_ask,
        "depth_counts": {"bids": len(bids), "asks": len(asks)},
        "totals": {"bid_qty": bid_qty, "ask_qty": ask_qty},
        "last_update_ts": STATE.last_update_ts,
        "bids": [[p, q] for p, q in bids],
        "asks": [[p, q] for p, q in asks],
    }

def _tape_snapshot() -> Dict[str, Any]:
    return {
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "tape": [{"ts": t.ts, "price": t.price, "qty": t.qty, "side": t.side} for t in list(STATE.tape)],
    }

def _estimate_tick_from_book() -> float:
    """
    Best-effort tick estimate from top-of-book levels (robust enough for UI binning).
    We compute the minimum positive price difference among top levels.
    """
    pxs: List[float] = []
    for p, _ in sorted(STATE.bids.items(), key=lambda x: x[0], reverse=True)[:30]:
        pxs.append(p)
    for p, _ in sorted(STATE.asks.items(), key=lambda x: x[0])[:30]:
        pxs.append(p)
    pxs = sorted(set(pxs))
    mind = None
    for i in range(1, len(pxs)):
        d = pxs[i] - pxs[i-1]
        if d > 0:
            if mind is None or d < mind:
                mind = d
    if mind is None or mind <= 0:
        return 0.1
    # normalize to a "nice" tick (avoid floating noise)
    # keep 1, 0.5, 0.1, 0.05, 0.01, 0.001 etc.
    # Here we round to 1e-6.
    return max(1e-6, round(mind, 6))

# ----------------------------
# WS connector
# ----------------------------

async def _ws_loop() -> None:
    backoff = 1.0
    while True:
        try:
            STATE.status = "CONNECTING"
            bootlog(f"CONNECTING {WS_URL} symbol=__SYMBOL__")
            async with websockets.connect(WS_URL, ping_interval=15, ping_timeout=15, close_timeout=5) as ws:
                # Subscribe to depth + trades (known working format from prior FIXes)
                sub_depth = {"method": "sub.depth", "param": {"symbol": SYMBOL, "depth": DEPTH_N}}
                sub_trade = {"method": "sub.deal", "param": {"symbol": SYMBOL}}
                await ws.send(json.dumps(sub_depth))
                await ws.send(json.dumps(sub_trade))

                STATE.status = "CONNECTED"
                backoff = 1.0
                bootlog("CONNECTED")

                async for msg in ws:
                    STATE.ws_msgs += 1
                    if isinstance(msg, str):
                        STATE.last_raw = msg[-RAW_EXCERPT_MAX:]
                        try:
                            obj = json.loads(msg)
                        except Exception:
                            continue
                    else:
                        continue

                    # Depth updates
                    try:
                        # Typical shape:
                        # {"channel":"push.depth","data":{"bids":[[p,q],...],"asks":[[p,q],...]...}}
                        if isinstance(obj, dict) and obj.get("channel") == "push.depth":
                            data = obj.get("data") or {}
                            bids = data.get("bids") or []
                            asks = data.get("asks") or []
                            if isinstance(bids, list):
                                _apply_side(STATE.bids, bids)
                            if isinstance(asks, list):
                                _apply_side(STATE.asks, asks)
                            _recompute_best()
                            STATE.last_update_ts = now()
                            continue
                    except Exception:
                        STATE.errors += 1

                    # Trades
                    try:
                        if isinstance(obj, dict) and obj.get("channel") == "push.deal":
                            data = obj.get("data") or []
                            # observed: list of trades; each trade dict {p,q,T, ...}
                            if isinstance(data, list):
                                for td in data[-10:]:
                                    if not isinstance(td, dict):
                                        continue
                                    p = _safe_float(td.get("p") or td.get("price"))
                                    q = _safe_float(td.get("q") or td.get("vol") or td.get("qty"))
                                    side = td.get("S") or td.get("side") or ""
                                    if p is None or q is None:
                                        continue
                                    s = "buy" if str(side).lower() in ("1", "buy", "b") else "sell"
                                    STATE.tape.append(Trade(ts=now(), price=p, qty=q, side=s))
                            continue
                    except Exception:
                        STATE.errors += 1

        except Exception as e:
            STATE.ws_reconnects += 1
            STATE.status = "RECONNECTING"
            bootlog(f"WS_ERROR: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)

# ----------------------------
# FastAPI
# ----------------------------

app = FastAPI()

@app.on_event("startup")
async def _startup() -> None:
    bootlog(f"BOOT {SERVICE} __BUILD__ PORT={PORT}")
    asyncio.create_task(_ws_loop())
    STATE.status = "WARMING"

@app.get("/bootlog.txt")
async def _bootlog_txt() -> PlainTextResponse:
    return PlainTextResponse("\n".join(list(BOOTLOG)))

@app.get("/raw.json")
async def _raw_json() -> JSONResponse:
    return JSONResponse({"service": SERVICE, "build": BUILD, "symbol": SYMBOL, "last_raw": STATE.last_raw})

@app.get("/book.json")
async def book_json() -> JSONResponse:
    return JSONResponse(_book_snapshot(DEPTH_N))

@app.get("/tape.json")
async def tape_json() -> JSONResponse:
    return JSONResponse(_tape_snapshot())

@app.get("/telemetry.json")
async def telemetry_json() -> JSONResponse:
    tick = _estimate_tick_from_book()
    return JSONResponse({
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "status": STATE.status,
        "ws_msgs": STATE.ws_msgs,
        "ws_reconnects": STATE.ws_reconnects,
        "frames": STATE.frames,
        "errors": STATE.errors,
        "best_bid": STATE.best_bid,
        "best_ask": STATE.best_ask,
        "tick_est": tick,
        "last_update_ts": STATE.last_update_ts,
        "uptime_s": now() - STATE.started_ts,
    })

@app.get("/health.json")
async def health_json() -> JSONResponse:
    fresh = (now() - STATE.last_update_ts) <= STALE_S
    ok = _book_ok()
    if STATE.status in ("CONNECTED", "WARMING") and fresh and ok:
        health = "GREEN"
    elif STATE.status in ("CONNECTED", "WARMING"):
        health = "YELLOW"
    else:
        health = "RED"
    return JSONResponse({
        "service": SERVICE,
        "build": BUILD,
        "symbol": SYMBOL,
        "health": health,
        "status": STATE.status,
        "fresh": fresh,
        "book_ok": ok,
        "best_bid": STATE.best_bid,
        "best_ask": STATE.best_ask,
        "last_update_ts": STATE.last_update_ts,
    })

# ----------------------------
# Render WS: send book snapshots for client-side Bookmap renderer
# ----------------------------

async def _ws_recv_loop(ws: WebSocket, view: Dict[str, Any]) -> None:
    """
    Receives client control messages. Server does not render, but we keep
    last view for telemetry/debug and future extensions.
    """
    while True:
        try:
            msg = await ws.receive_text()
        except Exception:
            return
        try:
            obj = json.loads(msg)
        except Exception:
            continue
        if isinstance(obj, dict) and obj.get("cmd") == "set_view":
            # store a minimal sanitized view snapshot
            view["ts"] = now()
            for k in ("center_idx","price_span_bins","time_offset_cols","time_scale_s","auto_follow"):
                if k in obj:
                    view[k] = obj[k]

@app.websocket("/render.ws")
async def render_ws(ws: WebSocket) -> None:
    await ws.accept()
    view: Dict[str, Any] = {}
    recv_task = asyncio.create_task(_ws_recv_loop(ws, view))
    interval = 1.0 / max(1.0, float(STREAM_FPS))
    try:
        while True:
            tick = _estimate_tick_from_book()
            snap = _book_snapshot(DEPTH_N)
            # enrich for UI
            snap["ts"] = now()
            snap["tick"] = tick
            # last trade
            if STATE.tape:
                lt = STATE.tape[-1]
                snap["last_trade"] = {"ts": lt.ts, "price": lt.price, "qty": lt.qty, "side": lt.side}
            else:
                snap["last_trade"] = None
            # include client view echo (debug)
            snap["client_view"] = view
            await ws.send_text(json.dumps(snap))
            STATE.frames += 1
            await asyncio.sleep(interval)
    except Exception:
        pass
    finally:
        try:
            recv_task.cancel()
        except Exception:
            pass

# ----------------------------
# UI: strict Bookmap coordinate renderer (client-side)
# ----------------------------

def _ui_html() -> str:
    html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"/>
  <title>QuantDesk Bookmap — __BUILD__</title>
  <style>
    body {{
      margin:0; padding:0;
      font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      background:#0b0f14; color:#e6eef6;
    }}
    .wrap {{ padding: 14px; max-width: 1100px; margin: 0 auto; }}
    .row {{ display:flex; gap:12px; flex-wrap: wrap; align-items: stretch; }}
    .card {{
      background:#101723;
      border:1px solid rgba(255,255,255,0.08);
      border-radius:14px;
      padding:12px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    }}
    .muted {{ color:#9bb0c7; }}
    .small {{ font-size: 12px; line-height: 1.4; }}
    .pill {{
      display:inline-block; padding: 2px 8px; border-radius:999px;
      border:1px solid rgba(255,255,255,0.18); font-size:12px;
    }}
    canvas {{
      width: 100%;
      height: auto;
      background:#070a10;
      border-radius:12px;
      border:1px solid rgba(255,255,255,0.10);
      touch-action: none; /* critical: allow our touch gestures */
      user-select: none;
      -webkit-user-select: none;
    }}
    .grid {{
      display:grid; grid-template-columns: 1fr; gap:10px;
    }}
    .legend {{
      display:flex; gap:10px; flex-wrap: wrap; align-items:center;
    }}
    .legend > span {{ opacity:0.9; }}
    .k {{
      display:flex; gap:10px; flex-wrap:wrap; align-items: center;
    }}
    .k b {{ color:#fff; }}
    a {{ color:#9ad1ff; text-decoration:none; }}
  </style>
</head>
<body>
<div class="wrap">
  <div class="row">
    <div class="card" style="flex: 1 1 680px;">
      <div class="k">
        <b>QuantDesk Bookmap</b>
        <span class="pill">__BUILD__</span>
        <span class="pill" id="pillHealth">HEALTH</span>
        <span class="pill" id="pillMode">LIVE</span>
        <span class="pill" id="pillScale">t=1.0s/col</span>
      </div>
      <div class="muted small" style="margin-top:6px;">
        STRICT Bookmap parity: absolute price axis, time scrolls left→right, liquidity bands pinned to price.
        <br/>Gestures: 1-finger drag = price pan. Pinch = price zoom. 2-finger horiz drag = time scrub. 2-finger vert drag = time scale.
      </div>
      <div style="height:10px"></div>
      <canvas id="cv" width="1024" height="560"></canvas>
      <div class="muted small" style="margin-top:8px;">
        If you scrub left (history), mode switches to HISTORY. Return to LIVE by scrubbing fully right.
      </div>
    </div>
    <div class="card" style="flex: 0 1 360px;">
      <div><b>Status</b></div>
      <div class="small muted" id="status" style="margin-top:6px;">Connecting…</div>
      <div style="height:10px"></div>
      <div class="legend small">
        <span>Heat colors: blue→cyan→yellow→red/white (higher intensity)</span>
      </div>
      <div style="height:10px"></div>
      <div class="small muted">
        Endpoints: <a href="/health.json">/health.json</a> · <a href="/telemetry.json">/telemetry.json</a> · <a href="/book.json">/book.json</a>
      </div>
    </div>
  </div>
</div>

<script>
(() => {{
  const cv = document.getElementById('cv');
  const ctx = cv.getContext('2d', {{ alpha: false }});
  const statusEl = document.getElementById('status');
  const pillHealth = document.getElementById('pillHealth');
  const pillMode = document.getElementById('pillMode');
  const pillScale = document.getElementById('pillScale');

  // --- Colormap LUT (256) : blue -> cyan -> yellow -> red -> white ---
  function makeLUT() {{
    const lut = new Uint8ClampedArray(256*3);
    function lerp(a,b,t) {{ return a + (b-a)*t; }}
    // stops in RGB
    const stops = [
      [0,   10, 25, 55],   // deep blue
      [80,  20, 200, 255], // cyan
      [160, 245, 230, 80], // yellow-ish
      [220, 255, 80, 40],  // red-ish
      [255, 255, 255, 255] // white
    ];
    for (let i=0;i<256;i++) {{
      let s0 = stops[0], s1 = stops[stops.length-1];
      for (let k=1;k<stops.length;k++) {{
        if (i <= stops[k][0]) {{ s0 = stops[k-1]; s1 = stops[k]; break; }}
      }}
      const t = (i - s0[0]) / Math.max(1, (s1[0]-s0[0]));
      const r = Math.round(lerp(s0[1], s1[1], t));
      const g = Math.round(lerp(s0[2], s1[2], t));
      const b = Math.round(lerp(s0[3], s1[3], t));
      lut[i*3+0]=r; lut[i*3+1]=g; lut[i*3+2]=b;
    }}
    return lut;
  }}
  const LUT = makeLUT();

  // --- World (absolute price space) ---
  let tick = 0.1;
  let minIdxSeen = null; // inclusive
  let maxIdxSeen = null; // inclusive

  // --- Time ring buffer ---
  const COLS = cv.width; // 1 column per pixel for simplicity
  const colMaps = new Array(COLS); // sparse: Map<priceIdx, uint8 intensity>
  const midIdxCols = new Int32Array(COLS); // price path per column (absolute idx)
  for (let i=0;i<COLS;i++) {{ colMaps[i] = new Map(); midIdxCols[i]=0; }}
  let headCol = 0; // newest column index in ring buffer
  let lastColTs = 0;

  // --- Viewport camera ---
  let centerIdx = null;        // absolute price idx at center of screen
  let priceSpanBins = 900;     // number of price bins visible (zoom)
  let autoFollow = true;

  // Time view
  let timeOffsetCols = 0;      // 0 = LIVE (newest at right); >0 view history
  let timeScaleS = 1.0;        // seconds per column (time zoom)
  const timeScaleMin = 0.12;
  const timeScaleMax = 8.0;

  // Gesture state
  let pointers = new Map(); // pointerId -> {x,y,type}
  let lastOneFinger = null; // {x,y,centerIdx0}
  let lastTwoFinger = null; // {cx,cy,dx,dy, timeOffset0, timeScale0}
  let lastPinch = null;     // {dist, span0}
  let lastSnapshot = null;
  let ws = null;

  function setHealthPill(health) {{
    pillHealth.textContent = 'HEALTH ' + (health || '?');
    let bg = 'rgba(255,255,255,0.10)';
    if (health === 'GREEN') bg = 'rgba(40,200,120,0.25)';
    if (health === 'YELLOW') bg = 'rgba(240,200,60,0.25)';
    if (health === 'RED') bg = 'rgba(240,80,60,0.25)';
    pillHealth.style.background = bg;
  }}

  function setModePill() {{
    if (timeOffsetCols === 0) {{
      pillMode.textContent = 'LIVE';
      pillMode.style.background = 'rgba(40,200,120,0.20)';
    }} else {{
      pillMode.textContent = 'HISTORY';
      pillMode.style.background = 'rgba(240,200,60,0.18)';
    }}
  }}

  function setScalePill() {{
    pillScale.textContent = 't=' + timeScaleS.toFixed(2) + 's/col';
    pillScale.style.background = 'rgba(255,255,255,0.10)';
  }}

  function priceToIdx(p) {{
    return Math.round(p / tick);
  }}
  function idxToPrice(idx) {{
    return idx * tick;
  }}

  function updateWorldRangeFromBook(bids, asks) {{
    // Expand min/max indices based on received levels
    if (!bids || !asks) return;
    for (const [p,q] of bids) {{
      const idx = priceToIdx(p);
      if (minIdxSeen === null || idx < minIdxSeen) minIdxSeen = idx;
      if (maxIdxSeen === null || idx > maxIdxSeen) maxIdxSeen = idx;
    }}
    for (const [p,q] of asks) {{
      const idx = priceToIdx(p);
      if (minIdxSeen === null || idx < minIdxSeen) minIdxSeen = idx;
      if (maxIdxSeen === null || idx > maxIdxSeen) maxIdxSeen = idx;
    }}
  }}

  function maybeAutoFollow(midIdx) {{
    if (!autoFollow) return;
    if (timeOffsetCols !== 0) return;
    if (centerIdx === null) {{ centerIdx = midIdx; return; }}
    const half = Math.floor(priceSpanBins/2);
    // If price drifts near edges, move camera; else keep stable.
    const top = centerIdx + Math.floor(half*0.70);
    const bot = centerIdx - Math.floor(half*0.70);
    if (midIdx > top) centerIdx = Math.round(centerIdx + (midIdx - top)*0.35);
    if (midIdx < bot) centerIdx = Math.round(centerIdx - (bot - midIdx)*0.35);
  }}

  // Build a new heatmap column (sparse) from the current book snapshot.
  // Intensity is a log-scaled resting liquidity magnitude, normalized by a rolling p95 estimator.
  const rollMax = [];
  const rollMaxN = 240; // ~last 20s at 12fps (but columns are timeScaleS-based)
  function p95(arr) {{
    if (arr.length===0) return 1.0;
    const a = arr.slice().sort((x,y)=>x-y);
    const k = Math.floor(0.95*(a.length-1));
    return a[Math.max(0, Math.min(a.length-1, k))];
  }}

  function buildColumn(bids, asks) {{
    const m = new Map();
    let colMax = 0;
    function addSide(levels) {{
      for (const [p,q] of levels) {{
        const idx = priceToIdx(p);
        // log scaling for magnitude compression
        const v = Math.log1p(Math.max(0, q));
        if (v > colMax) colMax = v;
        m.set(idx, v); // store float temporarily
      }}
    }}
    addSide(bids);
    addSide(asks);

    if (colMax <= 0) return new Map();

    rollMax.push(colMax);
    if (rollMax.length > rollMaxN) rollMax.shift();
    const denom = Math.max(1e-6, p95(rollMax));

    // Convert to 0..255 with gamma contrast and small noise floor
    const out = new Map();
    const gamma = 0.75;
    const floor = 0.03;
    for (const [idx, v] of m.entries()) {{
      let x = v / denom;
      x = Math.max(0, Math.min(2.0, x));
      x = Math.max(0, x - floor);
      x = Math.pow(x / (2.0 - floor), gamma);
      const u = Math.max(0, Math.min(255, Math.round(x * 255)));
      if (u > 0) out.set(idx, u);
    }}
    return out;
  }}

  function pushColumn(colMap, midIdx) {{
    headCol = (headCol + 1) % COLS;
    colMaps[headCol] = colMap;
    midIdxCols[headCol] = midIdx;
  }}

  function draw() {{
    // background
    ctx.fillStyle = '#070a10';
    ctx.fillRect(0,0,cv.width,cv.height);

    if (centerIdx === null || minIdxSeen === null || maxIdxSeen === null) {{
      ctx.fillStyle = '#9bb0c7';
      ctx.font = '14px sans-serif';
      ctx.fillText('Waiting for data…', 14, 24);
      return;
    }}

    const W = cv.width, H = cv.height;

    // Visible price bin range
    const halfBins = Math.floor(priceSpanBins/2);
    const pMin = centerIdx - halfBins;
    const pMax = centerIdx + halfBins;

    // pixels per bin (at least 1 bin per pixel row equivalence)
    const bins = Math.max(10, pMax - pMin);
    const pxPerBin = H / bins;

    // Visible time window: newest on the right edge in LIVE mode.
    // We'll map screen x to buffer column index considering timeOffsetCols.
    const newest = headCol;
    const offset = Math.max(0, Math.min(COLS-1, timeOffsetCols));
    // rightmost column in view corresponds to (newest - offset)
    const rightCol = (newest - offset + COLS) % COLS;

    // Draw heatmap by sparse columns
    // We draw from left->right in screen space, mapping to buffer columns.
    // For each x, we get the buffer column and paint its sparse levels.
    for (let x=0; x<W; x++) {{
      // buffer column index for this screen x
      // leftmost is W-1 columns behind right edge
      const back = (W-1 - x);
      let col = (rightCol - back) % COLS;
      if (col < 0) col += COLS;
      const cmap = colMaps[col];
      if (!cmap) continue;
      // paint sparse points
      cmap.forEach((u, idx) => {{
        if (idx < pMin || idx > pMax) return;
        const y = Math.floor((pMax - idx) * pxPerBin); // higher price at top
        const y2 = Math.floor((pMax - (idx+1)) * pxPerBin);
        const h = Math.max(1, y2 - y);
        const r = LUT[u*3+0], g = LUT[u*3+1], b = LUT[u*3+2];
        ctx.fillStyle = `rgb(${{r}},${{g}},${{b}})`;
        ctx.fillRect(x, y, 1, h);
      }});
    }}

    // Draw price path (mid) as line
    ctx.strokeStyle = 'rgba(255,255,255,0.85)';
    ctx.lineWidth = 1.0;
    ctx.beginPath();
    let started = false;
    for (let x=0; x<W; x++) {{
      const back = (W-1 - x);
      let col = (rightCol - back) % COLS; if (col<0) col += COLS;
      const mid = midIdxCols[col];
      if (!mid) continue;
      if (mid < pMin || mid > pMax) continue;
      const y = (pMax - mid) * pxPerBin;
      if (!started) {{ ctx.moveTo(x, y); started = true; }}
      else ctx.lineTo(x, y);
    }}
    ctx.stroke();

    // HUD (top-left): prices and mode
    ctx.fillStyle = 'rgba(0,0,0,0.45)';
    ctx.fillRect(8, 8, 320, 64);
    ctx.fillStyle = '#e6eef6';
    ctx.font = '12px sans-serif';

    const bid = lastSnapshot?.best_bid ?? 0;
    const ask = lastSnapshot?.best_ask ?? 0;
    const mid = (bid>0 && ask>0) ? (bid+ask)/2 : 0;
    ctx.fillText(`__SYMBOL__  bid ${bid.toFixed(2)}  ask ${ask.toFixed(2)}  mid ${mid.toFixed(2)}`, 14, 26);

    const topPrice = idxToPrice(pMax).toFixed(2);
    const botPrice = idxToPrice(pMin).toFixed(2);
    ctx.fillText(`Price span: ${topPrice} → ${botPrice}  (bins=${priceSpanBins})`, 14, 44);

    const offS = (timeOffsetCols * timeScaleS).toFixed(1);
    ctx.fillText(`Time: ${timeOffsetCols===0 ? "LIVE" : ("HISTORY  T-" + offS + "s")}`, 14, 62);

    // right-side price labels (sparse)
    ctx.fillStyle = 'rgba(155,176,199,0.95)';
    ctx.font = '11px sans-serif';
    const steps = 7;
    for (let i=0;i<=steps;i++) {{
      const frac = i/steps;
      const idx = Math.round(pMax - frac*bins);
      const y = Math.round(frac*H);
      const label = idxToPrice(idx).toFixed(2);
      ctx.fillText(label, W-70, y+4);
    }}
  }}

  function sendView() {{
    if (!ws || ws.readyState !== 1) return;
    const payload = {{
      cmd: "set_view",
      center_idx: centerIdx,
      price_span_bins: priceSpanBins,
      time_offset_cols: timeOffsetCols,
      time_scale_s: timeScaleS,
      auto_follow: autoFollow
    }};
    try {{ ws.send(JSON.stringify(payload)); }} catch (e) {{}}
  }}

  function onSnapshot(snap) {{
    lastSnapshot = snap;
    // health from endpoint data (if provided)
    // We'll derive health from freshness/book_ok quickly
    const fresh = (Date.now()/1000 - (snap.last_update_ts||0)) <= {STALE_S};
    const bookOk = (snap.best_bid||0) > 0 && (snap.best_ask||0) > 0 && (snap.best_bid < snap.best_ask);
    setHealthPill((fresh && bookOk) ? 'GREEN' : (bookOk ? 'YELLOW' : 'RED'));

    if (snap.tick && snap.tick > 0) tick = snap.tick;

    const bids = snap.bids || [];
    const asks = snap.asks || [];
    updateWorldRangeFromBook(bids, asks);

    const bid = snap.best_bid || 0;
    const ask = snap.best_ask || 0;
    if (bid>0 && ask>0) {{
      const mid = (bid+ask)/2;
      const midIdx = priceToIdx(mid);
      maybeAutoFollow(midIdx);

      // advance time columns according to timeScaleS
      const t = snap.ts || (Date.now()/1000);
      if (lastColTs === 0) lastColTs = t;

      if (t - lastColTs >= timeScaleS) {{
        // Add as many columns as needed to catch up (bounded)
        let steps = Math.min(8, Math.floor((t - lastColTs) / timeScaleS));
        for (let k=0;k<steps;k++) {{
          const col = buildColumn(bids, asks);
          pushColumn(col, midIdx);
          lastColTs += timeScaleS;
        }}
        // If we are LIVE, keep timeOffset at 0. If in HISTORY, keep offset.
        setModePill();
        setScalePill();
        sendView();
      }}
    }}

    draw();
  }}

  function connect() {{
    const proto = (location.protocol === 'https:') ? 'wss:' : 'ws:';
    const url = proto + '//' + location.host + '/render.ws';
    statusEl.textContent = 'Connecting WS…';
    ws = new WebSocket(url);
    ws.onopen = () => {{ statusEl.textContent = 'WS connected'; }};
    ws.onclose = () => {{
      statusEl.textContent = 'WS disconnected — retrying…';
      setTimeout(connect, 700);
    }};
    ws.onerror = () => {{}};
    ws.onmessage = (ev) => {{
      try {{
        const snap = JSON.parse(ev.data);
        onSnapshot(snap);
      }} catch (e) {{
        // ignore
      }}
    }};
  }}

  // --- Pointer / touch gestures ---
  function getTwoFingerMetrics() {{
    const pts = Array.from(pointers.values());
    if (pts.length !== 2) return null;
    const a = pts[0], b = pts[1];
    const cx = (a.x + b.x)/2;
    const cy = (a.y + b.y)/2;
    const dx = (b.x - a.x);
    const dy = (b.y - a.y);
    const dist = Math.hypot(dx, dy);
    return {{cx, cy, dx, dy, dist}};
  }}

  cv.addEventListener('pointerdown', (e) => {{
    e.preventDefault();
    cv.setPointerCapture(e.pointerId);
    pointers.set(e.pointerId, {{x:e.offsetX, y:e.offsetY, type:e.pointerType}});
    if (pointers.size === 1) {{
      lastOneFinger = {{x:e.offsetX, y:e.offsetY, centerIdx0: centerIdx, span0: priceSpanBins}};
      lastPinch = null;
    }} else if (pointers.size === 2) {{
      const m = getTwoFingerMetrics();
      if (m) {{
        lastTwoFinger = {{cx:m.cx, cy:m.cy, dx:m.dx, dy:m.dy, timeOffset0: timeOffsetCols, timeScale0: timeScaleS, centerIdx0: centerIdx, span0: priceSpanBins, dist0: m.dist}};
        lastPinch = {{dist:m.dist, span0: priceSpanBins}}; // allow pinch->price zoom
      }}
    }}
  }}, {{passive:false}});

  cv.addEventListener('pointermove', (e) => {{
    if (!pointers.has(e.pointerId)) return;
    e.preventDefault();
    pointers.set(e.pointerId, {{x:e.offsetX, y:e.offsetY, type:e.pointerType}});

    if (pointers.size === 1 && lastOneFinger && centerIdx !== null) {{
      // 1-finger vertical pan in price space
      const dy = e.offsetY - lastOneFinger.y;
      // Convert pixels to bins using current visible bins
      const bins = Math.max(10, priceSpanBins);
      const pxPerBin = cv.height / bins;
      const dBins = -dy / Math.max(1e-6, pxPerBin);
      centerIdx = Math.round(lastOneFinger.centerIdx0 + dBins);
      autoFollow = false;
      sendView();
      draw();
      return;
    }}

    if (pointers.size === 2 && lastTwoFinger) {{
      const m = getTwoFingerMetrics();
      if (!m) return;

      const dCx = m.cx - lastTwoFinger.cx;
      const dCy = m.cy - lastTwoFinger.cy;
      const dDist = m.dist - lastTwoFinger.dist0;

      // 1) Pinch = PRICE ZOOM (Bookmap parity)
      // We treat pinch as dominant when distance change is meaningful and exceeds translation.
      if (Math.abs(dDist) > 6 && Math.abs(dDist) > (Math.abs(dCx) + Math.abs(dCy)) * 0.80) {{
        const kPinch = 0.008;
        const factor = Math.exp(-dDist * kPinch); // pinch-out (dDist>0) => zoom-in price (smaller span)
        priceSpanBins = Math.round(clamp(lastTwoFinger.span0 * factor, 120, 20000));
        autoFollow = false;
        sendView();
        draw();
        return;
      }}

      // 2) Two-finger horizontal drag -> TIME SCRUB
      if (Math.abs(dCx) >= Math.abs(dCy)) {{
        const dCols = Math.round(-dCx); // drag left => positive offset (history)
        timeOffsetCols = Math.max(0, Math.min(COLS-1, lastTwoFinger.timeOffset0 + dCols));
        if (timeOffsetCols !== 0) autoFollow = false;
        setModePill();
        sendView();
        draw();
        return;
      }}

      // 3) Two-finger vertical drag -> TIME SCALE (seconds per column)
      const k = 0.012;
      const factor = Math.exp(dCy * k); // drag down => zoom out time (bigger seconds/col)
      timeScaleS = clamp(lastTwoFinger.timeScale0 * factor, timeScaleMin, timeScaleMax);
      setScalePill();
      sendView();
      draw();
      return;
    }}
  }}, {{passive:false}});

  function endPointer(e) {{
    try {{ pointers.delete(e.pointerId); }} catch(_) {{}}
    if (pointers.size === 0) {{
      lastOneFinger = null; lastTwoFinger = null; lastPinch = null;
      return;
    }}
    if (pointers.size === 1) {{
      // keep remaining pointer as 1-finger baseline
      const pt = Array.from(pointers.values())[0];
      lastOneFinger = {{x:pt.x, y:pt.y, centerIdx0:centerIdx, span0:priceSpanBins}};
      lastTwoFinger = null; lastPinch = null;
    }}
  }}

  cv.addEventListener('pointerup', endPointer, {{passive:false}});
  cv.addEventListener('pointercancel', endPointer, {{passive:false}});
  cv.addEventListener('pointerout', endPointer, {{passive:false}});
  cv.addEventListener('pointerleave', endPointer, {{passive:false}});

  // Pinch to zoom price span (iPad: we emulate pinch via 2 pointers distance change)
  // We handle it on pointermove when two pointers exist by comparing dist, but we reserved two-finger gestures
  // for time scrub/scale. So pinch is handled as iOS native gesture isn't available under pointer events.
  // Instead: use wheel on desktop + double-tap? We'll implement wheel + optional 3-finger pinch later.
  // For now: wheel/trackpad zoom still supported.
  cv.addEventListener('wheel', (e) => {{
    e.preventDefault();
    if (centerIdx === null) return;
    const factor = Math.exp(e.deltaY * 0.001);
    priceSpanBins = Math.round(clamp(priceSpanBins * factor, 120, 20000));
    autoFollow = false;
    sendView();
    draw();
  }}, {{passive:false}});

  // Minimal toggle: double tap to return to LIVE + autoFollow
  let lastTap = 0;
  cv.addEventListener('touchend', (e) => {{
    const t = Date.now();
    if (t - lastTap < 280) {{
      timeOffsetCols = 0;
      autoFollow = true;
      setModePill();
      sendView();
      draw();
    }}
    lastTap = t;
  }}, {{passive:true}});

  setModePill();
  setScalePill();
  connect();

  // redraw loop safety
  setInterval(() => {{
    if (lastSnapshot) draw();
  }}, 120);

}})();
</script>
</body>
</html>"""
    return html.replace("{{","{").replace("}}","}").replace("__BUILD__", BUILD).replace("__SYMBOL__", SYMBOL)
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
