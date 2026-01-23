# =========================================================
# QuantDesk Bookmap Service — FIX4 Render Bridge (Replit)
# FOUNDATION_PASS preserved:
# - FastAPI stays up
# - WS reconnect loop
# - Depth + trades flow
# - Bids/asks populated
#
# FIX4 adds:
# - Render Bridge: a static ladder/heat canvas from the CURRENT snapshot (no history buffer yet)
# - Axis mapping (price -> y)
# - Black background + blue->red palette (Bookmap-like direction; refined later)
# - Minimal pan/zoom + autofollow toggle (stable; refined later)
#
# Next planned phases (not implemented here):
# - Time-decay heat buffer (history)
# - Auto-exposure (percentile/log/gamma)
# - Band sparsify + smoothing + consumption
# - Bubbles
# - Persistence + iPad QA
# =========================================================

from __future__ import annotations

import os
import json
import time
import asyncio
from typing import Any, Dict, List, Tuple, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

try:
    import websockets  # type: ignore
except Exception:
    websockets = None  # allows service to stay up even if dep missing


# ---------------------------
# Config
# ---------------------------
SYMBOL = os.getenv("QD_SYMBOL_WS", "BTC_USDT").strip()
MEXC_WS = os.getenv("QD_WS_URL", "wss://contract.mexc.com/edge").strip()

PORT = int(os.getenv("PORT", os.getenv("QD_PORT", "5000")))

RANGE_USD = float(os.getenv("QD_RANGE_USD", "400"))      # half-range around mid for y mapping
BIN_USD = float(os.getenv("QD_BIN_USD", "5"))            # price bin size for snapshot ladder
TOPN_LEVELS = int(os.getenv("QD_TOPN_LEVELS", "200"))    # levels retained per side
SNAPSHOT_HZ = float(os.getenv("QD_SNAPSHOT_HZ", "2"))    # UI polling target

BUILD = "FIX4_RENDER_BRIDGE_FIX4_SNAPSHOT_GUARD"

app = FastAPI(title=f"QuantDesk Bookmap {BUILD}")

# ---------------------------
# Shared state (JSON-safe)
# ---------------------------
STATE: Dict[str, Any] = {
    "service": "quantdesk-bookmap",
    "build": BUILD,
    "symbol": SYMBOL,
    "ws_url": MEXC_WS,
    "ws_ok": False,
    "ws_last_rx_ms": None,
    "last_error": None,
    "last_trade_px": None,
    "best_bid": None,
    "best_ask": None,
    "mid": None,
    "bids_n": 0,
    "asks_n": 0,
    "depth_age_ms": None,
    "trade_age_ms": None,
    "prints_60s": 0,
}

STATE_LOCK = asyncio.Lock()

# ---------------------------
# In-memory orderbook + prints ring
# ---------------------------
BIDS: Dict[float, float] = {}
ASKS: Dict[float, float] = {}
LAST_DEPTH_MS: Optional[int] = None
LAST_TRADE_MS: Optional[int] = None

_PRINT_TS: List[float] = []  # timestamps (seconds) of prints, rolling 60s


def _now_ms() -> int:
    return int(time.time() * 1000)


def _set_err(msg: str) -> None:
    STATE["last_error"] = msg


def _track_print(now_s: float) -> None:
    _PRINT_TS.append(now_s)
    cutoff = now_s - 60.0
    # prune old
    i = 0
    for t0 in _PRINT_TS:
        if t0 >= cutoff:
            break
        i += 1
    if i:
        del _PRINT_TS[:i]
    STATE["prints_60s"] = len(_PRINT_TS)


def _recompute_tob() -> None:
    best_bid = max(BIDS.keys()) if BIDS else None
    best_ask = min(ASKS.keys()) if ASKS else None
    mid = None
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0

    STATE["best_bid"] = best_bid
    STATE["best_ask"] = best_ask
    STATE["mid"] = mid
    STATE["bids_n"] = len(BIDS)
    STATE["asks_n"] = len(ASKS)

    now = _now_ms()
    STATE["depth_age_ms"] = (now - LAST_DEPTH_MS) if LAST_DEPTH_MS else None
    STATE["trade_age_ms"] = (now - LAST_TRADE_MS) if LAST_TRADE_MS else None


def _prune_topn() -> None:
    # Keep only TOPN_LEVELS per side to cap memory and snapshot size
    if len(BIDS) > TOPN_LEVELS:
        # keep highest bids
        for px in sorted(BIDS.keys())[:-TOPN_LEVELS]:
            BIDS.pop(px, None)
    if len(ASKS) > TOPN_LEVELS:
        # keep lowest asks
        for px in sorted(ASKS.keys())[TOPN_LEVELS:]:
            ASKS.pop(px, None)


def _bin_price(px: float, bin_usd: float) -> float:
    if bin_usd <= 0:
        return px
    return round(px / bin_usd) * bin_usd


def _build_heat_bins(center: float, half_range: float, bin_usd: float) -> Tuple[List[List[float]], float]:
    """
    Build snapshot ladder bins from CURRENT depth only (no history).
    Returns:
      heat_bins: [[price_bin, intensity_qty], ...] within visible range
      heat_max:  max intensity
    """
    lo = center - half_range
    hi = center + half_range
    accum: Dict[float, float] = {}

    for p, q in BIDS.items():
        if lo <= p <= hi:
            pb = _bin_price(p, bin_usd)
            accum[pb] = accum.get(pb, 0.0) + float(q)
    for p, q in ASKS.items():
        if lo <= p <= hi:
            pb = _bin_price(p, bin_usd)
            accum[pb] = accum.get(pb, 0.0) + float(q)

    if not accum:
        return [], 0.0

    items = sorted(accum.items(), key=lambda x: x[0])
    heat_max = max(v for _, v in items) if items else 0.0
    # JSON-friendly lists
    heat_bins = [[float(p), float(v)] for p, v in items]
    return heat_bins, float(heat_max)


def _top_levels(side: Dict[float, float], n: int, desc: bool) -> List[List[float]]:
    # Return sorted [[price, qty], ...]
    if not side:
        return []
    ks = sorted(side.keys(), reverse=desc)
    out = []
    for p in ks[:n]:
        out.append([float(p), float(side.get(p, 0.0))])
    return out


# ---------------------------
# MEXC WS consumer
# ---------------------------
async def mexc_consumer_loop() -> None:
    """
    Subscribes to:
      - push.depth (levels [price, qty, count], qty=0 means remove)
      - push.deal  (prints p, v, T(1 buy,2 sell), t ms)
    Applies partial depth updates without wiping a side.
    """
    global LAST_DEPTH_MS, LAST_TRADE_MS

    if websockets is None:
        async with STATE_LOCK:
            STATE["ws_ok"] = False
            _set_err("Missing dependency: websockets (pip install websockets)")
        return

    backoff = 1.0
    while True:
        try:
            async with websockets.connect(MEXC_WS, ping_interval=20, ping_timeout=20) as ws:
                # Subscribe (keep params minimal; MEXC accepts)
                await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": SYMBOL, "depth": 20}}))
                await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": SYMBOL}}))

                async with STATE_LOCK:
                    STATE["ws_ok"] = True
                    STATE["last_error"] = None

                backoff = 1.0

                async for raw in ws:
                    now_ms = _now_ms()
                    async with STATE_LOCK:
                        STATE["ws_last_rx_ms"] = now_ms

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    ch = msg.get("channel") or msg.get("c") or ""
                    if not ch:
                        s = json.dumps(msg)
                        if "push.depth" in s:
                            ch = "push.depth"
                        elif "push.deal" in s:
                            ch = "push.deal"

                    if "push.depth" in ch:
                        data = msg.get("data") or {}
                        bids = data.get("bids") or []
                        asks = data.get("asks") or []

                        # Apply partial updates
                        for lvl in bids:
                            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                                continue
                            try:
                                p = float(lvl[0]); q = float(lvl[1])
                            except Exception:
                                continue
                            if q <= 0.0:
                                BIDS.pop(p, None)
                            else:
                                BIDS[p] = q

                        for lvl in asks:
                            if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                                continue
                            try:
                                p = float(lvl[0]); q = float(lvl[1])
                            except Exception:
                                continue
                            if q <= 0.0:
                                ASKS.pop(p, None)
                            else:
                                ASKS[p] = q

                        LAST_DEPTH_MS = now_ms
                        _prune_topn()
                        async with STATE_LOCK:
                            _recompute_tob()

                    elif "push.deal" in ch:
                        deals = msg.get("data") or []
                        if isinstance(deals, dict):
                            deals = [deals]
                        if isinstance(deals, list) and deals:
                            d0 = deals[-1]
                            try:
                                p = d0.get("p", None)
                                if p is not None:
                                    STATE["last_trade_px"] = float(p)
                            except Exception:
                                pass
                            LAST_TRADE_MS = now_ms
                            _track_print(time.time())
                            async with STATE_LOCK:
                                _recompute_tob()

        except Exception as e:
            async with STATE_LOCK:
                STATE["ws_ok"] = False
                _set_err(f"WS error: {type(e).__name__}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)


# ---------------------------
# FastAPI lifecycle
# ---------------------------
@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(mexc_consumer_loop())


# ---------------------------
# Routes
# ---------------------------

@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    # IMPORTANT: This HTML/JS/CSS must NOT use Python f-strings.
    # We inject a few numeric constants via token replacement.
    html = r"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>QuantDesk Bookmap — __BUILD__</title>
  <style>
    html, body {
      margin: 0; padding: 0; height: 100%; background: #000; color: #ddd;
      font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif;
      overflow: hidden;
    }
    #wrap { position: relative; width: 100%; height: 100%; }
    #c { width: 100%; height: 100%; display: block; touch-action: none; background: #000; }
    #hud {
      position: absolute; left: 12px; top: 10px;
      background: rgba(0,0,0,0.55);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 12px;
      line-height: 1.35;
      max-width: 92vw;
      z-index: 10;
      pointer-events: auto;
    }
    .row { white-space: nowrap; }
    .k { color: #9aa; }
    .v { color: #e6e6e6; }
    #btns { margin-top: 8px; display: flex; gap: 8px; flex-wrap: wrap; }
    button {
      background: rgba(255,255,255,0.08);
      color: #eee;
      border: 1px solid rgba(255,255,255,0.15);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
    }
    button:active { transform: scale(0.99); }
    #statusDot {
      display:inline-block; width:8px; height:8px; border-radius:50%;
      margin-right:6px; vertical-align:middle; background: #666;
    }
    #err {
      margin-top: 8px;
      color: #ffb3b3;
      max-width: 92vw;
      white-space: pre-wrap;
    }
  </style>
</head>
<body>
<div id="wrap">
  <canvas id="c"></canvas>
  <div id="hud">
    <div class="row"><span id="statusDot"></span><span class="k">Build:</span> <span class="v">__BUILD__</span></div>
    <div class="row"><span class="k">Status:</span> <span class="v" id="statusTxt">…</span></div>
    <div class="row"><span class="k">Last:</span> <span class="v" id="lastPx">—</span>
      <span class="k">Bid:</span> <span class="v" id="bestBid">—</span>
      <span class="k">Ask:</span> <span class="v" id="bestAsk">—</span>
      <span class="k">Mid:</span> <span class="v" id="midPx">—</span></div>
    <div class="row"><span class="k">Ages:</span> <span class="v" id="ages">—</span></div>
    <div class="row"><span class="k">Book:</span> <span class="v" id="bookInfo">—</span>
      <span class="k">Prints(60s):</span> <span class="v" id="prints60">—</span></div>

    <div id="btns">
      <button id="followBtn">Autofollow: ON</button>
      <button id="resetBtn">Reset view</button>
      <button id="testBtn">Test pattern: OFF</button>
    </div>
    <div id="err"></div>
  </div>
</div>

<script>
(function() {
  const CANVAS = document.getElementById('c');
  const ctx = CANVAS.getContext('2d');

  const elStatusDot = document.getElementById('statusDot');
  const elStatusTxt = document.getElementById('statusTxt');
  const elLast = document.getElementById('lastPx');
  const elBid = document.getElementById('bestBid');
  const elAsk = document.getElementById('bestAsk');
  const elMid = document.getElementById('midPx');
  const elAges = document.getElementById('ages');
  const elBook = document.getElementById('bookInfo');
  const elPrints = document.getElementById('prints60');
  const elErr = document.getElementById('err');

  const followBtn = document.getElementById('followBtn');
  const resetBtn = document.getElementById('resetBtn');
  const testBtn = document.getElementById('testBtn');

  let autofollow = true;
  let testPattern = false;
  let centerPx = null;
  let zoom = 1.0;
  let panPx = 0.0;
  let dragging = false;
  let lastY = 0;

  function resize() {
    const dpr = window.devicePixelRatio || 1;
    const w = Math.floor(window.innerWidth * dpr);
    const h = Math.floor(window.innerHeight * dpr);
    if (CANVAS.width !== w || CANVAS.height !== h) {
      CANVAS.width = w;
      CANVAS.height = h;
    }
  }
  window.addEventListener('resize', resize);
  resize();

  function fmt(x) {
    if (x === null || x === undefined) return '—';
    if (!isFinite(x)) return '—';
    return (Math.round(x * 100) / 100).toFixed(2);
  }

  function heatColor(t) {
    t = Math.max(0, Math.min(1, t));
    const r = Math.floor(255 * Math.pow(t, 0.85));
    const g = Math.floor(60 * Math.pow(t, 1.2));
    const b = Math.floor(255 * Math.pow(1 - t, 0.25));
    return `rgb(${r},${g},${b})`;
  }

  function draw(snapshot) {
    const W = CANVAS.width, H = CANVAS.height;
    ctx.fillStyle = '#000';
    ctx.fillRect(0, 0, W, H);

    const pxCenter = snapshot.center_px;
    if (autofollow && pxCenter !== null && isFinite(pxCenter)) {
      centerPx = pxCenter;
      panPx = 0.0;
    }
    if (centerPx === null) centerPx = pxCenter;

    if (centerPx === null || !isFinite(centerPx)) {
      if (testPattern) { centerPx = 0.0; panPx = 0.0; }
      else {
      ctx.fillStyle = 'rgba(255,255,255,0.75)';
      ctx.font = `${Math.floor(14 * (window.devicePixelRatio||1))}px sans-serif`;
      ctx.fillText('Waiting for first price/book…', 16, 28);
      return;
      }
    }

    const halfRange = snapshot.half_range || __RANGE_USD__;
    const effHalf = (halfRange / zoom);
    const lo = (centerPx + panPx) - effHalf;
    const hi = (centerPx + panPx) + effHalf;

    function yOfPrice(p) {
      return (hi - p) / (hi - lo) * H;
    }

    let bins = snapshot.heat_bins || [];
    let maxv = snapshot.heat_max || 0;
    const binUsd = snapshot.bin_usd || __BIN_USD__;

    if (testPattern) {
      bins = [];
      const steps = 40;
      for (let i=0; i<=steps; i++) {
        const p = lo + (hi-lo) * (i/steps);
        const t = i/steps;
        bins.push([p, t]);
      }
      maxv = 1.0;
    }

    if (bins.length > 0 && maxv > 0) {
      for (let i=0; i<bins.length; i++) {
        const p = bins[i][0];
        const v = bins[i][1];
        if (!(p >= lo && p <= hi)) continue;
        const t = v / maxv;

        const y = yOfPrice(p);
        const y2 = yOfPrice(p + binUsd);
        const hStripe = Math.max(1, Math.abs(y2 - y));

        ctx.fillStyle = heatColor(t);
        ctx.globalAlpha = 0.90;
        ctx.fillRect(0, y - hStripe*0.5, W, hStripe);
      }
      ctx.globalAlpha = 1.0;
    } else {
      ctx.fillStyle = 'rgba(255,255,255,0.55)';
      ctx.font = `${Math.floor(14 * (window.devicePixelRatio||1))}px sans-serif`;
      ctx.fillText('No liquidity bins in range (yet).', 16, 28);
    }

    if (snapshot.last_px !== null && isFinite(snapshot.last_px)) {
      const y = yOfPrice(snapshot.last_px);
      ctx.strokeStyle = 'rgba(255,255,255,0.9)';
      ctx.lineWidth = Math.max(1, Math.floor((window.devicePixelRatio||1)));
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }

    ctx.fillStyle = 'rgba(255,255,255,0.75)';
    ctx.font = `${Math.floor(12 * (window.devicePixelRatio||1))}px sans-serif`;
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';

    const ticks = 8;
    for (let i=0; i<=ticks; i++) {
      const p = lo + (hi - lo) * (i / ticks);
      const y = yOfPrice(p);
      ctx.fillText(fmt(p), W - 8, y);
      ctx.strokeStyle = 'rgba(255,255,255,0.08)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(W, y);
      ctx.stroke();
    }
  }

  async function fetchSnapshot() {
    try {
      const r = await fetch('/snapshot', { cache: 'no-store' });
      if (!r.ok) throw new Error('bad http');
      return await r.json();
    } catch (e) {
      return null;
    }
  }

  function setStatus(ok, txt) {
    elStatusTxt.textContent = txt;
    elStatusDot.style.background = ok ? '#2bd26b' : '#d24b2b';
  }

  async function tick() {
    try {
      const snap = await fetchSnapshot();
      if (!snap) {
        setStatus(false, 'API unreachable');
        return;
      }
      const wsOk = !!snap.ws_connected;
      const depthAge = snap.depth_age_ms;
      const tradeAge = snap.trade_age_ms;

      const green = wsOk && depthAge !== null && depthAge < 2500 && snap.book_bids_n > 0 && snap.book_asks_n > 0;
      const txt = green ? 'HEALTH GREEN' : (wsOk ? 'HEALTH YELLOW/RED' : 'WS DOWN');
      setStatus(green, txt);

      elLast.textContent = fmt(snap.last_px);
      elBid.textContent = fmt(snap.best_bid);
      elAsk.textContent = fmt(snap.best_ask);
      elMid.textContent = fmt(snap.mid);

      const da = (depthAge === null) ? '—' : `${Math.round(depthAge)}ms`;
      const ta = (tradeAge === null) ? '—' : `${Math.round(tradeAge)}ms`;
      elAges.textContent = `depth_age=${da} trade_age=${ta} ws_msg_age=${Math.round(snap.ws_msg_age_ms)}ms`;

      elBook.textContent = `bids=${snap.book_bids_n} asks=${snap.book_asks_n}`;
      elPrints.textContent = `${snap.prints_60s || 0}`;

      draw(snap);
      if (snap.ok === false && snap.error) { elErr.textContent = String(snap.error); } else { elErr.textContent = ''; }
    } catch (e) {
      elErr.textContent = String(e && e.stack ? e.stack : e);
    }
  }

  followBtn.addEventListener('click', () => {
    autofollow = !autofollow;
    followBtn.textContent = `Autofollow: ${autofollow ? 'ON' : 'OFF'}`;
  });
  resetBtn.addEventListener('click', () => {
    zoom = 1.0;
    panPx = 0.0;
    autofollow = true;
    followBtn.textContent = 'Autofollow: ON';
  });
  testBtn.addEventListener('click', () => {
    testPattern = !testPattern;
    testBtn.textContent = `Test pattern: ${testPattern ? 'ON' : 'OFF'}`;
  });

  CANVAS.addEventListener('pointerdown', (e) => {
    dragging = true;
    lastY = e.clientY;
    CANVAS.setPointerCapture(e.pointerId);
    autofollow = false;
    followBtn.textContent = 'Autofollow: OFF';
  });
  CANVAS.addEventListener('pointermove', (e) => {
    if (!dragging) return;
    const dy = (e.clientY - lastY);
    lastY = e.clientY;
    const pxPerScreen = (__RANGE_USD__ / zoom) * 2;
    const dPrice = (dy / window.innerHeight) * pxPerScreen;
    panPx += dPrice;
  });
  CANVAS.addEventListener('pointerup', (e) => {
    dragging = false;
    try { CANVAS.releasePointerCapture(e.pointerId); } catch(_) {}
  });
  CANVAS.addEventListener('wheel', (e) => {
    e.preventDefault();
    autofollow = false;
    followBtn.textContent = 'Autofollow: OFF';
    const delta = Math.sign(e.deltaY);
    const factor = (delta > 0) ? 1.08 : 0.92;
    zoom = Math.max(0.25, Math.min(6.0, zoom * factor));
  }, { passive: false });

  const intervalMs = Math.max(250, Math.floor(1000 / Math.max(0.2, __SNAPSHOT_HZ__)));
  setInterval(tick, intervalMs);
  tick();
})();
</script>
</body>
</html>
"""
    html = html.replace("__RANGE_USD__", str(RANGE_USD))
    html = html.replace("__BIN_USD__", str(BIN_USD))
    html = html.replace("__SNAPSHOT_HZ__", str(SNAPSHOT_HZ))
    html = html.replace("__BUILD__", BUILD)
    return html

@app.get("/state")
async def state() -> JSONResponse:
    # Compatibility endpoint for FOUNDATION checks
    async with STATE_LOCK:
        return JSONResponse(dict(STATE))


@app.get("/snapshot")
async def snapshot() -> JSONResponse:
    """
    UI snapshot for FIX4:
      - center_px: mid if available else last price else best side
      - heat_bins: snapshot ladder bins from current depth only
    IMPORTANT:
      - This handler must NEVER throw. If anything fails, return ok=false with safe defaults.
    """
    try:
        async with STATE_LOCK:
            ws_ok = bool(STATE.get("ws_ok"))
            ws_last_rx_ms = STATE.get("ws_last_rx_ms")
            now_ms = _now_ms()
            ws_msg_age_ms = (now_ms - ws_last_rx_ms) if ws_last_rx_ms else None

            best_bid = STATE.get("best_bid")
            best_ask = STATE.get("best_ask")
            mid = STATE.get("mid")
            last_px = STATE.get("last_trade_px")

            center_px = mid
            if center_px is None:
                center_px = last_px
            if center_px is None and best_bid is not None and best_ask is not None:
                center_px = (float(best_bid) + float(best_ask)) / 2.0
            if center_px is None and best_bid is not None:
                center_px = float(best_bid)
            if center_px is None and best_ask is not None:
                center_px = float(best_ask)

            heat_bins: List[List[float]] = []
            heat_max = 0.0
            if center_px is not None:
                heat_bins, heat_max = _build_heat_bins(float(center_px), float(RANGE_USD), float(BIN_USD))

            payload = {
                "ok": True,
                "build": BUILD,
                "symbol": SYMBOL,
                "ws_ok": ws_ok,
                "ws_msg_age_ms": (float(ws_msg_age_ms) if ws_msg_age_ms is not None else None),
                "depth_age_ms": STATE.get("depth_age_ms"),
                "trade_age_ms": STATE.get("trade_age_ms"),
                "prints_60s": STATE.get("prints_60s", 0),
                "last_px": last_px,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid": mid,
                "book_bids_n": int(STATE.get("bids_n") or 0),
                "book_asks_n": int(STATE.get("asks_n") or 0),
                "center_px": center_px,
                "half_range": float(RANGE_USD),
                "bin_usd": float(BIN_USD),
                "heat_bins": heat_bins,
                "heat_max": float(heat_max),
                "error": STATE.get("last_error"),
            }
            return JSONResponse(payload)
    except Exception as e:
        # Never fail the API surface — return a safe payload so the UI can keep running.
        payload = {
            "ok": False,
            "build": BUILD,
            "symbol": SYMBOL,
            "ws_ok": False,
            "ws_msg_age_ms": None,
            "depth_age_ms": None,
            "trade_age_ms": None,
            "prints_60s": 0,
            "last_px": None,
            "best_bid": None,
            "best_ask": None,
            "mid": None,
            "book_bids_n": 0,
            "book_asks_n": 0,
            "center_px": 0.0,  # allow test pattern rendering even with no data
            "half_range": float(RANGE_USD),
            "bin_usd": float(BIN_USD),
            "heat_bins": [],
            "heat_max": 0.0,
            "error": f"snapshot_exception: {type(e).__name__}: {e}",
        }
        return JSONResponse(payload)


# ---------------------------
# Local run (Replit uses: python app.py)
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
