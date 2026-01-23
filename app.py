import os
import json
import time
import gzip
import zlib
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

# -----------------------------
# Config (env)
# -----------------------------
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")  # MEXC contract WS uses BTC_USDT
PORT = int(os.environ.get("PORT", "8000"))

MAX_TRADES = int(os.environ.get("QD_MAX_TRADES", "20000"))
TOP_LEVELS = int(os.environ.get("QD_TOP_LEVELS", "200"))           # send depth levels each side
SNAPSHOT_HZ = float(os.environ.get("QD_SNAPSHOT_HZ", "6"))          # UI snapshots/sec

# Heatmap defaults (UI-side uses these as max history)
HEAT_WINDOW_SEC = int(os.environ.get("QD_HEAT_WINDOW_SEC", str(60 * 60)))  # 60m history stored
HEAT_COL_MS = int(os.environ.get("QD_HEAT_COL_MS", "1000"))                # 1s columns
HEAT_BINS = int(os.environ.get("QD_HEAT_BINS", "360"))                     # vertical resolution
DEFAULT_PRICE_SPAN = float(os.environ.get("QD_PRICE_SPAN", "600"))          # USD visible span initial

# -----------------------------
# Helpers
# -----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)

def try_decompress(payload: bytes) -> bytes:
    try:
        return gzip.decompress(payload)
    except Exception:
        pass
    try:
        return zlib.decompress(payload)
    except Exception:
        return payload

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# -----------------------------
# State
# -----------------------------
@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

    def apply_side_updates(self, side: str, levels: List[List[Any]]) -> None:
        # MEXC push.depth levels are commonly [price, qty, count]
        # qty==0 => remove
        book = self.bids if side == "bids" else self.asks
        for lvl in levels:
            if not lvl or len(lvl) < 2:
                continue
            p = safe_float(lvl[0])
            q = safe_float(lvl[1])
            if p <= 0:
                continue
            if q <= 0:
                book.pop(p, None)
            else:
                book[p] = q

    def top_n(self, n: int) -> Dict[str, List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0], reverse=False)[:n]
        return {"bids": bids_sorted, "asks": asks_sorted}

    def best_bid_ask(self) -> Tuple[Optional[float], Optional[float]]:
        bb = max(self.bids.keys()) if self.bids else None
        ba = min(self.asks.keys()) if self.asks else None
        return bb, ba

@dataclass
class Trades:
    dq: deque = field(default_factory=lambda: deque(maxlen=MAX_TRADES))

    def append(self, t: Dict[str, Any]) -> None:
        self.dq.append(t)

class MexcFeed:
    def __init__(self) -> None:
        self.book = OrderBook()
        self.trades = Trades()
        self.ws_ok = False
        self.last_px: Optional[float] = None
        self.last_trade_ms: Optional[int] = None
        self.last_depth_ms: Optional[int] = None
        self._stop = False

        self._clients: List[WebSocket] = []
        self._clients_lock = asyncio.Lock()

    async def add_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.append(ws)

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients = [c for c in self._clients if c is not ws]

    async def broadcast_snapshot(self) -> None:
        book = self.book.top_n(TOP_LEVELS)
        trades = list(self.trades.dq)[-2500:]  # keep payload sane

        bb, ba = self.book.best_bid_ask()
        mid = None
        if bb is not None and ba is not None and bb > 0 and ba > 0:
            mid = (bb + ba) / 2.0

        payload = {
            "ts": now_ms(),
            "ws_url": WS_URL,
            "symbol": SYMBOL_WS,
            "ws_ok": self.ws_ok,
            "last_px": self.last_px,
            "mid_px": mid,
            "best_bid": bb,
            "best_ask": ba,
            "book": {"bids": book["bids"], "asks": book["asks"]},
            "trades": trades,
            "health": {
                "depth_age_ms": (now_ms() - self.last_depth_ms) if self.last_depth_ms else None,
                "trade_age_ms": (now_ms() - self.last_trade_ms) if self.last_trade_ms else None,
                "bids_n": len(self.book.bids),
                "asks_n": len(self.book.asks),
                "trades_n": len(self.trades.dq),
            },
            "ui_defaults": {
                "heat_window_sec": HEAT_WINDOW_SEC,
                "heat_col_ms": HEAT_COL_MS,
                "heat_bins": HEAT_BINS,
                "default_price_span": DEFAULT_PRICE_SPAN,
            }
        }

        msg = json.dumps(payload)
        async with self._clients_lock:
            clients = list(self._clients)

        dead: List[WebSocket] = []
        for c in clients:
            try:
                await c.send_text(msg)
            except Exception:
                dead.append(c)

        if dead:
            async with self._clients_lock:
                self._clients = [c for c in self._clients if c not in dead]

    async def _snapshot_loop(self) -> None:
        period = max(0.05, 1.0 / max(1.0, SNAPSHOT_HZ))
        while not self._stop:
            try:
                await self.broadcast_snapshot()
            except Exception:
                pass
            await asyncio.sleep(period)

    async def run(self) -> None:
        asyncio.create_task(self._snapshot_loop())
        while not self._stop:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    self.ws_ok = True

                    # subscribe depth + deal (contract)
                    await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": SYMBOL_WS}}))
                    await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": SYMBOL_WS}}))

                    while not self._stop:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = try_decompress(raw).decode("utf-8", errors="ignore")
                        msg = json.loads(raw)

                        ch = msg.get("channel") or msg.get("ch")

                        if ch in ("push.depth", "rs.push.depth"):
                            data = msg.get("data", {}) or {}
                            bids = data.get("bids", []) or []
                            asks = data.get("asks", []) or []
                            # IMPORTANT: partial updates; never wipe other side
                            self.book.apply_side_updates("bids", bids)
                            self.book.apply_side_updates("asks", asks)
                            self.last_depth_ms = now_ms()

                        if ch in ("push.deal", "rs.push.deal"):
                            data = msg.get("data", [])
                            if isinstance(data, list):
                                for t in data:
                                    p = safe_float(t.get("p"))
                                    v = safe_float(t.get("v"))
                                    try:
                                        T = int(t.get("T", 0))
                                    except Exception:
                                        T = t.get("T", 0)
                                    tm = int(t.get("t", now_ms()))
                                    tt = {"p": p, "v": v, "T": T, "t": tm}
                                    self.trades.append(tt)
                                    if p > 0:
                                        self.last_px = p
                                    self.last_trade_ms = now_ms()

            except Exception:
                self.ws_ok = False
                await asyncio.sleep(1.5)

# -----------------------------
# Web App
# -----------------------------
app = FastAPI()
feed = MexcFeed()

HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1, maximum-scale=1, user-scalable=no" />
  <title>QuantDesk Bookmap UI (Time Zoom + True Heat)</title>
  <style>
    html, body { margin:0; padding:0; background:#0b0f14; color:#cbd5e1; height:100%; overflow:hidden; }
    #topbar {
      padding:10px 12px;
      font-family: -apple-system, system-ui, Arial;
      font-size:14px;
      display:flex;
      gap:10px;
      align-items:center;
      border-bottom:1px solid #111827;
      user-select:none;
      -webkit-user-select:none;
      flex-wrap:wrap;
    }
    #statusDot { width:10px; height:10px; border-radius:50%; display:inline-block; background:#ef4444; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:12px; color:#93c5fd; }
    .btn {
      padding:6px 10px;
      border:1px solid #1f2937;
      border-radius:10px;
      background:#0b1220;
      color:#cbd5e1;
      font-size:12px;
    }
    #wrap { height: calc(100% - 56px); display:flex; }
    canvas { width: 100%; height: 100%; display:block; touch-action:none; } /* IMPORTANT for iPad gestures */
  </style>
</head>
<body>
  <div id="topbar">
    <span id="statusDot"></span>
    <span id="sym"></span>
    <span class="mono" id="health"></span>

    <button class="btn" id="btnFollow">AutoFollow: ON</button>

    <button class="btn" id="btnTimeOut">-T</button>
    <button class="btn" id="btnTimeIn">+T</button>

    <button class="btn" id="btnPriceOut">-P</button>
    <button class="btn" id="btnPriceIn">+P</button>

    <span class="mono" id="viewInfo"></span>
  </div>
  <div id="wrap">
    <canvas id="cv"></canvas>
  </div>

<script>
(() => {
  const cv = document.getElementById("cv");
  const ctx = cv.getContext("2d");
  const symEl = document.getElementById("sym");
  const dot = document.getElementById("statusDot");
  const healthEl = document.getElementById("health");
  const viewInfoEl = document.getElementById("viewInfo");

  const btnFollow = document.getElementById("btnFollow");
  const btnTimeOut = document.getElementById("btnTimeOut");
  const btnTimeIn  = document.getElementById("btnTimeIn");
  const btnPriceOut = document.getElementById("btnPriceOut");
  const btnPriceIn  = document.getElementById("btnPriceIn");

  function resize() {
    const dpr = window.devicePixelRatio || 1;
    cv.width = Math.floor(cv.clientWidth * dpr);
    cv.height = Math.floor(cv.clientHeight * dpr);
    ctx.setTransform(dpr,0,0,dpr,0,0);
  }
  window.addEventListener("resize", resize);
  resize();

  // -----------------------------
  // Data state
  // -----------------------------
  let last = null;
  let trades = [];
  let lastPx = null;
  let midPx = null;

  // -----------------------------
  // View state (interactive)
  // -----------------------------
  let autoFollow = true;

  // Price axis
  let viewMid = null;                 // center price
  let viewSpan = 600;                 // visible price range (USD)
  const MIN_SPAN = 40;
  const MAX_SPAN = 20000;

  // Time axis
  let timeEndMs = null;               // right edge time (ms)
  let timeSpanMs = 12 * 60 * 1000;    // visible window (ms)
  const MIN_TIME_SPAN_MS = 15 * 1000;
  const MAX_TIME_SPAN_MS = 8 * 60 * 60 * 1000;

  // -----------------------------
  // Heatmap state (TRUE time×price)
  // Stored as ring buffer of columns.
  // -----------------------------
  let BINS = 360;
  let HEAT_COL_MS = 1000;
  let HEAT_WINDOW_MS = 60 * 60 * 1000;

  let heatCols = 3600;                  // derived
  let heat = null;                      // Float32Array length = BINS * heatCols (row-major: [bin * heatCols + col])
  let colTimes = null;                  // Int32Array length = heatCols (timestamps)
  let colWrite = 0;
  let lastColPushMs = null;

  // Exposure control
  let vmaxEwma = 1e-6;

  function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

  function initHeatFromDefaults(defs) {
    if (!defs) return;
    BINS = defs.heat_bins ?? BINS;
    HEAT_COL_MS = defs.heat_col_ms ?? HEAT_COL_MS;
    const winSec = defs.heat_window_sec ?? (HEAT_WINDOW_MS / 1000);
    HEAT_WINDOW_MS = Math.max(60*1000, winSec * 1000);

    heatCols = Math.max(120, Math.floor(HEAT_WINDOW_MS / HEAT_COL_MS));
    heat = new Float32Array(BINS * heatCols);
    colTimes = new Int32Array(heatCols);
    colWrite = 0;
    lastColPushMs = null;
    vmaxEwma = 1e-6;
  }

  function clearHeat() {
    if (!heat) return;
    heat.fill(0);
    colTimes.fill(0);
    colWrite = 0;
    lastColPushMs = null;
    vmaxEwma = 1e-6;
  }

  // -----------------------------
  // UI buttons
  // -----------------------------
  function setFollow(on) {
    autoFollow = !!on;
    btnFollow.textContent = `AutoFollow: ${autoFollow ? "ON" : "OFF"}`;
    if (autoFollow) {
      // snap time to now
      timeEndMs = Date.now();
    }
  }

  btnFollow.onclick = () => setFollow(!autoFollow);

  btnTimeOut.onclick = () => {
    setFollow(false);
    timeSpanMs = clamp(timeSpanMs * 1.25, MIN_TIME_SPAN_MS, MAX_TIME_SPAN_MS);
  };
  btnTimeIn.onclick = () => {
    setFollow(false);
    timeSpanMs = clamp(timeSpanMs / 1.25, MIN_TIME_SPAN_MS, MAX_TIME_SPAN_MS);
  };
  btnPriceOut.onclick = () => {
    setFollow(false);
    viewSpan = clamp(viewSpan * 1.25, MIN_SPAN, MAX_SPAN);
  };
  btnPriceIn.onclick = () => {
    setFollow(false);
    viewSpan = clamp(viewSpan / 1.25, MIN_SPAN, MAX_SPAN);
  };

  // -----------------------------
  // WebSocket
  // -----------------------------
  function wsUrl() {
    const proto = (location.protocol === "https:") ? "wss" : "ws";
    return `${proto}://${location.host}/ws`;
  }
  const WS = new WebSocket(wsUrl());

  WS.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    last = msg;

    dot.style.background = msg.ws_ok ? "#22c55e" : "#ef4444";

    lastPx = msg.last_px ?? lastPx;
    midPx  = msg.mid_px ?? midPx;

    // initialize heat defaults once
    if (heat === null && msg.ui_defaults) {
      initHeatFromDefaults(msg.ui_defaults);
      if (msg.ui_defaults.default_price_span) viewSpan = msg.ui_defaults.default_price_span;
    }

    // Initialize view from lastPx once
    if (viewMid === null && (lastPx || midPx)) {
      viewMid = (lastPx ?? midPx);
      viewSpan = viewSpan || 600;
      timeEndMs = Date.now();
      timeSpanMs = 12 * 60 * 1000;
      clearHeat();
    }

    // show price audit
    const lp = (lastPx != null) ? lastPx.toFixed(3) : "None";
    const mp = (midPx  != null) ? midPx.toFixed(3)  : "None";
    symEl.textContent = `${msg.symbol} | last_trade=${lp} | mid=${mp}`;

    healthEl.textContent =
      `book: bids=${msg.health.bids_n} asks=${msg.health.asks_n} ` +
      `age: depth=${msg.health.depth_age_ms ?? "None"}ms trade=${msg.health.trade_age_ms ?? "None"}ms ` +
      `WS=${(msg.ws_url || "").replace("wss://","")}`;

    // Maintain rolling trade window for memory (we will render per current time view)
    // We append trades from the server snapshot then de-dup roughly by timestamp+price+v+T.
    if (Array.isArray(msg.trades)) {
      for (const t of msg.trades) trades.push(t);
    }
    // Keep a generous trade memory, tied to HEAT_WINDOW_MS (plus slack)
    const now = Date.now();
    const keepMs = Math.max(HEAT_WINDOW_MS, timeSpanMs) + 5 * 60 * 1000;
    trades = trades.filter(t => (now - t.t) <= keepMs);

    // If follow is on, keep timeEnd pinned to now
    if (autoFollow) {
      timeEndMs = now;
    }
  };

  WS.onclose = () => { dot.style.background = "#ef4444"; };

  // -----------------------------
  // Interaction (iPad + desktop)
  // Gesture logic:
  // - 1 finger drag vertical => price pan
  // - 1 finger drag horizontal => time pan
  // - 2 finger pinch: dominant axis decides zoom target:
  //     mostly vertical => price zoom
  //     mostly horizontal => time zoom
  // -----------------------------
  let pointers = new Map(); // pointerId -> {x,y}
  let pinchStartDist = null;
  let pinchStartSpanPrice = null;
  let pinchStartSpanTime = null;
  let pinchAxisMode = null; // "price" | "time"

  let dragStartX = null;
  let dragStartY = null;
  let dragStartMid = null;
  let dragStartTimeEnd = null;
  let dragMode = null; // "price" | "time"

  cv.addEventListener("pointerdown", (e) => {
    cv.setPointerCapture(e.pointerId);
    pointers.set(e.pointerId, {x: e.clientX, y: e.clientY});

    if (pointers.size === 1) {
      dragStartX = e.clientX;
      dragStartY = e.clientY;
      dragStartMid = viewMid;
      dragStartTimeEnd = timeEndMs;
      dragMode = null; // decide on first meaningful move
    }
    if (pointers.size === 2) {
      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x;
      const dy = pts[0].y - pts[1].y;
      pinchStartDist = Math.sqrt(dx*dx + dy*dy);
      pinchStartSpanPrice = viewSpan;
      pinchStartSpanTime = timeSpanMs;
      pinchAxisMode = null; // decide based on dominant axis
    }
  });

  cv.addEventListener("pointermove", (e) => {
    if (!pointers.has(e.pointerId)) return;
    pointers.set(e.pointerId, {x: e.clientX, y: e.clientY});
    if (viewMid === null || timeEndMs === null) return;

    // One finger drag
    if (pointers.size === 1 && dragStartY !== null && dragStartX !== null) {
      setFollow(false);

      const dx = e.clientX - dragStartX;
      const dy = e.clientY - dragStartY;

      if (dragMode === null) {
        // decide mode: horizontal => time, vertical => price
        dragMode = (Math.abs(dx) > Math.abs(dy)) ? "time" : "price";
      }

      if (dragMode === "price") {
        const pxPerPrice = cv.clientHeight / Math.max(1e-6, viewSpan);
        const dPrice = dy / Math.max(1, pxPerPrice);
        viewMid = (dragStartMid ?? viewMid) + dPrice;
      } else {
        // time pan: dx pixels => ms shift
        const msPerPx = timeSpanMs / Math.max(1, cv.clientWidth);
        const dMs = dx * msPerPx;
        timeEndMs = (dragStartTimeEnd ?? timeEndMs) - dMs; // drag right shows past
        timeEndMs = clamp(timeEndMs, Date.now() - HEAT_WINDOW_MS, Date.now());
      }
    }

    // Two finger pinch
    if (pointers.size === 2 && pinchStartDist && pinchStartSpanPrice && pinchStartSpanTime) {
      setFollow(false);

      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x;
      const dy = pts[0].y - pts[1].y;
      const dist = Math.sqrt(dx*dx + dy*dy);

      if (pinchAxisMode === null) {
        pinchAxisMode = (Math.abs(dx) > Math.abs(dy)) ? "time" : "price";
      }

      const scale = pinchStartDist / Math.max(10, dist);

      if (pinchAxisMode === "price") {
        viewSpan = clamp(pinchStartSpanPrice * scale, MIN_SPAN, MAX_SPAN);
      } else {
        timeSpanMs = clamp(pinchStartSpanTime * scale, MIN_TIME_SPAN_MS, MAX_TIME_SPAN_MS);
      }
    }
  });

  function endPointer(e) {
    pointers.delete(e.pointerId);
    if (pointers.size < 2) {
      pinchStartDist = null;
      pinchStartSpanPrice = null;
      pinchStartSpanTime = null;
      pinchAxisMode = null;
    }
    if (pointers.size === 0) {
      dragStartX = null;
      dragStartY = null;
      dragStartMid = null;
      dragStartTimeEnd = null;
      dragMode = null;
    }
  }
  cv.addEventListener("pointerup", endPointer);
  cv.addEventListener("pointercancel", endPointer);
  cv.addEventListener("pointerout", endPointer);

  // Desktop wheel: zoom price. Shift+wheel: zoom time
  cv.addEventListener("wheel", (e) => {
    if (viewMid === null || timeEndMs === null) return;
    e.preventDefault();
    setFollow(false);

    const factor = (e.deltaY > 0) ? 1.10 : 0.90;

    if (e.shiftKey) {
      timeSpanMs = clamp(timeSpanMs * factor, MIN_TIME_SPAN_MS, MAX_TIME_SPAN_MS);
    } else {
      viewSpan = clamp(viewSpan * factor, MIN_SPAN, MAX_SPAN);
    }
  }, {passive:false});

  // -----------------------------
  // Rendering utilities
  // -----------------------------
  function yOf(p, pMin, pMax, h) {
    const t = (p - pMin) / (pMax - pMin);
    return h - t*h;
  }

  function heatIndex(p, pMin, pMax) {
    const t = (p - pMin) / (pMax - pMin);
    const idx = Math.floor((1 - t) * (BINS - 1)); // top=0
    return idx;
  }

  // Single-channel intensity colormap (Bookmap-like glow: dark->cyan/yellow->white)
  function heatRGBA(a) {
    // a in [0..1]
    // We'll create a smooth gradient: low=transparent dark, mid=cyan, high=yellow/white
    const t = clamp(a, 0, 1);
    const g1 = Math.pow(t, 0.55);

    let r, g, b, alpha;
    alpha = 0.05 + 0.90 * g1;

    if (t < 0.55) {
      // dark -> cyan/blue
      const u = t / 0.55;
      r = 20 + 40*u;
      g = 40 + 170*u;
      b = 80 + 170*u;
    } else {
      // cyan -> yellow -> white
      const u = (t - 0.55) / 0.45;
      r = 60 + 195*u;
      g = 210 + 45*u;
      b = 250 - 170*u;
    }
    return `rgba(${Math.floor(r)},${Math.floor(g)},${Math.floor(b)},${alpha.toFixed(3)})`;
  }

  function heatGet(bin, col) {
    return heat[bin * heatCols + col];
  }
  function heatSet(bin, col, v) {
    heat[bin * heatCols + col] = v;
  }

  function pushHeatColumn(pMin, pMax) {
    if (!last || !last.book || !heat) return;

    const bids = last.book.bids || [];
    const asks = last.book.asks || [];

    // Slow decay across entire matrix once per column push (true persistence)
    // Half-life ~ 90s (tunable)
    const halfLife = 90.0;
    const decay = Math.pow(0.5, (HEAT_COL_MS/1000.0) / halfLife);
    for (let i=0; i<heat.length; i++) {
      heat[i] *= decay;
    }

    // Build new column vector from RESTING book liquidity (both sides, intensity-based)
    const col = colWrite;
    // clear current column (after decay, set to 0 so it represents "new density added this second")
    for (let b=0; b<BINS; b++) heatSet(b, col, 0);

    // Intensity weight:
    // Use log(1+notional) to reduce domination by huge levels.
    function addLevels(levels) {
      for (const pair of levels) {
        const p = pair[0], q = pair[1];
        if (!(q > 0) || !(p > 0)) continue;
        const idx = heatIndex(p, pMin, pMax);
        if (idx < 0 || idx >= BINS) continue;
        const notional = p * q;
        const w = Math.log(1 + notional);
        heatSet(idx, col, heatGet(idx, col) + w);
      }
    }
    addLevels(bids);
    addLevels(asks);

    // timestamp this column
    colTimes[col] = Date.now();

    // update exposure (EWMA of column max)
    let cmax = 0;
    for (let b=0; b<BINS; b++) cmax = Math.max(cmax, heatGet(b, col));
    cmax = Math.max(1e-6, cmax);
    const alpha = 0.08;
    vmaxEwma = (1-alpha)*vmaxEwma + alpha*cmax;

    colWrite = (colWrite + 1) % heatCols;
  }

  function orderedColIndex(oldestToNewestIdx) {
    // Convert ordered index (0 oldest .. heatCols-1 newest) into ring physical index
    // Newest column is at (colWrite-1), oldest at colWrite
    // Our ring writes at colWrite then increments, so colWrite is the "oldest slot".
    const phys = (colWrite + oldestToNewestIdx) % heatCols;
    return phys;
  }

  // -----------------------------
  // Main draw loop
  // -----------------------------
  function draw() {
    const w = cv.clientWidth;
    const h = cv.clientHeight;
    ctx.clearRect(0,0,w,h);

    // background
    ctx.fillStyle = "#0b0f14";
    ctx.fillRect(0,0,w,h);

    if (!last || !last.book || viewMid === null || timeEndMs === null || !heat) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "14px -apple-system, system-ui, Arial";
      ctx.fillText("Waiting for data...", 12, 24);
      requestAnimationFrame(draw);
      return;
    }

    // Auto-follow keeps center locked to last price AND time pinned to now
    if (autoFollow) {
      if (lastPx) viewMid = lastPx;
      timeEndMs = Date.now();
    }

    const pMin = viewMid - viewSpan/2;
    const pMax = viewMid + viewSpan/2;

    // push heat columns on a fixed cadence
    const now = Date.now();
    if (lastColPushMs === null) lastColPushMs = now;
    while ((now - lastColPushMs) >= HEAT_COL_MS) {
      pushHeatColumn(pMin, pMax);
      lastColPushMs += HEAT_COL_MS;
    }

    // Time visible range
    const tEnd = timeEndMs;
    const tStart = tEnd - timeSpanMs;

    // Render TRUE heatmap: time×price
    // We draw vertical strips per heat column within visible time.
    // For each column, we draw bins with intensity.
    const colsToDraw = Math.min(heatCols, Math.max(60, Math.floor(timeSpanMs / HEAT_COL_MS)));
    // determine mapping from time -> ordered column indices by looking at colTimes
    // We'll draw by scanning columns and selecting those in [tStart..tEnd]
    // For speed, approximate by drawing all columns but skip those outside.
    const stripW = w / Math.max(1, colsToDraw);

    // Find columns in time window; build a list (oldest->newest)
    const selected = [];
    for (let k=0; k<heatCols; k++) {
      const phys = orderedColIndex(k);
      const ts = colTimes[phys] || 0;
      if (ts >= tStart && ts <= tEnd) selected.push(phys);
    }

    // If not enough selected (startup), use the most recent tail
    if (selected.length < 10) {
      selected.length = 0;
      for (let k=Math.max(0, heatCols - colsToDraw); k<heatCols; k++) {
        selected.push(orderedColIndex(k));
      }
    }

    // Draw heat strips
    const binH = h / BINS;
    const vmax = Math.max(1e-6, vmaxEwma);

    // Use mild gamma to increase low-intensity visibility
    const gamma = 0.65;

    // selected is in chronological order by orderedColIndex scanning; good enough
    const nSel = selected.length;
    for (let i=0; i<nSel; i++) {
      const col = selected[i];
      const x = (i / Math.max(1, nSel)) * w;
      // draw bins
      // (This is BINS*nSel rectangles per frame; BINS=360, nSel~720 => heavy but acceptable on modern devices.
      // If needed later, we optimize via ImageData/offscreen.)
      for (let b=0; b<BINS; b++) {
        const v = heatGet(b, col);
        if (v <= 0) continue;
        let a = clamp(v / vmax, 0, 1);
        a = Math.pow(a, gamma);
        if (a < 0.02) continue;
        ctx.fillStyle = heatRGBA(a);
        ctx.fillRect(x, b * binH, stripW + 1, binH + 1);
      }
    }

    // Light grid (price)
    ctx.strokeStyle = "rgba(148,163,184,0.08)";
    ctx.lineWidth = 1;
    const gridN = 8;
    for (let k=1;k<gridN;k++) {
      const yy = (h/gridN)*k;
      ctx.beginPath();
      ctx.moveTo(0, yy);
      ctx.lineTo(w, yy);
      ctx.stroke();
    }

    // Trade bubbles (time on X, price on Y) based on current time view
    const xOf = (tms) => {
      const frac = (tms - tStart) / Math.max(1, (tEnd - tStart));
      return clamp(frac, 0, 1) * w;
    };

    let vMax = 0;
    for (const t of trades) {
      if (t.t >= tStart && t.t <= tEnd) vMax = Math.max(vMax, t.v || 0);
    }
    vMax = Math.max(vMax, 1e-9);

    for (const t of trades) {
      const tms = t.t;
      if (!(tms >= tStart && tms <= tEnd)) continue;

      const p = t.p;
      if (!(p >= pMin && p <= pMax)) continue;

      const x = xOf(tms);
      const y = yOf(p, pMin, pMax, h);

      const v = (t.v || 0);
      const r = 1.5 + 16 * Math.sqrt(v / vMax);

      const isBuy = (t.T === 1);
      ctx.beginPath();
      ctx.fillStyle = isBuy ? "rgba(59,130,246,0.70)" : "rgba(245,158,11,0.70)";
      ctx.arc(x, y, r, 0, Math.PI*2);
      ctx.fill();
    }

    // Last trade price line
    if (lastPx) {
      const y = yOf(lastPx, pMin, pMax, h);
      ctx.strokeStyle = "rgba(226,232,240,0.95)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0,y);
      ctx.lineTo(w,y);
      ctx.stroke();

      ctx.fillStyle = "rgba(226,232,240,0.95)";
      ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`LAST ${lastPx.toFixed(3)}`, 10, clamp(y-6, 14, h-8));
    }

    // Mid price line (optional)
    if (midPx) {
      const y = yOf(midPx, pMin, pMax, h);
      ctx.strokeStyle = "rgba(148,163,184,0.55)";
      ctx.lineWidth = 1;
      ctx.setLineDash([4,4]);
      ctx.beginPath();
      ctx.moveTo(0,y);
      ctx.lineTo(w,y);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle = "rgba(148,163,184,0.85)";
      ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`MID ${midPx.toFixed(3)}`, 10, clamp(y+14, 18, h-4));
    }

    // View info (topbar)
    const spanSec = (timeSpanMs/1000).toFixed(1);
    const ageMs = (last && last.health) ? (last.health.trade_age_ms ?? "None") : "None";
    viewInfoEl.textContent = `Tspan=${spanSec}s | Pspan=${viewSpan.toFixed(1)} | trade_age=${ageMs}ms | history=${Math.floor(HEAT_WINDOW_MS/60000)}m`;

    requestAnimationFrame(draw);
  }

  requestAnimationFrame(draw);
})();
</script>
</body>
</html>
"""

@app.on_event("startup")
async def startup() -> None:
    asyncio.create_task(feed.run())

@app.get("/")
async def index():
    return HTMLResponse(HTML)

@app.get("/health")
async def health():
    bb, ba = feed.book.best_bid_ask()
    mid = (bb + ba)/2.0 if (bb is not None and ba is not None and bb > 0 and ba > 0) else None
    return {
        "service": "quantdesk-bookmap-ui",
        "symbol": SYMBOL_WS,
        "ws_url": WS_URL,
        "ws_ok": feed.ws_ok,
        "last_px": feed.last_px,
        "mid_px": mid,
        "best_bid": bb,
        "best_ask": ba,
        "bids_n": len(feed.book.bids),
        "asks_n": len(feed.book.asks),
        "trades_n": len(feed.trades.dq),
        "depth_age_ms": (now_ms() - feed.last_depth_ms) if feed.last_depth_ms else None,
        "trade_age_ms": (now_ms() - feed.last_trade_ms) if feed.last_trade_ms else None,
        "ui_defaults": {
            "heat_window_sec": HEAT_WINDOW_SEC,
            "heat_col_ms": HEAT_COL_MS,
            "heat_bins": HEAT_BINS,
            "default_price_span": DEFAULT_PRICE_SPAN,
        }
    }

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await feed.add_client(ws)
    try:
        while True:
            await asyncio.sleep(10)
    except Exception:
        pass
    finally:
        await feed.remove_client(ws)

if __name__ == "__main__":
    import uvicorn
    # Run directly without relying on filename/module import name
    uvicorn.run(app, host="0.0.0.0", port=PORT)
