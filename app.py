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
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")  # MEXC WS uses BTC_USDT
PORT = int(os.environ.get("PORT", "8000"))

MAX_TRADES = int(os.environ.get("QD_MAX_TRADES", "6000"))
TOP_LEVELS = int(os.environ.get("QD_TOP_LEVELS", "200"))         # send depth levels each side
SNAPSHOT_HZ = float(os.environ.get("QD_SNAPSHOT_HZ", "6"))        # UI snapshots/sec

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
        trades = list(self.trades.dq)[-1200:]  # keep payload sane

        payload = {
            "ts": now_ms(),
            "symbol": SYMBOL_WS,
            "ws_ok": self.ws_ok,
            "last_px": self.last_px,
            "book": {"bids": book["bids"], "asks": book["asks"]},
            "trades": trades,
            "health": {
                "depth_age_ms": (now_ms() - self.last_depth_ms) if self.last_depth_ms else None,
                "trade_age_ms": (now_ms() - self.last_trade_ms) if self.last_trade_ms else None,
                "bids_n": len(self.book.bids),
                "asks_n": len(self.book.asks),
                "trades_n": len(self.trades.dq),
            },
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

                    # subscribe depth + deal
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
  <title>Bookmap UI MVP (Interactive)</title>
  <style>
    html, body { margin:0; padding:0; background:#0b0f14; color:#cbd5e1; height:100%; overflow:hidden; }
    #topbar {
      padding:10px 12px;
      font-family: -apple-system, system-ui, Arial;
      font-size:14px;
      display:flex;
      gap:12px;
      align-items:center;
      border-bottom:1px solid #111827;
      user-select:none;
      -webkit-user-select:none;
    }
    #statusDot { width:10px; height:10px; border-radius:50%; display:inline-block; background:#ef4444; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:12px; color:#93c5fd; }
    #btn {
      margin-left:auto;
      padding:6px 10px;
      border:1px solid #1f2937;
      border-radius:10px;
      background:#0b1220;
      color:#cbd5e1;
      font-size:12px;
    }
    #wrap { height: calc(100% - 44px); display:flex; }
    canvas { width: 100%; height: 100%; display:block; touch-action:none; } /* IMPORTANT for iPad gestures */
  </style>
</head>
<body>
  <div id="topbar">
    <span id="statusDot"></span>
    <span id="sym"></span>
    <span class="mono" id="health"></span>
    <button id="btn">AutoFollow: ON</button>
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
  const btn = document.getElementById("btn");

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

  // -----------------------------
  // View state (interactive)
  // -----------------------------
  let autoFollow = true;
  let viewMid = null;    // center price
  let viewSpan = 600;    // visible price range (USD)
  const MIN_SPAN = 40;
  const MAX_SPAN = 8000;

  btn.onclick = () => {
    autoFollow = !autoFollow;
    btn.textContent = `AutoFollow: ${autoFollow ? "ON" : "OFF"}`;
  };

  // -----------------------------
  // Heatmap state (persistence)
  // We accumulate depth into bins and decay over time.
  // -----------------------------
  const BINS = 360;                 // vertical resolution
  let heatBid = new Float32Array(BINS);
  let heatAsk = new Float32Array(BINS);
  let lastHeatTs = performance.now();

  function resetHeat() {
    heatBid = new Float32Array(BINS);
    heatAsk = new Float32Array(BINS);
  }

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

    symEl.textContent = `${msg.symbol} | last_px=${(lastPx ?? "None")}`;
    healthEl.textContent =
      `bids=${msg.health.bids_n} asks=${msg.health.asks_n} trades=${msg.health.trades_n} ` +
      `depth_age_ms=${msg.health.depth_age_ms ?? "None"} trade_age_ms=${msg.health.trade_age_ms ?? "None"}`;

    // Maintain rolling trade window
    const now = Date.now();
    const windowMs = 12 * 60 * 1000; // increase to 12m for nicer flow
    if (Array.isArray(msg.trades)) {
      // NOTE: server sends last ~1200; we just append; filter by time
      for (const t of msg.trades) trades.push(t);
    }
    trades = trades.filter(t => (now - t.t) <= windowMs);

    // Initialize view from lastPx once
    if (viewMid === null && lastPx) {
      viewMid = lastPx;
      viewSpan = 600;
      resetHeat();
    }
  };

  WS.onclose = () => { dot.style.background = "#ef4444"; };

  // -----------------------------
  // Interaction (iPad + desktop)
  // - Drag: pan price
  // - Pinch: zoom price
  // -----------------------------
  let pointers = new Map(); // pointerId -> {x,y}
  let pinchStartDist = null;
  let pinchStartSpan = null;
  let dragStartY = null;
  let dragStartMid = null;

  function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

  cv.addEventListener("pointerdown", (e) => {
    cv.setPointerCapture(e.pointerId);
    pointers.set(e.pointerId, {x: e.clientX, y: e.clientY});

    if (pointers.size === 1) {
      dragStartY = e.clientY;
      dragStartMid = viewMid;
    }
    if (pointers.size === 2) {
      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x;
      const dy = pts[0].y - pts[1].y;
      pinchStartDist = Math.sqrt(dx*dx + dy*dy);
      pinchStartSpan = viewSpan;
    }
  });

  cv.addEventListener("pointermove", (e) => {
    if (!pointers.has(e.pointerId)) return;
    pointers.set(e.pointerId, {x: e.clientX, y: e.clientY});

    if (viewMid === null) return;

    // One finger drag: pan
    if (pointers.size === 1 && dragStartY !== null) {
      autoFollow = false;
      btn.textContent = "AutoFollow: OFF";

      const dy = e.clientY - dragStartY;
      // dy pixels -> price delta
      const pxPerPrice = cv.clientHeight / viewSpan;
      const dPrice = dy / Math.max(1, pxPerPrice);
      viewMid = (dragStartMid ?? viewMid) + dPrice;
    }

    // Two finger pinch: zoom
    if (pointers.size === 2 && pinchStartDist && pinchStartSpan) {
      autoFollow = false;
      btn.textContent = "AutoFollow: OFF";

      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x;
      const dy = pts[0].y - pts[1].y;
      const dist = Math.sqrt(dx*dx + dy*dy);
      const scale = pinchStartDist / Math.max(10, dist);
      viewSpan = clamp(pinchStartSpan * scale, MIN_SPAN, MAX_SPAN);
    }
  });

  function endPointer(e) {
    pointers.delete(e.pointerId);
    if (pointers.size < 2) {
      pinchStartDist = null;
      pinchStartSpan = null;
    }
    if (pointers.size === 0) {
      dragStartY = null;
      dragStartMid = null;
    }
  }
  cv.addEventListener("pointerup", endPointer);
  cv.addEventListener("pointercancel", endPointer);
  cv.addEventListener("pointerout", endPointer);

  // Desktop wheel zoom (optional)
  cv.addEventListener("wheel", (e) => {
    if (viewMid === null) return;
    e.preventDefault();
    autoFollow = false;
    btn.textContent = "AutoFollow: OFF";

    const factor = (e.deltaY > 0) ? 1.10 : 0.90;
    viewSpan = clamp(viewSpan * factor, MIN_SPAN, MAX_SPAN);
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

  function percentile(arr, q) {
    // arr: Float32Array, q in [0,1]
    const tmp = Array.from(arr);
    tmp.sort((a,b) => a-b);
    const i = Math.floor(q * (tmp.length-1));
    return tmp[clamp(i, 0, tmp.length-1)];
  }

  // Color map (simple "bookmap-ish" glow)
  // intensity -> rgba with stronger at high.
  function heatColor(isBid, a) {
    // a in [0,1]
    if (isBid) {
      // green/cyan glow
      const g = 180 + Math.floor(75*a);
      const b = 120 + Math.floor(90*a);
      return `rgba(40, ${g}, ${b}, ${0.10 + 0.70*a})`;
    } else {
      // red/orange glow
      const r = 200 + Math.floor(55*a);
      const g = 90 + Math.floor(60*a);
      return `rgba(${r}, ${g}, 80, ${0.10 + 0.70*a})`;
    }
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

    if (!last || !last.book || viewMid === null) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "14px -apple-system, system-ui, Arial";
      ctx.fillText("Waiting for data...", 12, 24);
      requestAnimationFrame(draw);
      return;
    }

    // Auto-follow keeps center locked to last price
    if (autoFollow && lastPx) {
      viewMid = lastPx;
    }

    const pMin = viewMid - viewSpan/2;
    const pMax = viewMid + viewSpan/2;

    // Decay heatmap over time
    const tNow = performance.now();
    const dt = Math.max(0.0, (tNow - lastHeatTs) / 1000.0);
    lastHeatTs = tNow;
    // half-life feel: ~8 seconds (tune)
    const decay = Math.pow(0.5, dt / 8.0);
    for (let i=0;i<BINS;i++) {
      heatBid[i] *= decay;
      heatAsk[i] *= decay;
    }

    // Accumulate latest depth into heat bins
    const bids = last.book.bids || [];
    const asks = last.book.asks || [];

    // Add log-scaled qty to bins (reduces domination by huge levels)
    for (const [p,q] of bids) {
      const idx = heatIndex(p, pMin, pMax);
      if (idx >= 0 && idx < BINS && q > 0) heatBid[idx] += Math.log(1 + q);
    }
    for (const [p,q] of asks) {
      const idx = heatIndex(p, pMin, pMax);
      if (idx >= 0 && idx < BINS && q > 0) heatAsk[idx] += Math.log(1 + q);
    }

    // Normalize heat by percentile (auto exposure)
    const bidP = Math.max(1e-6, percentile(heatBid, 0.98));
    const askP = Math.max(1e-6, percentile(heatAsk, 0.98));

    // Draw heatmap as vertical bins (full width)
    const binH = h / BINS;
    for (let i=0;i<BINS;i++) {
      const y = i * binH;
      const vb = clamp(heatBid[i] / bidP, 0, 1);
      const va = clamp(heatAsk[i] / askP, 0, 1);

      // Blend ask + bid; draw ask then bid for richer mixing
      if (va > 0.02) {
        ctx.fillStyle = heatColor(false, Math.pow(va, 0.75));
        ctx.fillRect(0, y, w, binH + 1);
      }
      if (vb > 0.02) {
        ctx.fillStyle = heatColor(true, Math.pow(vb, 0.75));
        ctx.fillRect(0, y, w, binH + 1);
      }
    }

    // Price axis grid (light)
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

    // Trade bubbles (time on X, price on Y)
    const now = Date.now();
    const windowMs = 12 * 60 * 1000;
    const xOf = (tms) => {
      const dtm = now - tms;
      const frac = 1 - (dtm / windowMs);
      return clamp(frac, 0, 1) * w;
    };

    let vMax = 0;
    for (const t of trades) vMax = Math.max(vMax, t.v || 0);
    vMax = Math.max(vMax, 1e-9);

    for (const t of trades) {
      const p = t.p;
      if (!(p >= pMin && p <= pMax)) continue;
      const x = xOf(t.t);
      const y = yOf(p, pMin, pMax, h);

      const v = (t.v || 0);
      const r = 1.5 + 16 * Math.sqrt(v / vMax);

      const isBuy = (t.T === 1);
      ctx.beginPath();
      ctx.fillStyle = isBuy ? "rgba(59,130,246,0.65)" : "rgba(245,158,11,0.65)";
      ctx.arc(x, y, r, 0, Math.PI*2);
      ctx.fill();
    }

    // Last price line + label
    if (lastPx) {
      const y = yOf(lastPx, pMin, pMax, h);
      ctx.strokeStyle = "rgba(226,232,240,0.9)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0,y);
      ctx.lineTo(w,y);
      ctx.stroke();

      ctx.fillStyle = "rgba(226,232,240,0.9)";
      ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`PX ${lastPx.toFixed(2)}`, 10, clamp(y-6, 14, h-8));
    }

    // Footer scale text
    ctx.fillStyle = "rgba(148,163,184,0.85)";
    ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
    ctx.fillText(`view: ${pMin.toFixed(1)} .. ${pMax.toFixed(1)} | span=${viewSpan.toFixed(1)} | 1-finger pan | pinch zoom`, 10, h - 10);

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
    return {
        "service": "bookmap-ui-mvp",
        "symbol": SYMBOL_WS,
        "ws_ok": feed.ws_ok,
        "last_px": feed.last_px,
        "bids_n": len(feed.book.bids),
        "asks_n": len(feed.book.asks),
        "trades_n": len(feed.trades.dq),
        "depth_age_ms": (now_ms() - feed.last_depth_ms) if feed.last_depth_ms else None,
        "trade_age_ms": (now_ms() - feed.last_trade_ms) if feed.last_trade_ms else None,
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
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
