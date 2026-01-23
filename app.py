import os
import json
import time
import gzip
import zlib
import asyncio
import base64
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

# ============================================================
# QuantDesk Bookmap Service (Absolute Grid Heatmap Fix)
# - Resting liquidity heatmap stored on an ABSOLUTE price grid
# - Time columns are fixed (COL_MS), kept in a ring buffer
# - UI preserves AutoFollow + pinch zoom (price) + time zoom (-T/+T)
# - Heat does NOT "jump" on zoom because price grid is fixed
# ============================================================

# -----------------------------
# Config (env)
# -----------------------------
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")  # MEXC WS uses BTC_USDT
PORT = int(os.environ.get("PORT", "5000"))

MAX_TRADES = int(os.environ.get("QD_MAX_TRADES", "6000"))

# Depth snapshot for UI (book ladder & trades)
TOP_LEVELS = int(os.environ.get("QD_TOP_LEVELS", "250"))          # send depth levels each side
SNAPSHOT_HZ = float(os.environ.get("QD_SNAPSHOT_HZ", "6"))         # UI snapshots/sec

# Heatmap timeline storage
HEAT_WINDOW_SEC = int(os.environ.get("QD_HEAT_WINDOW_SEC", "3600"))  # 60m
HEAT_COL_MS = int(os.environ.get("QD_HEAT_COL_MS", "1000"))          # 1s columns

# Absolute price grid
PRICE_BIN_USD = float(os.environ.get("QD_PRICE_BIN_USD", "1.0"))     # $ per row (absolute)
RANGE_USD = float(os.environ.get("QD_RANGE_USD", "4000"))            # total range stored (e.g., 4000 => +/-2000)
DEFAULT_PRICE_SPAN = float(os.environ.get("QD_DEFAULT_PRICE_SPAN", "600.0"))

# Heat quantization
HEAT_LOG_SCALE = float(os.environ.get("QD_HEAT_LOG_SCALE", "32.0"))  # higher => brighter; 32 works well for BTC depth
HEAT_CELL_MAX = int(os.environ.get("QD_HEAT_CELL_MAX", "255"))       # uint8

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

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
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

    def best_bid_ask(self) -> Tuple[Optional[float], Optional[float]]:
        bb = max(self.bids.keys()) if self.bids else None
        ba = min(self.asks.keys()) if self.asks else None
        return bb, ba

    def top_n(self, n: int) -> Dict[str, List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0], reverse=False)[:n]
        return {"bids": bids_sorted, "asks": asks_sorted}

@dataclass
class Trades:
    dq: deque = field(default_factory=lambda: deque(maxlen=MAX_TRADES))

    def append(self, t: Dict[str, Any]) -> None:
        self.dq.append(t)

class HeatmapRing:
    """
    Absolute price grid heat storage:
      - rows = fixed price bins (min_price + i*bin_usd)
      - cols = fixed time columns (every HEAT_COL_MS), stored in a ring
    We send *patches* (new columns) to clients; clients keep their own ring.
    """
    def __init__(self, bin_usd: float, range_usd: float, window_sec: int, col_ms: int) -> None:
        self.bin_usd = float(bin_usd)
        self.range_usd = float(range_usd)
        self.window_sec = int(window_sec)
        self.col_ms = int(col_ms)

        self.cols = max(30, int((self.window_sec * 1000) // max(250, self.col_ms)))
        half = max(50.0, self.range_usd / 2.0)
        self.rows = max(200, int((2.0 * half) / max(0.25, self.bin_usd)))  # ensure enough rows

        self._anchor_min_price: Optional[float] = None  # fixed for session once set
        self._anchor_min_bin: Optional[int] = None      # int bin index for min
        self._head_col_idx: int = -1
        self._grid: List[Optional[bytearray]] = [None] * self.cols
        self._last_col_open_ms: Optional[int] = None

    @property
    def anchor_min_price(self) -> Optional[float]:
        return self._anchor_min_price

    @property
    def anchor_min_bin(self) -> Optional[int]:
        return self._anchor_min_bin

    @property
    def head(self) -> int:
        return self._head_col_idx

    def _price_to_bin_index(self, p: float) -> int:
        # absolute bin index in units of bin_usd
        return int(math.floor(p / self.bin_usd))

    def ensure_anchor(self, mid_px: float) -> None:
        if self._anchor_min_price is not None:
            return
        half = self.range_usd / 2.0
        min_price = max(0.0, mid_px - half)
        # Align to bin grid
        min_bin = self._price_to_bin_index(min_price)
        self._anchor_min_bin = min_bin
        self._anchor_min_price = float(min_bin) * self.bin_usd

    def _row_of_price(self, p: float) -> Optional[int]:
        if self._anchor_min_bin is None:
            return None
        b = self._price_to_bin_index(p)
        row = b - self._anchor_min_bin
        if 0 <= row < self.rows:
            return row
        return None

    def maybe_open_new_column(self, t_ms: int) -> Optional[Tuple[int, bytearray]]:
        """
        Called frequently; opens a new column whenever col_ms boundary passes.
        Returns (col_idx, column_bytes) if a new column is opened, else None.
        """
        if self._last_col_open_ms is None:
            self._last_col_open_ms = t_ms
            self._head_col_idx += 1
            col = bytearray(self.rows)
            self._grid[self._head_col_idx % self.cols] = col
            return (self._head_col_idx, col)

        if (t_ms - self._last_col_open_ms) >= self.col_ms:
            steps = int((t_ms - self._last_col_open_ms) // self.col_ms)
            # advance in increments; if lagging, we open multiple empty columns
            for _ in range(steps):
                self._last_col_open_ms += self.col_ms
                self._head_col_idx += 1
                col = bytearray(self.rows)
                self._grid[self._head_col_idx % self.cols] = col
            return (self._head_col_idx, self._grid[self._head_col_idx % self.cols] or bytearray(self.rows))

        return None

    def write_level(self, p: float, q: float) -> None:
        """
        Writes resting liquidity snapshot into the CURRENT head column.
        We use side-neutral intensity: bids + asks contribute to same cell.
        """
        if self._head_col_idx < 0:
            return
        row = self._row_of_price(p)
        if row is None:
            return
        col = self._grid[self._head_col_idx % self.cols]
        if col is None:
            col = bytearray(self.rows)
            self._grid[self._head_col_idx % self.cols] = col

        # log intensity
        # typical depth qty can be huge; log compress keeps structure visible
        inten = math.log1p(max(0.0, q)) * HEAT_LOG_SCALE
        v = int(inten)
        if v <= 0:
            return
        if v > HEAT_CELL_MAX:
            v = HEAT_CELL_MAX

        # accumulate (cap at 255)
        nv = col[row] + v
        if nv > HEAT_CELL_MAX:
            nv = HEAT_CELL_MAX
        col[row] = nv

    def current_column_bytes(self) -> Optional[bytearray]:
        if self._head_col_idx < 0:
            return None
        return self._grid[self._head_col_idx % self.cols]

    def export_patch_b64(self, col_idx: int) -> Optional[str]:
        if col_idx < 0:
            return None
        col = self._grid[col_idx % self.cols]
        if col is None:
            return None
        # base64 encode raw bytes (small enough: rows ~ 4000 => 4KB per second)
        return base64.b64encode(bytes(col)).decode("ascii")

class MexcFeed:
    def __init__(self) -> None:
        self.book = OrderBook()
        self.trades = Trades()

        self.ws_ok = False
        self.last_px: Optional[float] = None
        self.last_trade_ms: Optional[int] = None
        self.last_depth_ms: Optional[int] = None

        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.mid_px: Optional[float] = None

        self._stop = False

        self._clients: List[WebSocket] = []
        self._clients_lock = asyncio.Lock()

        self.heat = HeatmapRing(
            bin_usd=PRICE_BIN_USD,
            range_usd=RANGE_USD,
            window_sec=HEAT_WINDOW_SEC,
            col_ms=HEAT_COL_MS,
        )

        self._last_patch_sent_head: int = -1

    async def add_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.append(ws)

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients = [c for c in self._clients if c is not ws]

    def _update_bba_mid(self) -> None:
        bb, ba = self.book.best_bid_ask()
        self.best_bid = bb
        self.best_ask = ba
        if bb is not None and ba is not None and bb > 0 and ba > 0:
            self.mid_px = (bb + ba) / 2.0
        elif self.last_px is not None:
            self.mid_px = self.last_px

    async def broadcast_snapshot(self) -> None:
        self._update_bba_mid()

        book = self.book.top_n(TOP_LEVELS)
        trades = list(self.trades.dq)[-2000:]  # keep payload sane

        # ensure heat anchor once we have a mid
        if self.mid_px is not None:
            self.heat.ensure_anchor(self.mid_px)

        # send only the newest heat column as a patch (client keeps ring)
        heat_patch = None
        if self.heat.anchor_min_price is not None and self.heat.head >= 0:
            if self.heat.head != self._last_patch_sent_head:
                b64 = self.heat.export_patch_b64(self.heat.head)
                if b64 is not None:
                    heat_patch = {
                        "col_idx": self.heat.head,
                        "b64": b64,
                    }
                self._last_patch_sent_head = self.heat.head

        payload = {
            "ts": now_ms(),
            "symbol": SYMBOL_WS,
            "ws_url": WS_URL,
            "ws_ok": self.ws_ok,
            "last_px": self.last_px,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid_px": self.mid_px,
            "book": {"bids": book["bids"], "asks": book["asks"]},
            "trades": trades,
            "health": {
                "depth_age_ms": (now_ms() - self.last_depth_ms) if self.last_depth_ms else None,
                "trade_age_ms": (now_ms() - self.last_trade_ms) if self.last_trade_ms else None,
                "bids_n": len(self.book.bids),
                "asks_n": len(self.book.asks),
                "trades_n": len(self.trades.dq),
            },
            "heat_meta": {
                "mode": "resting_density_absolute_grid",
                "window_sec": HEAT_WINDOW_SEC,
                "col_ms": HEAT_COL_MS,
                "cols": self.heat.cols,
                "bin_usd": self.heat.bin_usd,
                "range_usd": self.heat.range_usd,
                "rows": self.heat.rows,
                "anchor_min_price": self.heat.anchor_min_price,
                "anchor_min_bin": self.heat.anchor_min_bin,
                "head": self.heat.head,
            },
            "heat_patch": heat_patch,  # may be None
            "ui_defaults": {
                "heat_window_sec": HEAT_WINDOW_SEC,
                "heat_col_ms": HEAT_COL_MS,
                "heat_bins": self.heat.rows,
                "default_price_span": DEFAULT_PRICE_SPAN,
                "price_bin_usd": PRICE_BIN_USD,
                "range_usd": RANGE_USD,
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

    async def _heat_loop(self) -> None:
        """
        Fixed-timestep heat writer:
          - opens a new time column every HEAT_COL_MS
          - samples CURRENT resting book and writes into that column
        """
        while not self._stop:
            try:
                t = now_ms()
                self._update_bba_mid()
                if self.mid_px is not None:
                    self.heat.ensure_anchor(self.mid_px)

                opened = self.heat.maybe_open_new_column(t)
                if opened is not None:
                    # Fill new column with current resting liquidity snapshot (side-neutral)
                    # NOTE: This is the essential Bookmap-like behavior: persistent horizontal bands
                    # emerge because the same price levels remain present across consecutive columns.
                    # We iterate over full in-memory dicts (already bounded by exchange updates).
                    for p, q in self.book.bids.items():
                        if q > 0:
                            self.heat.write_level(p, q)
                    for p, q in self.book.asks.items():
                        if q > 0:
                            self.heat.write_level(p, q)
            except Exception:
                pass

            # run at a modest cadence; columns open based on timestamps
            await asyncio.sleep(0.05)

    async def run(self) -> None:
        asyncio.create_task(self._snapshot_loop())
        asyncio.create_task(self._heat_loop())

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
                            self._update_bba_mid()

                        if ch in ("push.deal", "rs.push.deal"):
                            data = msg.get("data", [])
                            if isinstance(data, list):
                                for t in data:
                                    p = safe_float(t.get("p"))
                                    v = safe_float(t.get("v"))
                                    T = safe_int(t.get("T", 0))
                                    tm = safe_int(t.get("t", now_ms()))
                                    tt = {"p": p, "v": v, "T": T, "t": tm}
                                    self.trades.append(tt)
                                    if p > 0:
                                        self.last_px = p
                                    self.last_trade_ms = now_ms()
                            self._update_bba_mid()

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
  <title>QuantDesk Bookmap (Absolute Grid Heat)</title>
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
    button {
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
    <span class="mono" id="sym"></span>
    <span class="mono" id="health"></span>
    <button id="btnAF">AutoFollow: ON</button>
    <button id="btnTm">-T</button>
    <button id="btnTp">+T</button>
    <button id="btnPm">-P</button>
    <button id="btnPp">+P</button>
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
  const btnAF = document.getElementById("btnAF");
  const btnTm = document.getElementById("btnTm");
  const btnTp = document.getElementById("btnTp");
  const btnPm = document.getElementById("btnPm");
  const btnPp = document.getElementById("btnPp");

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
  // Time window (seconds) — interactive (-T/+T)
  // -----------------------------
  let timeSpanSec = 90;         // visible time window in seconds
  const MIN_TSPAN = 30;
  const MAX_TSPAN = 60 * 60;    // up to 60m

  // -----------------------------
  // Price view state (interactive)
  // -----------------------------
  let autoFollow = true;
  let viewMid = null;                 // center price
  let viewSpan = null;                // visible price span USD
  const MIN_PSPAN = 40;
  const MAX_PSPAN = 20000;

  // -----------------------------
  // Heatmap ring on client (absolute price grid)
  // -----------------------------
  let heat = {
    ready: false,
    mode: null,
    windowSec: null,
    colMs: null,
    cols: null,
    binUsd: null,
    rangeUsd: null,
    rows: null,
    anchorMinPrice: null,
    head: -1,
    ring: [], // Array<Uint8Array|null> length cols
  };

  function initHeat(meta) {
    heat.mode = meta.mode;
    heat.windowSec = meta.window_sec;
    heat.colMs = meta.col_ms;
    heat.cols = meta.cols;
    heat.binUsd = meta.bin_usd;
    heat.rangeUsd = meta.range_usd;
    heat.rows = meta.rows;
    heat.anchorMinPrice = meta.anchor_min_price;
    heat.head = meta.head;
    heat.ring = new Array(heat.cols).fill(null);
    heat.ready = !!(heat.anchorMinPrice !== null && heat.rows && heat.cols);
  }

  function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

  btnAF.onclick = () => {
    autoFollow = !autoFollow;
    btnAF.textContent = `AutoFollow: ${autoFollow ? "ON" : "OFF"}`;
  };

  btnTm.onclick = () => { timeSpanSec = clamp(Math.floor(timeSpanSec / 1.5), MIN_TSPAN, MAX_TSPAN); };
  btnTp.onclick = () => { timeSpanSec = clamp(Math.floor(timeSpanSec * 1.5), MIN_TSPAN, MAX_TSPAN); };
  btnPm.onclick = () => { viewSpan = clamp(viewSpan * 0.75, MIN_PSPAN, MAX_PSPAN); };
  btnPp.onclick = () => { viewSpan = clamp(viewSpan * 1.33, MIN_PSPAN, MAX_PSPAN); };

  // -----------------------------
  // WebSocket
  // -----------------------------
  function wsUrl() {
    const proto = (location.protocol === "https:") ? "wss" : "ws";
    return `${proto}://${location.host}/ws`;
  }
  const WS = new WebSocket(wsUrl());

  function b64ToU8(b64) {
    const bin = atob(b64);
    const len = bin.length;
    const out = new Uint8Array(len);
    for (let i=0;i<len;i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  WS.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    last = msg;

    dot.style.background = msg.ws_ok ? "#22c55e" : "#ef4444";
    lastPx = (msg.last_px ?? lastPx);
    midPx = (msg.mid_px ?? midPx);

    symEl.textContent = `${msg.symbol} | last_trade=${(lastPx ?? "None")} | mid=${(midPx ?? "None")}`;
    healthEl.textContent =
      `book: bids=${msg.health.bids_n} asks=${msg.health.asks_n} age: depth=${msg.health.depth_age_ms ?? "None"}ms trade=${msg.health.trade_age_ms ?? "None"}ms WS=${msg.ws_url}`;

    // Init view once
    if (viewMid === null && (midPx || lastPx)) {
      viewMid = (midPx ?? lastPx);
      viewSpan = (msg.ui_defaults?.default_price_span ?? 600.0);
    }

    // Initialize heat ring once meta exists
    if (!heat.ready && msg.heat_meta && msg.heat_meta.anchor_min_price !== null) {
      initHeat(msg.heat_meta);
    } else if (heat.ready && msg.heat_meta) {
      // Keep head updated
      heat.head = msg.heat_meta.head;
    }

    // Apply incoming heat patch (one new column)
    if (heat.ready && msg.heat_patch && msg.heat_patch.b64) {
      const colIdx = msg.heat_patch.col_idx;
      const u8 = b64ToU8(msg.heat_patch.b64);
      if (u8.length === heat.rows) {
        heat.ring[colIdx % heat.cols] = u8;
      }
    }

    // Trades rolling window = min(history, but bounded for perf)
    const now = Date.now();
    const maxWindowMs = Math.min((heat.windowSec ?? 3600) * 1000, 60*60*1000);
    if (Array.isArray(msg.trades)) {
      for (const t of msg.trades) trades.push(t);
    }
    trades = trades.filter(t => (now - t.t) <= maxWindowMs);
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
    if (viewMid === null || viewSpan === null) return;

    // One finger drag: pan price
    if (pointers.size === 1 && dragStartY !== null) {
      autoFollow = false;
      btnAF.textContent = "AutoFollow: OFF";
      const dy = e.clientY - dragStartY;
      const pxPerPrice = cv.clientHeight / viewSpan;
      const dPrice = dy / Math.max(1, pxPerPrice);
      viewMid = (dragStartMid ?? viewMid) + dPrice;
    }

    // Two finger pinch: zoom price
    if (pointers.size === 2 && pinchStartDist && pinchStartSpan) {
      autoFollow = false;
      btnAF.textContent = "AutoFollow: OFF";
      const pts = Array.from(pointers.values());
      const dx = pts[0].x - pts[1].x;
      const dy = pts[0].y - pts[1].y;
      const dist = Math.sqrt(dx*dx + dy*dy);
      const scale = pinchStartDist / Math.max(10, dist);
      viewSpan = clamp(pinchStartSpan * scale, MIN_PSPAN, MAX_PSPAN);
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
    if (viewMid === null || viewSpan === null) return;
    e.preventDefault();
    autoFollow = false;
    btnAF.textContent = "AutoFollow: OFF";
    const factor = (e.deltaY > 0) ? 1.10 : 0.90;
    viewSpan = clamp(viewSpan * factor, MIN_PSPAN, MAX_PSPAN);
  }, {passive:false});

  // -----------------------------
  // Rendering utils
  // -----------------------------
  function yOf(p, pMin, pMax, h) {
    const t = (p - pMin) / (pMax - pMin);
    return h - t*h;
  }
  function priceOfRow(row) {
    return heat.anchorMinPrice + row * heat.binUsd;
  }
  function rowOfPrice(p) {
    return Math.floor((p - heat.anchorMinPrice) / heat.binUsd);
  }

  // Bookmap-like intensity palette (side-neutral)
  function heatRGBA(a) {
    // a in [0,1]
    // dark-blue -> cyan -> yellow -> orange
    const g = Math.pow(a, 0.75);
    let r=0, gg=0, b=0, alpha=0;

    if (g < 0.5) {
      // blue/cyan
      const t = g / 0.5;
      r = 10 + 20*t;
      gg = 40 + 160*t;
      b = 80 + 140*t;
      alpha = 0.05 + 0.55*g;
    } else {
      // yellow/orange
      const t = (g - 0.5) / 0.5;
      r = 80 + 170*t;
      gg = 180 + 60*(1-t);
      b = 80*(1-t);
      alpha = 0.10 + 0.75*g;
    }
    return [r|0, gg|0, b|0, clamp(alpha, 0, 1)];
  }

  function draw() {
    const w = cv.clientWidth;
    const h = cv.clientHeight;

    ctx.clearRect(0,0,w,h);
    ctx.fillStyle = "#0b0f14";
    ctx.fillRect(0,0,w,h);

    if (!last || viewMid === null || viewSpan === null) {
      ctx.fillStyle = "#94a3b8";
      ctx.font = "14px -apple-system, system-ui, Arial";
      ctx.fillText("Waiting for data...", 12, 24);
      requestAnimationFrame(draw);
      return;
    }

    // Auto-follow to mid (more stable than last trade)
    if (autoFollow && (midPx || lastPx)) {
      viewMid = (midPx ?? lastPx);
    }

    const pMin = viewMid - viewSpan/2;
    const pMax = viewMid + viewSpan/2;

    // -----------------------------
    // Draw HEATMAP (absolute grid) as an image
    // -----------------------------
    if (heat.ready && heat.head >= 0 && heat.anchorMinPrice !== null) {
      const colsVisible = clamp(Math.floor((timeSpanSec*1000) / heat.colMs), 10, heat.cols);
      const head = heat.head;
      const startCol = head - colsVisible + 1;

      // Determine which rows are visible in the current price window
      let r0 = rowOfPrice(pMax); // top
      let r1 = rowOfPrice(pMin); // bottom
      r0 = clamp(r0, 0, heat.rows-1);
      r1 = clamp(r1, 0, heat.rows-1);
      if (r1 < r0) { const tmp=r0; r0=r1; r1=tmp; }
      const rowsVisible = clamp((r1 - r0 + 1), 50, heat.rows);

      // Build ImageData at (colsVisible x rowsVisible), then scale to canvas
      const img = ctx.createImageData(colsVisible, rowsVisible);
      const data = img.data;

      // Exposure (simple robust): compute max over a small sample of recent data
      // (fast, stable enough). This avoids "all bright" or "all dark".
      let vmax = 1;
      const sampleCols = Math.min(colsVisible, 60);
      const sampleStep = Math.max(1, Math.floor(colsVisible / sampleCols));
      for (let ci = startCol; ci <= head; ci += sampleStep) {
        const col = heat.ring[ci % heat.cols];
        if (!col) continue;
        for (let rr = r0; rr <= r1; rr += 4) {
          const v = col[rr] || 0;
          if (v > vmax) vmax = v;
        }
      }
      vmax = Math.max(vmax, 16);

      // Fill pixels (x=time, y=price)
      // Note: y in ImageData goes top->bottom, which corresponds to high->low
      for (let x = 0; x < colsVisible; x++) {
        const colIdx = startCol + x;
        const col = heat.ring[((colIdx % heat.cols) + heat.cols) % heat.cols];
        if (!col) continue;

        for (let y = 0; y < rowsVisible; y++) {
          const row = r0 + y;
          const v = col[row] || 0;
          if (v === 0) continue;

          const a = clamp(v / vmax, 0, 1);
          if (a < 0.02) continue;

          const [rr, gg, bb, aa] = heatRGBA(a);
          const i = (y*colsVisible + x) * 4;
          data[i+0] = rr;
          data[i+1] = gg;
          data[i+2] = bb;
          data[i+3] = Math.floor(255 * aa);
        }
      }

      // Draw scaled to full canvas
      // Use nearest-neighbor for crisp "band" look
      ctx.imageSmoothingEnabled = false;
      ctx.putImageData(img, 0, 0);

      // Scale to canvas: we render at native img size then stretch using drawImage
      // To avoid allocating another canvas, we drawImage from the main canvas:
      // 1) snapshot that region to an offscreen bitmap via createImageBitmap would be async.
      // So we instead use a small trick: draw to an offscreen canvas.
      const off = document.createElement("canvas");
      off.width = colsVisible;
      off.height = rowsVisible;
      const octx = off.getContext("2d");
      octx.putImageData(img, 0, 0);

      // Draw to full area
      ctx.imageSmoothingEnabled = true; // smoother scaling
      ctx.drawImage(off, 0, 0, w, h);
    }

    // Light horizontal grid
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

    // -----------------------------
    // Trade bubbles (time on X, price on Y)
    // -----------------------------
    const now = Date.now();
    const windowMs = timeSpanSec * 1000;
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

    // Last price line + labels
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
      ctx.fillText(`LAST ${lastPx.toFixed(1)}`, 10, clamp(y-6, 14, h-8));
    }

    if (midPx) {
      const y = yOf(midPx, pMin, pMax, h);
      ctx.strokeStyle = "rgba(147,197,253,0.35)";
      ctx.lineWidth = 1;
      ctx.setLineDash([4,4]);
      ctx.beginPath();
      ctx.moveTo(0,y);
      ctx.lineTo(w,y);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle = "rgba(147,197,253,0.85)";
      ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      ctx.fillText(`MID  ${midPx.toFixed(1)}`, 10, clamp(y-6, 14, h-8));
    }

    // Footer
    ctx.fillStyle = "rgba(148,163,184,0.9)";
    ctx.font = "12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
    ctx.fillText(
      `Tspan=${timeSpanSec.toFixed(0)}s | Pspan=${viewSpan.toFixed(1)} | trade_age=${(last?.health?.trade_age_ms ?? "None")}ms | history=${(heat?.windowSec ?? 3600)/60}m`,
      10, h - 10
    );

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
    feed._update_bba_mid()
    return {
        "service": "quantdesk-bookmap-ui",
        "symbol": SYMBOL_WS,
        "ws_url": WS_URL,
        "ws_ok": feed.ws_ok,
        "last_px": feed.last_px,
        "mid_px": feed.mid_px,
        "best_bid": feed.best_bid,
        "best_ask": feed.best_ask,
        "bids_n": len(feed.book.bids),
        "asks_n": len(feed.book.asks),
        "trades_n": len(feed.trades.dq),
        "depth_age_ms": (now_ms() - feed.last_depth_ms) if feed.last_depth_ms else None,
        "trade_age_ms": (now_ms() - feed.last_trade_ms) if feed.last_trade_ms else None,
        "ui_defaults": {
            "heat_window_sec": HEAT_WINDOW_SEC,
            "heat_col_ms": HEAT_COL_MS,
            "heat_bins": feed.heat.rows,
            "default_price_span": DEFAULT_PRICE_SPAN,
            "price_bin_usd": PRICE_BIN_USD,
            "range_usd": RANGE_USD,
        },
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
    # Keep Procfile-compatible entry: uvicorn app:app --host 0.0.0.0 --port $PORT
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
