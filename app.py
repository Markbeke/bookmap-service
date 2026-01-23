# QuantDesk Bookmap UI — FOUNDATION_BUILD_1
# Single-file service for GitHub -> Replit "Run" workflow.
# Goal: stable Bookmap-like foundation on iPad/WebKit:
# - Depth+Trades ingestion (MEXC contract WS)
# - OrderBookState snapshot+delta correctness
# - Fixed time window heat buffer (no drift/squeeze)
# - Canvas renderer with NO pan/zoom/autofollow (Phase 1)
# - On-canvas error panel (never silent blank)

import os
import json
import time
import math
import base64
import asyncio
import traceback
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import aiohttp
from aiohttp import web

# -------------------------
# Build identity
# -------------------------
BUILD_TAG = "FOUNDATION_BUILD_1"

# -------------------------
# MEXC contract WS config
# -------------------------
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")  # contract WS symbol format

# -------------------------
# Heatmap foundation params
# -------------------------
WINDOW_SEC = int(os.environ.get("QD_WINDOW_SEC", "900"))         # 15 minutes
COL_DT_SEC = float(os.environ.get("QD_COL_DT_SEC", "1.0"))       # 1 second per column
RANGE_USD = float(os.environ.get("QD_RANGE_USD", "400"))         # +/- range around mid
BIN_USD = float(os.environ.get("QD_BIN_USD", "1.0"))             # price bin size (foundation)
ROWS = int((2.0 * RANGE_USD) / BIN_USD)                           # fixed rows
COLS = int(WINDOW_SEC / COL_DT_SEC)                               # fixed cols

# Safety clamps (iPad)
ROWS = max(128, min(1600, ROWS))
COLS = max(240, min(1800, COLS))

# -------------------------
# Utilities
# -------------------------
def now_ms() -> int:
    return int(time.time() * 1000)

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v

# -------------------------
# Order book state
# -------------------------
@dataclass
class OrderBookState:
    bids: Dict[float, float] = field(default_factory=dict)  # price -> qty
    asks: Dict[float, float] = field(default_factory=dict)
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_mid: Optional[float] = None
    last_px: Optional[float] = None
    last_trade_ts: Optional[int] = None
    last_depth_ts: Optional[int] = None
    trades_n: int = 0
    depth_updates_n: int = 0

    # to support consumption heuristics
    _prev_last_px: Optional[float] = None

    def apply_depth_update(self, side: str, levels: List[List[float]]) -> None:
        book = self.bids if side == "bids" else self.asks
        # MEXC depth levels are [price, qty, count] or [price, qty]
        for lv in levels:
            if not lv or len(lv) < 2:
                continue
            p = safe_float(lv[0], None)
            q = safe_float(lv[1], None)
            if p is None or q is None:
                continue
            if q <= 0:
                if p in book:
                    del book[p]
            else:
                book[p] = q

        # refresh bests
        if self.bids:
            self.best_bid = max(self.bids.keys())
        if self.asks:
            self.best_ask = min(self.asks.keys())
        if self.best_bid is not None and self.best_ask is not None:
            self.last_mid = (self.best_bid + self.best_ask) / 2.0

        self.last_depth_ts = now_ms()
        self.depth_updates_n += 1

    def apply_trade(self, p: float, v: float, ts_ms: int) -> None:
        if p is None:
            return
        self._prev_last_px = self.last_px
        self.last_px = p
        self.last_trade_ts = ts_ms or now_ms()
        self.trades_n += 1

    @property
    def bids_n(self) -> int:
        return len(self.bids)

    @property
    def asks_n(self) -> int:
        return len(self.asks)

# -------------------------
# Heat history (fixed window, fixed bins)
# -------------------------
class HeatHistory:
    def __init__(self, cols: int, rows: int):
        self.cols = cols
        self.rows = rows
        self.ring: List[Optional[bytearray]] = [None] * cols
        self.head: int = -1
        self.started_ms: int = now_ms()
        self.last_col_ts_ms: int = 0

        # exposure stats (EWMA)
        self.vmax_ema: float = 30.0  # seed
        self.vmax_alpha: float = 0.06

        # stats
        self.patches: int = 0

    def advance(self, ts_ms: int) -> int:
        """Advance to next column index and allocate a fresh column."""
        self.head = (self.head + 1) % self.cols
        self.last_col_ts_ms = ts_ms
        self.ring[self.head] = bytearray(self.rows)  # zero-init
        return self.head

    def current_col(self) -> Optional[bytearray]:
        if self.head < 0:
            return None
        return self.ring[self.head]

    def encode_col_b64(self, col: bytearray) -> str:
        return base64.b64encode(col).decode("ascii")

    def apply_consumption_corridor(self, price0: float, price1: float, mid: float) -> None:
        """Visual hint: reduce heat along the traded path in the latest column."""
        col = self.current_col()
        if col is None:
            return
        if price0 is None or price1 is None or mid is None:
            return
        pmin = mid - RANGE_USD
        pmax = mid + RANGE_USD
        if pmax <= pmin:
            return
        lo = min(price0, price1)
        hi = max(price0, price1)
        # widen corridor slightly
        lo -= (2.0 * BIN_USD)
        hi += (2.0 * BIN_USD)

        r_lo = int((lo - pmin) / BIN_USD)
        r_hi = int((hi - pmin) / BIN_USD)
        r_lo = clamp(r_lo, 0, self.rows - 1)
        r_hi = clamp(r_hi, 0, self.rows - 1)
        if r_hi < r_lo:
            return
        # Burn (cap) so trail becomes "cool"
        cap = 18  # small U8 cap (blue)
        for r in range(r_lo, r_hi + 1):
            v = col[r]
            if v > cap:
                col[r] = cap

# -------------------------
# Rasterize book -> column
# -------------------------
def rasterize_book_to_col(book: OrderBookState, mid: float, col: bytearray) -> Tuple[float, float]:
    """
    Fill 'col' in-place with intensities based on current book.
    Returns (vmax_used, floor_used).
    """
    # Collect values inside range, aggregate by bin.
    pmin = mid - RANGE_USD
    pmax = mid + RANGE_USD

    # Quick return if no book
    if not book.bids and not book.asks:
        return (1.0, 0.0)

    # temp accumulator (float) per row
    acc = [0.0] * len(col)

    def add_side(levels: Dict[float, float]):
        for p, q in levels.items():
            if p < pmin or p > pmax:
                continue
            r = int((p - pmin) / BIN_USD)
            if 0 <= r < len(acc):
                acc[r] += float(q)

    add_side(book.bids)
    add_side(book.asks)

    # Convert acc -> U8 with log compression and robust vmax
    # Use percentile vmax from acc (non-zero), then EWMA outside.
    nz = [v for v in acc if v > 0.0]
    if not nz:
        return (1.0, 0.0)

    nz.sort()
    # 99.5th percentile (robust)
    idx = int(0.995 * (len(nz) - 1))
    vmax = max(1e-9, nz[idx])

    # Floor to suppress haze: 30th percentile of non-zero
    floor_idx = int(0.30 * (len(nz) - 1))
    floor_v = nz[floor_idx]

    # Map: log1p, normalized against log1p(vmax)
    denom = math.log1p(vmax)
    if denom <= 0:
        denom = 1.0

    for i, v in enumerate(acc):
        if v <= floor_v:
            col[i] = 0
            continue
        x = math.log1p(v) / denom
        x = clamp(x, 0.0, 1.0)
        # gamma for contrast
        x = x ** 0.60
        col[i] = int(255.0 * x + 0.5)

    return (vmax, floor_v)

# -------------------------
# Web UI (no interaction)
# -------------------------
HTML = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
<title>QuantDesk Bookmap ({BUILD_TAG})</title>
<style>
  html, body {{
    margin:0; padding:0; height:100%; width:100%;
    background:#000; color:#cfd3da; font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
    overflow:hidden;
  }}
  body {{ display:flex; flex-direction:column; }}
  #topbar {{
    padding: 8px 10px;
    background: #0b0f14;
    border-bottom: 1px solid rgba(255,255,255,0.08);
    font-size: 13px;
    line-height: 1.25;
    white-space: normal;
  }}
  #wrap {{ flex: 1 1 auto; position: relative; min-height: 0; }}
  #cv {{
    width: 100%;
    height: 100%;
    display:block;
    background:#000;
  }}
  #statusDot {{
    display:inline-block; width:10px; height:10px; border-radius:50%;
    margin-right:6px; background:#444; vertical-align:middle;
  }}
  .muted {{ color:#8b93a1; }}
</style>
</head>
<body>
  <div id="topbar">
    <span id="statusDot"></span>
    <span><b>QuantDesk Bookmap</b> <span class="muted">(build={BUILD_TAG})</span></span>
    <span class="muted"> | symbol=</span><span id="sym"></span>
    <span class="muted"> | PRIMARY(MID)=</span><span id="mid"></span>
    <span class="muted"> TRADE=</span><span id="last"></span>
    <span class="muted"> bid=</span><span id="bid"></span>
    <span class="muted"> ask=</span><span id="ask"></span>
    <span class="muted"> | depth_age=</span><span id="dAge"></span><span class="muted">ms</span>
    <span class="muted"> trade_age=</span><span id="tAge"></span><span class="muted">ms</span>
    <span class="muted"> bids/asks=</span><span id="na"></span>
    <span class="muted"> trades=</span><span id="nt"></span>
  </div>
  <div id="wrap"><canvas id="cv"></canvas></div>

<script>
(() => {{
  const BUILD = "{BUILD_TAG}";
  const SYMBOL = "{SYMBOL_WS}";
  const WINDOW_SEC = {WINDOW_SEC};
  const COL_DT = {COL_DT_SEC};
  const RANGE_USD = {RANGE_USD};
  const BIN_USD = {BIN_USD};
  const ROWS = {ROWS};
  const COLS = {COLS};

  const dot = document.getElementById("statusDot");
  const elSym = document.getElementById("sym");
  const elMid = document.getElementById("mid");
  const elLast = document.getElementById("last");
  const elBid = document.getElementById("bid");
  const elAsk = document.getElementById("ask");
  const elDAge = document.getElementById("dAge");
  const elTAge = document.getElementById("tAge");
  const elNA = document.getElementById("na");
  const elNT = document.getElementById("nt");

  elSym.textContent = SYMBOL;

  const cv = document.getElementById("cv");
  const ctx = cv.getContext("2d", {{ alpha:false }});

  function resize() {{
    const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    const rect = cv.getBoundingClientRect();
    const w = Math.max(1, Math.floor(rect.width));
    const h = Math.max(1, Math.floor(rect.height));
    cv.width = Math.floor(w * dpr);
    cv.height = Math.floor(h * dpr);
    ctx.setTransform(dpr,0,0,dpr,0,0);
  }}
  window.addEventListener("resize", resize, {{ passive:true }});
  resize();

  // Heat ring on client: store last COLS columns of ROWS U8
  const heat = {{
    cols: COLS,
    rows: ROWS,
    ring: new Array(COLS).fill(null),
    head: -1,
    head_ts: 0,
    patches: 0,
  }};

  // trades for bubbles (Phase 1: minimal)
  const trades = []; // {p, v, ts, side}
  const TRADES_MAX = 2000;

  // last state
  let st = null;

  function fmt(x) {{
    if (x === null || x === undefined || !isFinite(x)) return "-";
    return Number(x).toFixed(2);
  }}

  function drawError(msg) {{
    const w = cv.width/(window.devicePixelRatio||1);
    const h = cv.height/(window.devicePixelRatio||1);
    ctx.save();
    ctx.setTransform(1,0,0,1,0,0);
    ctx.fillStyle = "#120000";
    ctx.fillRect(0,0,w,h);
    ctx.fillStyle = "#ff4d4d";
    ctx.font = "14px -apple-system, system-ui, sans-serif";
    const lines = (""+msg).split("\\n").slice(0, 10);
    let y = 20;
    for (const ln of lines) {{
      ctx.fillText(ln, 10, y);
      y += 18;
    }}
    ctx.restore();
  }}

  function draw() {{
    try {{
      const w = cv.width/(window.devicePixelRatio||1);
      const h = cv.height/(window.devicePixelRatio||1);

      // Background
      ctx.fillStyle = "#000";
      ctx.fillRect(0, 0, w, h);

      // If no state yet, show waiting text
      if (!st || !isFinite(st.mid_px)) {{
        ctx.fillStyle = "#9aa3b2";
        ctx.font = "14px -apple-system, system-ui, sans-serif";
        ctx.fillText("Waiting for data…", 10, 24);
        requestAnimationFrame(draw);
        return;
      }}

      const mid = st.mid_px;
      const pmin = mid - RANGE_USD;
      const pmax = mid + RANGE_USD;

      // Heat render
      // Map each column to x-range across canvas
      const colW = w / heat.cols;

      // Bookmap-like palette: blue (weak) -> cyan -> yellow -> orange -> red (strong)
      function colorFor(v) {{
        // v in [0..255]
        if (v <= 0) return null;
        const t = v / 255.0;
        // piecewise gradient
        let r=0,g=0,b=0,a=1.0;
        if (t < 0.25) {{
          // dark->light blue
          const u = t/0.25;
          r = 10*u; g = 80*u; b = 255;
          a = 0.70;
        }} else if (t < 0.5) {{
          const u = (t-0.25)/0.25;
          r = 0; g = 255*u; b = 255;
          a = 0.78;
        }} else if (t < 0.75) {{
          const u = (t-0.5)/0.25;
          r = 255*u; g = 255; b = 255*(1-u);
          a = 0.85;
        }} else {{
          const u = (t-0.75)/0.25;
          r = 255; g = 255*(1-u); b = 0;
          a = 0.92;
        }}
        return `rgba(${{r|0}},${{g|0}},${{b|0}},${{a}})`;
      }}

      for (let k = 0; k < heat.cols; k++) {{
        // oldest on left, newest on right
        const idx = (heat.head + 1 + k) % heat.cols;
        const col = heat.ring[idx];
        if (!col) continue;
        const x0 = k * colW;
        // draw as vertical strips of 1px-high rows (scaled)
        for (let r = 0; r < heat.rows; r++) {{
          const v = col[r];
          if (!v) continue;
          const y = h - (r+1) * (h / heat.rows);
          const c = colorFor(v);
          if (!c) continue;
          ctx.fillStyle = c;
          ctx.fillRect(x0, y, colW + 1, (h/heat.rows) + 1);
        }}
      }}

      // Price line (PRIMARY = mid)
      const py = h - ((mid - pmin) / (pmax - pmin)) * h;
      ctx.strokeStyle = "rgba(255,255,255,0.85)";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(0, py);
      ctx.lineTo(w, py);
      ctx.stroke();

      // Minimal bubbles (aggressive trades)
      // Draw last ~300 trades inside visible range
      const maxDraw = 300;
      let drawn = 0;
      for (let i = trades.length - 1; i >= 0 && drawn < maxDraw; i--) {{
        const tr = trades[i];
        const p = tr.p;
        if (p < pmin || p > pmax) continue;
        // x based on ts in window
        const ts = tr.ts || Date.now();
        const now = Date.now();
        const x = ( (ts - (now - WINDOW_SEC*1000)) / (WINDOW_SEC*1000) ) * w;
        const y = h - ((p - pmin) / (pmax - pmin)) * h;
        const v = Math.max(0.05, Math.min(6.0, Math.log1p(tr.v || 1)));
        ctx.fillStyle = (tr.side === "buy") ? "rgba(0,220,255,0.75)" : "rgba(255,90,90,0.75)";
        ctx.beginPath();
        ctx.arc(x, y, v, 0, Math.PI*2);
        ctx.fill();
        drawn++;
      }}

      requestAnimationFrame(draw);
    }} catch (e) {{
      drawError(`JS ERROR (${BUILD})\\n${{e && e.name ? e.name : "Error"}}: ${{e && e.message ? e.message : e}}\\n${{(e && e.stack) ? e.stack : ""}}`);
      requestAnimationFrame(draw);
    }}
  }}

  // WS connect to UI stream
  function wsUrl() {{
    const proto = (location.protocol === "https:") ? "wss:" : "ws:";
    return proto + "//" + location.host + "/uiws";
  }}

  function connect() {{
    const ws = new WebSocket(wsUrl());
    ws.binaryType = "arraybuffer";
    ws.onopen = () => {{ dot.style.background = "#2ecc71"; }};
    ws.onclose = () => {{ dot.style.background = "#e74c3c"; setTimeout(connect, 700); }};
    ws.onerror = () => {{ dot.style.background = "#e67e22"; }};
    ws.onmessage = (ev) => {{
      try {{
        const msg = JSON.parse(ev.data);
        if (msg.type === "state") {{
          st = msg.state;
          elMid.textContent = fmt(st.mid_px);
          elLast.textContent = fmt(st.last_px);
          elBid.textContent = fmt(st.best_bid);
          elAsk.textContent = fmt(st.best_ask);
          elDAge.textContent = (st.depth_age_ms ?? "-");
          elTAge.textContent = (st.trade_age_ms ?? "-");
          elNA.textContent = `${{st.bids_n}}/${{st.asks_n}}`;
          elNT.textContent = `${{st.trades_n}}`;
        }} else if (msg.type === "heat_col") {{
          const colIdx = msg.col_idx|0;
          const u8 = new Uint8Array(atob(msg.b64).split("").map(c => c.charCodeAt(0)));
          heat.ring[colIdx] = u8;
          heat.head = colIdx;
          heat.head_ts = msg.ts_ms|0;
          heat.patches += 1;
        }} else if (msg.type === "trade") {{
          const tr = msg.trade;
          if (tr && isFinite(tr.p)) {{
            trades.push(tr);
            if (trades.length > TRADES_MAX) trades.splice(0, trades.length - TRADES_MAX);
          }}
        }}
      }} catch (e) {{
        // show in canvas
        drawError(`WS MSG ERROR (${BUILD})\\n${{e && e.name ? e.name : "Error"}}: ${{e && e.message ? e.message : e}}\\nraw=${{String(ev.data).slice(0,200)}}`);
      }}
    }};
  }}

  connect();
  requestAnimationFrame(draw);
}})();
</script>
</body>
</html>
"""

# -------------------------
# MEXC WS ingestion task
# -------------------------
async def mexc_ws_task(app: web.Application) -> None:
    ob: OrderBookState = app["ob"]
    heat: HeatHistory = app["heat"]
    clients: set = app["clients"]

    # subscribe payloads
    sub_depth = {"method":"sub.depth", "param":{"symbol":SYMBOL_WS}}
    sub_deal  = {"method":"sub.deal",  "param":{"symbol":SYMBOL_WS}}

    backoff = 0.5
    while True:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(WS_URL, heartbeat=20, autoping=True, compress=0) as ws:
                    await ws.send_str(json.dumps(sub_depth))
                    await ws.send_str(json.dumps(sub_deal))

                    app["ws_ok"] = True
                    app["ws_since_ms"] = now_ms()
                    backoff = 0.5

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue
                            ch = data.get("channel") or data.get("c") or ""
                            if ch == "push.depth":
                                payload = data.get("data") or {}
                                bids = payload.get("bids") or []
                                asks = payload.get("asks") or []
                                if bids:
                                    ob.apply_depth_update("bids", bids)
                                if asks:
                                    ob.apply_depth_update("asks", asks)
                            elif ch == "push.deal":
                                payload = data.get("data") or []
                                # MEXC can send list of deals
                                for d in payload:
                                    p = safe_float(d.get("p"), None)
                                    v = safe_float(d.get("v"), 0.0)
                                    T = d.get("T")  # 1 buy, 2 sell
                                    ts = d.get("t") or d.get("ts") or now_ms()
                                    if p is None:
                                        continue
                                    side = "buy" if str(T) == "1" or T == 1 else "sell"
                                    ob.apply_trade(p, v, int(ts))
                                    # broadcast trade to clients
                                    if clients:
                                        out = {"type":"trade","trade":{"p":p,"v":v,"ts":int(ts),"side":side}}
                                        dead = []
                                        for c in list(clients):
                                            try:
                                                await c.send_str(json.dumps(out))
                                            except Exception:
                                                dead.append(c)
                                        for c in dead:
                                            clients.discard(c)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise msg.data
        except Exception:
            app["ws_ok"] = False
            # small backoff loop
            await asyncio.sleep(backoff)
            backoff = min(10.0, backoff * 1.7)

# -------------------------
# Heat writer task (fixed cadence)
# -------------------------
async def heat_writer_task(app: web.Application) -> None:
    ob: OrderBookState = app["ob"]
    heat: HeatHistory = app["heat"]
    clients: set = app["clients"]

    next_ts = time.time()
    prev_px = None

    while True:
        try:
            now = time.time()
            if now < next_ts:
                await asyncio.sleep(next_ts - now)
            next_ts += COL_DT_SEC

            # Need mid to center range
            mid = ob.last_mid
            if mid is None:
                continue

            # Advance column and rasterize book
            ts_ms = now_ms()
            col_idx = heat.advance(ts_ms)
            col = heat.current_col()
            if col is None:
                continue

            vmax, floor_v = rasterize_book_to_col(ob, mid, col)

            # Consumption hint: cap along path of last traded move
            if ob._prev_last_px is not None and ob.last_px is not None:
                heat.apply_consumption_corridor(ob._prev_last_px, ob.last_px, mid)

            # Broadcast heat column
            if clients:
                out = {"type":"heat_col","col_idx":col_idx,"ts_ms":ts_ms,"b64":heat.encode_col_b64(col)}
                dead = []
                payload = json.dumps(out)
                for c in list(clients):
                    try:
                        await c.send_str(payload)
                    except Exception:
                        dead.append(c)
                for c in dead:
                    clients.discard(c)

            heat.patches += 1
        except Exception:
            await asyncio.sleep(0.25)

# -------------------------
# State broadcaster task
# -------------------------
async def state_broadcast_task(app: web.Application) -> None:
    ob: OrderBookState = app["ob"]
    clients: set = app["clients"]
    while True:
        await asyncio.sleep(0.25)
        if not clients:
            continue
        d_age = (now_ms() - ob.last_depth_ts) if ob.last_depth_ts else None
        t_age = (now_ms() - ob.last_trade_ts) if ob.last_trade_ts else None
        st = {
            "service":"quantdesk-bookmap-ui",
            "build": BUILD_TAG,
            "symbol": SYMBOL_WS,
            "ws_url": WS_URL,
            "ws_ok": bool(app.get("ws_ok", False)),
            "last_px": ob.last_px,
            "mid_px": ob.last_mid,
            "best_bid": ob.best_bid,
            "best_ask": ob.best_ask,
            "bids_n": ob.bids_n,
            "asks_n": ob.asks_n,
            "trades_n": ob.trades_n,
            "depth_age_ms": d_age,
            "trade_age_ms": t_age,
            "cols": COLS,
            "rows": ROWS,
            "range_usd": RANGE_USD,
            "bin_usd": BIN_USD,
        }
        out = {"type":"state","state":st}
        dead = []
        payload = json.dumps(out)
        for c in list(clients):
            try:
                await c.send_str(payload)
            except Exception:
                dead.append(c)
        for c in dead:
            clients.discard(c)

# -------------------------
# HTTP handlers
# -------------------------
async def handle_root(request: web.Request) -> web.Response:
    return web.Response(text=HTML, content_type="text/html")

async def handle_health(request: web.Request) -> web.Response:
    app = request.app
    ob: OrderBookState = app["ob"]
    d_age = (now_ms() - ob.last_depth_ts) if ob.last_depth_ts else None
    t_age = (now_ms() - ob.last_trade_ts) if ob.last_trade_ts else None
    return web.json_response({
        "service":"quantdesk-bookmap-ui",
        "build": BUILD_TAG,
        "symbol": SYMBOL_WS,
        "ws_url": WS_URL,
        "ws_ok": bool(app.get("ws_ok", False)),
        "last_px": ob.last_px,
        "mid_px": ob.last_mid,
        "best_bid": ob.best_bid,
        "best_ask": ob.best_ask,
        "bids_n": ob.bids_n,
        "asks_n": ob.asks_n,
        "trades_n": ob.trades_n,
        "depth_age_ms": d_age,
        "trade_age_ms": t_age,
        "window_sec": WINDOW_SEC,
        "col_dt_sec": COL_DT_SEC,
        "cols": COLS,
        "rows": ROWS,
        "range_usd": RANGE_USD,
        "bin_usd": BIN_USD,
    })

async def handle_uiws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    request.app["clients"].add(ws)

    # Send an immediate state snapshot
    ob: OrderBookState = request.app["ob"]
    d_age = (now_ms() - ob.last_depth_ts) if ob.last_depth_ts else None
    t_age = (now_ms() - ob.last_trade_ts) if ob.last_trade_ts else None
    st = {
        "service":"quantdesk-bookmap-ui",
        "build": BUILD_TAG,
        "symbol": SYMBOL_WS,
        "ws_url": WS_URL,
        "ws_ok": bool(request.app.get("ws_ok", False)),
        "last_px": ob.last_px,
        "mid_px": ob.last_mid,
        "best_bid": ob.best_bid,
        "best_ask": ob.best_ask,
        "bids_n": ob.bids_n,
        "asks_n": ob.asks_n,
        "trades_n": ob.trades_n,
        "depth_age_ms": d_age,
        "trade_age_ms": t_age,
        "cols": COLS,
        "rows": ROWS,
        "range_usd": RANGE_USD,
        "bin_usd": BIN_USD,
    }
    await ws.send_str(json.dumps({"type":"state","state":st}))

    # Keep connection open
    try:
        async for _ in ws:
            pass
    finally:
        request.app["clients"].discard(ws)
    return ws

# -------------------------
# App bootstrap
# -------------------------
async def on_startup(app: web.Application) -> None:
    app["ws_ok"] = False
    app["ws_since_ms"] = None
    app["ob"] = OrderBookState()
    app["heat"] = HeatHistory(COLS, ROWS)
    app["clients"] = set()

    app["tasks"] = [
        asyncio.create_task(mexc_ws_task(app)),
        asyncio.create_task(heat_writer_task(app)),
        asyncio.create_task(state_broadcast_task(app)),
    ]

async def on_cleanup(app: web.Application) -> None:
    for t in app.get("tasks", []):
        t.cancel()
    await asyncio.gather(*app.get("tasks", []), return_exceptions=True)

def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/uiws", handle_uiws)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("QD_PORT", "8000")))
    host = os.environ.get("QD_HOST", "0.0.0.0")
    web.run_app(make_app(), host=host, port=port, access_log=None)
