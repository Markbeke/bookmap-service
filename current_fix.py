#!/usr/bin/env python3
# =========================================================
# QuantDesk Bookmap Service — FIX17_HEATMAP_TIME_DENSITY_LAYER
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
BUILD = "FIX20/P15"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "5000"))

SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT")
WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")

# Render controls (minimal)
RENDER_FPS = float(os.environ.get("QD_RENDER_FPS", "8"))

# Heatmap (resting liquidity) layer — FIX17
# We maintain decayed intensity per price level (separately for bid/ask sides)
# so liquidity "bands" persist briefly, similar to Bookmap's heat shading.
HEAT_HALFLIFE_S = float(os.environ.get("QD_HEAT_HALFLIFE_S", "12.0"))  # decay half-life
HEAT_MAX_KEYS = int(os.environ.get("QD_HEAT_MAX_KEYS", "4000"))         # safety cap for dict size
HEAT_ALPHA = float(os.environ.get("QD_HEAT_ALPHA", "0.55"))             # max opacity for heat overlay (0..1)
RENDER_LEVELS = int(os.environ.get("QD_LEVELS", "140"))   # visible rows
RENDER_STEP = float(os.environ.get("QD_STEP", "0.1"))      # price tick for ladder rows
ANCHOR_ALPHA = float(os.environ.get("QD_ANCHOR_ALPHA", "0.25"))  # smoothing factor
ANCHOR_HISTORY_SEC = float(os.environ.get("QD_ANCHOR_HISTORY_SEC", "90"))  # overlay window seconds

# Bookmap parity pivot — FIX20/P01
# Absolute price grid + horizontal time ring buffer + camera viewport.
BM_BASE_COL_DT = float(os.environ.get("QD_BM_BASE_COL_DT", "0.25"))   # seconds per base column (producer cadence)
BM_MAX_COLS = int(os.environ.get("QD_BM_MAX_COLS", "1200"))           # ring buffer width (columns)

# Fixed, instrument-specific price universe (camera can move into empty space)
BM_GLOBAL_MIN_PRICE = float(os.environ.get("QD_BM_GLOBAL_MIN_PRICE", "0"))
BM_GLOBAL_MAX_PRICE = float(os.environ.get("QD_BM_GLOBAL_MAX_PRICE", "200000"))
BM_DEFAULT_PRICE_SPAN_BINS = int(os.environ.get("QD_BM_PRICE_SPAN_BINS", "700"))  # visible price bins (zoom)

# Instrument-specific price universe (Bookmap-like: pan/zoom across wide absolute ranges).
# For BTC_USDT we default to a generous universe so you can jump/pan to 95k/120k etc.
# This is a VIEW constraint only; it does not invent data (empty space remains empty).
BM_UNIVERSE_MIN_PRICE = float(os.getenv("QD_BM_UNIVERSE_MIN_PRICE", "0"))
BM_UNIVERSE_MAX_PRICE = float(os.getenv("QD_BM_UNIVERSE_MAX_PRICE", "200000"))

# Price LOD: keep per-column point counts bounded when zoomed out massively.
# We aggregate intensities into coarser bins when the visible span is very large.
BM_PRICE_LOD_TARGET_BINS = int(os.getenv("QD_BM_PRICE_LOD_TARGET_BINS", "2200"))  # ~points/col max

BM_DEFAULT_TIME_WINDOW_S = float(os.environ.get("QD_BM_TIME_WINDOW_S", "90"))     # target visible time span in seconds at default scale
BM_MAX_DOWNSAMPLE = int(os.environ.get("QD_BM_MAX_DOWNSAMPLE", "64"))             # max column skip factor at extreme zoom-out
BM_MIN_PRICE_SPAN_BINS = int(os.environ.get("QD_BM_MIN_PRICE_SPAN_BINS", "80"))
BM_MAX_PRICE_SPAN_BINS = int(os.environ.get("QD_BM_MAX_PRICE_SPAN_BINS", "400000"))

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

    # Heatmap (FIX17): decayed resting liquidity intensity per price
    heat_bid: Dict[float, float] = field(default_factory=dict)
    heat_ask: Dict[float, float] = field(default_factory=dict)
    heat_last_ts: Optional[float] = None

    # Bookmap tape (FIX20): time ring buffer of liquidity snapshots in absolute price bins.
    # Each column is {"ts": float, "col": Dict[int, float]} where keys are price_idx.
    bm_tick: float = 0.1  # inferred/overridden tick size for price binning
    bm_min_idx_seen: Optional[int] = None
    bm_max_idx_seen: Optional[int] = None

    bm_global_min_price: float = BM_GLOBAL_MIN_PRICE
    bm_global_max_price: float = BM_GLOBAL_MAX_PRICE
    bm_intensity: Dict[int, float] = field(default_factory=dict)
    bm_prev_qty: Dict[int, float] = field(default_factory=dict)
    bm_tape: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=BM_MAX_COLS))
    bm_last_col_ts: Optional[float] = None

    # Default camera state (server-side hints; UI owns interaction)
    bm_center_idx: Optional[int] = None
    bm_price_span_bins: int = BM_DEFAULT_PRICE_SPAN_BINS
    bm_time_offset_cols: int = 0
    bm_time_downsample: int = 1

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

# ----------------------------
# Heatmap (FIX17): decayed liquidity intensity per price
# ----------------------------

def _heat_decay_factor(dt: float) -> float:
    # exponential decay by half-life
    if dt <= 0:
        return 1.0
    hl = max(0.5, float(HEAT_HALFLIFE_S))
    return math.exp(-math.log(2.0) * dt / hl)

def _heat_update_and_extract(prices: List[float], bid_qty: List[float], ask_qty: List[float]) -> Tuple[List[float], List[float]]:
    """Update decayed heat maps and return normalized heat arrays aligned to `prices`."""
    if not prices:
        return [], []
    ts = _now()
    if STATE.heat_last_ts is None:
        dt = 0.0
    else:
        dt = max(0.0, ts - float(STATE.heat_last_ts))
    decay = _heat_decay_factor(dt)
    STATE.heat_last_ts = ts

    # Update intensities for current visible window
    # Safety: limit dict growth
    pmin = prices[0]
    pmax = prices[-1]
    # Because prices are ascending, this defines the visible range
    for i, px in enumerate(prices):
        bq = float(bid_qty[i] or 0.0)
        aq = float(ask_qty[i] or 0.0)
        if bq > 0.0:
            prev = float(STATE.heat_bid.get(px, 0.0)) * decay
            STATE.heat_bid[px] = max(prev, bq)
        else:
            # still decay existing
            if px in STATE.heat_bid:
                STATE.heat_bid[px] = float(STATE.heat_bid[px]) * decay

        if aq > 0.0:
            prev = float(STATE.heat_ask.get(px, 0.0)) * decay
            STATE.heat_ask[px] = max(prev, aq)
        else:
            if px in STATE.heat_ask:
                STATE.heat_ask[px] = float(STATE.heat_ask[px]) * decay

    # Prune keys outside current view range and tiny values
    def _prune_side(d: Dict[float, float]) -> None:
        if not d:
            return
        # soft prune: remove out-of-range or near-zero
        kill = []
        for k, v in d.items():
            if (k < pmin - 1e-9) or (k > pmax + 1e-9) or (v <= 0.0) or (v < 1e-9):
                kill.append(k)
        for k in kill:
            try:
                del d[k]
            except Exception:
                pass
        # hard cap
        if len(d) > HEAT_MAX_KEYS:
            # drop smallest values
            items = sorted(d.items(), key=lambda kv: kv[1])
            for k, _ in items[: max(0, len(d) - HEAT_MAX_KEYS)]:
                try:
                    del d[k]
                except Exception:
                    pass

    _prune_side(STATE.heat_bid)
    _prune_side(STATE.heat_ask)

    # Extract aligned arrays
    hb = [float(STATE.heat_bid.get(px, 0.0)) for px in prices]
    ha = [float(STATE.heat_ask.get(px, 0.0)) for px in prices]

    # Normalize with log scaling to [0,1]
    # Use max over window; avoid divide by 0
    vmax = max(1e-9, max(hb + ha + [1e-9]))
    denom = math.log1p(vmax)
    if denom <= 0:
        return [0.0 for _ in hb], [0.0 for _ in ha]

    def _norm(v: float) -> float:
        return min(1.0, max(0.0, math.log1p(max(0.0, v)) / denom))

    hb_n = [_norm(v) for v in hb]
    ha_n = [_norm(v) for v in ha]
    return hb_n, ha_n

# Render frame (bridge contract)
# ----------------------------


# ----------------------------
# Bookmap tape (FIX20): absolute price grid + time ring buffer
# ----------------------------

def _infer_tick_from_book() -> float:
    """Infer a plausible tick size from current book levels.
    This is a best-effort heuristic; if it fails we fall back to current STATE.bm_tick.
    """
    try:
        # sample up to 50 nearest levels across bids+asks
        pxs = []
        if STATE.book.best_bid is not None:
            pxs.extend(sorted(STATE.book.bids.keys(), reverse=True)[:25])
        if STATE.book.best_ask is not None:
            pxs.extend(sorted(STATE.book.asks.keys())[:25])
        pxs = sorted(set(float(p) for p in pxs))
        if len(pxs) < 3:
            return float(STATE.bm_tick)
        diffs = []
        for a,b in zip(pxs, pxs[1:]):
            d = abs(b-a)
            if d > 0:
                diffs.append(d)
        if not diffs:
            return float(STATE.bm_tick)
        # pick a low quantile to approximate the smallest regular increment
        diffs.sort()
        cand = diffs[max(0, int(0.10*len(diffs))-1)]
        # snap to reasonable decimals
        if cand >= 1:
            tick = round(cand, 2)
        elif cand >= 0.1:
            tick = round(cand, 3)
        else:
            tick = round(cand, 6)
        # safety clamp
        if tick <= 0:
            return float(STATE.bm_tick)
        return float(tick)
    except Exception:
        return float(STATE.bm_tick)
def _update_universe_idx(tick: float) -> None:
    """Update absolute price-universe bounds in index space (depends on tick)."""
    try:
        if tick is None or not math.isfinite(tick) or tick <= 0:
            return
        sym = str(getattr(STATE, "symbol", "") or os.getenv("QD_SYMBOL", "BTC_USDT"))

        umin = BM_UNIVERSE_MIN_PRICE
        umax = BM_UNIVERSE_MAX_PRICE

        if getattr(STATE, "bm_universe_min_price", None) is not None:
            umin = float(STATE.bm_universe_min_price)
        if getattr(STATE, "bm_universe_max_price", None) is not None:
            umax = float(STATE.bm_universe_max_price)

        if not math.isfinite(umin):
            umin = 0.0
        if not math.isfinite(umax):
            umax = 200000.0
        if umax <= umin:
            umax = umin + (200000.0 if "BTC" in sym else 10000.0)

        STATE.bm_min_idx_universe = int(round(umin / tick))
        STATE.bm_max_idx_universe = int(round(umax / tick))
    except Exception:
        return



def _price_to_idx(price: float, tick: float) -> int:
    return int(round(float(price) / float(tick)))


def _idx_to_price(idx: int, tick: float) -> float:
    return float(idx) * float(tick)


def _build_bm_column(tick: float) -> Dict[str, Any]:
    ts = _now()

    # Simple, Bookmap-like significance accumulation (FIX20_P02)
    halflife_s = float(os.environ.get("QD_BM_INT_HALFLIFE_S", "18"))
    accum_gain = float(os.environ.get("QD_BM_ACCUM_GAIN", "1.0"))
    remove_gain = float(os.environ.get("QD_BM_REMOVE_GAIN", "1.75"))
    prune_floor = float(os.environ.get("QD_BM_PRUNE_FLOOR", "0.02"))

    dt = max(1e-3, BM_BASE_COL_DT)
    decay = 0.5 ** (dt / max(1e-6, halflife_s))

    def sig(q: float) -> float:
        return math.log1p(max(0.0, float(q)))

    bids = sorted(STATE.book.bids.items(), key=lambda kv: kv[0], reverse=True)[:MAX_LEVELS_PER_SIDE]
    asks = sorted(STATE.book.asks.items(), key=lambda kv: kv[0])[:MAX_LEVELS_PER_SIDE]

    seen: Dict[int, float] = {}
    for px, qty in bids:
        if qty <= 0: 
            continue
        idx = _price_to_idx(float(px), tick)
        seen[idx] = seen.get(idx, 0.0) + float(qty)
        if STATE.bm_min_idx_seen is None or idx < STATE.bm_min_idx_seen:
            STATE.bm_min_idx_seen = idx
        if STATE.bm_max_idx_seen is None or idx > STATE.bm_max_idx_seen:
            STATE.bm_max_idx_seen = idx
    for px, qty in asks:
        if qty <= 0:
            continue
        idx = _price_to_idx(float(px), tick)
        seen[idx] = seen.get(idx, 0.0) + float(qty)
        if STATE.bm_min_idx_seen is None or idx < STATE.bm_min_idx_seen:
            STATE.bm_min_idx_seen = idx
        if STATE.bm_max_idx_seen is None or idx > STATE.bm_max_idx_seen:
            STATE.bm_max_idx_seen = idx

    # decay
    for k in list(STATE.bm_intensity.keys()):
        STATE.bm_intensity[k] = float(STATE.bm_intensity[k]) * decay
        if STATE.bm_intensity[k] < prune_floor:
            STATE.bm_intensity.pop(k, None)

    # accumulate + removal penalty
    for idx, qty in seen.items():
        prev_i = float(STATE.bm_intensity.get(idx, 0.0))
        prev_qty = float(STATE.bm_prev_qty.get(idx, 0.0))
        new_i = prev_i + accum_gain * sig(qty) * dt
        if prev_qty > 0.0 and qty < prev_qty:
            new_i = max(0.0, new_i - remove_gain * sig(prev_qty - qty) * dt)
        STATE.bm_intensity[idx] = new_i
        STATE.bm_prev_qty[idx] = float(qty)

    col = {int(k): float(v) for (k, v) in STATE.bm_intensity.items() if float(v) >= prune_floor}
    return {"ts": ts, "col": col}


async def _bm_tape_loop() -> None:
    """Continuously append columns into the Bookmap tape ring buffer."""
    # Warm start
    await asyncio.sleep(0.25)
    while True:
        try:
            # Only build columns when book is at least minimally populated
            if STATE.book.best_bid is None or STATE.book.best_ask is None or (len(STATE.book.bids) == 0) or (len(STATE.book.asks) == 0):
                await asyncio.sleep(0.25)
                continue

            # Update tick estimate slowly
            inferred = _infer_tick_from_book()
            if inferred > 0:
                # low-pass to avoid jumpiness
                STATE.bm_tick = 0.9*float(STATE.bm_tick) + 0.1*float(inferred)

            tick = float(STATE.bm_tick)

            # cadence
            ts = _now()
            if STATE.bm_last_col_ts is not None and (ts - STATE.bm_last_col_ts) < BM_BASE_COL_DT:
                await asyncio.sleep(max(0.01, BM_BASE_COL_DT*0.5))
                continue

            col = _build_bm_column(tick)
            STATE.bm_last_col_ts = col["ts"]
            STATE.bm_tape.append(col)

            # Set default camera center if missing
            if STATE.bm_center_idx is None:
                mid = _mid_px()
                if mid is not None:
                    STATE.bm_center_idx = _price_to_idx(float(mid), tick)

            await asyncio.sleep(max(0.01, BM_BASE_COL_DT))
        except Exception:
            # Never crash the service due to tape generation
            await asyncio.sleep(0.25)

def _render_frame(levels: int, step: float, pan_ticks: float = 0.0, *, bm_center_idx: Optional[int] = None, bm_price_span_bins: Optional[int] = None, bm_time_offset_cols: int = 0, bm_time_downsample: int = 1) -> Dict[str, Any]:
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

    
    # Heatmap density (FIX17): update decayed liquidity intensity and extract normalized arrays
    heat_bid, heat_ask = _heat_update_and_extract(prices, bid_qty, ask_qty)
# Trades (last N)
    tape = [{"ts": t.ts, "px": t.px, "qty": t.qty, "side": t.side} for t in list(STATE.trades)[-60:]]

    # Anchor history for time window overlay
    hist = [{"ts": hts, "px": hpx} for (hts, hpx) in list(STATE.anchor_hist)[-900:]]


    # Bookmap viewport slice (FIX20): absolute price grid + horizontal time columns
    tick = float(STATE.bm_tick) if getattr(STATE, "bm_tick", None) else float(RENDER_STEP)
    if bm_center_idx is None:
        bm_center_idx = STATE.bm_center_idx
    if bm_price_span_bins is None:
        bm_price_span_bins = int(STATE.bm_price_span_bins)

    bm_center_idx = int(bm_center_idx) if bm_center_idx is not None else None
    bm_price_span_bins = int(max(BM_MIN_PRICE_SPAN_BINS, min(BM_MAX_PRICE_SPAN_BINS, bm_price_span_bins)))

    bm_time_offset_cols = int(max(0, bm_time_offset_cols))
    bm_time_downsample = int(max(1, min(BM_MAX_DOWNSAMPLE, bm_time_downsample)))

    bm_view = {
        "tick": tick,
        "center_idx": bm_center_idx,
        "price_span_bins": bm_price_span_bins,
        "time_offset_cols": bm_time_offset_cols,
        "time_downsample": bm_time_downsample,
        "live": True,
        "time_scale_s_per_col": BM_BASE_COL_DT * bm_time_downsample,
        "t_minus_s": 0.0,
        "cols": [],
        "min_idx": None,
        "max_idx": None,
    }

    bm_tape_list = list(getattr(STATE, "bm_tape", []))
    if bm_center_idx is not None and bm_price_span_bins > 0 and len(bm_tape_list) > 0:
        half = bm_price_span_bins // 2
        min_idx = bm_center_idx - half
        max_idx = bm_center_idx + half
        bm_view["min_idx"] = int(min_idx)
        bm_view["max_idx"] = int(max_idx)

        # Clamp to instrument universe so user can pan/jump across the full range (Bookmap-like),
        # while still allowing empty space where no data exists.
        umin = getattr(STATE, "bm_min_idx_universe", None)
        umax = getattr(STATE, "bm_max_idx_universe", None)
        if umin is None or umax is None:
            # Fallback to observed range if universe not ready yet
            umin = getattr(STATE, "bm_min_idx_seen", None)
            umax = getattr(STATE, "bm_max_idx_seen", None)
        if umin is not None and umax is not None:
            if min_idx < umin:
                min_idx = umin
                max_idx = min_idx + bm_price_span_bins
            if max_idx > umax:
                max_idx = umax
                min_idx = max_idx - bm_price_span_bins
            bm_view["min_idx"] = int(min_idx)
            bm_view["max_idx"] = int(max_idx)


        # Determine which columns to show
        # We keep a target visible time span; downsample controls scale.
        target_cols = max(120, min(900, int(BM_DEFAULT_TIME_WINDOW_S / (BM_BASE_COL_DT * bm_time_downsample))))
        newest_ix = len(bm_tape_list) - 1
        right_ix = max(0, newest_ix - bm_time_offset_cols)
        left_ix = max(0, right_ix - target_cols * bm_time_downsample)

        # build columns from left->right, skipping by downsample
        cols = []
        idxs = list(range(left_ix, right_ix + 1, bm_time_downsample))
        for j in idxs[-target_cols:]:
            c = bm_tape_list[j]
            sparse = c.get("col", {})
            # Filter to viewport

            # Filter to viewport with price LOD aggregation when zoomed out massively
            span_bins = max(1, int(max_idx - min_idx))
            lod = 1
            if span_bins > BM_PRICE_LOD_TARGET_BINS:
                lod = int(math.ceil(span_bins / float(BM_PRICE_LOD_TARGET_BINS)))
            agg = {}
            for (k, v) in sparse.items():
                try:
                    ik = int(k)
                except Exception:
                    continue
                if ik < min_idx or ik > max_idx:
                    continue
                if lod > 1:
                    b = int((ik - min_idx) // lod)
                    ik2 = int(min_idx + b * lod)
                else:
                    ik2 = ik
                fv = float(v)
                prev = agg.get(ik2)
                if prev is None or fv > prev:
                    agg[ik2] = fv
            pts = [[int(k), float(v)] for (k, v) in agg.items()]
            cols.append({"ts": float(c.get("ts", ts)), "pts": pts})
        bm_view["cols"] = cols
        bm_view["live"] = (bm_time_offset_cols == 0)
        if not bm_view["live"] and len(cols) > 0:
            bm_view["t_minus_s"] = float(bm_tape_list[right_ix].get("ts", ts)) - float(cols[-1]["ts"])
            # approximate offset from newest
            newest_ts = float(bm_tape_list[newest_ix].get("ts", ts))
            cur_ts = float(bm_tape_list[right_ix].get("ts", ts))
            bm_view["t_minus_s"] = max(0.0, newest_ts - cur_ts)
    last_event_age = None
    if STATE.last_event_ts is not None:
        last_event_age = ts - STATE.last_event_ts

    # Health gating:
    # - GREEN only if connected AND fresh data AND parity ok
    # - YELLOW if warming / no frames yet / not connected
    # - RED if connector ERROR
    parity_ok = abs(STATE.last_px - STATE.last_trade_px) <= (STATE.price_tick * 5.0) if (STATE.last_px>0 and STATE.last_trade_px>0 and STATE.price_tick>0) else True
    age = now - float(STATE.last_event_ts or 0.0)

    # Health hysteresis:
    # - GREEN requires a connected feed and *recent* events.
    # - We keep GREEN for a short grace period to avoid flicker on brief gaps.
    green_age = 6.0
    green_grace = 8.0
    yellow_age = 20.0

    if STATE.status == "ERROR":
        health = "RED"
    elif STATE.status == "CONNECTED" and parity_ok and (age <= green_age or (STATE.last_health == "GREEN" and age <= green_grace)):
        health = "GREEN"
    elif STATE.status == "CONNECTED" and age <= yellow_age:
        health = "YELLOW"
    else:
        health = "RED"

    if health != STATE.last_health:
        STATE.last_health = health
        STATE.last_health_change_ts = now

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
            "heat_alpha": HEAT_ALPHA,
            "prices": prices,
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
            "heat_bid": heat_bid,
            "heat_ask": heat_ask,
            "anchor_hist": hist,
            "bm": bm_view,
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
    asyncio.create_task(_bm_tape_loop())


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
    """
    Bidirectional render bridge:
      - Server -> client: continuous render frames (JSON)
      - Client -> server: {"cmd":"set_view","levels":..,"step":..,"pan_ticks":..}
    FIX16_P03:
      - Reliable receive loop (timeout=0.0 caused control messages to be missed)
    """
    await ws.accept()

    # Per-connection interactive view state (mutable)
    view_state = {
        # legacy ladder view params (kept for backward compatibility)
        "levels": int(RENDER_LEVELS),
        "step": float(RENDER_STEP),
        "pan_ticks": 0.0,

        # Bookmap camera params (FIX20)
        "bm_center_idx": None,
        "bm_price_span_bins": int(BM_DEFAULT_PRICE_SPAN_BINS),
        "bm_time_offset_cols": 0,
        "bm_time_downsample": 1,
    }

    def _apply_view(payload: dict) -> None:
        try:
            if "levels" in payload:
                lv = int(payload["levels"])
                if 40 <= lv <= 800:
                    view_state["levels"] = lv
            if "step" in payload:
                st = float(payload["step"])
                # Clamp to avoid degenerate windows
                if st > 0:
                    view_state["step"] = max(1e-6, min(st, 1e6))
            if "pan_ticks" in payload:
                pt = float(payload["pan_ticks"])
                view_state["pan_ticks"] = max(-2000.0, min(pt, 2000.0))

            # Bookmap camera (FIX20)
            if "bm_center_idx" in payload and payload["bm_center_idx"] is not None:
                ci = int(payload["bm_center_idx"])
                view_state["bm_center_idx"] = ci
            if "bm_price_span_bins" in payload:
                ps = int(payload["bm_price_span_bins"])
                view_state["bm_price_span_bins"] = max(int(BM_MIN_PRICE_SPAN_BINS), min(int(BM_MAX_PRICE_SPAN_BINS), ps))
            if "bm_time_offset_cols" in payload:
                to = int(payload["bm_time_offset_cols"])
                view_state["bm_time_offset_cols"] = max(0, to)
            if "bm_time_downsample" in payload:
                td = int(payload["bm_time_downsample"])
                view_state["bm_time_downsample"] = max(1, min(int(BM_MAX_DOWNSAMPLE), td))
        except Exception:
            # ignore malformed values
            return

    async def _recv_loop() -> None:
        while True:
            try:
                msg = await ws.receive_text()
            except Exception:
                return
            try:
                payload = json.loads(msg)
                if isinstance(payload, dict) and payload.get("cmd") == "set_view":
                    _apply_view(payload)
            except Exception:
                # ignore malformed control messages
                pass

    recv_task = asyncio.create_task(_recv_loop())
    try:
        interval = 1.0 / max(1.0, float(RENDER_FPS))
        while True:
            frame = _render_frame(
                int(view_state["levels"]),
                float(view_state["step"]),
                pan_ticks=float(view_state["pan_ticks"]),
                bm_center_idx=view_state.get("bm_center_idx"),
                bm_price_span_bins=int(view_state.get("bm_price_span_bins", BM_DEFAULT_PRICE_SPAN_BINS)),
                bm_time_offset_cols=int(view_state.get("bm_time_offset_cols", 0)),
                bm_time_downsample=int(view_state.get("bm_time_downsample", 1)),
            )
            STATE.frames += 1
            await ws.send_text(json.dumps(frame))
            await asyncio.sleep(interval)
    except Exception:
        return
    finally:
        try:
            recv_task.cancel()
        except Exception:
            pass




def _ui_html() -> str:
    # NOTE: This function must NOT be an f-string.
    # JS template literals contain ${...} which would be interpreted by Python f-strings.
    html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"/>
<title>QuantDesk Bookmap</title>
<style>
  :root{
    --bg:#070a10;
    --panel:#0b1220;
    --panel2:#0e1628;
    --text:#cfd7e6;
    --muted:#93a4bf;
    --green:#2dd36f;
    --yellow:#ffcc00;
    --red:#ff3b30;
    --blue:#3b82f6;
    --heat1:#1e40af;
    --heat2:#facc15;
  }
  html,body{height:100%;margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;}
  #topbar{position:fixed;left:0;right:0;top:0;height:64px;display:flex;align-items:center;gap:10px;padding:10px 12px;background:linear-gradient(180deg, rgba(7,10,16,0.98), rgba(7,10,16,0.60));backdrop-filter: blur(6px);z-index:5}
  .pill{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;background:rgba(11,18,32,0.92);border:1px solid rgba(255,255,255,0.08);font-size:13px;line-height:1}
  .pill b{font-weight:700}
  .pill .dot{width:9px;height:9px;border-radius:999px;background:#666}
  #spacer{flex:1}
  #build{opacity:0.9}
  #canvasWrap{position:fixed;left:0;right:0;top:64px;bottom:0}
  canvas{width:100%;height:100%;display:block;touch-action:none}
  #hint{position:fixed;left:12px;bottom:12px;max-width:620px;background:rgba(11,18,32,0.92);border:1px solid rgba(255,255,255,0.08);border-radius:14px;padding:10px 12px;color:var(--muted);font-size:12px;z-index:6}
  #hint code{color:var(--text)}
</style>
</head>
<body>
  <div id="topbar">
    <div class="pill" id="jumpBtn" style="cursor:pointer;user-select:none;">JUMP</div>
    <div class="pill" id="healthPill"><span class="dot" id="healthDot"></span><b id="healthTxt">HEALTH:</b><span id="healthVal">—</span></div>
    <div class="pill"><b id="modeTxt">LIVE</b></div>
    <div class="pill"><b>TIME:</b> <span id="timeTxt">—</span></div>
    <div class="pill"><b>OFFSET:</b> <span id="offTxt">—</span></div>
    <div id="spacer"></div>
    <div class="pill" id="build">__BUILD__ · __SYMBOL__</div>
  </div>

  <div id="canvasWrap">
    <canvas id="cv"></canvas>
  </div>

  <div id="hint">
    Gestures: <code>1‑finger vertical</code> pan (price), <code>pinch</code> (price zoom), <code>2‑finger horizontal</code> scrub (time), <code>2‑finger vertical</code> zoom (time). Auto‑follow ON by default; turns OFF when you scrub/pan away from LIVE.
  </div>

<script>
(() => {
  const BUILD = "__BUILD__";
  const SYMBOL = "__SYMBOL__";
  const cv = document.getElementById("cv");
  const ctx = cv.getContext("2d", { alpha: false, desynchronized: true });
  const off = document.createElement("canvas");
  const offCtx = off.getContext("2d", { alpha: false });

  const healthDot = document.getElementById("healthDot");
  const healthVal = document.getElementById("healthVal");
  const modeTxt = document.getElementById("modeTxt");
  const timeTxt = document.getElementById("timeTxt");
  const offTxt  = document.getElementById("offTxt");
  const jumpBtn = document.getElementById("jumpBtn");

  let dpr = 1;
  function resize() {
    dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    const r = cv.getBoundingClientRect();
    cv.width = Math.max(1, Math.floor(r.width * dpr));
    cv.height = Math.max(1, Math.floor(r.height * dpr));
  }
  window.addEventListener("resize", resize, { passive: true });
  resize();

  // ---------------------------
  // View state (Bookmap)
  // ---------------------------
  const view = {
    bm_center_idx: null,
    bm_price_span_bins: 6000,
    bm_time_offset_cols: 0,
    bm_time_downsample: 1,
    live: true,
    tick: 1.0,
    min_idx: null,
    max_idx: null,
    lastBm: null,
    lastFrameTs: 0,
  };

  // Interaction + LOD: while interacting, we render by transforming the last fully-rendered bitmap.
  let interacting = false;
  let lastInteractTs = 0;
  let baseBitmapValid = false;
  let baseMinIdx = null;
  let baseMaxIdx = null;

  // Debounced server update
  let pendingSend = null;
  function scheduleSend(ms=160) {
    if (pendingSend) clearTimeout(pendingSend);
    pendingSend = setTimeout(() => {
      pendingSend = null;
      sendView();
    }, ms);
  }

  function setHealth(h) {
    healthVal.textContent = h || "—";
    let c = "#666";
    if (h === "GREEN") c = getComputedStyle(document.documentElement).getPropertyValue("--green");
    else if (h === "YELLOW") c = getComputedStyle(document.documentElement).getPropertyValue("--yellow");
    else if (h === "RED") c = getComputedStyle(document.documentElement).getPropertyValue("--red");
    healthDot.style.background = c;
  }

  function fmtTimeScale() {
    const s = 0.25 * Math.max(1, view.bm_time_downsample); // BM_BASE_COL_DT is 0.25s in server code
    return s.toFixed(2) + "s/col";
  }

  function fmtOffset() {
    if (view.bm_time_offset_cols <= 0) return "0s";
    const s = view.bm_time_offset_cols * 0.25 * Math.max(1, view.bm_time_downsample);
    return "T-" + s.toFixed(1) + "s";
  }

  function updateHUD() {
    modeTxt.textContent = view.bm_time_offset_cols === 0 ? "LIVE" : "HISTORY";
    timeTxt.textContent = fmtTimeScale();
    offTxt.textContent = fmtOffset();
  }

  // ---------------------------
  // WebSocket (render stream)
  // ---------------------------
  let ws = null;
  let wsOpen = false;
  let lastPayload = null;

  function wsUrl() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    return proto + "//" + location.host + "/render.ws";
  }

  function wsConnect() {
    try { if (ws) ws.close(); } catch(e) {}
    ws = new WebSocket(wsUrl());
    wsOpen = false;

    ws.onopen = () => {
      wsOpen = true;
      sendView(true);
    };

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (!msg || !msg.kind) return;
        if (msg.kind === "hello") {
          setHealth(msg.health || "—");
          if (msg.bm && msg.bm.center_idx != null) {
            view.bm_center_idx = msg.bm.center_idx;
            view.bm_price_span_bins = msg.bm.price_span_bins || view.bm_price_span_bins;
            view.bm_time_offset_cols = msg.bm.time_offset_cols || 0;
            view.bm_time_downsample = msg.bm.time_downsample || 1;
            view.tick = msg.bm.tick || view.tick;
            updateHUD();
          }
        } else if (msg.kind === "frame") {
          // Keep latest only (drop older frames) to preserve interactivity.
          lastPayload = msg;
        }
      } catch(e) {
        // ignore malformed frames
      }
    };

    ws.onclose = () => {
      wsOpen = false;
      setTimeout(wsConnect, 600);
    };
    ws.onerror = () => {
      wsOpen = false;
      try { ws.close(); } catch(e) {}
    };
  }

  function send(obj) {
    if (!wsOpen) return;
    try { ws.send(JSON.stringify(obj)); } catch(e) {}
  }

  function sendView(force=false) {
    if (view.bm_center_idx == null) return;
    // When not force, avoid hammering the server during pointer moves; scheduleSend() handles this.
    if (!force && interacting) return;
    send({
      cmd: "set_view",
      bm_center_idx: view.bm_center_idx,
      bm_price_span_bins: view.bm_price_span_bins,
      bm_time_offset_cols: view.bm_time_offset_cols,
      bm_time_downsample: view.bm_time_downsample
    });
  }

  wsConnect();

  // ---------------------------
  // Rendering
  // ---------------------------
  function clearCanvas() {
    ctx.fillStyle = "#070a10";
    ctx.fillRect(0,0,cv.width,cv.height);
  }

  function drawBmToOffscreen(bm) {
    if (!bm || !bm.cols) return false;
    const w = cv.width;
    const h = cv.height;
    off.width = w;
    off.height = h;

    // Background
    offCtx.fillStyle = "#070a10";
    offCtx.fillRect(0,0,w,h);

    // Determine y mapping from indices
    const minIdx = bm.min_idx;
    const maxIdx = bm.max_idx;
    if (minIdx == null || maxIdx == null || maxIdx === minIdx) return false;

    // Precompute y for each idx to avoid repeated division.
    const span = (maxIdx - minIdx);
    function yFromIdx(idx) {
      // higher idx => higher price => top of screen
      const t = (idx - minIdx) / span;
      return Math.round((1.0 - t) * h);
    }

    // Draw heatmap: lines across each column segment.
    // Cols are left->right; we map to x.
    const cols = bm.cols;
    const n = cols.length;
    const xStep = (n <= 1) ? w : (w / (n - 1));
    for (let i=0;i<n;i++) {
      const x = Math.round(i * xStep);
      const pts = cols[i].pts || [];
      for (let j=0;j<pts.length;j++) {
        const idx = pts[j][0];
        const v = pts[j][1];
        if (v <= 0) continue;
        const y = yFromIdx(idx);
        // map v -> intensity
        const t = Math.max(0, Math.min(1, Math.log10(1 + v) / 4.0));
        // lerp between blue and yellow
        const r = Math.round(30 + (250-30)*t);
        const g = Math.round(64 + (204-64)*t);
        const b = Math.round(175 + (21-175)*t);
        offCtx.fillStyle = "rgba(" + r + "," + g + "," + b + "," + (0.12 + 0.55*t) + ")";
        offCtx.fillRect(x, y, Math.max(1, Math.ceil(xStep)), 1);
      }
    }

    baseBitmapValid = true;
    baseMinIdx = minIdx;
    baseMaxIdx = maxIdx;
    return true;
  }

  function drawTransformedBitmap() {
    if (!baseBitmapValid || baseMinIdx == null || baseMaxIdx == null) return false;
    const w = cv.width, h = cv.height;

    const spanBase = (baseMaxIdx - baseMinIdx);
    const spanNow  = (view.bm_price_span_bins);
    if (!spanBase || !spanNow) return false;

    // Compute current min/max based on center/span (server clamps; client preview approximates).
    const minNow = view.bm_center_idx - Math.floor(spanNow/2);
    const maxNow = view.bm_center_idx + Math.floor(spanNow/2);

    // Affine in y: map base idx range to current idx range
    const scaleY = spanBase / (maxNow - minNow);
    const y0Base = 0;
    // translation to align minNow with baseMinIdx in idx-space
    // We want idx=minNow to map to baseMinIdx: in y-space, that means shifting.
    const dyIdx = (minNow - baseMinIdx);
    const dyPx = (dyIdx / spanBase) * h;

    ctx.setTransform(1,0,0,1,0,0);
    ctx.fillStyle = "#070a10";
    ctx.fillRect(0,0,w,h);

    // Apply vertical scale about top; then translate
    ctx.save();
    ctx.translate(0, dyPx);
    ctx.scale(1, scaleY);
    ctx.drawImage(off, 0, 0);
    ctx.restore();

    // Reset
    ctx.setTransform(1,0,0,1,0,0);
    return true;
  }

  function renderLoop() {
    // consume latest frame
    if (lastPayload && lastPayload.kind === "frame") {
      setHealth(lastPayload.health || "—");
      const bm = (lastPayload.render && lastPayload.render.bm) ? lastPayload.render.bm : null;
      if (bm && bm.center_idx != null) {
        view.bm_center_idx = bm.center_idx;
        view.bm_price_span_bins = bm.price_span_bins || view.bm_price_span_bins;
        view.bm_time_offset_cols = bm.time_offset_cols || 0;
        view.bm_time_downsample = bm.time_downsample || 1;
        view.tick = bm.tick || view.tick;
        view.min_idx = bm.min_idx;
        view.max_idx = bm.max_idx;
        view.lastBm = bm;
        view.lastFrameTs = lastPayload.ts || 0;
        updateHUD();

        // Update base bitmap only when not interacting (or if base is invalid)
        if (!interacting || !baseBitmapValid) {
          drawBmToOffscreen(bm);
          // draw 1:1
          ctx.setTransform(1,0,0,1,0,0);
          ctx.drawImage(off, 0, 0);
        }
      }
      lastPayload = null;
    }

    if (interacting) {
      // During interaction: fast transform preview
      drawTransformedBitmap();
      if (performance.now() - lastInteractTs > 220) {
        // Consider interaction ended
        interacting = false;
        scheduleSend(10);
      }
    } else if (!baseBitmapValid) {
      clearCanvas();
    }

    requestAnimationFrame(renderLoop);
  }
  requestAnimationFrame(renderLoop);

  // ---------------------------
  // Pointer gestures (iPad-first contract)
  // ---------------------------
  const pointers = new Map();
  let start = null;

  function clampSpanBins(x) {
    x = Math.round(x);
    x = Math.max(800, Math.min(24000, x));
    return x;
  }
  function clampDownsample(x) {
    x = Math.round(x);
    x = Math.max(1, Math.min(64, x));
    return x;
  }
  function clampOffsetCols(x) {
    x = Math.round(x);
    x = Math.max(0, Math.min(60000, x));
    return x;
  }

  function onDown(ev) {
    ev.preventDefault();
    cv.setPointerCapture(ev.pointerId);
    pointers.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});
    if (pointers.size === 1) {
      start = { one: {...pointers.values().next().value}, view: {...view} };
    } else if (pointers.size === 2) {
      const pts = Array.from(pointers.values());
      const mid = {x:(pts[0].x+pts[1].x)/2, y:(pts[0].y+pts[1].y)/2};
      const dist = Math.hypot(pts[0].x-pts[1].x, pts[0].y-pts[1].y);
      start = { two: {pts, mid, dist}, view: {...view} };
    }
  }

  function onMove(ev) {
    if (!pointers.has(ev.pointerId) || view.bm_center_idx == null) return;
    ev.preventDefault();
    pointers.set(ev.pointerId, {x: ev.clientX, y: ev.clientY});

    interacting = true;
    lastInteractTs = performance.now();

    const rect = cv.getBoundingClientRect();
    const w = rect.width, h = rect.height;

    if (pointers.size === 1 && start && start.view) {
      const p = pointers.values().next().value;
      const dy = (p.y - start.one.y);
      // positive dy => drag down => move price window down => center_idx decreases
      const deltaBins = Math.round((dy / h) * start.view.bm_price_span_bins);
      view.bm_center_idx = start.view.bm_center_idx - deltaBins;
      view.bm_time_offset_cols = start.view.bm_time_offset_cols; // keep
      updateHUD();
      scheduleSend(240);
      return;
    }

    if (pointers.size === 2 && start && start.two && start.view) {
      const pts = Array.from(pointers.values());
      const mid = {x:(pts[0].x+pts[1].x)/2, y:(pts[0].y+pts[1].y)/2};
      const dist = Math.hypot(pts[0].x-pts[1].x, pts[0].y-pts[1].y);

      const dx = (mid.x - start.two.mid.x);
      const dy = (mid.y - start.two.mid.y);
      const scale = dist / Math.max(1e-6, start.two.dist);

      // Pinch => price zoom
      const span = clampSpanBins(start.view.bm_price_span_bins / scale);
      view.bm_price_span_bins = span;

      // Two-finger horizontal drag => time scrub
      if (Math.abs(dx) > Math.abs(dy)) {
        const colsPerPx = 0.6; // sensitivity
        view.bm_time_offset_cols = clampOffsetCols(start.view.bm_time_offset_cols + dx * colsPerPx);
      } else {
        // Two-finger vertical drag => time zoom (downsample)
        const dsPerPx = 0.03;
        view.bm_time_downsample = clampDownsample(start.view.bm_time_downsample * (1 + dy * dsPerPx));
      }
      updateHUD();
      scheduleSend(260);
    }
  }

  function onUp(ev) {
    if (pointers.has(ev.pointerId)) pointers.delete(ev.pointerId);
    if (pointers.size === 0) {
      start = null;
      interacting = true;
      lastInteractTs = performance.now();
      scheduleSend(80);
    }
  }

  cv.addEventListener("pointerdown", onDown, { passive: false });
  cv.addEventListener("pointermove", onMove, { passive: false });
  cv.addEventListener("pointerup", onUp, { passive: false });
  cv.addEventListener("pointercancel", onUp, { passive: false });

  // Jump back to LIVE + recent center from last bm
  jumpBtn.addEventListener("click", () => {
    if (view.lastBm && view.lastBm.center_idx != null) {
      view.bm_center_idx = view.lastBm.center_idx;
    }
    view.bm_time_offset_cols = 0;
    updateHUD();
    interacting = false;
    baseBitmapValid = false;
    sendView(true);
  });

})();
</script>
</body>
</html>
"""
    html = html.replace("__BUILD__", BUILD).replace("__SYMBOL__", SYMBOL)
    return html
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
