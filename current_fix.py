#!/usr/bin/env python3
# =========================================================
# QuantDesk Bookmap Service — FIX20_P02_HEATMAP_SIGNIFICANCE_CONTRAST_PARITY
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
BUILD = "FIX20/P02"

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
BM_DEFAULT_PRICE_SPAN_BINS = int(os.environ.get("QD_BM_PRICE_SPAN_BINS", "2200"))  # visible price bins (zoom)
BM_DEFAULT_TIME_WINDOW_S = float(os.environ.get("QD_BM_TIME_WINDOW_S", "90"))     # target visible time span in seconds at default scale
BM_MAX_DOWNSAMPLE = int(os.environ.get("QD_BM_MAX_DOWNSAMPLE", "64"))             # max column skip factor at extreme zoom-out
BM_MIN_PRICE_SPAN_BINS = int(os.environ.get("QD_BM_MIN_PRICE_SPAN_BINS", "80"))
BM_MAX_PRICE_SPAN_BINS = int(os.environ.get("QD_BM_MAX_PRICE_SPAN_BINS", "80000"))

# Heatmap significance/contrast tuning (FIX20/P02)
BM_INT_HALFLIFE_S = float(os.environ.get("QD_BM_INT_HALFLIFE_S", "18.0"))     # intensity decay half-life (seconds)
BM_ACCUM_GAIN = float(os.environ.get("QD_BM_ACCUM_GAIN", "1.0"))              # accumulation gain
BM_REMOVE_GAIN = float(os.environ.get("QD_BM_REMOVE_GAIN", "1.75"))           # removal penalty gain (scar)
BM_PRUNE_FLOOR = float(os.environ.get("QD_BM_PRUNE_FLOOR", "0.02"))           # prune tiny intensities
BM_SIG_MODE = os.environ.get("QD_BM_SIG_MODE", "log")                         # log|sqrt (liquidity->signal)

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
    bm_tape: Deque[Dict[str, Any]] = field(default_factory=lambda: deque(maxlen=BM_MAX_COLS))
    bm_last_col_ts: Optional[float] = None

    # Bookmap significance engine (FIX20/P02)
    bm_intensity: Dict[int, float] = field(default_factory=dict)  # decayed integrated intensity per price_idx
    bm_prev_qty: Dict[int, float] = field(default_factory=dict)   # last seen raw qty per price_idx
    bm_last_int_ts: Optional[float] = None                        # last intensity update time

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


def _price_to_idx(price: float, tick: float) -> int:
    return int(round(float(price) / float(tick)))


def _idx_to_price(idx: int, tick: float) -> float:
    return float(idx) * float(tick)


def _build_bm_column(tick: float) -> Dict[str, Any]:
    """
    Build a sparse column representing *integrated* resting liquidity significance.

    FIX20/P02 changes vs FIX20/P01:
    - We do NOT render raw snapshot depth as intensity.
    - We maintain a decayed, time-integrated intensity field per price_idx:
        intensity[idx] <- intensity[idx]*decay + gain*f(qty)*dt  (persistence reinforcement)
      and apply a removal penalty when qty drops (scar/vacuum preservation).
    """
    ts = _now()

    # Determine dt for integration
    if STATE.bm_last_int_ts is None:
        dt = BM_BASE_COL_DT
    else:
        dt = max(0.001, min(2.0, ts - float(STATE.bm_last_int_ts)))
    STATE.bm_last_int_ts = ts

    # Exponential decay from half-life
    hl = max(0.5, float(BM_INT_HALFLIFE_S))
    decay = math.exp(-dt * math.log(2.0) / hl)

    # Pull book snapshot (bounded)
    bids = sorted(STATE.book.bids.items(), key=lambda kv: kv[0], reverse=True)[:MAX_LEVELS_PER_SIDE]
    asks = sorted(STATE.book.asks.items(), key=lambda kv: kv[0])[:MAX_LEVELS_PER_SIDE]

    cur_qty: Dict[int, float] = {}

    def sig(qty: float) -> float:
        q = max(0.0, float(qty))
        mode = str(BM_SIG_MODE or "log").lower()
        if mode == "sqrt":
            return math.sqrt(q)
        # default: log scaling
        return math.log1p(q)

    def add(px: float, qty: float) -> None:
        if qty <= 0:
            return
        idx = _price_to_idx(px, tick)
        cur_qty[idx] = cur_qty.get(idx, 0.0) + float(qty)

        if STATE.bm_min_idx_seen is None or idx < STATE.bm_min_idx_seen:
            STATE.bm_min_idx_seen = idx
        if STATE.bm_max_idx_seen is None or idx > STATE.bm_max_idx_seen:
            STATE.bm_max_idx_seen = idx

    for px, qty in bids:
        add(float(px), float(qty))
    for px, qty in asks:
        add(float(px), float(qty))

    # Decay existing intensity field (only keys we keep)
    if STATE.bm_intensity:
        for k in list(STATE.bm_intensity.keys()):
            STATE.bm_intensity[k] = float(STATE.bm_intensity.get(k, 0.0)) * decay
            if STATE.bm_intensity[k] < BM_PRUNE_FLOOR:
                # prune tiny residuals to prevent dict growth
                del STATE.bm_intensity[k]
                if k in STATE.bm_prev_qty:
                    del STATE.bm_prev_qty[k]

    # Update intensities with integrated contribution + removal penalty
    for k, q in cur_qty.items():
        prev = float(STATE.bm_prev_qty.get(k, 0.0))
        dq = float(q) - prev

        # persistence reinforcement: integrated contribution
        inc = float(BM_ACCUM_GAIN) * sig(q) * dt
        STATE.bm_intensity[k] = float(STATE.bm_intensity.get(k, 0.0)) + inc

        # liquidity removal scar: when qty drops, subtract additional penalty
        if dq < 0:
            penalty = float(BM_REMOVE_GAIN) * sig(-dq)
            STATE.bm_intensity[k] = max(0.0, float(STATE.bm_intensity.get(k, 0.0)) - penalty)

        STATE.bm_prev_qty[k] = float(q)

    # Build the sparse column from the current intensity field (viewport will filter)
    col: Dict[int, float] = {int(k): float(v) for (k, v) in STATE.bm_intensity.items()}

    # Safety cap: keep only strongest keys if we get too large (rare; protects runtime)
    if len(col) > HEAT_MAX_KEYS:
        items = sorted(col.items(), key=lambda kv: kv[1], reverse=True)[:HEAT_MAX_KEYS]
        col = {int(k): float(v) for (k, v) in items}
        # align state to cap
        keep = set(col.keys())
        STATE.bm_intensity = {int(k): float(v) for (k, v) in col.items()}
        STATE.bm_prev_qty = {int(k): float(STATE.bm_prev_qty.get(k, 0.0)) for k in keep}

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
            pts = [[int(k), float(v)] for (k, v) in sparse.items() if min_idx <= int(k) <= max_idx]
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
    # Bookmap parity demo (FIX20):
    # - Absolute price axis (bins) + horizontal time columns
    # - Camera viewport (price pan/zoom, time scrub/zoom)
    # - iPad-first gestures:
    #     1-finger vertical drag: price pan
    #     pinch: price zoom
    #     2-finger horizontal drag: time scrub
    #     2-finger vertical drag: time zoom (downsample)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, user-scalable=no" />
  <title>QuantDesk Bookmap</title>
  <style>
    html, body {{ height: 100%; margin: 0; background: #0b0f14; color: #e6eef8; }}
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; overflow:hidden; }}
    #topbar {{
      position: fixed; left: 0; right: 0; top: 0;
      display: flex; gap: 10px; align-items: center; padding: 10px 12px;
      background: rgba(11,15,20,0.88); backdrop-filter: blur(8px);
      border-bottom: 1px solid rgba(255,255,255,0.06);
      z-index: 20;
    }}
    .pill {{ padding: 6px 10px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.14); font-weight: 650; font-size: 13px; }}
    .pill.green {{ border-color: rgba(46,125,50,0.8); color: #76ff7a; }}
    .pill.yellow {{ border-color: rgba(249,168,37,0.8); color: #ffd36b; }}
    .pill.red {{ border-color: rgba(198,40,40,0.8); color: #ff8a80; }}
    .spacer {{ flex: 1; }}
    .muted {{ opacity: 0.8; font-size: 13px; }}
    #cvwrap {{ position: fixed; left: 0; right: 0; top: 52px; bottom: 0; }}
    canvas {{ display:block; width: 100%; height: 100%; touch-action: none; }}
    #hint {{
      position: fixed; left: 12px; bottom: 12px;
      font-size: 12px; opacity: 0.75; background: rgba(0,0,0,0.25);
      padding: 8px 10px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.08);
      max-width: 70ch;
    }}
    a {{ color: inherit; }}
  </style>
</head>
<body>
  <div id="topbar">
    <div class="pill" id="jump">JUMP</div>
    <div id="health" class="pill yellow">HEALTH: YELLOW</div>
    <div id="mode" class="pill">LIVE</div>
    <div id="scale" class="pill">TIME: --</div>
    <div id="offset" class="pill">OFFSET: --</div>
    <div class="spacer"></div>
    <div class="muted">{SERVICE} {BUILD} · {SYMBOL}</div>
  </div>

  <div id="cvwrap"><canvas id="cv"></canvas></div>
  <div id="hint">
    Gestures: 1-finger vertical pan (price), pinch (price zoom), 2-finger horizontal scrub (time), 2-finger vertical drag (time zoom).
    Auto-follow ON by default; it turns OFF when you scrub away from LIVE.
  </div>

<script>
(() => {{
  const cv = document.getElementById('cv');
  const ctx = cv.getContext('2d', {{ alpha: false }});
  const healthEl = document.getElementById('health');
  const modeEl = document.getElementById('mode');
  const scaleEl = document.getElementById('scale');
  const offsetEl = document.getElementById('offset');
  const jumpEl = document.getElementById('jump');

  function resize() {{
    const dpr = window.devicePixelRatio || 1;
    const rect = cv.getBoundingClientRect();
    cv.width = Math.max(1, Math.floor(rect.width * dpr));
    cv.height = Math.max(1, Math.floor(rect.height * dpr));
  }}
  window.addEventListener('resize', resize, {{ passive: true }});
  resize();

  // View (camera) state owned by UI
  const view = {{
    centerIdx: null,        // price bin index
    priceSpan: {BM_DEFAULT_PRICE_SPAN_BINS}, // bins
    timeOffset: 0,          // columns from newest
    timeDownsample: 1,      // 1=base scale; >1 zoom-out in time
    autoFollow: true,
  }};

  // WS render bridge
  let lastTick = null;
  let ws = null;
  function wsUrl() {{
    const proto = (location.protocol === 'https:') ? 'wss:' : 'ws:';
    return proto + '//' + location.host + '/render.ws';
  }}

  function setHealth(h) {{
    healthEl.classList.remove('green','yellow','red');
    if (h === 'GREEN') healthEl.classList.add('green');
    else if (h === 'RED') healthEl.classList.add('red');
    else healthEl.classList.add('yellow');
    healthEl.textContent = 'HEALTH: ' + (h || 'YELLOW');
  }}

  function fmtTimeScale(sPerCol) {{
    if (!isFinite(sPerCol) || sPerCol <= 0) return '--';
    if (sPerCol < 1) return sPerCol.toFixed(2) + 's/col';
    if (sPerCol < 10) return sPerCol.toFixed(1) + 's/col';
    return Math.round(sPerCol) + 's/col';
  }}

  function sendView() {{
    if (!ws || ws.readyState !== 1) return;
    const payload = {{
      cmd: "set_view",
      // legacy params left intact; server will ignore for Bookmap draw
      levels: {RENDER_LEVELS},
      step: {RENDER_STEP},
      pan_ticks: 0.0,

      bm_center_idx: view.centerIdx,
      bm_price_span_bins: Math.round(view.priceSpan),
      bm_time_offset_cols: Math.round(view.timeOffset),
      bm_time_downsample: Math.round(view.timeDownsample),
    }};
    try {{ ws.send(JSON.stringify(payload)); }} catch(e) {{}}
  }}

  let lastSend = 0;
  function throttledSend() {{
    const now = performance.now();
    if (now - lastSend > 50) {{ lastSend = now; sendView(); }}
  }}

  function connect() {{
    ws = new WebSocket(wsUrl());
    ws.onopen = () => {{ sendView(); }};
    ws.onmessage = (ev) => {{
      try {{
        const frame = JSON.parse(ev.data);
        draw(frame);
      }} catch(e) {{}}
    }};
    ws.onclose = () => {{
      setTimeout(connect, 750);
    }};
  }}
  connect();

  // Jump to an absolute price (fast navigation for wide ranges)
  jumpEl.addEventListener('click', () => {{
    const s = prompt('Jump to price (e.g., 90000):', '');
    if (!s) return;
    const px = parseFloat(s);
    if (!isFinite(px) || px <= 0) return;
    // centerIdx = round(price/tick). tick is inferred server-side; use last tick seen from frames if available.
    if (lastTick && isFinite(lastTick) && lastTick > 0) {{
      view.centerIdx = Math.round(px / lastTick);
      view.autoFollow = false;
      throttledSend();
    }}
  }});


  // Color mapping: value -> intensity -> RGB. (Simple, stable; not tuned yet.)
  function valToRGB(v, vmax, floor) {{
    if (!isFinite(v) || v <= floor || vmax <= floor) return [11,15,20];
    const x0 = (v - floor) / Math.max(1e-9, (vmax - floor));
    const x = Math.max(0, Math.min(1, x0));
    // Bookmap-like contrast curve: push midtones darker, preserve highlights
    const y = Math.pow(x, 0.55);
    // blue -> cyan -> yellow -> white (highlights)
    const r = Math.floor(20 + 235 * Math.pow(y, 0.95));
    const g = Math.floor(40 + 215 * Math.pow(y, 0.75));
    const b = Math.floor(85 + 150 * Math.pow(1 - y, 0.55));
    return [r,g,b];
  }}

  function draw(frame) {{
    const W = cv.width, H = cv.height;
    ctx.fillStyle = '#0b0f14';
    ctx.fillRect(0,0,W,H);

    setHealth(frame.health);

    const bm = frame.render && frame.render.bm ? frame.render.bm : null;
    if (bm && bm.tick) {{ lastTick = bm.tick; }}
    if (!bm || !bm.cols) {{
      ctx.fillStyle = 'rgba(230,238,248,0.85)';
      ctx.font = '16px system-ui';
      ctx.fillText('Warming up...', 16, 24);
      return;
    }}

    // initialize centerIdx from server once
    if (view.centerIdx === null && bm.center_idx !== null && bm.center_idx !== undefined) {{
      view.centerIdx = bm.center_idx;
      throttledSend();
    }}

    const live = !!bm.live && (view.timeOffset === 0);
    modeEl.textContent = live ? 'LIVE' : 'HISTORY';
    modeEl.className = 'pill' + (live ? ' green' : '');

    scaleEl.textContent = 'TIME: ' + fmtTimeScale(bm.time_scale_s_per_col);
    offsetEl.textContent = live ? 'OFFSET: 0s' : ('OFFSET: T-' + (bm.t_minus_s || 0).toFixed(1) + 's');

    // Update auto-follow
    if (live) view.autoFollow = true;

    const cols = bm.cols;
    const minIdx = bm.min_idx, maxIdx = bm.max_idx;
    const tick = bm.tick || 0.1;

    // Layout
    const leftPad = Math.floor(W * 0.08); // price labels area
    const rightPad = 8;
    const topPad = 8;
    const botPad = 8;
    const plotW = W - leftPad - rightPad;
    const plotH = H - topPad - botPad;

    // Determine robust scaling (percentile-based) for Bookmap-like contrast
    const sample = [];
    const stride = Math.max(1, Math.floor(cols.length / 140));
    for (let i=0;i<cols.length;i+=stride) {{
      const pts = cols[i].pts || [];
      const pstride = Math.max(1, Math.floor(pts.length / 80));
      for (let j=0;j<pts.length;j+=pstride) {{
        const v = pts[j][1];
        if (isFinite(v) && v > 0) sample.push(v);
      }}
    }}
    sample.sort((a,b)=>a-b);
    function q(p) {{
      if (sample.length === 0) return 0;
      const ix = Math.max(0, Math.min(sample.length-1, Math.floor(p*(sample.length-1))));
      return sample[ix];
    }}
    const p50 = q(0.50);
    const p95 = q(0.95);
    const floor = Math.max(0, p50 * 0.15);
    let vmax = Math.max(1e-9, p95);

    // Render columns
    const colW = Math.max(1, Math.floor(plotW / Math.max(1, cols.length)));
    const span = Math.max(1, (maxIdx - minIdx));
    const pxPerBin = plotH / span;

    for (let ci=0; ci<cols.length; ci++) {{
      const x0 = leftPad + ci * colW;
      const pts = cols[ci].pts || [];
      for (let k=0; k<pts.length; k++) {{
        const idx = pts[k][0];
        const v = pts[k][1];
        const y = topPad + (maxIdx - idx) * pxPerBin; // higher idx at top
        const h = Math.max(1, Math.ceil(pxPerBin));
        const rgb = valToRGB(v, vmax, floor);
        ctx.fillStyle = `rgb(${{rgb[0]}},${{rgb[1]}},${{rgb[2]}})`;
        ctx.fillRect(x0, y, colW, h);
      }}
    }}

    // Draw price labels (few ticks)
    ctx.fillStyle = 'rgba(230,238,248,0.80)';
    ctx.font = (Math.max(10, Math.floor(H/48))) + 'px system-ui';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    const labelCount = 6;
    for (let i=0;i<=labelCount;i++) {{
      const t = i/labelCount;
      const idx = Math.round(maxIdx - t*(maxIdx - minIdx));
      const y = topPad + t*plotH;
      const px = (idx * tick);
      ctx.fillText(px.toFixed(1), leftPad - 8, y);
      // grid line
      ctx.strokeStyle = 'rgba(255,255,255,0.04)';
      ctx.beginPath();
      ctx.moveTo(leftPad, y);
      ctx.lineTo(W-rightPad, y);
      ctx.stroke();
    }}

    // Price line (best bid/ask mid) overlay
    if (frame.book && frame.book.best_bid !== null && frame.book.best_ask !== null) {{
      const mid = (frame.book.best_bid + frame.book.best_ask)/2.0;
      const midIdx = Math.round(mid / tick);
      const y = topPad + (maxIdx - midIdx) * pxPerBin;
      ctx.strokeStyle = 'rgba(255,255,255,0.70)';
      ctx.beginPath();
      ctx.moveTo(leftPad, y);
      ctx.lineTo(W-rightPad, y);
      ctx.stroke();

      // auto-follow: keep mid within viewport by nudging centerIdx on UI side
      if (view.autoFollow && view.centerIdx !== null) {{
        const margin = Math.floor(view.priceSpan * 0.12);
        if (midIdx > (view.centerIdx + margin) || midIdx < (view.centerIdx - margin)) {{
          view.centerIdx = midIdx;
          throttledSend();
        }}
      }}
    }}
  }}

  // Touch gesture handling
  let active = false;
  let startTouches = [];
  let startMid = null;
  let startDist = 0;
  let startCenter = null;
  let startSpan = null;
  let startOffset = null;
  let startDownsample = null;

  function getTouches(e) {{
    const t = [];
    for (let i=0;i<e.touches.length;i++) {{
      const touch = e.touches[i];
      t.push({{ id: touch.identifier, x: touch.clientX, y: touch.clientY }});
    }}
    return t;
  }}

  function dist(a,b) {{
    const dx = a.x-b.x, dy = a.y-b.y;
    return Math.sqrt(dx*dx + dy*dy);
  }}

  function midpoint(a,b) {{
    return {{ x: (a.x+b.x)/2, y: (a.y+b.y)/2 }};
  }}

  function clamp(v, lo, hi) {{
    return Math.max(lo, Math.min(hi, v));
  }}

  cv.addEventListener('touchstart', (e) => {{
    if (!view) return;
    active = true;
    startTouches = getTouches(e);
    if (startTouches.length === 1) {{
      startCenter = view.centerIdx;
    }} else if (startTouches.length >= 2) {{
      const a = startTouches[0], b = startTouches[1];
      startMid = midpoint(a,b);
      startDist = dist(a,b);
      startSpan = view.priceSpan;
      startOffset = view.timeOffset;
      startDownsample = view.timeDownsample;
    }}
  }}, {{ passive: false }});

  cv.addEventListener('touchmove', (e) => {{
    if (!active) return;
    e.preventDefault();
    const t = getTouches(e);
    if (t.length === 1) {{
      // 1-finger vertical pan (price)
      const dy = t[0].y - startTouches[0].y;
      if (view.centerIdx === null) return;
      const binsPerPx = (view.priceSpan / Math.max(50, cv.getBoundingClientRect().height));
      const deltaBins = Math.round(dy * binsPerPx);
      view.centerIdx = (startCenter === null ? view.centerIdx : startCenter) + deltaBins;
      view.autoFollow = false;
      throttledSend();
      return;
    }}
    if (t.length >= 2) {{
      const a = t[0], b = t[1];
      const mid = midpoint(a,b);
      const d = dist(a,b);
      const dx = mid.x - startMid.x;
      const dy = mid.y - startMid.y;

      const pinchRatio = (startDist > 0) ? (d / startDist) : 1.0;
      const pinchDelta = pinchRatio - 1.0;

      // Rule:
      // - If pinch changes distance noticeably -> price zoom
      // - Else: horizontal move -> time scrub, vertical move -> time zoom
      if (Math.abs(pinchDelta) > 0.03) {{
        // pinch: price zoom (inverse ratio)
        const newSpan = (startSpan || view.priceSpan) / pinchRatio;
        view.priceSpan = clamp(newSpan, {BM_MIN_PRICE_SPAN_BINS}, {BM_MAX_PRICE_SPAN_BINS});
        view.autoFollow = false;
        throttledSend();
        return;
      }}

      if (Math.abs(dx) >= Math.abs(dy)) {{
        // two-finger horizontal scrub
        const rect = cv.getBoundingClientRect();
        const colsPerPx = 900 / Math.max(200, rect.width); // heuristic; server clamps anyway
        const deltaCols = Math.round(-dx * colsPerPx);
        view.timeOffset = Math.max(0, (startOffset || view.timeOffset) + deltaCols);
        view.autoFollow = (view.timeOffset === 0);
        throttledSend();
        return;
      }} else {{
        // two-finger vertical drag: time zoom (downsample)
        // drag down => zoom out => increase downsample
        const rect = cv.getBoundingClientRect();
        const units = dy / Math.max(120, rect.height);
        const factor = Math.pow(2, units * 6); // ~64x over full height
        const base = (startDownsample || view.timeDownsample);
        const newDs = clamp(Math.round(base * factor), 1, {BM_MAX_DOWNSAMPLE});
        view.timeDownsample = newDs;
        view.autoFollow = false;
        throttledSend();
        return;
      }}
    }}
  }}, {{ passive: false }});

  cv.addEventListener('touchend', (e) => {{
    active = false;
  }}, {{ passive: true }});
  cv.addEventListener('touchcancel', (e) => {{
    active = false;
  }}, {{ passive: true }});

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
