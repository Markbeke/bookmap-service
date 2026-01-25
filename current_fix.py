# QuantDesk Bookmap Service - current_fix.py
# Build: FIX11/P01
# Subsystem: FOUNDATION+STREAM+INCREMENTAL_LADDER_MERGE_ENGINE+TRADE_TAPE+UI_INDEX
# Venue: MEXC Futures (BTC_USDT Perpetuals)
# Date: 2026-01-25
#
# FIX09 objective:
# - Correct depth semantics for MEXC Futures: push.depth messages may be partial (one-sided / incremental-like).
# - Replace FIX08 "replace-on-snapshot" with "merge" semantics:
#     * Update only sides present in the message
#     * Do NOT wipe the opposite side when missing
#     * Remove price levels when qty <= 0
# - Maintain deterministic single-writer state; keep invariants; expose book + telemetry.
#
# Notes (truth/limits):
# - We still subscribe via sub.depth / sub.deal on wss://contract.mexc.com/edge.
# - If MEXC provides an explicit full snapshot flag/type, we will incorporate it once observed in raw frames.
# - No visualization/heatmap yet.

import asyncio
import json
import os
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import websockets
import uvicorn
from collections import deque

SERVICE = "qd-bookmap"
BUILD = "FIX11/P01"
SUBSYSTEM = "FOUNDATION+STREAM+INCREMENTAL_LADDER_MERGE_ENGINE+TRADE_TAPE+UI_INDEX"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5000))

WS_URL = os.environ.get("QD_MEXC_CONTRACT_WS", "wss://contract.mexc.com/edge").strip()
SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT").strip().upper()

PING_SECS = float(os.environ.get("QD_WS_PING_SECS", "15"))
TOP_N = int(os.environ.get("QD_BOOK_TOP_N", "25"))
RAW_RING_MAX = int(os.environ.get("QD_RAW_RING_MAX", "60"))
FRESH_SECS_GREEN = float(os.environ.get("QD_FRESH_SECS_GREEN", "5.0"))

DEAL_COMPRESS = os.environ.get("QD_DEAL_COMPRESS", "false").strip().lower() in ("1", "true", "yes", "y", "on")


def _now() -> float:
    return time.time()


def _safe_str(e: BaseException) -> str:
    return f"{type(e).__name__}: {str(e)}"[:800]


class Ring:
    def __init__(self, maxlen: int) -> None:
        self.maxlen = maxlen
        self._buf: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def push(self, item: Dict[str, Any]) -> None:
        with self._lock:
            self._buf.append(item)
            if len(self._buf) > self.maxlen:
                self._buf = self._buf[-self.maxlen :]

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._buf)


RAW_RING = Ring(RAW_RING_MAX)
START_TS = _now()
LOCK = threading.Lock()

# Recent trades ring (most recent last)
TAPE = deque(maxlen=300)


class GlobalLadderBook:
    def __init__(self) -> None:
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.version: Optional[int] = None
        self.last_update_ts: float = 0.0
        self.last_side_update_ts: Dict[str, float] = {"bids": 0.0, "asks": 0.0}

    def best_bid(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.bids:
            return None
        p = max(self.bids.keys())
        return p, self.bids[p]

    def best_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.asks:
            return None
        p = min(self.asks.keys())
        return p, self.asks[p]

    def depth_counts(self) -> Dict[str, int]:
        return {"bids": len(self.bids), "asks": len(self.asks)}

    def top_n(self, n: int) -> Dict[str, List[List[str]]]:
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return {
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
        }

    def totals(self) -> Dict[str, str]:
        bid_qty = sum(self.bids.values(), Decimal("0"))
        ask_qty = sum(self.asks.values(), Decimal("0"))
        return {"bid_qty": str(bid_qty), "ask_qty": str(ask_qty)}


BOOK = GlobalLadderBook()


def _to_dec(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        if isinstance(x, Decimal):
            return x
        if isinstance(x, (int, float)):
            return Decimal(str(x))
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            return Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    return None


def _parse_depth_side(side: Any) -> Optional[List[Tuple[Decimal, Decimal]]]:
    """
    MEXC futures push.depth format: [[price, orderCount, qty], ...]
    Use price (index 0) and qty (index 2). If only [price, qty] exists, accept (index 1).
    """
    if side is None:
        return None
    if not isinstance(side, list):
        return None
    out: List[Tuple[Decimal, Decimal]] = []
    for row in side:
        if not isinstance(row, list) or len(row) < 2:
            return None
        p = _to_dec(row[0])
        q = _to_dec(row[2] if len(row) >= 3 else row[1])
        if p is None or q is None:
            return None
        out.append((p, q))
    return out


def _check_invariants() -> Tuple[bool, str]:
    bb = BOOK.best_bid()
    ba = BOOK.best_ask()
    if bb and ba and bb[0] >= ba[0]:
        return False, f"crossed_book best_bid={bb[0]} best_ask={ba[0]}"
    return True, ""


state: Dict[str, Any] = {
    "status": "DISCONNECTED",
    "frames": 0,
    "reconnects": 0,
    "last_error": "",
    "last_close_code": None,
    "last_close_reason": "",
    "last_connect_ts": 0.0,
    "last_event_ts": 0.0,
    "engine": "WARMING",
    "depth_msgs": 0,
    "merge_applied": 0,
    "parse_hits": 0,
    "parse_misses": 0,
    "invariants_ok": True,
    "last_invariant_violation": "",
    "last_trade_px": None,
    "last_trade_side": None,
    "last_trade_qty": None,
    "last_trade_ts": 0.0,
    "trades_seen": 0,
    "sides_seen": {"bids": 0, "asks": 0},
}


def _health() -> str:
    with LOCK:
        fresh = (_now() - BOOK.last_update_ts) <= FRESH_SECS_GREEN
        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        if state["engine"] == "ACTIVE" and fresh and bb and ba and state["invariants_ok"]:
            return "GREEN"
        if state["status"] in ("CONNECTED", "ERROR"):
            return "YELLOW"
        return "RED"


def _sub_depth_msg() -> Dict[str, Any]:
    return {"method": "sub.depth", "param": {"symbol": SYMBOL}}


def _sub_deal_msg() -> Dict[str, Any]:
    param: Dict[str, Any] = {"symbol": SYMBOL}
    if DEAL_COMPRESS is False:
        param["compress"] = False
    return {"method": "sub.deal", "param": param}


async def _ping_loop(ws) -> None:
    while True:
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return
        await asyncio.sleep(PING_SECS)


def _merge_side(side_name: str, levels: List[Tuple[Decimal, Decimal]]) -> None:
    book_side = BOOK.bids if side_name == "bids" else BOOK.asks
    for p, q in levels:
        if q <= 0:
            if p in book_side:
                del book_side[p]
        else:
            book_side[p] = q
    BOOK.last_side_update_ts[side_name] = _now()


def _apply_depth_merge(depth_data: Dict[str, Any]) -> None:
    raw_asks = depth_data.get("asks", None)
    raw_bids = depth_data.get("bids", None)
    ver = depth_data.get("version", None)

    asks_lv = _parse_depth_side(raw_asks) if raw_asks is not None else None
    bids_lv = _parse_depth_side(raw_bids) if raw_bids is not None else None

    if asks_lv is None and bids_lv is None:
        state["parse_misses"] += 1
        return

    if asks_lv is not None:
        _merge_side("asks", asks_lv)
        state["sides_seen"]["asks"] += 1

    if bids_lv is not None:
        _merge_side("bids", bids_lv)
        state["sides_seen"]["bids"] += 1

    if isinstance(ver, int):
        BOOK.version = ver

    BOOK.last_update_ts = _now()

    ok, viol = _check_invariants()
    state["invariants_ok"] = ok
    state["last_invariant_violation"] = viol
    if not ok:
        state["engine"] = "ERROR"
        state["last_error"] = viol
    else:
        state["engine"] = "ACTIVE"
        state["last_error"] = ""

    state["merge_applied"] += 1


async def _ws_once() -> None:
    async with websockets.connect(
        WS_URL,
        ping_interval=None,
        close_timeout=10,
        max_queue=256,
    ) as ws:
        with LOCK:
            state["status"] = "CONNECTED"
            state["last_connect_ts"] = _now()
            state["last_error"] = ""
            state["last_close_code"] = None
            state["last_close_reason"] = ""

        ptask = asyncio.create_task(_ping_loop(ws))

        await ws.send(json.dumps(_sub_depth_msg()))
        await ws.send(json.dumps(_sub_deal_msg()))

        while True:
            msg = await ws.recv()
            ts = _now()
            with LOCK:
                state["frames"] += 1
                state["last_event_ts"] = ts

            if isinstance(msg, bytes):
                msg_s = msg.decode("utf-8", errors="replace")
            else:
                msg_s = str(msg)

            RAW_RING.push({"ts": ts, "raw": msg_s[:4000]})

            try:
                j = json.loads(msg_s)
            except Exception:
                with LOCK:
                    state["parse_misses"] += 1
                continue

            if not isinstance(j, dict):
                with LOCK:
                    state["parse_misses"] += 1
                continue

            ch = str(j.get("channel", ""))
            sym = str(j.get("symbol", "")).upper() if j.get("symbol") else ""

            if sym and sym != SYMBOL.upper():
                continue

            with LOCK:
                if ch == "push.depth":
                    state["depth_msgs"] += 1
                    data = j.get("data")
                    if isinstance(data, dict):
                        _apply_depth_merge(data)
                        state["parse_hits"] += 1
                    else:
                        state["parse_misses"] += 1
                elif ch == "push.deal":
                    data = j.get("data")
                    if isinstance(data, list) and data:
                        for t in data:
                            if not isinstance(t, dict):
                                continue
                            px = t.get("p")
                            qty = t.get("v")
                            side = t.get("T")
                            # MEXC spot push.deal: T=1 buy, T=2 sell
                            side_s = "BUY" if str(side) == "1" else ("SELL" if str(side) == "2" else None)
                            evt = {"ts": ts, "px": str(px) if px is not None else None, "qty": str(qty) if qty is not None else None, "side": side_s}
                            TAPE.append(evt)
                            state["trades_seen"] += 1
                            if evt["px"] is not None:
                                state["last_trade_px"] = evt["px"]
                            if evt["qty"] is not None:
                                state["last_trade_qty"] = evt["qty"]
                            if evt["side"] is not None:
                                state["last_trade_side"] = evt["side"]
                            state["last_trade_ts"] = ts
                    state["parse_hits"] += 1
