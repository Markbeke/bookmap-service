# QuantDesk Bookmap Service - current_fix.py
# Build: FIX10/P01
# Subsystem: FOUNDATION+STREAM+INCREMENTAL_LADDER_MERGE_ENGINE+TRADE_TAPE_INGESTION
# Venue: MEXC Futures (BTC_USDT Perpetuals)
# Date: 2026-01-25
#
# FIX10 objective:
# - Add Trade Tape ingestion for MEXC contract WS, alongside the FIX09 ladder merge engine.
# - Persist a small rolling trade tape ring for downstream visualization (bubbles/delta later).
# - Expose /tape.json and extend telemetry with tape stats.
#
# Truth/limits:
# - MEXC contract WS may use push.deal or push.trade for prints depending on API version.
#   This FIX listens to BOTH channels and records whichever arrives, without guessing beyond the observed schema.
# - We only record raw trade prints (px/qty/side/time if present). No delta/aggregation yet.

import asyncio
import json
import os
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import websockets
import uvicorn

SERVICE = "qd-bookmap"
BUILD = "FIX10/P01"
SUBSYSTEM = "FOUNDATION+STREAM+INCREMENTAL_LADDER_MERGE_ENGINE+TRADE_TAPE_INGESTION"

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5000))

WS_URL = os.environ.get("QD_MEXC_CONTRACT_WS", "wss://contract.mexc.com/edge").strip()
SYMBOL = os.environ.get("QD_SYMBOL", "BTC_USDT").strip().upper()

PING_SECS = float(os.environ.get("QD_WS_PING_SECS", "15"))
TOP_N = int(os.environ.get("QD_BOOK_TOP_N", "25"))
RAW_RING_MAX = int(os.environ.get("QD_RAW_RING_MAX", "80"))
FRESH_SECS_GREEN = float(os.environ.get("QD_FRESH_SECS_GREEN", "5.0"))

TAPE_RING_MAX = int(os.environ.get("QD_TAPE_RING_MAX", "300"))


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
TAPE_RING = Ring(TAPE_RING_MAX)

START_TS = _now()
LOCK = threading.Lock()


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
    Expected MEXC contract push.depth format variants observed historically:
      - [[price, orderCount, qty], ...]
      - [[price, qty], ...]
    We use price and qty.
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
    "sides_seen": {"bids": 0, "asks": 0},
    # tape
    "trade_msgs": 0,
    "trades_seen": 0,
    "last_trade_px": None,
    "last_trade_side": None,
    "last_trade_qty": None,
    "last_trade_ts": 0.0,
    "tape_channel": None,
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
    # MEXC often uses sub.deal for prints. Keep it.
    return {"method": "sub.deal", "param": {"symbol": SYMBOL}}


def _sub_trade_msg() -> Dict[str, Any]:
    # Some variants may require sub.trade. Sending it is harmless if ignored.
    return {"method": "sub.trade", "param": {"symbol": SYMBOL}}


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


def _norm_side(side_val: Any) -> Optional[str]:
    # Observed: T=1 buy, T=2 sell. Some feeds may provide "BUY"/"SELL".
    if side_val is None:
        return None
    s = str(side_val).strip().upper()
    if s in ("1", "BUY", "B"):
        return "BUY"
    if s in ("2", "SELL", "S"):
        return "SELL"
    return None


def _record_trade(tr: Dict[str, Any], channel: str, recv_ts: float) -> None:
    """
    Record one trade print into tape ring.
    We keep both raw fields and normalized fields where possible.
    """
    px = tr.get("p", tr.get("price"))
    qty = tr.get("v", tr.get("q", tr.get("qty", tr.get("size"))))
    side = tr.get("T", tr.get("side"))
    ts = tr.get("t", tr.get("ts", tr.get("time")))

    side_n = _norm_side(side)

    # Normalize timestamp: if numeric, keep as-is; else use recv_ts
    ts_out: Any = ts
    if isinstance(ts, (int, float)):
        ts_out = ts
    elif isinstance(ts, str):
        # keep original string; cannot confirm format
        ts_out = ts
    else:
        ts_out = recv_ts

    tape_item = {
        "recv_ts": recv_ts,
        "channel": channel,
        "px": str(px) if px is not None else None,
        "qty": str(qty) if qty is not None else None,
        "side": side_n,
        "ts": ts_out,
        "raw": {k: tr.get(k) for k in list(tr.keys())[:25]},
    }
    TAPE_RING.push(tape_item)

    state["trades_seen"] += 1
    state["trade_msgs"] += 1
    state["tape_channel"] = channel
    state["last_trade_px"] = tape_item["px"] or state["last_trade_px"]
    state["last_trade_qty"] = tape_item["qty"] or state["last_trade_qty"]
    state["last_trade_side"] = tape_item["side"] or state["last_trade_side"]
    state["last_trade_ts"] = recv_ts


def _handle_trade_payload(data: Any, channel: str, recv_ts: float) -> None:
    """
    Accepts common payload shapes:
      - list[dict]   (push.deal often)
      - dict         (single trade)
      - dict with 'data' list (some variants)
    """
    if isinstance(data, list):
        for it in data:
            if isinstance(it, dict):
                _record_trade(it, channel, recv_ts)
    elif isinstance(data, dict):
        # sometimes nested
        inner = data.get("data")
        if isinstance(inner, list):
            for it in inner:
                if isinstance(it, dict):
                    _record_trade(it, channel, recv_ts)
        else:
            _record_trade(data, channel, recv_ts)


async def _ws_once() -> None:
    async with websockets.connect(
        WS_URL,
        ping_interval=None,
        close_timeout=10,
        max_queue=512,
    ) as ws:
        with LOCK:
            state["status"] = "CONNECTED"
            state["last_connect_ts"] = _now()
            state["last_error"] = ""
            state["last_close_code"] = None
            state["last_close_reason"] = ""

        ptask = asyncio.create_task(_ping_loop(ws))

        # subscriptions
        await ws.send(json.dumps(_sub_depth_msg()))
        await ws.send(json.dumps(_sub_deal_msg()))
        await ws.send(json.dumps(_sub_trade_msg()))

        while True:
            msg = await ws.recv()
            ts = _now()
            with LOCK:
                state["frames"] += 1
                state["last_event_ts"] = ts

            msg_s = msg.decode("utf-8", errors="replace") if isinstance(msg, bytes) else str(msg)
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

                elif ch in ("push.deal", "push.trade"):
                    data = j.get("data")
                    if data is not None:
                        _handle_trade_payload(data, ch, ts)
                        state["parse_hits"] += 1
                    else:
                        state["parse_misses"] += 1

                elif ch in ("pong", ""):
                    # ignore pongs and empty
                    pass
                else:
                    # ignore other channels
                    pass

        ptask.cancel()


async def _ws_forever() -> None:
    backoff = 0.5
    backoff_max = 15.0
    while True:
        try:
            await _ws_once()
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except Exception as e:
            with LOCK:
                state["status"] = "ERROR"
                state["last_error"] = _safe_str(e)
                state["reconnects"] += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, backoff_max)


def _start_ws_thread() -> None:
    def runner() -> None:
        try:
            asyncio.run(_ws_forever())
        except Exception as e:
            with LOCK:
                state["status"] = "ERROR"
                state["last_error"] = "ws_thread_crashed: " + _safe_str(e)

    threading.Thread(target=runner, name="qd_ws", daemon=True).start()


app = FastAPI(title="QuantDesk Bookmap", version=BUILD)


@app.on_event("startup")
def startup() -> None:
    _start_ws_thread()


@app.get("/")
def root() -> HTMLResponse:
    with LOCK:
        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        payload = {
            "service": SERVICE,
            "build": BUILD,
            "subsystem": SUBSYSTEM,
            "health": _health(),
            "uptime_s": round(_now() - START_TS, 3),
            "ws_url": WS_URL,
            "symbol": SYMBOL,
            "state": state,
            "book": {
                "best_bid": [str(bb[0]), str(bb[1])] if bb else None,
                "best_ask": [str(ba[0]), str(ba[1])] if ba else None,
                "depth_counts": BOOK.depth_counts(),
                "totals": BOOK.totals(),
                "version": BOOK.version,
                "last_update_ts": BOOK.last_update_ts,
                "last_side_update_ts": BOOK.last_side_update_ts,
            },
            "links": {
                "health": "/health.json",
                "telemetry": "/telemetry.json",
                "book": "/book.json",
                "tape": "/tape.json",
                "raw": "/raw.json",
            },
        }
    return HTMLResponse("<pre>" + json.dumps(payload, indent=2) + "</pre>")


@app.get("/health.json")
def health_json() -> JSONResponse:
    return JSONResponse(
        {
            "service": SERVICE,
            "build": BUILD,
            "subsystem": SUBSYSTEM,
            "health": _health(),
            "uptime_s": round(_now() - START_TS, 3),
            "ws_url": WS_URL,
            "symbol": SYMBOL,
        }
    )


@app.get("/telemetry.json")
def telemetry_json() -> JSONResponse:
    with LOCK:
        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        return JSONResponse(
            {
                "service": SERVICE,
                "build": BUILD,
                "subsystem": SUBSYSTEM,
                "health": _health(),
                "uptime_s": round(_now() - START_TS, 3),
                "connector": {
                    "status": state["status"],
                    "frames": state["frames"],
                    "reconnects": state["reconnects"],
                    "last_error": state["last_error"],
                    "last_connect_ts": state["last_connect_ts"],
                    "last_event_ts": state["last_event_ts"],
                },
                "engine": {
                    "status": state["engine"],
                    "depth_msgs": state["depth_msgs"],
                    "merge_applied": state["merge_applied"],
                    "parse_hits": state["parse_hits"],
                    "parse_misses": state["parse_misses"],
                    "invariants_ok": state["invariants_ok"],
                    "last_invariant_violation": state["last_invariant_violation"],
                    "sides_seen": state["sides_seen"],
                    "book_version": BOOK.version,
                    "book_last_update_ts": BOOK.last_update_ts,
                    "best_bid": [str(bb[0]), str(bb[1])] if bb else None,
                    "best_ask": [str(ba[0]), str(ba[1])] if ba else None,
                    "depth_counts": BOOK.depth_counts(),
                    "totals": BOOK.totals(),
                },
                "tape": {
                    "trade_msgs": state["trade_msgs"],
                    "trades_seen": state["trades_seen"],
                    "tape_channel": state["tape_channel"],
                    "last_trade_px": state["last_trade_px"],
                    "last_trade_side": state["last_trade_side"],
                    "last_trade_qty": state["last_trade_qty"],
                    "last_trade_ts": state["last_trade_ts"],
                    "ring_size": len(TAPE_RING.snapshot()),
                },
            }
        )


@app.get("/book.json")
def book_json() -> JSONResponse:
    with LOCK:
        bb = BOOK.best_bid()
        ba = BOOK.best_ask()
        return JSONResponse(
            {
                "service": SERVICE,
                "build": BUILD,
                "symbol": SYMBOL,
                "health": _health(),
                "book": {
                    "best_bid": [str(bb[0]), str(bb[1])] if bb else None,
                    "best_ask": [str(ba[0]), str(ba[1])] if ba else None,
                    "depth_counts": BOOK.depth_counts(),
                    "totals": BOOK.totals(),
                    "version": BOOK.version,
                    "last_update_ts": BOOK.last_update_ts,
                    "top": BOOK.top_n(TOP_N),
                },
            }
        )


@app.get("/tape.json")
def tape_json() -> JSONResponse:
    # no lock required for ring snapshot; ring has internal lock
    tape = TAPE_RING.snapshot()
    with LOCK:
        out = {
            "service": SERVICE,
            "build": BUILD,
            "symbol": SYMBOL,
            "health": _health(),
            "stats": {
                "trade_msgs": state["trade_msgs"],
                "trades_seen": state["trades_seen"],
                "tape_channel": state["tape_channel"],
                "last_trade_px": state["last_trade_px"],
                "last_trade_side": state["last_trade_side"],
                "last_trade_qty": state["last_trade_qty"],
                "last_trade_ts": state["last_trade_ts"],
                "ring_size": len(tape),
                "ring_max": TAPE_RING_MAX,
            },
            "tape": tape,
        }
    return JSONResponse(out)


@app.get("/raw.json")
def raw_json() -> JSONResponse:
    return JSONResponse(
        {
            "service": SERVICE,
            "build": BUILD,
            "symbol": SYMBOL,
            "raw_ring": RAW_RING.snapshot(),
        }
    )


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
