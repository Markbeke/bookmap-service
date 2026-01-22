import os
import json
import time
import gzip
import zlib
import asyncio
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
import websockets
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

WS_URL = os.environ.get("QD_WS_URL", "wss://contract.mexc.com/edge")
SYMBOL_WS = os.environ.get("QD_SYMBOL_WS", "BTC_USDT")
REST_BASE = os.environ.get("QD_REST_BASE", "https://contract.mexc.com")
PORT = int(os.environ.get("PORT", "8000"))

def now_ms():
    return int(time.time() * 1000)

def try_decompress(payload: bytes) -> bytes:
    try:
        return gzip.decompress(payload)
    except:
        try:
            return zlib.decompress(payload)
        except:
            return payload

@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)

@dataclass
class Trades:
    dq: deque = field(default_factory=lambda: deque(maxlen=5000))

class MexcFeed:
    def __init__(self):
        self.book = OrderBook()
        self.trades = Trades()
        self.ws_ok = False

    async def run(self):
        async with websockets.connect(WS_URL) as ws:
            self.ws_ok = True
            await ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": SYMBOL_WS}}))
            await ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": SYMBOL_WS}}))

            while True:
                raw = await ws.recv()
                if isinstance(raw, bytes):
                    raw = try_decompress(raw).decode()

                msg = json.loads(raw)
                ch = msg.get("channel", "")

                if ch == "push.depth":
                    for p, q, *_ in msg["data"].get("bids", []):
                        self.book.bids[float(p)] = float(q)
                    for p, q, *_ in msg["data"].get("asks", []):
                        self.book.asks[float(p)] = float(q)

                if ch == "push.deal":
                    for t in msg["data"]:
                        self.trades.dq.append(t)

app = FastAPI()
feed = MexcFeed()

@app.on_event("startup")
async def startup():
    asyncio.create_task(feed.run())

@app.get("/")
async def index():
    return {"service": "bookmap-phase1", "symbol": SYMBOL_WS}

@app.get("/health")
async def health():
    return {
        "ws_ok": feed.ws_ok,
        "bids": len(feed.book.bids),
        "asks": len(feed.book.asks),
        "trades": len(feed.trades.dq),
    }

@app.get("/depth")
async def depth():
    return {
        "bids": sorted(feed.book.bids.items(), reverse=True)[:50],
        "asks": sorted(feed.book.asks.items())[:50],
    }

@app.get("/trades")
async def trades():
    return list(feed.trades.dq)[-200:]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
