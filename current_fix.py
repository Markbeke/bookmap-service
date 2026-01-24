# QuantDesk Bookmap Service - current_fix.py
# Build: FIX07/P02
# Purpose: Sanitize Unicode characters (U+2013/U+2014) causing SyntaxError in Replit
# Venue: MEXC Futures (BTC_USDT Perpetuals)
# Date: 2026-01-25

import asyncio
import json
import os
import threading
import time
from typing import Dict, Any, List

from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import websockets
import uvicorn

SERVICE = "qd-bookmap"
BUILD = "FIX07/P02"
WS_URL = "wss://contract.mexc.com/edge"
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5000))

SYMBOL = "BTC_USDT"

state: Dict[str, Any] = {
    "status": "DISCONNECTED",
    "frames": 0,
    "last_error": "",
    "last_event_ts": 0.0,
    "engine": "WARMING",
    "best_bid": None,
    "best_ask": None,
}

app = FastAPI(title="QuantDesk Bookmap", version=BUILD)


async def ws_loop():
    try:
        async with websockets.connect(WS_URL, ping_interval=None) as ws:
            state["status"] = "CONNECTED"

            # Subscribe to depth and trades (per MEXC Futures spec)
            await ws.send(json.dumps({
                "method": "sub.depth",
                "param": {"symbol": SYMBOL}
            }))
            await ws.send(json.dumps({
                "method": "sub.deal",
                "param": {"symbol": SYMBOL}
            }))

            last_ping = time.time()

            while True:
                if time.time() - last_ping > 15:
                    await ws.send(json.dumps({"method": "ping"}))
                    last_ping = time.time()

                msg = await ws.recv()
                state["frames"] += 1
                state["last_event_ts"] = time.time()

                if isinstance(msg, str):
                    data = json.loads(msg)

                    # Depth snapshot/update
                    if data.get("channel") == "push.depth":
                        bids = data.get("data", {}).get("bids")
                        asks = data.get("data", {}).get("asks")
                        if bids and asks:
                            state["best_bid"] = bids[0][0]
                            state["best_ask"] = asks[0][0]
                            state["engine"] = "ACTIVE"

    except Exception as e:
        state["status"] = "ERROR"
        state["last_error"] = str(e)


def start_ws():
    asyncio.run(ws_loop())


@app.on_event("startup")
def startup():
    t = threading.Thread(target=start_ws, daemon=True)
    t.start()


@app.get("/health.json")
def health():
    health = "GREEN" if state["engine"] == "ACTIVE" else "YELLOW"
    return JSONResponse({
        "service": SERVICE,
        "build": BUILD,
        "health": health,
        "state": state,
    })


@app.get("/")
def root():
    return HTMLResponse(
        "<h2>QuantDesk Bookmap</h2>"
        f"<p>Build {BUILD}</p>"
        f"<pre>{json.dumps(state, indent=2)}</pre>"
    )


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)
