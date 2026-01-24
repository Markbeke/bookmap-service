# QuantDesk Bookmap Service

## Overview
A FastAPI-based service for connecting to MEXC cryptocurrency exchange WebSocket streams. It provides health monitoring and telemetry endpoints for the Bookmap connector.

## Project Structure
- `current_fix.py` - Main FastAPI application with WebSocket connector
- `QD_BOOKMAP_*.py` - Historical development iterations (reference files)
- `requirements.txt` - Python dependencies

## Running the Application
The application runs on port 5000 with:
```bash
python current_fix.py
```

## Endpoints
- `/` - Status HTML dashboard
- `/health.json` - Health check endpoint
- `/telemetry.json` - Detailed telemetry data

## Configuration (Environment Variables)
- `QD_HOST` - Bind host (default: 0.0.0.0)
- `QD_PORT` - Bind port (default: 5000)
- `QD_SYMBOL` - Trading symbol (default: BTCUSDT)
- `MEXC_WS_WSS` - WebSocket secure URL
- `MEXC_WS_WS` - WebSocket fallback URL

## Recent Changes
- 2026-01-24: Configured for Replit environment, port updated to 5000
