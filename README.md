# Trade Event Backend

This cleanup branch replaces the old Pet Simulator X gambling bot with a small authenticated backend for recording deposit and withdrawal events.

## What it does

- Accepts authenticated WebSocket messages on port `8765`
- Provides an HTTP health endpoint and optional event endpoint on port `8766`
- Validates usernames, UserIds, item types, item IDs, and amounts
- Stores balances in `trade_ledger.json`
- Avoids duplicate processing when an `eventId` is supplied
- Sends optional Discord webhook embeds for completed deposits and withdrawals

## What it does not do

- It does not run Roblox executor code
- It does not accept or confirm Roblox trades
- It does not send, claim, or transfer in-game items
- It does not contain gambling games

## Setup

```bash
python -m venv .venv
```

Windows:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
python server_websocket.py
```

Linux/macOS:

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python server_websocket.py
```

Edit `.env` before starting. `WS_PASSWORD` must contain at least 12 characters.

## Endpoints

- WebSocket: `ws://SERVER_IP:8765`
- Health: `http://SERVER_IP:8766/health`
- HTTP event reporting: `POST http://SERVER_IP:8766/trade`

The first WebSocket message must be:

```json
{"type":"auth","password":"YOUR_PASSWORD"}
```

A successful authentication returns:

```json
{"type":"auth_ok","timestamp":"..."}
```

See `SECURITY_AUDIT.md` for the removed files and security findings.
