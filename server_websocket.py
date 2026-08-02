#!/usr/bin/env python3
"""
Authenticated WebSocket + HTTP backend for trade-event logging.

This service records completed deposit/withdrawal events, maintains a local
ledger, and sends Discord webhook embeds. It does not control Roblox clients
or move items.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hmac
import json
import os
from pathlib import Path
from typing import Any

import requests
from aiohttp import web
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosed

try:
    from websockets.asyncio.server import serve
except ImportError:
    from websockets import serve


load_dotenv()

WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", "8766"))

PASSWORD = os.getenv("WS_PASSWORD", "")
DEPOSIT_WEBHOOK = os.getenv("DEPOSIT_WEBHOOK", "")
WITHDRAW_WEBHOOK = os.getenv("WITHDRAW_WEBHOOK", "")
LEDGER_FILE = Path(os.getenv("LEDGER_FILE", "trade_ledger.json"))

MAX_MESSAGE_BYTES = 256_000
WEBHOOK_TIMEOUT = 8
MAX_ITEMS = 100

ledger: dict[str, dict[str, Any]] = {}
ledger_lock = asyncio.Lock()
processed_event_ids: set[str] = set()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_ledger() -> None:
    global ledger
    if not LEDGER_FILE.exists():
        ledger = {}
        return

    try:
        decoded = json.loads(LEDGER_FILE.read_text(encoding="utf-8"))
        ledger = decoded if isinstance(decoded, dict) else {}
        print(f"Loaded {len(ledger)} ledger entries")
    except (OSError, json.JSONDecodeError) as exc:
        print("Ledger load error:", exc)
        ledger = {}


def save_ledger_sync(snapshot: dict[str, Any]) -> None:
    temporary = LEDGER_FILE.with_suffix(LEDGER_FILE.suffix + ".tmp")
    temporary.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(LEDGER_FILE)


def validate_user_id(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("userId must be a positive integer")

    try:
        user_id = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("userId must be a positive integer") from exc

    if user_id <= 0:
        raise ValueError("userId must be positive")

    return user_id


def validate_username(value: Any) -> str:
    username = str(value or "").strip()
    if not username:
        raise ValueError("username is required")
    return username[:50]


def normalize_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError("items must be an array")
    if len(value) > MAX_ITEMS:
        raise ValueError(f"items may contain at most {MAX_ITEMS} entries")

    output: list[dict[str, Any]] = []

    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ValueError(f"items[{index}] must be an object")

        item_id = str(raw.get("id") or raw.get("ItemID") or "").strip()
        item_name = str(raw.get("name") or item_id).strip()
        item_type = str(raw.get("type") or raw.get("ItemType") or "").strip()

        try:
            amount = int(raw.get("amount", raw.get("Amount", 1)))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"items[{index}].amount is invalid") from exc

        if not item_id:
            raise ValueError(f"items[{index}].id is required")
        if item_type not in {"Weapons", "Pets"}:
            raise ValueError(f"items[{index}].type must be Weapons or Pets")
        if amount <= 0:
            raise ValueError(f"items[{index}].amount must be positive")

        output.append(
            {
                "id": item_id[:100],
                "name": (item_name or item_id)[:150],
                "type": item_type,
                "amount": amount,
            }
        )

    return output


def items_to_text(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No items"

    text = "\n".join(
        f"• {item['name']} x{item['amount']} (`{item['id']}`, {item['type']})"
        for item in items
    )
    return text if len(text) <= 1024 else text[:1021] + "..."


def post_webhook_sync(webhook_url: str, embed: dict[str, Any]) -> None:
    if not webhook_url:
        print("Webhook not configured; event logged locally")
        return

    response = requests.post(
        webhook_url,
        json={"username": "Trade Server", "embeds": [embed]},
        timeout=WEBHOOK_TIMEOUT,
    )
    response.raise_for_status()


async def post_webhook(webhook_url: str, embed: dict[str, Any]) -> None:
    try:
        await asyncio.to_thread(post_webhook_sync, webhook_url, embed)
    except requests.RequestException as exc:
        print("Discord webhook error:", exc)


async def update_ledger(
    event_type: str,
    user_id: int,
    username: str,
    items: list[dict[str, Any]],
) -> None:
    async with ledger_lock:
        key = str(user_id)
        record = ledger.setdefault(
            key,
            {
                "username": username,
                "items": {"Weapons": {}, "Pets": {}},
                "updatedAt": utc_now(),
            },
        )

        record["username"] = username
        record.setdefault("items", {})
        record["items"].setdefault("Weapons", {})
        record["items"].setdefault("Pets", {})

        direction = 1 if event_type == "deposit" else -1

        for item in items:
            category: dict[str, int] = record["items"][item["type"]]
            current = int(category.get(item["id"], 0))
            updated = current + direction * item["amount"]

            if updated > 0:
                category[item["id"]] = updated
            else:
                category.pop(item["id"], None)

        record["updatedAt"] = utc_now()
        snapshot = json.loads(json.dumps(ledger))

    await asyncio.to_thread(save_ledger_sync, snapshot)


async def process_event(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("message must be a JSON object")

    message_type = data.get("type")

    if message_type == "ping":
        return {"type": "pong", "timestamp": utc_now()}

    allowed_types = {
        "trade_request",
        "deposit",
        "withdrawal",
        "withdraw_queued",
        "withdraw_failed",
    }

    if message_type not in allowed_types:
        raise ValueError(f"unknown message type: {message_type!r}")

    event_id = str(data.get("eventId") or "").strip()
    if event_id:
        if event_id in processed_event_ids:
            return {"type": f"{message_type}_ok", "duplicate": True}
        processed_event_ids.add(event_id)

    user_id = validate_user_id(data.get("userId"))
    username = validate_username(data.get("username", "unknown"))

    if message_type == "withdraw_queued":
        print(f"Withdrawal queued: {username} ({user_id})")
        return {"type": "withdraw_queued_ok", "userId": user_id}

    if message_type == "withdraw_failed":
        reason = str(data.get("reason", "unknown"))[:500]
        print(f"Withdrawal failed: {username} ({user_id}) - {reason}")
        return {"type": "withdraw_failed_ok", "userId": user_id}

    avatar_url = (
        "https://www.roblox.com/headshot-thumbnail/image"
        f"?userId={user_id}&width=420&height=420&format=png"
    )

    if message_type == "trade_request":
        embed = {
            "title": "📩 New Trade Request",
            "color": 0x3498DB,
            "thumbnail": {"url": avatar_url},
            "fields": [
                {
                    "name": "User",
                    "value": f"{username} (`{user_id}`)",
                    "inline": True,
                }
            ],
            "timestamp": utc_now(),
        }
        print(f"TRADE REQUEST: {username} ({user_id})")
        await post_webhook(DEPOSIT_WEBHOOK, embed)
        return {"type": "trade_request_ok", "userId": user_id}

    items = normalize_items(data.get("items", []))
    await update_ledger(message_type, user_id, username, items)

    is_deposit = message_type == "deposit"
    embed = {
        "title": "📥 New Deposit" if is_deposit else "📤 Withdrawal",
        "color": 0x00FF00 if is_deposit else 0xFF0000,
        "thumbnail": {"url": avatar_url},
        "fields": [
            {
                "name": "User",
                "value": f"{username} (`{user_id}`)",
                "inline": True,
            },
            {
                "name": "Items",
                "value": items_to_text(items),
                "inline": False,
            },
        ],
        "timestamp": utc_now(),
    }

    print(
        f"{message_type.upper()}: {username} ({user_id}) - "
        f"{len(items)} item entries"
    )

    await post_webhook(
        DEPOSIT_WEBHOOK if is_deposit else WITHDRAW_WEBHOOK,
        embed,
    )

    return {
        "type": f"{message_type}_ok",
        "userId": user_id,
        "itemEntries": len(items),
    }


async def handle_websocket(websocket) -> None:
    remote = getattr(websocket, "remote_address", None)
    print("WebSocket connected:", remote)
    authenticated = False

    try:
        async for raw_message in websocket:
            if not isinstance(raw_message, str):
                await websocket.send(
                    json.dumps(
                        {
                            "type": "error",
                            "error": "binary messages are not supported",
                        }
                    )
                )
                continue

            try:
                data = json.loads(raw_message)
            except json.JSONDecodeError:
                await websocket.send(
                    json.dumps({"type": "error", "error": "invalid_json"})
                )
                continue

            if not isinstance(data, dict):
                await websocket.send(
                    json.dumps(
                        {
                            "type": "error",
                            "error": "message must be a JSON object",
                        }
                    )
                )
                continue

            if not authenticated:
                received_password = str(data.get("password", ""))
                if (
                    data.get("type") != "auth"
                    or not hmac.compare_digest(received_password, PASSWORD)
                ):
                    await websocket.send(json.dumps({"type": "auth_fail"}))
                    await websocket.close(code=1008, reason="Authentication failed")
                    return

                authenticated = True
                await websocket.send(
                    json.dumps({"type": "auth_ok", "timestamp": utc_now()})
                )
                print("WebSocket authenticated:", remote)
                continue

            try:
                response = await process_event(data)
                await websocket.send(json.dumps(response))
            except ValueError as exc:
                await websocket.send(
                    json.dumps({"type": "error", "error": str(exc)})
                )
            except Exception as exc:
                print("WebSocket processing error:", repr(exc))
                await websocket.send(
                    json.dumps({"type": "error", "error": "internal_error"})
                )

    except ConnectionClosed:
        pass
    finally:
        print("WebSocket disconnected:", remote)


async def handle_http_trade(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        supplied_password = str(data.get("password", ""))
        if not hmac.compare_digest(supplied_password, PASSWORD):
            return web.json_response({"error": "Unauthorized"}, status=403)

        response = await process_event(data)
        return web.json_response(response)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)
    except Exception as exc:
        print("HTTP processing error:", repr(exc))
        return web.json_response({"error": "internal_error"}, status=500)


async def handle_health(_: web.Request) -> web.Response:
    return web.json_response(
        {
            "status": "ok",
            "websocketPort": WS_PORT,
            "httpPort": HTTP_PORT,
            "timestamp": utc_now(),
        }
    )


async def main() -> None:
    if len(PASSWORD) < 12:
        raise RuntimeError("WS_PASSWORD must be set to at least 12 characters")

    load_ledger()

    app = web.Application(client_max_size=MAX_MESSAGE_BYTES)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/trade", handle_http_trade)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HTTP_HOST, HTTP_PORT)
    await site.start()

    print(f"HTTP health: http://127.0.0.1:{HTTP_PORT}/health")
    print(f"HTTP events: http://127.0.0.1:{HTTP_PORT}/trade")

    async with serve(
        handle_websocket,
        WS_HOST,
        WS_PORT,
        max_size=MAX_MESSAGE_BYTES,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
    ) as websocket_server:
        print(f"WebSocket server: ws://0.0.0.0:{WS_PORT}")
        await websocket_server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped")
