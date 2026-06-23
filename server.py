import asyncio
import websockets
import json
import uuid
import os

connected_users = {}

async def handle_connection(websocket, path=None):
    username = None
    try:
        message = await websocket.recv()
        data = json.loads(message)
        username = data["username"]
        connected_users[username] = websocket
        print(f"✅ {username} connected")
        await broadcast_status()

        async for message in websocket:
            data = json.loads(message)
            await handle_message(username, data)

    finally:
        if username:
            del connected_users[username]
            print(f"❌ {username} disconnected")
            await broadcast_status()

async def handle_message(sender, data):
    msg_type = data.get("type")

    if msg_type == "message":
        to = data.get("to")
        if to in connected_users:
            payload = json.dumps({
                "type": "message",
                "from": sender,
                "id": str(uuid.uuid4()),
                "text": data["text"]
            })
            await connected_users[to].send(payload)
            print(f"📨 {sender} → {to}: {data['text']}")
        else:
            print(f"⚠️ {to} is not online!")

    elif msg_type == "reaction":
        to = data.get("to")
        if to in connected_users:
            payload = json.dumps({
                "type": "reaction",
                "from": sender,
                "msgId": data["msgId"],
                "emoji": data["emoji"]
            })
            await connected_users[to].send(payload)

async def broadcast_status():
    users = list(connected_users.keys())
    payload = json.dumps({"type": "status_update", "users": users})
    for ws in connected_users.values():
        await ws.send(payload)

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(
        handle_connection,
        "0.0.0.0",
        port,
        ping_interval=20,
        ping_timeout=60
    ):
        print(f"🚀 Server running on port {port}")
        await asyncio.Future()

asyncio.run(main())
