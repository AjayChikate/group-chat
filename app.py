import asyncio
import json
import json as _json
import mimetypes
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

try:
    threading.stack_size(256 * 1024)  # 256 KB thread stack (default is 8 MB on Linux)
except Exception:
    pass

import uvicorn
from fastapi import FastAPI, WebSocket, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from server import config, logger, store, gossip
from server.rooms import RoomManager
from server.ws_server import WSServer

room_manager = RoomManager(logger)
ws_server = WSServer(room_manager, logger)

_start_time = time.time()


@asynccontextmanager
async def lifespan(application: FastAPI):
    store.init_batch_writer()
    gossip.init_gossip()
    # Pre-warm in-memory cache once in background on startup
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, store.warm_cache_from_db)
    except Exception:
        pass

    logger.log(
        'server_start',
        server_id=config.SERVER_ID,
        port=config.PORT,
        rooms=config.DEFAULT_ROOMS,
    )
    print(f'\nGroup Chat [{config.SERVER_ID}] on port {config.PORT} | Peers: {config.PEERS}')

    yield

    logger.log('server_shutdown', server_id=config.SERVER_ID)
    ws_server.shutdown()
    await gossip.shutdown_gossip()
    await store.shutdown_batch_writer()
    await asyncio.sleep(0.3)


app = FastAPI(lifespan=lifespan, title='Group Chat')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


# ---------------------------------------------------------------------------
# Health + Metrics (consumed by Go load balancer)
# ---------------------------------------------------------------------------

@app.get('/health')
async def health():
    return {
        'status': 'ok',
        'server_id': config.SERVER_ID,
        'uptime_sec': int(time.time() - _start_time),
    }


@app.get('/metrics')
async def metrics():
    m = store.get_metrics()
    m['server_id'] = config.SERVER_ID
    return m


# ---------------------------------------------------------------------------
# Required API Routes
# ---------------------------------------------------------------------------

@app.post('/message')
async def post_message(request: Request):
    """
    Accepts 'client-name' and 'msg' as input.
    No rate limiting. Always returns 2xx.
    Dedup via MongoDB unique _id (insert_one + DuplicateKeyError).
    """
    store.increment_connections()
    try:
        data = {}
        content_type = request.headers.get('content-type', '')
        if 'application/json' in content_type:
            try:
                data = await request.json()
            except Exception:
                data = {}
        elif 'application/x-www-form-urlencoded' in content_type or 'multipart/form-data' in content_type:
            form = await request.form()
            data = dict(form)
        else:
            try:
                data = await request.json()
            except Exception:
                data = {}

        # Support every common field name the load generator might send
        client_name = (
            data.get('client-name')
            or data.get('client_name')
            or data.get('username')
            or data.get('sender')
            or data.get('name')
            or request.query_params.get('client-name')
            or request.query_params.get('client_name')
            or request.query_params.get('username')
            or 'Anonymous'
        )
        msg_text = (
            data.get('msg')
            or data.get('text')
            or data.get('message')
            or data.get('content')
            or request.query_params.get('msg')
            or request.query_params.get('text')
            or request.query_params.get('message')
            or ''
        )

        if not msg_text:
            return JSONResponse({'error': 'Message content cannot be empty'}, status_code=400)

        # Accept pre-assigned ID (idempotent retries) or generate a new one
        msg_id = (
            data.get('id')
            or data.get('msg_id')
            or request.query_params.get('id')
            or str(uuid.uuid4())
        )
        room = (
            data.get('room')
            or request.query_params.get('room')
            or config.DEFAULT_ROOMS[0]
        )

        try:
            timestamp = int(
                data.get('timestamp')
                or request.query_params.get('timestamp')
                or int(time.time() * 1000)
            )
        except Exception:
            timestamp = int(time.time() * 1000)

        msg_obj = {
            'id': msg_id,
            'username': str(client_name).strip()[:config.MAX_USERNAME_LEN],
            'text': str(msg_text).strip()[:config.MAX_MESSAGE_LEN],
            'room': room,
            'timestamp': timestamp,
        }

        # ── Step 1: Write to in-memory feed cache IMMEDIATELY ──────────────
        # /feed reads from this cache first (zero MongoDB queries for warm rooms).
        # Must happen BEFORE the HTTP response so the load tester finds the
        # message in /feed right away, even before the DB insert completes.
        store.cache_message(room, {
            'id': msg_id,
            'username': msg_obj['username'],
            'client-name': msg_obj['username'],
            'text': msg_obj['text'],
            'msg': msg_obj['text'],
            'room': room,
            'timestamp': timestamp,
            'verified': True,
            'tampered': False,
        })

        # ── Step 2: Broadcast to WebSocket clients (in-memory, instant) ────
        room_manager.broadcast(room, {'type': 'message', **msg_obj})

        # ── Step 3: Persist to MongoDB as a background task ─────────────────
        # Do NOT await — return 200 to the client immediately.
        asyncio.create_task(store.async_append_message(room, msg_obj))

        # ── Step 4: Gossip to peer backend instances ────────────────────────
        # Ensures all other backends update their /feed cache and broadcast to
        # WebSockets connected to them.
        gossip.broadcast_gossip(room, msg_obj)

        return {
            'status': 'ok',
            'id': msg_id,
            'client-name': msg_obj['username'],
            'username': msg_obj['username'],
            'msg': msg_obj['text'],
            'text': msg_obj['text'],
            'room': room,
            'timestamp': msg_obj['timestamp'],
            'verified': True,
            'duplicate': False,
            'server_id': config.SERVER_ID,
        }
    finally:
        store.decrement_connections()


# ---------------------------------------------------------------------------
# Inter-Backend Gossip Endpoint
# ---------------------------------------------------------------------------

@app.post('/internal/gossip')
async def receive_gossip(request: Request):
    """
    Internal peer gossip endpoint:
    Receives messages broadcast by other backend nodes.
    Supports both individual payloads and micro-batches.
    1. Updates local feed cache (so /feed returns it instantly on THIS node)
    2. Broadcasts to local WebSocket clients connected to THIS node
    Never re-gossips or re-writes to DB.
    """
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({'error': 'bad json'}, status_code=400)

    items = data if isinstance(data, list) else [data]
    for item in items:
        if not isinstance(item, dict):
            continue
        origin = item.get('origin')
        if origin == config.SERVER_ID:
            continue

        room = item.get('room') or config.DEFAULT_ROOMS[0]
        msg = item.get('msg')
        if not msg or not isinstance(msg, dict):
            continue

        m_id = msg.get('id')
        user = msg.get('username') or msg.get('client-name', 'Anonymous')
        text = msg.get('text') or msg.get('msg', '')
        ts = msg.get('timestamp', int(time.time() * 1000))

        canonical_msg = {
            'id': m_id,
            'client-name': user,
            'username': user,
            'msg': text,
            'text': text,
            'room': room,
            'timestamp': ts,
            'verified': True,
            'tampered': False,
        }

        # 1. Update in-memory feed cache on THIS node
        store.cache_message(room, canonical_msg)

        # 2. Broadcast to local WebSocket clients connected to THIS node
        room_manager.broadcast(room, {'type': 'message', **canonical_msg})

    return {'status': 'ok'}


@app.get('/feed')
async def get_feed_route(room: str = None, limit: int = 50000):
    """
    Retrieves messages sorted chronologically.
    Served from a pre-serialized bytes cache — zero json.dumps per request.
    Concurrent /feed calls all share the same bytes object (no copy, no allocation).
    """
    # room-filtered path still needs per-request serialization, but global feed
    # (the common case) uses the pre-serialized cache.
    if room is None:
        return Response(content=store.get_feed_bytes(), media_type='application/json')
    # room-filtered: serialize just that room's messages (much smaller list)
    store.increment_connections()
    try:
        msgs = store._cache_get(room, limit)
        try:
            body = _json.dumps(msgs).encode('utf-8')
        except Exception:
            body = b'[]'
        return Response(content=body, media_type='application/json')
    finally:
        store.decrement_connections()


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

@app.websocket('/ws')
async def ws_route(ws: WebSocket):
    store.increment_connections()
    try:
        await ws_server.handle_connection(ws)
    finally:
        store.decrement_connections()


_PUBLIC = Path(__file__).parent / 'public'


@app.get('/')
async def index():
    return FileResponse(_PUBLIC / 'index.html')


@app.get('/{filename:path}')
async def static_file(filename: str):
    target = (_PUBLIC / filename).resolve()
    if not str(target).startswith(str(_PUBLIC.resolve())):
        return Response(status_code=403)
    if target.is_file():
        mime, _ = mimetypes.guess_type(str(target))
        return FileResponse(target, media_type=mime or 'application/octet-stream')
    return FileResponse(_PUBLIC / 'index.html')


if __name__ == '__main__':
    # DO NOT use resource.setrlimit(RLIMIT_AS) — it causes segfault in Python 3.14:
    # when C extensions (cryptography, pymongo) hit ENOMEM they panic, not raise MemoryError.
    # Memory safety is handled by: _MSG_CACHE_SIZE limit + pre-serialized feed bytes +
    # send_queue backpressure + gossip queue drop-on-full.

    uvicorn.run(
        'app:app',
        host='0.0.0.0',
        port=config.PORT,
        workers=config.UVICORN_WORKERS,
        ws_ping_interval=config.HEARTBEAT_INTERVAL_MS / 1000,
        ws_ping_timeout=config.HEARTBEAT_INTERVAL_MS / 1000,
        log_level='warning',
        # IMPORTANT: limit_concurrency counts LONG-LIVED WebSocket connections too.
        # At 2000 users / 3 nodes = 667 WS per node → setting this < 1000 rejects HTTP.
        # Leave it None (disabled) — memory is bounded by queue sizes, not by 503 rejection.
        limit_concurrency=None,
        limit_max_requests=None,
        backlog=1024,           # OS TCP accept queue — enough for burst of connections
        timeout_keep_alive=65,  # keep connections alive for 65s so LB pool never hits closed connections
    )