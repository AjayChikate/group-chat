import asyncio
import mimetypes
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from server import config, logger, store
from server.rooms import RoomManager
from server.ws_server import WSServer

room_manager = RoomManager(logger)
ws_server = WSServer(room_manager, logger)

_start_time = time.time()


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.log(
        'server_start',
        server_id=config.SERVER_ID,
        port=config.PORT,
        rooms=config.DEFAULT_ROOMS,
    )
    print(f'\nGroup Chat [{config.SERVER_ID}] on port {config.PORT}')

    yield

    logger.log('server_shutdown', server_id=config.SERVER_ID)
    ws_server.shutdown()
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

        saved = await store.async_append_message(room, msg_obj)
        room_manager.broadcast(room, {'type': 'message', **msg_obj})

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
            'duplicate': saved.get('duplicate', False),
            'server_id': config.SERVER_ID,
        }
    finally:
        store.decrement_connections()


@app.get('/feed')
async def get_feed_route(room: str = None, limit: int = 100):
    """
    Retrieves messages.  Fast path — reads from in-memory cache first (0 MongoDB queries
    for warm rooms).  Falls back to MongoDB on cold start.  Hard-capped at 500 to prevent
    runaway cursors from saturating the connection pool.
    """
    store.increment_connections()
    try:
        limit = min(limit, 500)          # hard cap — protect the connection pool
        return await store.async_get_feed(room_id=room, limit=limit)
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
    uvicorn.run(
        'app:app',
        host='0.0.0.0',
        port=config.PORT,
        workers=config.UVICORN_WORKERS,
        ws_ping_interval=config.HEARTBEAT_INTERVAL_MS / 1000,
        ws_ping_timeout=config.HEARTBEAT_INTERVAL_MS / 1000,
        log_level='warning',
    )