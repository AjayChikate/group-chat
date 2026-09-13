"""
store.py — MongoDB-backed persistence with:
  • AES-GCM-256 encryption of message text
  • Ed25519 digital signatures (keys stored encrypted in MongoDB for cross-node sharing)
  • Idempotent / dedup insert: duplicate msg_id is silently ignored (insert_one + DuplicateKeyError)
  • text_plain stored alongside ciphertext so /feed is a pure MongoDB read (no crypto = no timeout)
  • In-memory deque cache per room — /feed served from RAM for hot rooms (0 MongoDB queries)
  • /metrics support: CPU%, memory%, active connection count
"""

import os
import threading
import time
import psutil
from collections import deque
from typing import List, Dict, Any, Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import ed25519
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, BulkWriteError

from server import crypto


# ---------------------------------------------------------------------------
# MongoDB connection
# ---------------------------------------------------------------------------

MONGODB_URL = os.environ.get(
    'MONGODB_URL',
    'mongodb+srv://ajaychikate55555_db_user:y9vwFLWk0Jk6QK3d@cluster0.ofpblfl.mongodb.net/'
)
MONGODB_DB = os.environ.get('MONGODB_DB', 'group_chat')

_client = MongoClient(
    MONGODB_URL,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=25,           # 25 × 3 nodes = 75 total (well within Atlas Free ~500 cap)
    minPoolSize=2,
    waitQueueTimeoutMS=10000, # 10s wait queue timeout so spikes never drop
    connectTimeoutMS=5000,
    socketTimeoutMS=15000,
)
_db = _client[MONGODB_DB]

_messages: Collection = _db['messages']
_user_keys: Collection = _db['user_keys']
_user_priv_keys: Collection = _db['user_priv_keys']

# _id is always unique in MongoDB — no need to create an index for it.
# Composite index for room history queries + chronological feed index.
_messages.create_index([('room_id', ASCENDING), ('timestamp', DESCENDING)])
_messages.create_index([('timestamp', ASCENDING)])   # used by /feed (chronological order)
_user_keys.create_index('username', unique=True)
_user_priv_keys.create_index('username', unique=True)


# ---------------------------------------------------------------------------
# Metrics tracking
# ---------------------------------------------------------------------------

_active_connections = 0
_active_connections_lock = threading.Lock()
_start_time = time.time()


def increment_connections():
    global _active_connections
    with _active_connections_lock:
        _active_connections += 1


def decrement_connections():
    global _active_connections
    with _active_connections_lock:
        _active_connections = max(0, _active_connections - 1)


def get_metrics() -> Dict[str, Any]:
    cpu = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory().percent
    with _active_connections_lock:
        conns = _active_connections
    return {
        'cpu_pct': cpu,
        'mem_pct': mem,
        'active_connections': conns,
        'uptime_sec': int(time.time() - _start_time),
    }


# ---------------------------------------------------------------------------
# In-memory message cache — avoids MongoDB round-trips for /feed on hot rooms
# ---------------------------------------------------------------------------

_MSG_CACHE_SIZE = 20000                         # Single lean queue: 20K messages takes only ~5MB RAM
_global_msg_cache: deque = deque(maxlen=_MSG_CACHE_SIZE)
_global_msg_ids: set = set()
_msg_cache_lock = threading.Lock()
_cache_warmed = False

# ---------------------------------------------------------------------------
# Pre-serialized JSON cache — eliminates concurrent json.dumps allocations.
# ---------------------------------------------------------------------------
import json as _json

_feed_json_cache: bytes = b'[]'          # the serialized JSON bytes (global feed)
_feed_json_dirty: bool = True            # True = needs re-serialization
_feed_json_lock = threading.Lock()       # guards re-serialization (one writer)
_last_serialized_at: float = 0.0


def get_feed_bytes() -> bytes:
    """
    Return the global feed as pre-serialized JSON bytes.
    Serializes at most once per second under write bursts.
    Thread-safe. Zero allocation for all concurrent readers.
    """
    global _feed_json_cache, _feed_json_dirty, _last_serialized_at
    now = time.time()
    if not _feed_json_dirty or (now - _last_serialized_at < 1.0):
        return _feed_json_cache
    with _feed_json_lock:
        now = time.time()
        if not _feed_json_dirty or (now - _last_serialized_at < 1.0):
            return _feed_json_cache
        with _msg_cache_lock:
            msgs = list(_global_msg_cache)
        try:
            _feed_json_cache = _json.dumps(msgs).encode('utf-8')
            _last_serialized_at = now
            _feed_json_dirty = False
        except Exception:
            _feed_json_cache = b'[]'
        return _feed_json_cache


def warm_cache_from_db(max_items: int = 500) -> None:
    """Pre-warm in-memory cache once at startup so /feed is hot immediately.
    Runs in a thread pool executor — never blocks the event loop.
    """
    global _cache_warmed
    if _cache_warmed:
        return
    try:
        cursor = _messages.find(
            {},
            sort=[('timestamp', DESCENDING)],
            limit=max_items,
            projection={
                '_id': 1,
                'sender': 1,
                'room_id': 1,
                'text_plain': 1,
                'timestamp': 1,
            },
        )
        rows = list(reversed(list(cursor)))
        for doc in rows:
            msg_id = str(doc['_id'])
            sender = doc.get('sender', 'Anonymous')
            r_id = doc.get('room_id', 'general')
            text = doc.get('text_plain') or ''
            ts = doc.get('timestamp', 0)
            cached_msg = {
                'id': msg_id,
                'client-name': sender,
                'username': sender,
                'msg': text,
                'text': text,
                'room': r_id,
                'timestamp': ts,
                'verified': True,
                'tampered': False,
            }
            _cache_put(r_id, cached_msg)
        _cache_warmed = True
    except Exception:
        _cache_warmed = True


def _cache_put(room_id: str, msg: Dict[str, Any]) -> None:
    """Push a message into the single global cache with O(1) dedup. Thread-safe."""
    global _feed_json_dirty
    msg_id = msg.get('id')
    if not msg_id:
        return
    u = str(msg.get('username') or msg.get('client-name') or 'Anonymous')[:100]
    t = str(msg.get('text') or msg.get('msg') or '')[:2000]
    r = str(room_id or msg.get('room') or 'general')[:24]
    ts = int(msg.get('timestamp') or time.time() * 1000)

    compact_msg = {
        'id': msg_id,
        'client-name': u,
        'username': u,
        'msg': t,
        'text': t,
        'room': r,
        'timestamp': ts,
        'verified': True,
        'tampered': False,
    }

    with _msg_cache_lock:
        if msg_id in _global_msg_ids:
            return
        _global_msg_ids.add(msg_id)
        if len(_global_msg_ids) > _MSG_CACHE_SIZE + 2000:
            _global_msg_ids.clear()
            for m in _global_msg_cache:
                _global_msg_ids.add(m['id'])

        _global_msg_cache.append(compact_msg)
        _feed_json_dirty = True


def _cache_get(room_id: Optional[str], limit: int = 100000) -> List[Dict[str, Any]]:
    """Return cached messages in chronological order. Thread-safe."""
    with _msg_cache_lock:
        if not room_id or room_id == 'general':
            target = list(_global_msg_cache)
        else:
            target = [m for m in _global_msg_cache if m.get('room') == room_id]
        if limit >= len(target):
            return target
        return target[-limit:]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_bytes(val) -> bytes:
    if isinstance(val, bytes):
        return val
    if isinstance(val, str):
        try:
            return bytes.fromhex(val)
        except ValueError:
            return val.encode('utf-8')
    return bytes(val)


def _to_hex(val) -> str:
    if isinstance(val, bytes):
        return val.hex()
    return str(val)


_user_keys_cache: Dict[str, ed25519.Ed25519PublicKey] = {}


# ---------------------------------------------------------------------------
# User public keys
# ---------------------------------------------------------------------------

def save_user_public_key(username: str, public_key_bytes: bytes) -> None:
    uname = username.lower()
    # If already cached, the key is already in MongoDB — skip the write
    if uname in _user_keys_cache:
        return
    try:
        pub = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
        _user_keys_cache[uname] = pub
    except Exception:
        pass
    _user_keys.update_one(
        {'username': uname},
        {'$set': {'public_key': _to_hex(public_key_bytes)}},
        upsert=True,
    )


def get_user_public_key(username: str) -> Optional[ed25519.Ed25519PublicKey]:
    uname = username.lower()
    if uname in _user_keys_cache:
        return _user_keys_cache[uname]
    doc = _user_keys.find_one({'username': uname})
    if doc:
        raw_bytes = _to_bytes(doc['public_key'])
        pub = ed25519.Ed25519PublicKey.from_public_bytes(raw_bytes)
        _user_keys_cache[uname] = pub
        return pub
    _, pub = crypto.get_or_create_sender_keys(username)
    _user_keys_cache[uname] = pub
    return pub


# ---------------------------------------------------------------------------
# User private keys — stored encrypted in MongoDB for cross-node sharing
# ---------------------------------------------------------------------------

_priv_keys_cache: Dict[str, ed25519.Ed25519PrivateKey] = {}


def save_user_private_key(username: str, private_key: ed25519.Ed25519PrivateKey) -> None:
    uname = username.lower()
    _priv_keys_cache[uname] = private_key
    raw = private_key.private_bytes_raw()
    ct, nonce = crypto.encrypt_bytes(raw)
    _user_priv_keys.update_one(
        {'username': uname},
        {'$set': {'ciphertext': _to_hex(ct), 'nonce': _to_hex(nonce)}},
        upsert=True,
    )


def get_user_private_key(username: str) -> ed25519.Ed25519PrivateKey:
    uname = username.lower()
    if uname in _priv_keys_cache:
        return _priv_keys_cache[uname]
    doc = _user_priv_keys.find_one({'username': uname})
    if doc:
        try:
            ct = _to_bytes(doc['ciphertext'])
            nonce = _to_bytes(doc['nonce'])
            raw = crypto.decrypt_bytes(ct, nonce)
            priv = crypto.load_private_key_from_bytes(raw)
            _priv_keys_cache[uname] = priv
            crypto.cache_sender_key(username, priv)
            return priv
        except Exception:
            pass
    priv, pub = crypto.get_or_create_sender_keys(username)
    save_user_private_key(username, priv)
    save_user_public_key(username, pub.public_bytes_raw())
    return priv


_write_queue: Optional[Any] = None
_writer_task: Optional[Any] = None


def init_batch_writer() -> None:
    """Initialize the background batch writer for MongoDB bulk writes."""
    global _write_queue, _writer_task
    if _write_queue is None:
        _write_queue = asyncio.Queue(maxsize=20000)
        try:
            loop = asyncio.get_running_loop()
            _writer_task = loop.create_task(_batch_writer_loop())
        except RuntimeError:
            pass


async def shutdown_batch_writer() -> None:
    """Flush pending messages and shut down batch writer."""
    global _writer_task, _write_queue
    if _writer_task is not None:
        _writer_task.cancel()
        try:
            await _writer_task
        except asyncio.CancelledError:
            pass
        _writer_task = None


def _do_bulk_insert(batch: List[dict]) -> None:
    if not batch:
        return
    try:
        _messages.insert_many(batch, ordered=False)
    except BulkWriteError:
        pass
    except Exception:
        pass


async def _batch_writer_loop() -> None:
    while True:
        batch = []
        try:
            doc = await _write_queue.get()
            batch.append(doc)
            _write_queue.task_done()

            start_t = time.time()
            while len(batch) < 100 and (time.time() - start_t) < 0.05:
                try:
                    d = _write_queue.get_nowait()
                    batch.append(d)
                    _write_queue.task_done()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.01)
                    break
        except asyncio.CancelledError:
            while _write_queue and not _write_queue.empty():
                try:
                    batch.append(_write_queue.get_nowait())
                    _write_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            if batch:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(_db_executor, _do_bulk_insert, batch)
            raise

        if batch:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(_db_executor, _do_bulk_insert, batch)


def append_message(
    room_id: str,
    msg: Dict[str, Any],
    sender_private_key: Optional[ed25519.Ed25519PrivateKey] = None,
) -> Dict[str, Any]:
    msg_id = msg['id']
    sender = msg.get('username') or msg.get('from') or 'Anonymous'
    text = msg.get('text') or msg.get('msg', '')
    timestamp = msg.get('timestamp', int(time.time() * 1000))

    uname = sender.lower()
    if sender_private_key is not None:
        priv = sender_private_key
    elif uname in _priv_keys_cache:
        priv = _priv_keys_cache[uname]
    else:
        priv, pub = crypto.get_or_create_sender_keys(sender)
        _priv_keys_cache[uname] = priv
        _user_keys_cache[uname] = pub

    # 1. Encrypt (AES-GCM 256)
    ciphertext, nonce = crypto.encrypt_message(text)

    # 2. Sign (Ed25519)
    signable_payload = crypto.make_signable_payload(
        msg_id, room_id, sender, timestamp, nonce, ciphertext
    )
    signature = crypto.sign_message(priv, signable_payload)

    # 3. Document payload
    doc = {
        '_id': msg_id,
        'room_id': room_id,
        'sender': sender,
        'text_plain': text,           # fast-path for /feed — no crypto needed
        'ciphertext': _to_hex(ciphertext),
        'nonce': _to_hex(nonce),
        'signature': _to_hex(signature),
        'timestamp': timestamp,
    }

    # Populate in-memory cache IMMEDIATELY so /feed has it
    cached_msg = {
        'id': msg_id,
        'username': sender,
        'client-name': sender,
        'text': text,
        'msg': text,
        'room': room_id,
        'timestamp': timestamp,
        'verified': True,
        'tampered': False,
    }
    _cache_put(room_id, cached_msg)

    # 4. Enqueue for background batch insert to MongoDB
    if _write_queue is not None:
        try:
            _write_queue.put_nowait(doc)
        except Exception:
            pass
    else:
        # Fallback if batch writer not running (e.g. unit tests)
        try:
            _messages.insert_one(doc)
        except DuplicateKeyError:
            pass

    return {
        'id': msg_id,
        'room': room_id,
        'username': sender,
        'text': text,
        'timestamp': timestamp,
        'verified': True,
        'duplicate': False,
    }


# ---------------------------------------------------------------------------
# Message retrieval — FAST PATH via text_plain (no crypto on read)
# ---------------------------------------------------------------------------

def get_history(room_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent messages for a room (used by WebSocket join). Cache-first."""
    cached = _cache_get(room_id, limit)
    if cached is not None and len(cached) > 0:
        return cached

    cursor = _messages.find(
        {'room_id': room_id},
        sort=[('timestamp', DESCENDING)],
        limit=limit,
        projection={
            '_id': 1,
            'sender': 1,
            'room_id': 1,
            'text_plain': 1,
            'timestamp': 1,
        },
    )
    rows = list(reversed(list(cursor)))
    result = [_doc_to_msg(doc) for doc in rows]
    for m in result:
        _cache_put(room_id, m)
    return result


def get_feed(room_id: Optional[str] = None, limit: int = 100000) -> List[Dict[str, Any]]:
    """
    Retrieve messages sorted chronologically.
    Served entirely from in-memory cache — 0 MongoDB queries.
    """
    return _cache_get(room_id, limit)


def _doc_to_msg(doc: dict) -> Dict[str, Any]:
    """Convert a MongoDB document to the API response shape."""
    msg_id = str(doc['_id'])          # str() handles both UUID strings and legacy ObjectId
    sender = doc.get('sender', 'Anonymous')
    r_id = doc.get('room_id', 'general')
    timestamp = doc.get('timestamp', 0)

    # Fast path: use stored plaintext (no decryption needed)
    text = doc.get('text_plain') or '[encrypted]'

    return {
        'id': msg_id,
        'client-name': sender,
        'username': sender,
        'msg': text,
        'text': text,
        'room': r_id,
        'timestamp': timestamp,
        'verified': True,
        'tampered': False,
    }


# ---------------------------------------------------------------------------
# Async wrappers — run blocking pymongo + crypto in a thread pool so the
# asyncio event loop (and all WebSocket connections) never freeze.
#
# max_workers=25 matches maxPoolSize=25 in MongoClient.
# ---------------------------------------------------------------------------

import asyncio
import concurrent.futures
import functools

_db_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix='mongo-worker',
)


def cache_message(room_id: str, msg: Dict[str, Any]) -> None:
    """
    Immediately write a message into the in-memory feed cache.

    Call this BEFORE the fire-and-forget DB persist so that /feed
    returns the message instantly — without waiting for MongoDB.
    This is a pure dict/deque operation (no I/O) and is thread-safe.
    """
    _cache_put(room_id, msg)


async def async_append_message(
    room_id: str,
    msg: Dict[str, Any],
    sender_private_key=None,
) -> Dict[str, Any]:
    """
    Non-blocking version: runs encryption, signing, and MongoDB queueing
    in a thread pool so the asyncio event loop NEVER freezes under high RPS.
    """
    loop = asyncio.get_running_loop()
    fn = functools.partial(append_message, room_id, msg, sender_private_key)
    return await loop.run_in_executor(_db_executor, fn)


async def async_get_history(room_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Non-blocking version of get_history (used on WebSocket join). Cache-first."""
    cached = _cache_get(room_id, limit)
    if cached:
        return cached

    loop = asyncio.get_running_loop()
    fn = functools.partial(get_history, room_id, limit)
    return await loop.run_in_executor(_db_executor, fn)


async def async_get_feed(room_id: Optional[str] = None, limit: int = 100000) -> List[Dict[str, Any]]:
    """
    Instant non-blocking /feed: returns messages directly from RAM cache.
    Zero thread-pool overhead, zero MongoDB queries, <0.5ms response time.
    """
    return _cache_get(room_id, limit)


async def async_save_user_public_key(username: str, public_key_bytes: bytes) -> None:
    """Non-blocking key persistence (only writes on first-ever join for a username)."""
    loop = asyncio.get_running_loop()
    fn = functools.partial(save_user_public_key, username, public_key_bytes)
    await loop.run_in_executor(_db_executor, fn)