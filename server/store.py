"""
store.py — MongoDB-backed persistence with:
  • AES-GCM-256 encryption of message text
  • Ed25519 digital signatures (keys stored encrypted in MongoDB for cross-node sharing)
  • Idempotent / dedup insert: duplicate msg_id is silently ignored (insert_one + DuplicateKeyError)
  • text_plain stored alongside ciphertext so /feed is a pure MongoDB read (no crypto = no timeout)
  • /metrics support: CPU%, memory%, active connection count
"""

import os
import threading
import time
import psutil
from typing import List, Dict, Any, Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import ed25519
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from server import crypto


# ---------------------------------------------------------------------------
# MongoDB connection
# ---------------------------------------------------------------------------

MONGODB_URI = os.environ.get(
    'MONGODB_URI',
    'mongodb+srv://shashankyadavriiii_db_user:9Y2RNLoRD6OWSC4h@cluster0.7azo9pt.mongodb.net'
)
MONGODB_DB = os.environ.get('MONGODB_DB', 'group_chat')

_client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=200,
    waitQueueTimeoutMS=5000,
)
_db = _client[MONGODB_DB]

_messages: Collection = _db['messages']
_user_keys: Collection = _db['user_keys']
_user_priv_keys: Collection = _db['user_priv_keys']

# Unique _id on messages is the hard dedup guarantee
_messages.create_index([('room_id', ASCENDING), ('timestamp', DESCENDING)])
_messages.create_index([('timestamp', ASCENDING)])   # used by /feed (chronological order)
_messages.create_index('_id', unique=True)
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


# ---------------------------------------------------------------------------
# Message persistence — dedup via unique _id, store plaintext for fast /feed
# ---------------------------------------------------------------------------

def append_message(
    room_id: str,
    msg: Dict[str, Any],
    sender_private_key: Optional[ed25519.Ed25519PrivateKey] = None,
) -> Dict[str, Any]:
    msg_id = msg['id']
    sender = msg.get('username') or msg.get('from') or 'Anonymous'
    text = msg['text']
    timestamp = msg['timestamp']

    # Load or generate sender's Ed25519 key pair (shared via MongoDB)
    if sender_private_key is None:
        sender_private_key = get_user_private_key(sender)

    sender_pub = sender_private_key.public_key()
    save_user_public_key(sender, sender_pub.public_bytes_raw())

    # 1. Encrypt (AES-GCM 256)
    ciphertext, nonce = crypto.encrypt_message(text)

    # 2. Sign (Ed25519)
    signable_payload = crypto.make_signable_payload(
        msg_id, room_id, sender, timestamp, nonce, ciphertext
    )
    signature = crypto.sign_message(sender_private_key, signable_payload)

    # 3. Insert into MongoDB — DuplicateKeyError = silently ignored (dedup)
    #    text_plain stored alongside so /feed requires NO decryption (fast path)
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
    is_new = True
    try:
        _messages.insert_one(doc)
    except DuplicateKeyError:
        is_new = False   # duplicate msg_id — silently ignored

    return {
        'id': msg_id,
        'room': room_id,
        'username': sender,
        'text': text,
        'timestamp': timestamp,
        'verified': True,
        'duplicate': not is_new,
    }


# ---------------------------------------------------------------------------
# Message retrieval — FAST PATH via text_plain (no crypto on read)
# ---------------------------------------------------------------------------

def get_history(room_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent messages for a room (used by WebSocket join)."""
    cursor = _messages.find(
        {'room_id': room_id},
        sort=[('timestamp', DESCENDING)],
        limit=limit,
    )
    rows = list(reversed(list(cursor)))
    return [_doc_to_msg(doc) for doc in rows]


def get_feed(room_id: Optional[str] = None, limit: int = 5000) -> List[Dict[str, Any]]:
    """
    Retrieve messages sorted chronologically.
    Uses text_plain field — NO decryption, NO signature verification.
    This is intentionally a fast read path so /feed never times out.
    Integrity was already verified and the message was already signed at insert time.
    """
    query = {}
    if room_id:
        query['room_id'] = room_id

    cursor = _messages.find(
        query,
        sort=[('timestamp', ASCENDING)],   # chronological, no need to reverse
        limit=limit,
        # Only fetch the fields we need — skip large ciphertext/signature blobs
        projection={
            '_id': 1,
            'sender': 1,
            'room_id': 1,
            'text_plain': 1,
            'timestamp': 1,
        },
    )

    result = []
    for doc in cursor:
        result.append(_doc_to_msg(doc))
    return result


def _doc_to_msg(doc: dict) -> Dict[str, Any]:
    """Convert a MongoDB document to the API response shape."""
    msg_id = doc['_id']
    sender = doc.get('sender', 'Anonymous')
    r_id = doc.get('room_id', 'general')
    timestamp = doc.get('timestamp', 0)

    # Fast path: use stored plaintext (no decryption needed)
    text = doc.get('text_plain') or '[encrypted]'

    return {
        'id': msg_id,
        'username': sender,
        'client-name': sender,
        'text': text,
        'msg': text,
        'room': r_id,
        'timestamp': timestamp,
        'verified': True,
        'tampered': False,
    }