"""
crypto.py — AES-GCM-256 encryption + Ed25519 signing.

AES master key priority:
  1. AES_MASTER_KEY env var (64-char hex = 32 bytes)   ← use this for multi-node deploys
  2. Local data/master.key file                         ← single-node / dev fallback
"""

import hashlib
import hmac
import json
import os
from typing import Tuple

from cryptography.exceptions import InvalidTag, InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
KEYS_DIR = os.path.join(DATA_DIR, 'keys')
MASTER_KEY_FILE = os.path.join(DATA_DIR, 'master.key')


# In-memory caches to avoid redundant disk I/O during high-concurrency bursts
_cached_master_key: bytes = None
_cached_sender_keys: dict = {}


def ensure_crypto_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(KEYS_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# 1. Symmetric encryption (AES-GCM 256-bit)
# ---------------------------------------------------------------------------

def get_master_key() -> bytes:
    """
    Load the AES-256 master key.  Multi-node deployments MUST set
    AES_MASTER_KEY to the same 64-hex-char string on every backend node
    so that ciphertexts written by one node can be decrypted by another.
    """
    global _cached_master_key
    if _cached_master_key is not None:
        return _cached_master_key

    # Priority 1: environment variable (required for shared-key multi-node setup)
    env_hex = os.environ.get('AES_MASTER_KEY', '').strip()
    if env_hex:
        try:
            key = bytes.fromhex(env_hex)
            if len(key) == 32:
                _cached_master_key = key
                return key
        except ValueError:
            pass  # fall through to file

    # Priority 2: local file (dev / single-node fallback)
    ensure_crypto_dirs()
    if os.path.exists(MASTER_KEY_FILE):
        with open(MASTER_KEY_FILE, 'rb') as f:
            key = f.read()
            if len(key) == 32:
                _cached_master_key = key
                return key

    # Generate a new key and save it (single-node mode)
    key = AESGCM.generate_key(bit_length=256)
    with open(MASTER_KEY_FILE, 'wb') as f:
        f.write(key)
    print(
        f'\n[crypto] Generated new AES master key.\n'
        f'[crypto] For multi-node deploy, set this env var on ALL backends:\n'
        f'[crypto]   AES_MASTER_KEY={key.hex()}\n'
    )
    _cached_master_key = key
    return key


def encrypt_message(plaintext: str, key: bytes = None) -> Tuple[bytes, bytes]:
    if key is None:
        key = get_master_key()
    aes = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit standard nonce for GCM
    ciphertext = aes.encrypt(nonce, plaintext.encode('utf-8'), None)
    return ciphertext, nonce


def decrypt_message(ciphertext: bytes, nonce: bytes, key: bytes = None) -> str:
    if key is None:
        key = get_master_key()
    aes = AESGCM(key)
    plaintext_bytes = aes.decrypt(nonce, ciphertext, None)
    return plaintext_bytes.decode('utf-8')


def encrypt_bytes(plaintext: bytes, key: bytes = None) -> Tuple[bytes, bytes]:
    """Encrypt raw bytes (used for storing Ed25519 private keys in MongoDB)."""
    if key is None:
        key = get_master_key()
    aes = AESGCM(key)
    nonce = os.urandom(12)
    ciphertext = aes.encrypt(nonce, plaintext, None)
    return ciphertext, nonce


def decrypt_bytes(ciphertext: bytes, nonce: bytes, key: bytes = None) -> bytes:
    """Decrypt raw bytes (used for loading Ed25519 private keys from MongoDB)."""
    if key is None:
        key = get_master_key()
    aes = AESGCM(key)
    return aes.decrypt(nonce, ciphertext, None)


# ---------------------------------------------------------------------------
# 2. Digital Signatures (Ed25519 Asymmetric Key Pairs)
# ---------------------------------------------------------------------------

def get_or_create_sender_keys(username: str) -> Tuple[ed25519.Ed25519PrivateKey, ed25519.Ed25519PublicKey]:
    """
    Returns (private_key, public_key) for the given username.
    Derives keys deterministically from the master key for instant 0.001ms generation,
    zero disk I/O, zero MongoDB round-trips, and identical keys across all backend nodes.
    """
    sanitized = "".join(c for c in username if c.isalnum() or c in ('_', '-')) or 'user'
    if sanitized in _cached_sender_keys:
        return _cached_sender_keys[sanitized]

    # Priority 1: Check existing key file on disk if present (e.g. pre-provisioned keys)
    key_path = os.path.join(KEYS_DIR, f"{sanitized}.key")
    if os.path.exists(key_path):
        try:
            with open(key_path, 'rb') as f:
                raw_private = f.read()
                if len(raw_private) == 32:
                    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(raw_private)
                    pair = (private_key, private_key.public_key())
                    _cached_sender_keys[sanitized] = pair
                    return pair
        except Exception:
            pass

    # Priority 2: Deterministic derivation from master key (0ms, 0 disk I/O, shared across nodes)
    master_key = get_master_key()
    seed = hmac.digest(master_key, f"user-ed25519:{sanitized}".encode('utf-8'), 'sha256')
    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
    pair = (private_key, private_key.public_key())

    if len(_cached_sender_keys) > 10000:
        _cached_sender_keys.clear()
    _cached_sender_keys[sanitized] = pair
    return pair


def load_private_key_from_bytes(raw_private: bytes) -> ed25519.Ed25519PrivateKey:
    """Load an Ed25519 private key from 32 raw bytes."""
    return ed25519.Ed25519PrivateKey.from_private_bytes(raw_private)


def cache_sender_key(username: str, private_key: ed25519.Ed25519PrivateKey) -> None:
    """Cache a sender key pair loaded from MongoDB (avoids disk lookup next time)."""
    sanitized = "".join(c for c in username if c.isalnum() or c in ('_', '-')) or 'user'
    _cached_sender_keys[sanitized] = (private_key, private_key.public_key())


def make_signable_payload(
    msg_id: str, room_id: str, sender: str, timestamp: int, nonce: bytes, ciphertext: bytes
) -> bytes:
    header = f"{msg_id}|{room_id}|{sender}|{timestamp}|".encode('utf-8')
    return header + nonce + b"|" + ciphertext


def sign_message(private_key: ed25519.Ed25519PrivateKey, payload: bytes) -> bytes:
    return private_key.sign(payload)


def verify_signature(public_key: ed25519.Ed25519PublicKey, signature: bytes, payload: bytes) -> bool:
    try:
        public_key.verify(signature, payload)
        return True
    except (InvalidSignature, Exception):
        return False
