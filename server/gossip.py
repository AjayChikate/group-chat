import asyncio
import json
import logging
from typing import Any, Dict, Optional

from server import config

logger = logging.getLogger('gossip')

try:
    import httpx
    _HAS_HTTPX = True
except ImportError:
    _HAS_HTTPX = False

_async_client: Optional[Any] = None


def _get_client() -> Optional[Any]:
    global _async_client
    if _async_client is None and _HAS_HTTPX:
        _async_client = httpx.AsyncClient(
            timeout=httpx.Timeout(1.0, connect=0.5),
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=100),
        )
    return _async_client


async def _send_httpx(client: Any, target_url: str, payload: Dict[str, Any]) -> None:
    try:
        await client.post(target_url, json=payload)
    except Exception:
        # Gossip is best effort: peers may be restarting or shutting down
        pass


def _send_urllib(target_url: str, payload: Dict[str, Any]) -> None:
    import urllib.request
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            target_url,
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            resp.read()
    except Exception:
        pass


def broadcast_gossip(room: str, msg: Dict[str, Any]) -> None:
    """
    Fire-and-forget: sends this message to all peer backend instances
    so they can update their local feed caches and broadcast to local WebSockets.
    Never blocks the caller.
    """
    peers = [p.rstrip('/') for p in config.PEERS if p and p.strip()]
    if not peers:
        return

    payload = {
        'origin': config.SERVER_ID,
        'room': room,
        'msg': msg,
    }

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    client = _get_client()

    for peer in peers:
        # Skip if peer URL is clearly pointing to this instance on localhost
        if f':{config.PORT}' in peer and ('localhost' in peer or '127.0.0.1' in peer):
            continue

        target = f'{peer}/internal/gossip'

        if loop and loop.is_running():
            if client is not None:
                loop.create_task(_send_httpx(client, target, payload))
            else:
                loop.run_in_executor(None, _send_urllib, target, payload)
        else:
            # Running outside event loop: use background thread
            import threading
            threading.Thread(target=_send_urllib, args=(target, payload), daemon=True).start()


async def shutdown_gossip() -> None:
    global _async_client
    if _async_client is not None:
        try:
            await _async_client.aclose()
        except Exception:
            pass
        _async_client = None

