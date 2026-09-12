import asyncio
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from server import config

logger = logging.getLogger('gossip')

try:
    import httpx
    _HAS_HTTPX = True
except ImportError:
    _HAS_HTTPX = False

_async_client: Optional[Any] = None
_gossip_queue: Optional[asyncio.Queue] = None
_gossip_worker_task: Optional[asyncio.Task] = None


def _get_client() -> Optional[Any]:
    global _async_client
    if _async_client is None and _HAS_HTTPX:
        _async_client = httpx.AsyncClient(
            timeout=httpx.Timeout(2.0, connect=0.5),
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )
    return _async_client


def init_gossip() -> None:
    """Initialize gossip worker for batched peer synchronization."""
    global _gossip_queue, _gossip_worker_task
    if _gossip_queue is None:
        _gossip_queue = asyncio.Queue(maxsize=50000)
        try:
            loop = asyncio.get_running_loop()
            _gossip_worker_task = loop.create_task(_gossip_worker_loop())
        except RuntimeError:
            pass


async def _send_httpx(client: Any, target_url: str, payload: Any) -> None:
    try:
        await client.post(target_url, json=payload)
    except Exception:
        pass


def _send_urllib(target_url: str, payload: Any) -> None:
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


async def _gossip_worker_loop() -> None:
    client = _get_client()
    peers = [p.rstrip('/') for p in config.PEERS if p and p.strip()]
    valid_peers = []
    for peer in peers:
        if f':{config.PORT}' in peer and ('localhost' in peer or '127.0.0.1' in peer):
            continue
        valid_peers.append(peer)

    if not valid_peers:
        return

    while True:
        try:
            item = await _gossip_queue.get()
            batch = [item]
            _gossip_queue.task_done()

            start_t = time.time()
            while len(batch) < 50 and (time.time() - start_t) < 0.02:
                try:
                    b_item = _gossip_queue.get_nowait()
                    batch.append(b_item)
                    _gossip_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            payload = batch if len(batch) > 1 else batch[0]
            for peer in valid_peers:
                target = f'{peer}/internal/gossip'
                try:
                    if client is not None:
                        asyncio.create_task(_send_httpx(client, target, payload))
                    else:
                        loop = asyncio.get_running_loop()
                        loop.run_in_executor(None, _send_urllib, target, payload)
                except Exception:
                    pass
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.05)


def broadcast_gossip(room: str, msg: Dict[str, Any]) -> None:
    """
    Fire-and-forget: enqueues this message for batched peer gossip.
    Never blocks the caller.
    """
    payload = {
        'origin': config.SERVER_ID,
        'room': room,
        'msg': msg,
    }

    if _gossip_queue is not None:
        try:
            _gossip_queue.put_nowait(payload)
            return
        except Exception:
            pass

    peers = [p.rstrip('/') for p in config.PEERS if p and p.strip()]
    if not peers:
        return

    client = _get_client()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    for peer in peers:
        if f':{config.PORT}' in peer and ('localhost' in peer or '127.0.0.1' in peer):
            continue

        target = f'{peer}/internal/gossip'
        if loop and loop.is_running():
            if client is not None:
                loop.create_task(_send_httpx(client, target, payload))
            else:
                loop.run_in_executor(None, _send_urllib, target, payload)
        else:
            threading.Thread(target=_send_urllib, args=(target, payload), daemon=True).start()


async def shutdown_gossip() -> None:
    global _async_client, _gossip_worker_task
    if _gossip_worker_task is not None:
        _gossip_worker_task.cancel()
        try:
            await _gossip_worker_task
        except asyncio.CancelledError:
            pass
        _gossip_worker_task = None

    if _async_client is not None:
        try:
            await _async_client.aclose()
        except Exception:
            pass
        _async_client = None

