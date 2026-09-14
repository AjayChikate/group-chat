import asyncio
import json
import re
import time
import threading
import uuid

from fastapi import WebSocket, WebSocketDisconnect

from server import config, store, crypto, gossip
from server.rate_limiter import TokenBucket

ROOM_NAME_RE = re.compile(r'^[a-zA-Z0-9_-]{1,24}$')


class WSServer:
    def __init__(self, room_manager, logger):
        self.room_manager = room_manager
        self.logger = logger
        self.clients_by_id = {}    # client_id -> client dict
        self.username_to_id = {}   # lowercase username -> client_id
        self._loop: asyncio.AbstractEventLoop | None = None  # set on first connection

        for room in config.DEFAULT_ROOMS:
            self.room_manager.ensure_room(room)

        self._presence_stop = threading.Event()
        self._presence_thread = threading.Thread(target=self._presence_sweep_loop, daemon=True)
        self._presence_thread.start()

    # Asyncio loop access — captured once from the first request
    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _is_valid_room_name(name) -> bool:
        return isinstance(name, str) and bool(ROOM_NAME_RE.match(name))

    def _send(self, client: dict, obj: dict) -> None:
        if not client.get('connected', False):
            return
        payload = json.dumps(obj)
        q = client.get('send_queue')
        if q is not None:
            try:
                loop = client.get('loop')
                try:
                    running_loop = asyncio.get_running_loop()
                    if running_loop is loop:
                        q.put_nowait(payload)
                        return
                except RuntimeError:
                    pass
                if loop and loop.is_running():
                    loop.call_soon_threadsafe(q.put_nowait, payload)
            except Exception:
                pass
            return
        ws: WebSocket = client['ws']
        loop = self._ensure_loop()
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(payload), loop)
        except Exception:
            pass

    async def _client_sender_loop(self, client: dict) -> None:
        ws: WebSocket = client['ws']
        q: asyncio.Queue = client['send_queue']
        while client.get('connected', False):
            try:
                payload = await q.get()
                # 0.5-second timeout: if TCP send buffer is full (slow client),
                # drop connection quickly so event loop stays completely responsive
                await asyncio.wait_for(ws.send_text(payload), timeout=0.5)
                q.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                client['connected'] = False
                break

    def _close_ws(self, client: dict, code: int = 1001) -> None:
        """Schedule an async ws.close() from any thread."""
        if not client.get('connected', False):
            return
        ws: WebSocket = client['ws']
        loop = self._ensure_loop()
        client['connected'] = False
        try:
            asyncio.run_coroutine_threadsafe(ws.close(code=code), loop)
        except Exception:
            pass

    def _send_error(self, client: dict, code: str, text: str) -> None:
        self._send(client, {'type': 'error', 'code': code, 'text': text})

    def _broadcast_to_all(self, obj: dict, exclude_client_id: str = None) -> None:
        for cid, c in list(self.clients_by_id.items()):
            if cid == exclude_client_id:
                continue
            self._send(c, obj)

    def _broadcast_presence(self, client: dict, status: str) -> None:
        self._broadcast_to_all({'type': 'presence', 'username': client['username'], 'status': status})

    def _unique_username(self, requested: str) -> str:
        taken = set(self.username_to_id.keys())
        base = requested
        final = base
        n = 1
        while final.lower() in taken:
            final = f'{base}({n})'
            n += 1
        return final

    # -----------------------------------------------------------------------
    # Per-connection entry point
    # -----------------------------------------------------------------------

    async def handle_connection(self, ws: WebSocket) -> None:
        await ws.accept()

        # Capture the event loop on the first connection.
        self._ensure_loop()

        client_id = str(uuid.uuid4())
        send_queue = asyncio.Queue(maxsize=1000)

        # Detect optional query parameters (e.g. ?room=general&username=alice)
        qp = getattr(ws, 'query_params', {})
        req_room = qp.get('room') if self._is_valid_room_name(qp.get('room')) else config.DEFAULT_ROOMS[0]
        req_user = qp.get('username') or qp.get('client-name') or None

        client = {
            'id': client_id,
            'ws': ws,
            'send_queue': send_queue,
            'connected': True,
            'loop': self._loop,
            'username': req_user,
            'room': req_room,
            'is_admin': False,
            'muted': False,
            'presence': 'online',
            'last_activity': time.time(),
            'bucket': TokenBucket(config.RATE_LIMIT['BURST'], config.RATE_LIMIT['REFILL_PER_SEC']),
            'lock': threading.Lock(),
            '_explicitly_joined': False,
        }
        self.clients_by_id[client_id] = client

        # Auto-join default room so broadcasts reach this socket even before/without 'join' frame
        self.room_manager.join(req_room, client_id, {
            'ws': ws,
            'send_queue': send_queue,
            'connected': True,
            'loop': self._loop,
            'username': req_user or f"User{client_id[:4]}",
            'lock': client['lock'],
        })

        sender_task = asyncio.create_task(self._client_sender_loop(client))

        try:
            while True:
                try:
                    raw = await ws.receive_text()
                except WebSocketDisconnect:
                    break
                except Exception as exc:
                    self.logger.log('socket_error', client_id=client_id, message=str(exc))
                    break

                if raw is None:
                    break

                try:
                    data = json.loads(raw)
                except (ValueError, TypeError):
                    continue  # ignore malformed frames

                client['last_activity'] = time.time()
                if client['presence'] == 'away':
                    client['presence'] = 'online'
                    self._broadcast_presence(client, 'online')

                # _dispatch is async — awaiting it keeps the event loop free
                # between MongoDB calls while still processing messages in order.
                await self._dispatch(client, data)
        finally:
            client['connected'] = False
            sender_task.cancel()
            self._handle_close(client)

    # -----------------------------------------------------------------------
    # Dispatcher — async so individual handlers can await DB calls
    # -----------------------------------------------------------------------

    async def _dispatch(self, client: dict, data: dict) -> None:
        msg_type = data.get('type')
        if msg_type == 'join':
            await self._handle_join(client, data)
        elif msg_type == 'message':
            await self._handle_message(client, data)
        elif msg_type == 'private_message':
            await self._handle_private_message(client, data)
        elif msg_type == 'switch_room':
            await self._handle_switch_room(client, data)
        elif msg_type == 'create_room':
            await self._handle_create_room(client, data)
        elif msg_type == 'typing':
            self._handle_typing(client)
        elif msg_type == 'kick':
            self._handle_moderation(client, data, 'kick')
        elif msg_type == 'mute':
            self._handle_moderation(client, data, 'mute')
        elif msg_type == 'unmute':
            self._handle_moderation(client, data, 'unmute')
        # unknown type: ignore rather than crash the connection

    def _handle_close(self, client: dict) -> None:
        self.clients_by_id.pop(client['id'], None)
        username = client.get('username')
        explicitly_joined = client.get('_explicitly_joined', False)

        if username:
            self.username_to_id.pop(username.lower(), None)

        if client.get('room'):
            self.room_manager.leave(client['room'], client['id'])
            # Only broadcast leave notification if user actually joined with a username
            if explicitly_joined and username:
                self.room_manager.broadcast(client['room'], {
                    'type': 'notification',
                    'text': f"{username} left #{client['room']}",
                    'users': self.room_manager.get_usernames(client['room']),
                    'room': client['room'],
                })

        if explicitly_joined and username:
            self.logger.log('disconnect', username=username, client_id=client['id'])

    # -----------------------------------------------------------------------
    # Handler: join
    # Awaits get_history (needs data for welcome msg) + fire-and-forgets key save
    # -----------------------------------------------------------------------

    async def _handle_join(self, client: dict, data: dict) -> None:
        if client.get('_explicitly_joined'):
            return  # already joined; ignore repeat joins

        requested = str(data.get('username') or '').strip()[:config.MAX_USERNAME_LEN] \
            or f"User{client['id'][:4]}"
        username = self._unique_username(requested)
        room = data.get('room') if self._is_valid_room_name(data.get('room')) else config.DEFAULT_ROOMS[0]

        # Leave previous room if auto-joined to a different one
        old_room = client.get('room')
        if old_room and old_room != room:
            self.room_manager.leave(old_room, client['id'])

        client['username'] = username
        client['room'] = room
        client['is_admin'] = username.lower() in config.ADMIN_USERNAMES
        client['_explicitly_joined'] = True

        # Key generation is pure CPU/in-memory (no DB if already cached in crypto module)
        priv_key, pub_key = crypto.get_or_create_sender_keys(username)
        client['private_key'] = priv_key
        client['public_key'] = pub_key

        # Fire-and-forget the key persistence — only hits MongoDB on first-ever join
        # for this username; subsequent joins are a no-op due to in-memory cache check.
        asyncio.create_task(
            store.async_save_user_public_key(username, pub_key.public_bytes_raw())
        )

        self.username_to_id[username.lower()] = client['id']
        self.room_manager.join(room, client['id'], {
            'ws': client['ws'],
            'send_queue': client.get('send_queue'),
            'connected': True,
            'loop': client['loop'],
            'username': username,
            'lock': client['lock'],
        })

        # Await history — we need it in the welcome payload (served cache-first)
        history = await store.async_get_history(room, config.HISTORY_LIMIT)

        self._send(client, {
            'type': 'welcome',
            'username': username,
            'room': room,
            'isAdmin': client['is_admin'],
            'users': self.room_manager.get_usernames(room),
            'rooms': self.room_manager.list_rooms(),
            'onlineUsers': list(self.username_to_id.keys()),
            'history': history,
        })

        self.room_manager.broadcast(room, {
            'type': 'notification',
            'text': f'{username} joined #{room}',
            'users': self.room_manager.get_usernames(room),
            'room': room,
        }, client['id'])

        # Only send room_list + presence to the joining client, not all clients
        # Broadcasting to all N clients on every join = O(N^2) at 200 users → OOM
        self._send(client, {'type': 'room_list', 'rooms': self.room_manager.list_rooms()})
        self.logger.log('join', username=username, room=room, client_id=client['id'], is_admin=client['is_admin'])

    # -----------------------------------------------------------------------
    # Handler: room chat message
    # Broadcasts instantly; DB persist is fire-and-forget (create_task).
    # The event loop is NEVER blocked — the client always gets an instant ack.
    # -----------------------------------------------------------------------

    async def _handle_message(self, client: dict, data: dict) -> None:
        if not client['username']:
            return

        text = str(data.get('text') or data.get('msg') or '').strip()[:config.MAX_MESSAGE_LEN]
        if not text:
            return

        msg_id = str(data.get('id') or uuid.uuid4())
        timestamp = int(data.get('timestamp') or time.time() * 1000)

        msg = {
            'id': msg_id,
            'client-name': client['username'],
            'username': client['username'],
            'msg': text,
            'text': text,
            'room': client['room'],
            'timestamp': timestamp,
            'verified': True,
            'tampered': False,
        }

        # Cache immediately for /feed
        store.cache_message(client['room'], msg)

        # Broadcast to local room immediately — no waiting for DB
        self.room_manager.broadcast(client['room'], {'type': 'message', **msg})
        self._send(client, {'type': 'delivered', 'id': msg['id']})

        # Persist asynchronously in background — fire and forget.
        # The message is already visible to all clients; DB is for durability.
        asyncio.create_task(
            store.async_append_message(client['room'], msg, client.get('private_key'))
        )

        # Broadcast gossip to peer backends so their WS clients and caches get it
        gossip.broadcast_gossip(client['room'], msg)

    # -----------------------------------------------------------------------
    # Handler: private message
    # -----------------------------------------------------------------------

    async def _handle_private_message(self, client: dict, data: dict) -> None:
        if not client['username']:
            return

        target_name = str(data.get('to') or '').strip()
        target_id = self.username_to_id.get(target_name.lower())
        if not target_id:
            return self._send_error(client, 'user_offline', f'{target_name} is not online.')

        text = str(data.get('text') or '').strip()[:config.MAX_MESSAGE_LEN]
        if not text:
            return

        dm = {
            'id': str(uuid.uuid4()),
            'from': client['username'],
            'to': target_name,
            'text': text,
            'timestamp': int(time.time() * 1000),
        }

        # Deliver instantly
        target_client = self.clients_by_id.get(target_id)
        if target_client:
            self._send(target_client, {'type': 'private_message', **dm})
        self._send(client, {'type': 'private_message', **dm})  # echo to sender

        # Persist in background
        pair_key = '__'.join(sorted([client['username'].lower(), target_name.lower()]))
        asyncio.create_task(
            store.async_append_message(f'dm-{pair_key}', dm, client.get('private_key'))
        )

    # -----------------------------------------------------------------------
    # Handler: switch_room
    # Awaits get_history for the new room (needed for room_switched payload)
    # -----------------------------------------------------------------------

    async def _handle_switch_room(self, client: dict, data: dict) -> None:
        if not client['username']:
            return
        new_room = data.get('room')
        if not self._is_valid_room_name(new_room) or not self.room_manager.room_exists(new_room):
            return self._send_error(client, 'no_such_room', f'Room "{new_room}" doesn\'t exist.')
        if new_room == client['room']:
            return

        old_room = client['room']
        self.room_manager.leave(old_room, client['id'])
        self.room_manager.broadcast(old_room, {
            'type': 'notification',
            'text': f"{client['username']} left #{old_room}",
            'users': self.room_manager.get_usernames(old_room),
            'room': old_room,
        })

        client['room'] = new_room
        self.room_manager.join(new_room, client['id'], {
            'ws': client['ws'],
            'connected': client.get('connected', True),
            'loop': client['loop'],
            'username': client['username'],
            'lock': client['lock'],
        })

        # Await history for the new room
        history = await store.async_get_history(new_room, config.HISTORY_LIMIT)
        self._send(client, {
            'type': 'room_switched',
            'room': new_room,
            'users': self.room_manager.get_usernames(new_room),
            'history': history,
        })

        self.room_manager.broadcast(new_room, {
            'type': 'notification',
            'text': f"{client['username']} joined #{new_room}",
            'users': self.room_manager.get_usernames(new_room),
            'room': new_room,
        }, client['id'])

    # -----------------------------------------------------------------------
    # Handler: create_room
    # -----------------------------------------------------------------------

    async def _handle_create_room(self, client: dict, data: dict) -> None:
        if not client['username']:
            return
        name = str(data.get('room') or '').strip()
        if not self._is_valid_room_name(name):
            return self._send_error(client, 'bad_room_name', 'Room names: 1-24 chars, letters/numbers/_/- only.')
        is_new = not self.room_manager.room_exists(name)
        self.room_manager.ensure_room(name)
        if is_new:
            self._broadcast_to_all({'type': 'room_list', 'rooms': self.room_manager.list_rooms()})
        await self._handle_switch_room(client, {'room': name})

    # -----------------------------------------------------------------------
    # Handler: typing indicator (pure in-memory, no DB, stays sync)
    # -----------------------------------------------------------------------

    def _handle_typing(self, client: dict) -> None:
        if not client['username'] or not client['room']:
            return
        self.room_manager.broadcast(
            client['room'],
            {'type': 'typing', 'username': client['username'], 'room': client['room']},
            client['id'],
        )

    # -----------------------------------------------------------------------
    # Handler: moderation (kick / mute / unmute) — admin only, stays sync
    # -----------------------------------------------------------------------

    def _handle_moderation(self, client: dict, data: dict, action: str) -> None:
        if not client['username']:
            return
        if not client['is_admin']:
            return self._send_error(client, 'not_admin', 'Only moderators can do that.')

        target_name = str(data.get('target') or '').strip()
        target_id = self.username_to_id.get(target_name.lower())
        target = self.clients_by_id.get(target_id) if target_id else None
        if not target:
            return self._send_error(client, 'user_offline', f'{target_name} is not online.')

        if action == 'kick':
            self._send(target, {'type': 'kicked', 'by': client['username']})
            self._close_ws(target, code=4001)
            self.logger.log('moderation_kick', by=client['username'], target=target_name)
        elif action == 'mute':
            target['muted'] = True
            if target['room']:
                self.room_manager.broadcast(target['room'], {
                    'type': 'notification',
                    'text': f"{target_name} was muted by {client['username']}",
                    'users': self.room_manager.get_usernames(target['room']),
                    'room': target['room'],
                })
            self.logger.log('moderation_mute', by=client['username'], target=target_name)
        elif action == 'unmute':
            target['muted'] = False
            if target['room']:
                self.room_manager.broadcast(target['room'], {
                    'type': 'notification',
                    'text': f"{target_name} was unmuted by {client['username']}",
                    'users': self.room_manager.get_usernames(target['room']),
                    'room': target['room'],
                })
            self.logger.log('moderation_unmute', by=client['username'], target=target_name)

    # -----------------------------------------------------------------------
    # Background: presence sweep (idle → "away")
    # -----------------------------------------------------------------------

    def _presence_sweep_loop(self) -> None:
        interval_sec = config.PRESENCE_CHECK_INTERVAL_MS / 1000
        while not self._presence_stop.wait(interval_sec):
            now = time.time()
            for client in list(self.clients_by_id.values()):
                if not client['username']:
                    continue
                idle_for_ms = (now - client['last_activity']) * 1000
                if idle_for_ms > config.IDLE_TIMEOUT_MS and client['presence'] != 'away':
                    client['presence'] = 'away'
                    self._broadcast_presence(client, 'away')

    def shutdown(self) -> None:
        self._presence_stop.set()
        self._broadcast_to_all({'type': 'system_shutdown', 'text': 'Server is shutting down. You will be disconnected.'})
        for client in list(self.clients_by_id.values()):
            self._close_ws(client, code=1001)