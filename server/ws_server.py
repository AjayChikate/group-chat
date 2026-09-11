import asyncio
import json
import re
import time
import threading
import uuid

from fastapi import WebSocket, WebSocketDisconnect

from server import config, store, crypto
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
    # so background threads can schedule coroutines on it.
    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        return self._loop

    # Helpers
    @staticmethod
    def _is_valid_room_name(name) -> bool:
        return isinstance(name, str) and bool(ROOM_NAME_RE.match(name))

    def _send(self, client: dict, obj: dict) -> None:
        if not client.get('connected', False):
            return
        ws: WebSocket = client['ws']
        loop = self._ensure_loop()
        payload = json.dumps(obj)
        try:
            asyncio.run_coroutine_threadsafe(ws.send_text(payload), loop)
        except Exception:
            pass

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
    # Per-connection entry point — called from app.py's @app.websocket
    async def handle_connection(self, ws: WebSocket) -> None:
        await ws.accept()

        # Capture the event loop on the first connection (we're on the
        # asyncio thread here, so get_event_loop() is correct).
        self._ensure_loop()

        client_id = str(uuid.uuid4())
        client = {
            'id': client_id,
            'ws': ws,
            'connected': True,
            'loop': self._loop,
            'username': None,
            'room': None,
            'is_admin': False,
            'muted': False,
            'presence': 'online',
            'last_activity': time.time(),
            'bucket': TokenBucket(config.RATE_LIMIT['BURST'], config.RATE_LIMIT['REFILL_PER_SEC']),
            'lock': threading.Lock(),
        }
        self.clients_by_id[client_id] = client

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

                self._dispatch(client, data)
        finally:
            client['connected'] = False
            self._handle_close(client)

    def _dispatch(self, client: dict, data: dict) -> None:
        msg_type = data.get('type')
        if msg_type == 'join':
            self._handle_join(client, data)
        elif msg_type == 'message':
            self._handle_message(client, data)
        elif msg_type == 'private_message':
            self._handle_private_message(client, data)
        elif msg_type == 'switch_room':
            self._handle_switch_room(client, data)
        elif msg_type == 'create_room':
            self._handle_create_room(client, data)
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
        if client['username']:
            self.username_to_id.pop(client['username'].lower(), None)

        if client['room']:
            self.room_manager.leave(client['room'], client['id'])
            self.room_manager.broadcast(client['room'], {
                'type': 'notification',
                'text': f"{client['username']} left #{client['room']}",
                'users': self.room_manager.get_usernames(client['room']),
                'room': client['room'],
            })
        if client['username']:
            self._broadcast_presence(client, 'offline')
            self.logger.log('disconnect', username=client['username'], client_id=client['id'])

    # Handler: join
    def _handle_join(self, client: dict, data: dict) -> None:
        if client['username']:
            return  # already joined; ignore repeat joins

        requested = str(data.get('username') or '').strip()[:config.MAX_USERNAME_LEN] \
            or f"User{client['id'][:4]}"
        username = self._unique_username(requested)
        room = data.get('room') if self._is_valid_room_name(data.get('room')) else config.DEFAULT_ROOMS[0]

        client['username'] = username
        client['room'] = room
        client['is_admin'] = username.lower() in config.ADMIN_USERNAMES

        # Initialize sender's asymmetric Ed25519 signing keypair
        priv_key, pub_key = crypto.get_or_create_sender_keys(username)
        client['private_key'] = priv_key
        client['public_key'] = pub_key
        store.save_user_public_key(username, pub_key.public_bytes_raw())

        self.username_to_id[username.lower()] = client['id']
        self.room_manager.join(room, client['id'], {
            'ws': client['ws'],
            'connected': True,
            'loop': client['loop'],
            'username': username,
            'lock': client['lock'],
        })

        history = store.get_history(room, config.HISTORY_LIMIT)

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

        self._broadcast_to_all({'type': 'room_list', 'rooms': self.room_manager.list_rooms()})
        self._broadcast_presence(client, 'online')
        self.logger.log('join', username=username, room=room, client_id=client['id'], is_admin=client['is_admin'])

    # Handler: room chat message
    def _handle_message(self, client: dict, data: dict) -> None:
        if not client['username']:
            return

        text = str(data.get('text') or '').strip()[:config.MAX_MESSAGE_LEN]
        if not text:
            return

        msg = {
            'id': str(uuid.uuid4()),
            'username': client['username'],
            'text': text,
            'room': client['room'],
            'timestamp': int(time.time() * 1000),
        }

        # Encrypt (AES-GCM) -> Sign (Ed25519) -> Store in MongoDB (dedup) -> Broadcast
        store.append_message(client['room'], msg, client.get('private_key'))
        self.room_manager.broadcast(client['room'], {'type': 'message', **msg})
        self._send(client, {'type': 'delivered', 'id': msg['id']})

    def _handle_private_message(self, client: dict, data: dict) -> None:
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

        # Persisted encrypted & signed in SQLite
        pair_key = '__'.join(sorted([client['username'].lower(), target_name.lower()]))
        store.append_message(f'dm-{pair_key}', dm, client.get('private_key'))

        target_client = self.clients_by_id.get(target_id)
        if target_client:
            self._send(target_client, {'type': 'private_message', **dm})
        self._send(client, {'type': 'private_message', **dm})  # echo to sender

    # Handler: switch_room
    def _handle_switch_room(self, client: dict, data: dict) -> None:
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

        history = store.get_history(new_room, config.HISTORY_LIMIT)
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

        self._broadcast_to_all({'type': 'room_list', 'rooms': self.room_manager.list_rooms()})

    # Handler: create_room
    def _handle_create_room(self, client: dict, data: dict) -> None:
        if not client['username']:
            return
        name = str(data.get('room') or '').strip()
        if not self._is_valid_room_name(name):
            return self._send_error(client, 'bad_room_name', 'Room names: 1-24 chars, letters/numbers/_/- only.')
        is_new = not self.room_manager.room_exists(name)
        self.room_manager.ensure_room(name)
        if is_new:
            self._broadcast_to_all({'type': 'room_list', 'rooms': self.room_manager.list_rooms()})
        self._handle_switch_room(client, {'room': name})

    # Handler: typing indicator  
    def _handle_typing(self, client: dict) -> None:
        if not client['username'] or not client['room']:
            return
        self.room_manager.broadcast(
            client['room'],
            {'type': 'typing', 'username': client['username'], 'room': client['room']},
            client['id'],
        )

    
    # Handler: moderation (kick / mute / unmute) — admin only 
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

    # Background: presence (idle -> "away") sweep
    # Replaces node's setInterval with a daemon thread + Event.wait,
    # which also doubles as the sleep (wait() returns False on timeout).

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