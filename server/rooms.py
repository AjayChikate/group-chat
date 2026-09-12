
import asyncio
import json
import time


def _safe_send(member: dict, payload: str) -> None:
    if not member.get('connected', False):
        return
    q = member.get('send_queue')
    if q is not None:
        try:
            loop = member.get('loop')
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

    ws = member.get('ws')
    loop = member.get('loop')
    if not ws or not loop:
        return
    try:
        asyncio.run_coroutine_threadsafe(ws.send_text(payload), loop)
    except Exception:
        pass


class RoomManager:
    def __init__(self, logger):
        self.logger = logger
        self.rooms = {}

    def ensure_room(self, name: str) -> dict:
        if name not in self.rooms:
            self.rooms[name] = {'name': name, 'members': {}, 'created_at': time.time()}
            self.logger.log('room_created', room=name)
        return self.rooms[name]

    def room_exists(self, name: str) -> bool:
        return name in self.rooms

    def list_rooms(self) -> list:
        rooms = [
            {'name': r['name'], 'memberCount': len(r['members'])}
            for r in self.rooms.values()
        ]
        return sorted(rooms, key=lambda r: r['name'])

    def join(self, room_name: str, client_id: str, member: dict) -> None:
        room = self.ensure_room(room_name)
        room['members'][client_id] = member

    def leave(self, room_name: str, client_id: str) -> None:
        room = self.rooms.get(room_name)
        if room:
            room['members'].pop(client_id, None)

    def get_members(self, room_name: str) -> list:
        room = self.rooms.get(room_name)
        return list(room['members'].values()) if room else []

    def get_usernames(self, room_name: str) -> list:
        return [m['username'] for m in self.get_members(room_name)]

    def broadcast(self, room_name: str, obj: dict, exclude_client_id: str = None) -> None:
        room = self.rooms.get(room_name)
        if not room:
            return
        payload = json.dumps(obj)
        for cid, member in list(room['members'].items()):
            if cid == exclude_client_id:
                continue
            _safe_send(member, payload)