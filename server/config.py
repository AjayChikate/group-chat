
import os


def csv(name: str, fallback: list) -> list:
    raw = os.environ.get(name)
    if not raw:
        return fallback
    return [s.strip() for s in raw.split(',') if s.strip()]


PORT = int(os.environ.get('PORT', '5000'))

# Identity of this backend node (backend1 / backend2 / backend3)
SERVER_ID = os.environ.get('SERVER_ID', 'backend-unknown')

# Number of uvicorn worker processes
UVICORN_WORKERS = int(os.environ.get('UVICORN_WORKERS', '4'))

# Rooms that always exist, even with nobody in them.
DEFAULT_ROOMS = csv('DEFAULT_ROOMS', ['general', 'random', 'tech'])

# Usernames (case-insensitive) granted moderator powers (kick/mute).
ADMIN_USERNAMES = [s.lower() for s in csv('ADMIN_USERNAMES', ['admin'])]

# Past messages replayed to a client when it joins a room.
HISTORY_LIMIT = int(os.environ.get('HISTORY_LIMIT', '50'))

# Ping every N ms, drop clients that never ping back.
HEARTBEAT_INTERVAL_MS = int(os.environ.get('HEARTBEAT_INTERVAL_MS', '30000'))

# Mark a user "away" after this much inactivity.
IDLE_TIMEOUT_MS = int(os.environ.get('IDLE_TIMEOUT_MS', '300000'))

PRESENCE_CHECK_INTERVAL_MS = 15000

# Rate limiting is DISABLED — TokenBucket always returns True.
# These values are kept only so ws_server.py can still instantiate a bucket.
RATE_LIMIT = {
    'BURST': 999999,
    'REFILL_PER_SEC': 999999.0,
}

MAX_USERNAME_LEN = 2000
MAX_MESSAGE_LEN = 200000   # increased from 1000
MAX_ROOM_NAME_LEN = 2400