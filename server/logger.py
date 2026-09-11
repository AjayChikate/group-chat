
import json
from datetime import datetime, timezone


def log(event: str, **meta) -> None:
    line = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'event': event,
        **meta,
    }
    print(json.dumps(line))