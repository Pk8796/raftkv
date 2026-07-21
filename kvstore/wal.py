import json
import os
import threading


class WAL:
    """Append-only log on disk. Every entry is one JSON line.

    The important bit is append(): we flush + fsync before returning so the
    caller can safely acknowledge a write knowing it survived a crash.
    """

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        open(self.path, "a").close()
        self._f = open(self.path, "a")

    def append(self, entry):
        line = json.dumps(entry, separators=(",", ":"))
        with self._lock:
            self._f.write(line + "\n")
            self._f.flush()
            os.fsync(self._f.fileno())

    def replay(self):
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # a torn last line from a hard crash mid-write; skip it
                    continue

    def entries_since(self, since):
        return [e for e in self.replay() if e.get("index", 0) > since]

    def close(self):
        try:
            self._f.close()
        except Exception:
            pass
