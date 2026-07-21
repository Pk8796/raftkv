import threading


class Store:
    def __init__(self):
        self._d = {}
        self._lock = threading.Lock()

    def apply(self, entry):
        with self._lock:
            if entry["op"] == "set":
                self._d[entry["key"]] = entry["value"]
            elif entry["op"] == "del":
                self._d.pop(entry["key"], None)

    def get(self, key):
        with self._lock:
            return self._d.get(key)

    def has(self, key):
        with self._lock:
            return key in self._d

    def size(self):
        with self._lock:
            return len(self._d)
