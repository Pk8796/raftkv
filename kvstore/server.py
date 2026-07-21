import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .node import NotLeader

log = logging.getLogger("kv")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def node(self):
        return self.server.node

    def _send(self, code, obj, content_type="application/json"):
        if content_type == "application/json":
            body = json.dumps(obj).encode()
        else:
            body = obj.encode() if isinstance(obj, str) else obj
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _raw_body(self):
        n = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(n) if n else b""

    def _json_body(self):
        raw = self._raw_body()
        return json.loads(raw) if raw else {}

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/health":
            return self._send(200, {"ok": True})
        if u.path == "/status":
            return self._send(200, self.node.status())
        if u.path == "/metrics":
            return self._send(200, self.node.metrics(), "text/plain")
        if u.path == "/log":
            since = int(parse_qs(u.query).get("since", ["0"])[0])
            return self._send(200, self.node.get_log(since))
        if u.path.startswith("/kv/"):
            key = u.path[len("/kv/"):]
            val = self.node.client_get(key)
            if val is None:
                return self._send(404, {"error": "not found"})
            return self._send(200, {"key": key, "value": val})
        return self._send(404, {"error": "unknown path"})

    def do_PUT(self):
        u = urlparse(self.path)
        if u.path.startswith("/kv/"):
            key = u.path[len("/kv/"):]
            value = self._raw_body().decode()
            try:
                self.node.client_set(key, value)
            except NotLeader as e:
                return self._send(409, {"error": "not leader", "leader": e.leader})
            return self._send(200, {"ok": True, "key": key})
        return self._send(404, {"error": "unknown path"})

    def do_DELETE(self):
        u = urlparse(self.path)
        if u.path.startswith("/kv/"):
            key = u.path[len("/kv/"):]
            try:
                self.node.client_delete(key)
            except NotLeader as e:
                return self._send(409, {"error": "not leader", "leader": e.leader})
            return self._send(200, {"ok": True})
        return self._send(404, {"error": "unknown path"})

    def do_POST(self):
        u = urlparse(self.path)
        try:
            msg = self._json_body()
        except json.JSONDecodeError:
            return self._send(400, {"error": "bad json"})
        if u.path == "/replicate":
            return self._send(200, self.node.on_replicate(msg))
        if u.path == "/heartbeat":
            return self._send(200, self.node.on_heartbeat(msg))
        if u.path == "/request_vote":
            return self._send(200, self.node.on_request_vote(msg))
        return self._send(404, {"error": "unknown path"})

    def log_message(self, *args):
        pass  # the node emits its own structured logs; keep the access log quiet


def serve(node):
    server = ThreadingHTTPServer((node.host, node.port), Handler)
    server.node = node
    log.info("node %s listening on %s:%d", node.id, node.host, node.port)
    server.serve_forever()
