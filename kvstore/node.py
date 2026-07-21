import logging
import os
import random
import threading
import time

from . import peers
from .store import Store
from .wal import WAL

log = logging.getLogger("kv")


class NotLeader(Exception):
    def __init__(self, leader_addr):
        super().__init__("not leader")
        self.leader = leader_addr


class Node:
    """A single cluster member.

    Holds the durable log + in-memory state (Rung 1), replicates writes to
    followers (Rung 2), and runs a trimmed-down Raft election so the cluster
    keeps serving when the leader dies (Rung 3).

    One RLock guards all mutable state. Network calls are always made *outside*
    the lock so a slow/dead peer can't stall the whole node.
    """

    def __init__(self, cfg):
        self.id = cfg["id"]
        self.host = cfg.get("host", "127.0.0.1")
        self.port = int(cfg["port"])
        self.peers = {p["id"]: f'{p["host"]}:{p["port"]}' for p in cfg.get("peers", [])}

        data_dir = cfg.get("data_dir", f"./data/{self.id}")
        os.makedirs(data_dir, exist_ok=True)
        self.wal = WAL(os.path.join(data_dir, "wal.log"))
        self.store = Store()

        self.lock = threading.RLock()
        self.current_term = 0
        self.voted_for = None
        self.role = "follower"
        self.leader_id = None
        self.last_index = 0

        self.hb_interval = cfg.get("heartbeat_interval", 1.0)
        self.timeout_low = cfg.get("election_timeout_low", 2.5)
        self.timeout_high = cfg.get("election_timeout_high", 4.0)
        self.sync_interval = cfg.get("sync_interval", 2.0)
        self.election_deadline = 0.0

        self.writes = 0
        self.reads = 0
        self.started = time.time()

        self._running = False

        self._recover()

        if cfg.get("start_as_leader"):
            self.current_term = 1
            self.role = "leader"
            self.leader_id = self.id
            self.voted_for = self.id

    def _recover(self):
        n = 0
        for entry in self.wal.replay():
            self.store.apply(entry)
            self.last_index = max(self.last_index, entry.get("index", 0))
            n += 1
        if n:
            log.info("recovered %d entries, last_index=%d", n, self.last_index)

    # ------------------------------------------------------------------ util
    def _addr(self, node_id):
        return self.peers.get(node_id)

    def _url(self, node_id, path):
        return f"http://{self._addr(node_id)}{path}"

    def _maybe_advance_term(self, term):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.role = "follower"
            self.leader_id = None

    def _reset_election_timer(self):
        self.election_deadline = time.time() + random.uniform(self.timeout_low, self.timeout_high)

    # ------------------------------------------------------------- lifecycle
    def start(self):
        self._running = True
        self._reset_election_timer()
        for target in (self._election_loop, self._heartbeat_loop, self._sync_loop):
            threading.Thread(target=target, daemon=True).start()

    def stop(self):
        self._running = False

    # ----------------------------------------------------------- client ops
    def client_get(self, key):
        with self.lock:
            self.reads += 1
            return self.store.get(key)

    def client_set(self, key, value):
        entry = self._leader_append("set", key, value)
        self._replicate(entry)

    def client_delete(self, key):
        entry = self._leader_append("del", key, None)
        self._replicate(entry)

    def _leader_append(self, op, key, value):
        with self.lock:
            if self.role != "leader":
                raise NotLeader(self._addr(self.leader_id))
            idx = self.last_index + 1
            entry = {"index": idx, "term": self.current_term, "op": op, "key": key}
            if op == "set":
                entry["value"] = value
            self.wal.append(entry)      # durable before we ack
            self.store.apply(entry)
            self.last_index = idx
            self.writes += 1
        return entry

    def _replicate(self, entry):
        with self.lock:
            targets = list(self.peers.keys())
        for pid in targets:
            try:
                peers.post_json(self._url(pid, "/replicate"), entry, timeout=1.0, retries=2)
            except Exception as e:
                # follower down: it will catch up via /log on its next sync pass
                log.warning("replicate to %s failed: %s", pid, e)

    # --------------------------------------------------------------- rpc in
    def on_replicate(self, entry):
        with self.lock:
            if entry["term"] < self.current_term:
                return {"ok": False, "term": self.current_term}
            self._maybe_advance_term(entry["term"])
            self.role = "follower"
            self._reset_election_timer()
            idx = entry["index"]
            if idx <= self.last_index:
                return {"ok": True, "term": self.current_term}
            if idx == self.last_index + 1:
                self.wal.append(entry)
                self.store.apply(entry)
                self.last_index = idx
                return {"ok": True, "term": self.current_term}
            return {"ok": False, "gap": True, "have": self.last_index, "term": self.current_term}

    def on_heartbeat(self, msg):
        with self.lock:
            if msg["term"] < self.current_term:
                return {"ok": False, "term": self.current_term}
            self.current_term = msg["term"]
            self.role = "follower"
            self.leader_id = msg["leader_id"]
            self._reset_election_timer()
            return {"ok": True, "term": self.current_term}

    def on_request_vote(self, msg):
        with self.lock:
            term, cid = msg["term"], msg["candidate_id"]
            if term < self.current_term:
                return {"granted": False, "term": self.current_term}
            self._maybe_advance_term(term)
            if self.voted_for in (None, cid):
                self.voted_for = cid
                self._reset_election_timer()
                return {"granted": True, "term": self.current_term}
            return {"granted": False, "term": self.current_term}

    def get_log(self, since):
        with self.lock:
            last = self.last_index
        return {"entries": self.wal.entries_since(since), "last_index": last}

    # ------------------------------------------------------- background loops
    def _election_loop(self):
        while self._running:
            time.sleep(0.1)
            with self.lock:
                if self.role == "leader":
                    continue
                due = time.time() > self.election_deadline
            if due:
                self._start_election()

    def _start_election(self):
        with self.lock:
            self.role = "candidate"
            self.current_term += 1
            self.voted_for = self.id
            term = self.current_term
            self._reset_election_timer()
            peer_ids = list(self.peers.keys())
        log.info("starting election term=%d", term)

        votes = 1
        for pid in peer_ids:
            try:
                resp = peers.post_json(
                    self._url(pid, "/request_vote"),
                    {"term": term, "candidate_id": self.id},
                    timeout=1.0, retries=1,
                )
            except Exception:
                continue
            if resp.get("term", 0) > term:
                with self.lock:
                    self._maybe_advance_term(resp["term"])
                return
            if resp.get("granted"):
                votes += 1

        total = len(peer_ids) + 1
        if votes > total // 2:
            with self.lock:
                if self.role == "candidate" and self.current_term == term:
                    self.role = "leader"
                    self.leader_id = self.id
                    log.info("won election term=%d votes=%d/%d", term, votes, total)

    def _heartbeat_loop(self):
        while self._running:
            time.sleep(self.hb_interval)
            with self.lock:
                if self.role != "leader":
                    continue
                msg = {"term": self.current_term, "leader_id": self.id, "last_index": self.last_index}
                peer_ids = list(self.peers.keys())
            for pid in peer_ids:
                try:
                    resp = peers.post_json(self._url(pid, "/heartbeat"), msg, timeout=1.0, retries=0)
                    if resp.get("term", 0) > msg["term"]:
                        with self.lock:
                            self._maybe_advance_term(resp["term"])
                except Exception:
                    pass

    def _sync_loop(self):
        while self._running:
            time.sleep(self.sync_interval)
            with self.lock:
                if self.role == "leader" or not self.leader_id or self.leader_id == self.id:
                    continue
                since, leader = self.last_index, self.leader_id
            try:
                resp = peers.get_json(self._url(leader, f"/log?since={since}"), timeout=1.5, retries=1)
            except Exception:
                continue
            for entry in resp.get("entries", []):
                with self.lock:
                    if entry["index"] == self.last_index + 1:
                        self.wal.append(entry)
                        self.store.apply(entry)
                        self.last_index = entry["index"]

    # -------------------------------------------------------------- introspect
    def status(self):
        with self.lock:
            return {
                "id": self.id,
                "role": self.role,
                "term": self.current_term,
                "leader": self.leader_id,
                "last_index": self.last_index,
                "keys": self.store.size(),
                "peers": self.peers,
                "uptime_s": round(time.time() - self.started, 1),
            }

    def metrics(self):
        with self.lock:
            role, keys = self.role, self.store.size()
            writes, reads = self.writes, self.reads
            term, last_index = self.current_term, self.last_index
        return "\n".join([
            "# HELP kv_writes_total Total write operations acknowledged",
            "# TYPE kv_writes_total counter",
            f"kv_writes_total {writes}",
            "# HELP kv_reads_total Total read operations served",
            "# TYPE kv_reads_total counter",
            f"kv_reads_total {reads}",
            "# HELP kv_keys Current number of keys held",
            "# TYPE kv_keys gauge",
            f"kv_keys {keys}",
            "# HELP kv_last_index Last applied log index",
            "# TYPE kv_last_index gauge",
            f"kv_last_index {last_index}",
            f'kv_role{{role="{role}"}} 1',
            f"kv_term {term}",
        ]) + "\n"
