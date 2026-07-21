import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORTS = {"node1": 7101, "node2": 7102, "node3": 7103}


def _req(method, url, data=None, timeout=3):
    body = data.encode() if isinstance(data, str) else data
    req = urllib.request.Request(url, data=body, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _status(port):
    _, body = _req("GET", f"http://127.0.0.1:{port}/status")
    return json.loads(body)


def _wait_for_leader(ports, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for p in ports:
            try:
                st = _status(p)
                if st["role"] == "leader":
                    return st["id"], p
            except Exception:
                pass
        time.sleep(0.3)
    raise AssertionError("no leader elected in time")


def _write_config(tmp_path, node_id, leader=False):
    peers = [
        {"id": nid, "host": "127.0.0.1", "port": PORTS[nid]}
        for nid in PORTS if nid != node_id
    ]
    cfg = {
        "id": node_id,
        "host": "127.0.0.1",
        "port": PORTS[node_id],
        "data_dir": str(tmp_path / node_id),
        "heartbeat_interval": 0.5,
        "election_timeout_low": 1.0,
        "election_timeout_high": 2.0,
        "sync_interval": 0.5,
        "peers": peers,
    }
    if leader:
        cfg["start_as_leader"] = True
    path = tmp_path / f"{node_id}.json"
    path.write_text(json.dumps(cfg))
    return str(path)


@pytest.fixture
def cluster(tmp_path):
    procs = {}
    for nid in PORTS:
        cfg = _write_config(tmp_path, nid, leader=(nid == "node1"))
        procs[nid] = subprocess.Popen(
            [sys.executable, "-m", "kvstore", "--config", cfg, "--log-level", "WARNING"],
            cwd=ROOT,
        )
    _wait_for_leader(PORTS.values())
    yield procs, tmp_path
    for p in procs.values():
        p.terminate()
    for p in procs.values():
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def test_replication(cluster):
    _, _ = cluster
    _req("PUT", "http://127.0.0.1:7101/kv/color", "blue")
    time.sleep(1.0)
    code, body = _req("GET", "http://127.0.0.1:7102/kv/color")
    assert code == 200 and json.loads(body)["value"] == "blue"


def test_follower_catches_up_after_restart(cluster):
    procs, tmp_path = cluster
    _req("PUT", "http://127.0.0.1:7101/kv/a", "1")
    time.sleep(0.8)

    procs["node3"].terminate()
    procs["node3"].wait(timeout=5)

    for i in range(5):
        _req("PUT", "http://127.0.0.1:7101/kv/a", str(i))
    time.sleep(0.5)

    cfg = str(tmp_path / "node3.json")
    procs["node3"] = subprocess.Popen(
        [sys.executable, "-m", "kvstore", "--config", cfg, "--log-level", "WARNING"], cwd=ROOT
    )
    time.sleep(3.0)
    code, body = _req("GET", "http://127.0.0.1:7103/kv/a")
    assert code == 200 and json.loads(body)["value"] == "4"


def test_failover_when_leader_dies(cluster):
    procs, _ = cluster
    _req("PUT", "http://127.0.0.1:7101/kv/before", "x")
    time.sleep(0.8)

    procs["node1"].terminate()
    procs["node1"].wait(timeout=5)

    survivors = [7102, 7103]
    leader_id, leader_port = _wait_for_leader(survivors, timeout=15)
    assert leader_id in ("node2", "node3")

    # new leader must accept writes
    code, _ = _req("PUT", f"http://127.0.0.1:{leader_port}/kv/after", "y")
    assert code == 200
    code, body = _req("GET", f"http://127.0.0.1:{leader_port}/kv/after")
    assert code == 200 and json.loads(body)["value"] == "y"
