# distributed-kv

A small fault-tolerant distributed key-value store in pure Python. It stays
**durable** (no data lost when a process crashes) and **available** (keeps
serving when the leader node dies), built up from a single node to a replicated
cluster with automatic leader election.

No external database, no web framework, no third-party runtime dependencies —
just the standard library. Each "node" is the same program run with a different
config file, so a cluster is just three terminals on one laptop.

> One-liner: *a datastore that loses no data when a process crashes, and keeps
> serving when the leader node dies.*

## Why this exists

Writing a KV store on a single machine is trivial — it's a dictionary. The
engineering is in making it survive disaster. This project builds that up in
three defensible stages:

1. **Durability** — a write-ahead log (WAL) that's `fsync`'d before a write is
   acknowledged, replayed on startup. Kill the process mid-run and nothing is
   lost. This is what Postgres, SQLite, and Kafka all do.
2. **Replication** — a leader ships each log entry to followers; any node can
   serve reads (followers may lag by milliseconds — eventual consistency).
3. **Failover** — followers watch for the leader's heartbeat; if it stops, they
   run a leader election and one of them takes over, no manual intervention.

## Architecture

```
     client ──HTTP──▶  ┌──────────┐  replicate log   ┌────────────┐
     GET/PUT/DELETE     │  LEADER  │ ───────────────▶ │  FOLLOWER  │
                        │  :7001   │                  │   :7002    │
                        └────┬─────┘                  └─────┬──────┘
                             │ append + fsync before ack    │ replay WAL on restart
                             ▼                              ▼
                        wal-node1.log                  wal-node2.log

     Heartbeats: leader → followers every 1s.
     No heartbeat within the timeout → election → new leader keeps serving.
```

## Quick start

```bash
# no install needed to run; everything is stdlib
./scripts/run_cluster.sh          # starts node1(:7001, leader) node2(:7002) node3(:7003)
```

In another terminal:

```bash
./kv.py set hello world                     # writes to the leader
./kv.py --node 127.0.0.1:7002 get hello     # read it back from a follower
./kv.py --node 127.0.0.1:7002 set foo bar   # write to a follower -> auto-redirected to leader
./kv.py status                              # role / term / leader / last index
```

You can also hit the HTTP API directly:

```bash
curl -X PUT  http://127.0.0.1:7001/kv/name -d 'Pavan'
curl         http://127.0.0.1:7001/kv/name
curl -X DELETE http://127.0.0.1:7001/kv/name
curl         http://127.0.0.1:7001/metrics   # Prometheus text format
curl         http://127.0.0.1:7001/status
```

## The failure demos

**1. Durability (Rung 1)** — data survives a hard crash:

```bash
python -m kvstore --config config/node1.json &
curl -X PUT http://127.0.0.1:7001/kv/name -d 'Pavan'
kill -9 %1                    # hard crash, no clean shutdown
python -m kvstore --config config/node1.json &
curl http://127.0.0.1:7001/kv/name    # -> "Pavan", replayed from the WAL
```

**2. Follower catch-up (Rung 2)** — restart a follower and it syncs the log it
missed via `GET /log?since=<index>`.

**3. Failover (Rung 3)** — kill the leader while traffic flows:

```bash
kill -9 <leader-pid>
# within a few seconds a follower's status flips to "leader" and writes work again
```

## HTTP API

| Method | Path              | Purpose                                   |
|--------|-------------------|-------------------------------------------|
| GET    | `/kv/<key>`       | read a value (404 if missing)             |
| PUT    | `/kv/<key>`       | set value (body = value); 409 if follower |
| DELETE | `/kv/<key>`       | delete a key                              |
| GET    | `/status`         | role, term, leader, last index, peers     |
| GET    | `/metrics`        | Prometheus-format counters/gauges         |
| GET    | `/log?since=N`    | log entries after index N (used for sync) |
| POST   | `/replicate`      | leader → follower log shipping (internal) |
| POST   | `/heartbeat`      | leader → follower liveness (internal)     |
| POST   | `/request_vote`   | candidate → peer during elections (internal) |

A `PUT`/`DELETE` sent to a follower returns `409` with the leader's address in
the body; `kv.py` follows that automatically.

## Configuration

Each node reads a JSON config (see `config/`). Fields:

- `id`, `host`, `port` — this node's identity and listen address
- `peers` — the other cluster members
- `start_as_leader` — bootstraps node1 as leader so the happy path is
  deterministic; it can still be replaced by an election if it dies
- `heartbeat_interval`, `election_timeout_low/high`, `sync_interval` — timers

## Tests

```bash
pip install pytest
pytest -q
```

`test_wal.py` and `test_store.py` are unit tests. `test_cluster.py` spins up a
real 3-process cluster on ports 7101–7103 and asserts replication, follower
catch-up after restart, and failover after the leader is killed.

## Honest scope

The election is a **simplified Raft** — it does term-based voting and majority
election, which is enough to keep the cluster available, but it deliberately
skips full log-matching safety and commit-index rules. That means a write
acknowledged by the old leader but not yet replicated can be lost on failover.
Real Raft (or etcd/Consul) closes that gap; the point here is to understand
durability, replication, and failover deeply enough to reason about the
tradeoffs, not to reimplement etcd.

Also out of scope: SQL, multi-key transactions, cross-datacenter replication,
and log compaction/snapshotting (the WAL grows unbounded — fine at this scale).

## Layout

```
kvstore/
  node.py      state machine: durability + replication + election
  server.py    HTTP routing (ThreadingHTTPServer)
  wal.py       append-only log, fsync on every write
  store.py     in-memory dict
  peers.py     peer HTTP client with retry/backoff
  config.py    config loader
  __main__.py  entry point: python -m kvstore --config <file>
kv.py          CLI client
config/        node1/2/3 configs
scripts/       run_cluster.sh
tests/
```
