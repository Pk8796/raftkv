#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.error
import urllib.request


def _req(method, url, data=None):
    body = data.encode() if isinstance(data, str) else data
    req = urllib.request.Request(url, data=body, method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _base(node):
    return node if node.startswith("http") else f"http://{node}"


def _write(method, node, key, value=None):
    url = f"{_base(node)}/kv/{key}"
    code, body = _req(method, url, value)
    if code == 409:
        # hit a follower; it told us who the leader is, so redo the write there
        leader = json.loads(body).get("leader")
        if leader:
            code, body = _req(method, f"{_base(leader)}/kv/{key}", value)
    print(code, body)


def cmd_get(node, key):
    code, body = _req("GET", f"{_base(node)}/kv/{key}")
    if code == 200:
        print(json.loads(body)["value"])
    else:
        print(f"({code}) {body}", file=sys.stderr)
        sys.exit(1)


def cmd_status(node):
    code, body = _req("GET", f"{_base(node)}/status")
    print(json.dumps(json.loads(body), indent=2))


def main():
    ap = argparse.ArgumentParser(prog="kv")
    ap.add_argument("--node", default="127.0.0.1:7001")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("set"); p.add_argument("key"); p.add_argument("value")
    p = sub.add_parser("get"); p.add_argument("key")
    p = sub.add_parser("del"); p.add_argument("key")
    sub.add_parser("status")
    args = ap.parse_args()

    if args.cmd == "set":
        _write("PUT", args.node, args.key, args.value)
    elif args.cmd == "get":
        cmd_get(args.node, args.key)
    elif args.cmd == "del":
        _write("DELETE", args.node, args.key)
    elif args.cmd == "status":
        cmd_status(args.node)


if __name__ == "__main__":
    main()
