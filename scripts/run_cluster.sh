#!/usr/bin/env bash
# Launch a 3-node cluster in the background. Ctrl-C tears it all down.
set -euo pipefail
cd "$(dirname "$0")/.."

pids=()
cleanup() {
  echo
  echo "stopping cluster..."
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

for n in 1 2 3; do
  python -m kvstore --config "config/node${n}.json" &
  pids+=($!)
  sleep 0.3
done

echo "cluster up: node1=:7001 (leader) node2=:7002 node3=:7003"
echo "try:  ./kv.py set hello world   then   ./kv.py --node 127.0.0.1:7002 get hello"
wait
