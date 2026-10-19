#!/usr/bin/env bash
# Starts a 3-master Redis Cluster on ports 7100-7102 for test/live-cluster.test.ts.
# Run from Windows:   wsl -e bash /mnt/c/GitHub/redi.ts/scripts/cluster-up.sh
# Run inside WSL:     bash scripts/cluster-up.sh [basePort]
#
# Idempotent: nodes already answering PING are reused; an already-formed
# cluster is not re-created. Logs live in /tmp/redi-cluster-<port>/redis.log.
set -euo pipefail

BASE="${1:-7100}"
NODES=3
REDIS_SERVER="${REDIS_SERVER:-redis-server}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"

port_up() { "$REDIS_CLI" -p "$1" ping >/dev/null 2>&1; }

for i in $(seq 0 $((NODES - 1))); do
    port=$((BASE + i))
    if port_up "$port"; then
        echo "port $port already up — reusing"
        continue
    fi
    dir="/tmp/redi-cluster-$port"
    mkdir -p "$dir"
    "$REDIS_SERVER" \
        --port "$port" \
        --bind 127.0.0.1 \
        --cluster-enabled yes \
        --cluster-config-file "$dir/nodes.conf" \
        --cluster-node-timeout 5000 \
        --appendonly no \
        --save '' \
        --dir "$dir" \
        >"$dir/redis.log" 2>&1 &
    echo "started redis-server on $port (pid $!)"
done

for i in $(seq 0 $((NODES - 1))); do
    port=$((BASE + i))
    for _ in $(seq 1 50); do
        port_up "$port" && break
        sleep 0.2
    done
    port_up "$port" || { echo "node on $port failed to start; see /tmp/redi-cluster-$port/redis.log" >&2; exit 1; }
done

if "$REDIS_CLI" -p "$BASE" cluster info 2>/dev/null | grep -q 'cluster_state:ok'; then
    echo "cluster already formed"
else
    addrs=()
    for i in $(seq 0 $((NODES - 1))); do addrs+=("127.0.0.1:$((BASE + i))"); done
    "$REDIS_CLI" --cluster create "${addrs[@]}" --cluster-replicas 0 --cluster-yes \
        >/tmp/redi-cluster-create.log 2>&1
fi

"$REDIS_CLI" -p "$BASE" cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_known_nodes'
echo "cluster ready on ports $BASE-$((BASE + NODES - 1))"
