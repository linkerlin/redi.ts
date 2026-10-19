#!/usr/bin/env bash
# Stops the 3-master test cluster started by scripts/cluster-up.sh and removes
# its /tmp state. Run from Windows:
#   wsl -e bash /mnt/c/GitHub/redi.ts/scripts/cluster-down.sh
set -u

BASE="${1:-7100}"
REDIS_CLI="${REDIS_CLI:-redis-cli}"

for i in 0 1 2; do
    port=$((BASE + i))
    if "$REDIS_CLI" -p "$port" ping >/dev/null 2>&1; then
        "$REDIS_CLI" -p "$port" shutdown nosave >/dev/null 2>&1 || true
        echo "stopped node on $port"
    fi
    rm -rf "/tmp/redi-cluster-$port"
done
echo "cluster down"
