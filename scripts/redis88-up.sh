#!/usr/bin/env bash
# Local redis:8 (>= 8.8: AR.* / CMS.* / TS.* / HIMPORT / GCRA) on 127.0.0.1:6390
# via docker inside WSL. The externally-managed instance on 6379 is never touched.
#
#   bash scripts/redis88-up.sh
#   REDIS_URL=redis://127.0.0.1:6390 npx vitest run test/live-phase73.test.ts
#   REDIS_URL=redis://127.0.0.1:6390 npm test        # whole suite against 8.10
set -euo pipefail

WL() { env PATH="/c/Windows/System32:/c/Windows:/usr/bin:/bin" MSYS_NO_PATHCONV=1 wsl -d Ubuntu "$@"; }

# docker daemon inside WSL; setsid detaches it so it outlives this script
WL -u root -e sh -c 'pgrep -x dockerd >/dev/null || { rm -f /var/run/docker.pid /var/run/docker.sock; (setsid dockerd >/var/log/dockerd.out 2>&1 </dev/null &); sleep 6; }'

# {} config: public images need no credentials, and the default credsStore
# points at docker-credential-desktop which only exists while Desktop runs
WL bash -c 'mkdir -p /tmp/dcfg && echo "{}" > /tmp/dcfg/config.json
docker --config /tmp/dcfg pull -q redis:8
docker --config /tmp/dcfg rm -f redits88 >/dev/null 2>&1 || true
docker --config /tmp/dcfg run -d --name redits88 -p 6390:6379 redis:8 --save "" --appendonly no >/dev/null
sleep 1
docker --config /tmp/dcfg exec redits88 redis-cli ping' >/dev/null

echo "redis:8 ready at redis://127.0.0.1:6390 ($(REDIS_URL=redis://127.0.0.1:6390 node -e '
const net = require("net");
const s = net.connect(6390, "127.0.0.1");
s.on("connect", () => { console.log("reachable"); s.end(); });
s.on("error", () => console.log("UNREACHABLE from Windows"));
'))'
