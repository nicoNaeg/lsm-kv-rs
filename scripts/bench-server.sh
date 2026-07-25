#!/bin/bash
# Compares this server against Redis, same machine, same tool.
#
#   ./scripts/bench-server.sh [requests] [clients]
#
# Needs redis-benchmark and redis-server on the PATH (brew install redis) and a
# release build of the workspace (cargo build --release).

set -u

REQUESTS=${1:-20000}
CLIENTS=${2:-50}
KV_PORT=6390
# Distinct keys the memtable comparison spreads its writes over.
KEYSPACE=100000
REDIS_PORT=6391
BIN=$(dirname "$0")/../target/release/lsmkv-server
WORK=$(mktemp -d)
SERVER_PID=

for tool in redis-benchmark redis-server; do
  command -v $tool > /dev/null || { echo "$tool is not on the PATH"; exit 1; }
done
[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 1; }

cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2> /dev/null
  wait 2> /dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# Prints the SET and GET rates of the server on $2, labelled $1.
#
# A pipelined run needs more requests to mean anything: 16 commands per round
# trip turns 20 000 requests into 1250 of them.
# A key space of 1 (the redis-benchmark default) is every request hitting the
# same key. That is fine for comparing servers, and misleading for comparing
# memtables: a structure that keeps one version per key never grows, one that
# appends a version per write fills up. The memtable section passes a real key
# space for that reason.
measure() {
  local label=$1 port=$2 pipeline=${3:-1} keyspace=${4:-1} rates requests=$REQUESTS
  [ "$pipeline" -gt 1 ] && requests=$((REQUESTS * 10))
  rates=$(redis-benchmark -p "$port" -t set,get -n "$requests" -c "$CLIENTS" \
    -P "$pipeline" -r "$keyspace" -q 2> /dev/null \
    | tr '\r' '\n' | grep 'requests per second' \
    | sed -E 's/.*: ([0-9]+)\.[0-9]+ requests.*/\1/')
  printf '| %-42s | %10s | %10s |\n' "$label" $rates
}

# Starts this server with a fresh data directory and the given sync policy.
start_kv() {
  rm -rf "$WORK/kv"
  "$BIN" --dir "$WORK/kv" --port $KV_PORT --sync "$1" --memtable "${2:-btree}" \
    > /dev/null 2>&1 &
  SERVER_PID=$!
  sleep 1
}

start_redis() {
  rm -rf "$WORK/redis"
  mkdir -p "$WORK/redis"
  redis-server --port $REDIS_PORT --save '' --dir "$WORK/redis" "$@" > /dev/null 2>&1 &
  SERVER_PID=$!
  sleep 1
}

stop() {
  kill "$SERVER_PID" 2> /dev/null
  wait "$SERVER_PID" 2> /dev/null
  SERVER_PID=
}

echo "$REQUESTS requests, $CLIENTS clients, redis $(redis-server --version | sed -E 's/.*v=([^ ]+).*/\1/')"
echo
printf '| %-42s | %10s | %10s |\n' 'server' 'SET/s' 'GET/s'
printf '|%44s|%12s|%12s|\n' "$(printf -- '-%.0s' {1..44})" \
  "$(printf -- '-%.0s' {1..12})" "$(printf -- '-%.0s' {1..12})"

start_redis --appendonly no
measure 'redis, default' $REDIS_PORT
stop

start_redis --appendonly yes --appendfsync always
measure 'redis, appendfsync always' $REDIS_PORT
stop

start_kv 10
measure 'lsm-kv-rs, interval 10 ms' $KV_PORT
stop

start_kv group
measure 'lsm-kv-rs, group commit' $KV_PORT
stop

start_kv always
measure 'lsm-kv-rs, flush per write' $KV_PORT
stop

echo
echo "Pipelined, -P 16, $((REQUESTS * 10)) requests:"
echo
printf '| %-42s | %10s | %10s |\n' 'server' 'SET/s' 'GET/s'
printf '|%44s|%12s|%12s|\n' "$(printf -- '-%.0s' {1..44})" \
  "$(printf -- '-%.0s' {1..12})" "$(printf -- '-%.0s' {1..12})"

start_redis --appendonly no
measure 'redis, default' $REDIS_PORT 16
stop

start_kv 10
measure 'lsm-kv-rs, interval 10 ms' $KV_PORT 16
stop

echo
echo "Memtable, interval 10 ms, $KEYSPACE distinct keys:"
echo
printf '| %-42s | %10s | %10s |\n' 'memtable' 'SET/s' 'GET/s'
printf '|%44s|%12s|%12s|\n' "$(printf -- '-%.0s' {1..44})" \
  "$(printf -- '-%.0s' {1..12})" "$(printf -- '-%.0s' {1..12})"

for kind in btree skiplist; do
  start_kv 10 $kind
  measure "lsm-kv-rs, $kind" $KV_PORT 1 $KEYSPACE
  stop
done

echo
printf '| %-42s | %10s | %10s |\n' 'memtable, pipelined -P 16' 'SET/s' 'GET/s'
printf '|%44s|%12s|%12s|\n' "$(printf -- '-%.0s' {1..44})" \
  "$(printf -- '-%.0s' {1..12})" "$(printf -- '-%.0s' {1..12})"

for kind in btree skiplist; do
  start_kv 10 $kind
  measure "lsm-kv-rs, $kind" $KV_PORT 16 $KEYSPACE
  stop
done
