#!/bin/bash
# Profiles the server under a redis-benchmark write load and writes a flamegraph.
#
#   ./scripts/flamegraph-server.sh [output.svg] [seconds]
#
# Needs redis-benchmark on the PATH (brew install redis), a release build of the
# workspace (cargo build --release) and the profiling tools:
#
#   cargo install inferno rustfilt
#
# The load runs longer than the sampling window and is killed with the server,
# so this script reports no throughput: ./scripts/bench-server.sh is where
# numbers come from.
#
# The server is profiled under SyncPolicy::Interval, where the device flush is
# out of the way and what remains in the profile is the store's own write path.

set -u

OUT=${1:-docs/flamegraph-write-path.svg}
SECONDS_SAMPLED=${2:-5}
# Enough that the load outlives the sampling window at any throughput this
# server has reached.
REQUESTS=20000000
PIPELINE=16
CLIENTS=50
PORT=6393
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=$ROOT/target/release/lsmkv-server
WORK=$(mktemp -d)
SERVER_PID=
LOAD_PID=

for tool in redis-benchmark inferno-collapse-sample inferno-flamegraph rustfilt; do
  command -v $tool > /dev/null || { echo "$tool is not on the PATH"; exit 1; }
done
[ -x "$BIN" ] || { echo "build first: cargo build --release"; exit 1; }

cleanup() {
  [ -n "$LOAD_PID" ] && kill "$LOAD_PID" 2> /dev/null
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2> /dev/null
  wait 2> /dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

case "$OUT" in
  /*) ;;
  *) OUT=$ROOT/$OUT ;;
esac
mkdir -p "$(dirname "$OUT")"

"$BIN" --dir "$WORK/kv" --port $PORT --sync 10 > /dev/null 2>&1 &
SERVER_PID=$!
sleep 1
kill -0 "$SERVER_PID" 2> /dev/null || { echo "the server did not start"; exit 1; }

redis-benchmark -p $PORT -t set -n $REQUESTS -c $CLIENTS -P $PIPELINE -q > /dev/null 2>&1 &
LOAD_PID=$!
# Long enough for the connections to be up and the load to reach its rate, so
# the profile is the steady state and not the ramp.
sleep 2

echo "sampling for ${SECONDS_SAMPLED}s under SET at -P $PIPELINE"
sample "$SERVER_PID" "$SECONDS_SAMPLED" -f "$WORK/stacks.txt" > /dev/null || {
  echo "the sampler failed"
  exit 1
}

# sample(1) reports a call tree; inferno folds it, rustfilt turns the v0 mangled
# Rust symbols back into paths, inferno renders the SVG.
inferno-collapse-sample "$WORK/stacks.txt" \
  | rustfilt \
  | inferno-flamegraph \
      --title "lsm-kv-rs write path" \
      --subtitle "redis-benchmark -t set -c $CLIENTS -P $PIPELINE, interval 10 ms" \
      > "$OUT" || { echo "rendering the flamegraph failed"; exit 1; }

# An empty profile still renders a valid SVG, so the frame count is what says
# whether anything was actually sampled.
frames=$(grep -o '<rect' "$OUT" | wc -l | tr -d ' ')
[ "$frames" -gt 1 ] || { echo "the flamegraph holds $frames frames, nothing was sampled"; exit 1; }

echo "wrote $OUT, $frames frames"
