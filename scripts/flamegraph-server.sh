#!/bin/bash
# Profiles the server under a redis-benchmark load and writes a flamegraph.
#
#   ./scripts/flamegraph-server.sh [output.svg] [seconds] [write|read]
#
# Needs redis-benchmark on the PATH (brew install redis), a release build of the
# workspace (cargo build --release) and the profiling tools:
#
#   cargo install inferno rustfilt
#
# The load runs longer than the sampling window and is killed with the server,
# so this script reports no throughput: ./scripts/bench-server.sh is where
# numbers come from. It does report the engine counters, which is what says the
# profile is of the path it claims.
#
# Both loads run under SyncPolicy::Interval, where the device flush is out of
# the way and what remains is the store's own work.
#
# The read load first writes a key space several times larger than the memtable
# and waits for it to settle, so the lookups it then profiles reach the files
# rather than the table the keys were just written to.

set -u

OUT=${1:-docs/flamegraph-write-path.svg}
SECONDS_SAMPLED=${2:-5}
LOAD=${3:-write}
# Enough that the load outlives the sampling window at any throughput this
# server has reached.
REQUESTS=20000000
PIPELINE=16
CLIENTS=50
PORT=6393
# Small enough that the read load's key space does not fit, so it flushes.
MEMTABLE_BYTES=1048576
KEYSPACE=200000
POPULATE=400000
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=$ROOT/target/release/lsmkv-server
WORK=$(mktemp -d)
SERVER_PID=
LOAD_PID=

case "$LOAD" in
  write|read) ;;
  *) echo "the load is write or read, not $LOAD"; exit 1 ;;
esac

for tool in redis-benchmark redis-cli inferno-collapse-sample inferno-flamegraph rustfilt; do
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

"$BIN" --dir "$WORK/kv" --port $PORT --sync 10 --memtable-bytes $MEMTABLE_BYTES \
  > /dev/null 2>&1 &
SERVER_PID=$!
sleep 1
kill -0 "$SERVER_PID" 2> /dev/null || { echo "the server did not start"; exit 1; }

counter() {
  redis-cli -p $PORT info 2> /dev/null | tr -d '\r' | grep "^$1:" | cut -d: -f2
}

if [ "$LOAD" = read ]; then
  echo "populating $KEYSPACE keys"
  redis-benchmark -p $PORT -t set -n $POPULATE -c $CLIENTS -P $PIPELINE \
    -r $KEYSPACE -q > /dev/null 2>&1
  # Compaction is background work, and profiling it here would be profiling the
  # write path again under another name.
  sleep 5
  echo "populated: $(counter files) files across levels $(counter files_per_level)"
  before_reads=$(counter block_reads)
  redis-benchmark -p $PORT -t get -n $REQUESTS -c $CLIENTS -P $PIPELINE \
    -r $KEYSPACE -q > /dev/null 2>&1 &
else
  redis-benchmark -p $PORT -t set -n $REQUESTS -c $CLIENTS -P $PIPELINE -q \
    > /dev/null 2>&1 &
fi
LOAD_PID=$!
# Long enough for the connections to be up and the load to reach its rate, so
# the profile is the steady state and not the ramp.
sleep 2

command=$([ "$LOAD" = read ] && echo GET || echo SET)
echo "sampling for ${SECONDS_SAMPLED}s under $command at -P $PIPELINE"
sample "$SERVER_PID" "$SECONDS_SAMPLED" -f "$WORK/stacks.txt" > /dev/null || {
  echo "the sampler failed"
  exit 1
}

if [ "$LOAD" = read ]; then
  echo "blocks read while sampling: $(( $(counter block_reads) - before_reads ))"
fi

# sample(1) reports a call tree; inferno folds it, rustfilt turns the v0 mangled
# Rust symbols back into paths, inferno renders the SVG.
inferno-collapse-sample "$WORK/stacks.txt" \
  | rustfilt \
  | inferno-flamegraph \
      --title "lsm-kv-rs $LOAD path" \
      --subtitle "redis-benchmark -t $LOAD -c $CLIENTS -P $PIPELINE, interval 10 ms" \
      > "$OUT" || { echo "rendering the flamegraph failed"; exit 1; }

# An empty profile still renders a valid SVG, so the frame count is what says
# whether anything was actually sampled.
frames=$(grep -o '<rect' "$OUT" | wc -l | tr -d ' ')
[ "$frames" -gt 1 ] || { echo "the flamegraph holds $frames frames, nothing was sampled"; exit 1; }

echo "wrote $OUT, $frames frames"
