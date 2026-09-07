#!/usr/bin/env bash
#
# M5 evidence script. Runs a real 3-node cluster as
# three separate OS processes (cmd/server) talking real gRPC over
# loopback TCP — not simnet, not go test's in-process cluster — and
# demonstrates, with a real transcript:
#
#   A. Killing the leader mid-write: a client write loop keeps running
#      while the leader process is SIGKILLed, and the cluster elects a
#      new leader and keeps accepting writes.
#   B. Isolating one node: cmd/server's SIGUSR1 toggle (see
#      transport.Server.SetPartitioned) cuts one follower off from every
#      peer in both directions, without touching the OS network — no
#      root/iptables/toxiproxy needed, see doc/DECISIONS.md's M5 section
#      for why this is an honest substitute. The isolated node must never
#      report itself LEADER (1 vote out of 3 is not a majority) while the
#      remaining two-node majority keeps serving writes throughout.
#
# Usage: scripts/fault_tolerance_demo.sh [output-dir]
#   Everything this run produces (binaries, node data dirs, node logs,
#   the write-loop log) goes under output-dir (default: tmp/faulttolerance,
#   already gitignored the same way transport's own tests use tmp/).
#
# Run from the repo root: scripts/fault_tolerance_demo.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${1:-tmp/faulttolerance}"
rm -rf "$OUT"
mkdir -p "$OUT/bin"

log() { printf '[%s] %s\n' "$(date '+%H:%M:%S.%3N')" "$*"; }
fail() { log "FAIL: $*"; exit 1; }

log "building cmd/server, cmd/client, cmd/clusterstatus into $OUT/bin"
go build -o "$OUT/bin/server" ./cmd/server
go build -o "$OUT/bin/client" ./cmd/client
go build -o "$OUT/bin/clusterstatus" ./cmd/clusterstatus

PIDS=()

# stop_cluster kills every tracked server PID and clears PIDS, so each
# scenario starts from a clean slate.
stop_cluster() {
	for pid in "${PIDS[@]:-}"; do
		[ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
	done
	for pid in "${PIDS[@]:-}"; do
		[ -n "$pid" ] && wait "$pid" 2>/dev/null || true
	done
	PIDS=()
}
trap stop_cluster EXIT

# start_cluster datadir_prefix starts one cmd/server per address in PEERS
# (global, comma-separated), logging each to its own file, and fills PIDS
# index-aligned with PEERS.
start_cluster() {
	local datadir_prefix="$1"
	local addrs
	IFS=',' read -ra addrs <<<"$PEERS"
	for i in "${!addrs[@]}"; do
		"$OUT/bin/server" -peers="$PEERS" -me="$i" -dir="$OUT/${datadir_prefix}-$i" \
			>"$OUT/${datadir_prefix}-node$i.log" 2>&1 &
		PIDS[$i]=$!
	done
}

status() { "$OUT/bin/clusterstatus" -peers="$PEERS" -timeout=300ms; }

# leader_index echoes the node index currently reporting LEADER, or ""
# if none does right now.
leader_index() {
	status | grep -oE 'node[0-9]+=[^ ]*:LEADER' | grep -oE '^node[0-9]+' | grep -oE '[0-9]+' || true
}

wait_for_leader() {
	local budget="$1" waited=0
	while [ "$waited" -lt "$budget" ]; do
		local li
		li=$(leader_index)
		if [ -n "$li" ]; then
			echo "$li"
			return 0
		fi
		sleep 0.2
		waited=$((waited + 1))
	done
	return 1
}

echo
echo "==================== Scenario A: kill leader mid-write ===================="
PEERS="localhost:9201,localhost:9202,localhost:9203"
start_cluster nodeA
log "started 3-node cluster: $PEERS (pids: ${PIDS[*]})"

li=$(wait_for_leader 25) || fail "no leader elected within 5s of startup"
log "initial leader: node$li (pid ${PIDS[$li]})"
log "status: $(status)"

WRITES_LOG="$OUT/writes.log"
: >"$WRITES_LOG"
(
	i=0
	while :; do
		i=$((i + 1))
		ts=$(date '+%H:%M:%S.%3N')
		if "$OUT/bin/client" -peers="$PEERS" -timeout=1s put "midwrite-$i" "v$i" >/dev/null 2>&1; then
			echo "$ts OK   midwrite-$i"
		else
			echo "$ts FAIL midwrite-$i"
		fi
		sleep 0.1
	done
) >>"$WRITES_LOG" &
WRITE_LOOP_PID=$!

sleep 1.2
before_kill_ok=$(grep -c ' OK ' "$WRITES_LOG" || true)
log "write loop has $before_kill_ok successful writes before the kill"
[ "$before_kill_ok" -ge 3 ] || fail "write loop made too little progress before kill ($before_kill_ok writes) — increase warmup"

killed_leader="$li"
killed_pid="${PIDS[$killed_leader]}"
log "SIGKILL leader node$killed_leader (pid $killed_pid) — mid-write, write loop keeps running"
kill -9 "$killed_pid"
unset 'PIDS[killed_leader]'

new_li=""
for _ in $(seq 1 25); do
	sleep 0.2
	cand=$(leader_index)
	if [ -n "$cand" ] && [ "$cand" != "$killed_leader" ]; then
		new_li="$cand"
		break
	fi
done
[ -n "$new_li" ] || fail "no new leader elected within 5s of killing node$killed_leader"
log "new leader after kill: node$new_li"
log "status: $(status)"

sleep 1.5
after_kill_ok=$(grep -c ' OK ' "$WRITES_LOG" || true)
kill -9 "$WRITE_LOOP_PID" 2>/dev/null || true
wait "$WRITE_LOOP_PID" 2>/dev/null || true
log "write loop has $after_kill_ok successful writes after the kill (was $before_kill_ok before)"
[ "$after_kill_ok" -gt "$before_kill_ok" ] || fail "write loop made no further progress after the leader kill"
log "PASS: cluster elected node$new_li and kept accepting writes after killing leader node$killed_leader"
log "full write-loop transcript: $WRITES_LOG"

stop_cluster

echo
echo "==================== Scenario B: isolate one node ===================="
PEERS="localhost:9211,localhost:9212,localhost:9213"
start_cluster nodeB
log "started fresh 3-node cluster: $PEERS (pids: ${PIDS[*]})"

li=$(wait_for_leader 25) || fail "no leader elected within 5s of startup"
log "initial leader: node$li"

follower=""
for i in 0 1 2; do
	if [ "$i" != "$li" ]; then
		follower="$i"
		break
	fi
done
follower_pid="${PIDS[$follower]}"
log "isolating follower node$follower (pid $follower_pid) via SIGUSR1 (transport.Server.SetPartitioned(true))"
kill -USR1 "$follower_pid"
sleep 0.3

log "polling every 200ms for 3s: node$follower must never report LEADER, majority must keep accepting writes"
saw_minority_as_leader=0
for n in $(seq 1 15); do
	line=$(status)
	log "  $line"
	if echo "$line" | grep -q "node${follower}=[^ ]*:LEADER"; then
		saw_minority_as_leader=1
	fi
	if ! "$OUT/bin/client" -peers="$PEERS" -timeout=1s put "duringpartition-$n" "v$n" >/dev/null 2>&1; then
		fail "majority side rejected a write while node$follower was isolated (attempt $n)"
	fi
	sleep 0.2
done
[ "$saw_minority_as_leader" -eq 0 ] || fail "isolated node$follower declared itself LEADER — 1-of-3 minority must never win an election"
log "PASS: node$follower (1-of-3 minority) never became leader; majority served every write while it was isolated"

log "healing partition on node$follower via SIGUSR1 again"
kill -USR1 "$follower_pid"
sleep 1.0
log "status after healing: $(status)"

"$OUT/bin/client" -peers="$PEERS" -timeout=5s put "afterheal" "ok" >/dev/null || fail "Put failed after healing the partition"
value=$("$OUT/bin/client" -peers="$PEERS" -timeout=5s get "afterheal")
log "Get(afterheal) after healing = $value"
[ "$value" = '"ok"' ] || fail "unexpected value after healing: $value"
log "PASS: cluster healthy again after healing the partition on node$follower"

stop_cluster
trap - EXIT

echo
log "M5 demo complete: both scenarios PASSED. Evidence in $OUT/ (node logs, $WRITES_LOG)."
