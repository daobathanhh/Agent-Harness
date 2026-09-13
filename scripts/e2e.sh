#!/usr/bin/env bash

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
DB=$(mktemp /tmp/harness-e2e-XXXXXX.db)
PORT=$((19090 + RANDOM % 1000))
PASS=0
FAIL=0
SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f "$DB" "${DB}-wal" "${DB}-shm"
}
trap cleanup EXIT

stop_server() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=""
        sleep 1
    fi
}

start_server() {
    local mcp_args="${1:-}"
    local extra_flags="${2:-}"
    stop_server
    rm -f "$DB" "${DB}-wal" "${DB}-shm"
    launch_server "$mcp_args" "$extra_flags"
}

restart_server() {
    local mcp_args="${1:-}"
    local extra_flags="${2:-}"
    stop_server
    launch_server "$mcp_args" "$extra_flags"
}

launch_server() {
    local mcp_args="${1:-}"
    local extra_flags="${2:-}"
    local -a cmd=("$BIN/harness" "--port=$PORT" "--db=$DB" "--mcp-cmd=$BIN/mockmcp" "--provider=mock")
    if [[ -n "$mcp_args" ]]; then
        cmd+=("--mcp-args=$mcp_args")
    fi
    if [[ -n "$extra_flags" ]]; then
        cmd+=("$extra_flags")
    fi
    "${cmd[@]}" &>/dev/null &
    SERVER_PID=$!
    sleep 1
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        fail "Server failed to start"
        return 1
    fi
}

log()  { printf "\033[1;34m==> %s\033[0m\n" "$*"; }
pass() { printf "\033[1;32m  PASS: %s\033[0m\n" "$*"; PASS=$((PASS+1)); }
fail() { printf "\033[1;31m  FAIL: %s\033[0m\n" "$*"; FAIL=$((FAIL+1)); }

assert_contains() {
    local output="$1" pattern="$2" label="$3"
    if echo "$output" | grep -qE "$pattern"; then
        pass "$label"
    else
        fail "$label (expected '$pattern')"
        echo "    got: $(echo "$output" | head -3)"
    fi
}

assert_status() {
    local run_id="$1" expected="$2" label="$3"
    local out
    out=$("$BIN/harnessctl" run "$run_id" 2>&1) || true
    if echo "$out" | grep -qE "Status: +$expected"; then
        pass "$label"
    else
        fail "$label (expected status $expected)"
        echo "    got: $(echo "$out" | grep Status)"
    fi
}

wait_run_done() {
    local run_id="$1" max="${2:-15}"
    local i
    for i in $(seq 1 "$max"); do
        local out
        out=$("$BIN/harnessctl" run "$run_id" 2>&1) || true
        if echo "$out" | grep -qE "Status: +(succeeded|failed|cancelled|interrupted)"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

new_session() {
    "$BIN/harnessctl" session new 2>&1 | sed -n 's/.*Session created: \(\S\+\).*/\1/p'
}

send_msg() {
    local sid="$1" msg="$2"
    "$BIN/harnessctl" send "$sid" "$msg" 2>&1 | sed -n 's/.*Run started: \(\S\+\).*/\1/p'
}

# ── Build ──
log "Building binaries"
if ! (cd "$ROOT" && go build -o "$BIN/harness" ./cmd/harness && go build -o "$BIN/harnessctl" ./cmd/harnessctl && go build -o "$BIN/mockmcp" ./cmd/mockmcp); then
    fail "Build failed"
    exit 1
fi
pass "Build succeeded"

# ── Start server ──
log "Starting harness server (port $PORT, mock provider, 2s latency)"
export HARNESS_ADDR="localhost:$PORT"
start_server "" "--mock-latency=2s"
pass "Server started (pid $SERVER_PID)"

# ── Phase 1: Happy path ──
log "Phase 1: Happy path (session -> send -> watch -> done)"

SID=$(new_session)
pass "Create session"

RUN_ID=$(send_msg "$SID" "Check machine CNC-001 and create a work order if needed")
pass "Send message"

if wait_run_done "$RUN_ID" 20; then
    assert_status "$RUN_ID" "succeeded" "Run succeeded"
else
    fail "Run did not finish in time"
fi

WATCH_FILE=$(mktemp)
timeout 5 "$BIN/harnessctl" watch "$SID" --from-seq 0 > "$WATCH_FILE" 2>&1 || true
WATCH_OUT=$(cat "$WATCH_FILE")
rm -f "$WATCH_FILE"

assert_contains "$WATCH_OUT" "run_started" "Events contain run_started"
assert_contains "$WATCH_OUT" "tool_call_started" "Events contain tool_call_started"
assert_contains "$WATCH_OUT" "tool_call_succeeded" "Events contain tool_call_succeeded"
assert_contains "$WATCH_OUT" "run_succeeded" "Events contain run_succeeded"

# ── Phase 2: Cancel mid-run ──
log "Phase 2: Cancel a running run"

SID2=$(new_session)
RUN2_ID=$(send_msg "$SID2" "Check machine CNC-001")

sleep 1
OUT=$("$BIN/harnessctl" cancel "$RUN2_ID" 2>&1) || true
assert_contains "$OUT" "Cancel requested" "Cancel accepted"

if wait_run_done "$RUN2_ID" 10; then
    assert_status "$RUN2_ID" "cancelled" "Run cancelled"
else
    fail "Cancelled run did not terminate"
fi

# ── Phase 3: Concurrent run rejection ──
log "Phase 3: Concurrent run rejection"

SID3=$(new_session)
RUN3_ID=$(send_msg "$SID3" "Check machine CNC-001")

sleep 1
OUT=$(timeout 5 "$BIN/harnessctl" send "$SID3" "This should be rejected" 2>&1) || true
assert_contains "$OUT" "already has a running run|AlreadyExists" "Concurrent run rejected"

"$BIN/harnessctl" cancel "$RUN3_ID" &>/dev/null || true
wait_run_done "$RUN3_ID" 10 || true

# ── Phase 4: Session close + rejection ──
log "Phase 4: Close session, then reject send"

SID4=$(new_session)
OUT=$("$BIN/harnessctl" close "$SID4" 2>&1) || true
assert_contains "$OUT" "Session closed" "Session closed"

OUT=$("$BIN/harnessctl" send "$SID4" "Should fail" 2>&1) || true
assert_contains "$OUT" "closed|FailedPrecondition" "Send to closed session rejected"

# ── Phase 5: Empty message rejection ──
log "Phase 5: Empty message"

SID5=$(new_session)
OUT=$("$BIN/harnessctl" send "$SID5" "" 2>&1) || true
assert_contains "$OUT" "empty|InvalidArgument" "Empty message rejected"

# ── Phase 6: Multi-turn ──
log "Phase 6: Multi-turn history"

SID6=$(new_session)

MT_RUN1=$(send_msg "$SID6" "Check machine CNC-001")
wait_run_done "$MT_RUN1" 20 || true
assert_status "$MT_RUN1" "succeeded" "Multi-turn run 1 succeeded"

MT_RUN2=$(send_msg "$SID6" "Now create a work order for it")
wait_run_done "$MT_RUN2" 20 || true
assert_status "$MT_RUN2" "succeeded" "Multi-turn run 2 succeeded"

# ── Phase 7: Tool failure (isError from MCP) ──
log "Phase 7: Tool returns isError, model handles gracefully"

start_server "--fail-rate,1.0" "--mock-latency=0s"
pass "Server restarted with fail-rate=1.0"

SID7=$(new_session)
RUN7=$(send_msg "$SID7" "Check machine CNC-001")
if wait_run_done "$RUN7" 20; then
    assert_status "$RUN7" "succeeded" "Run succeeded despite tool errors"
else
    fail "Run did not finish (tool failure phase)"
fi

WATCH7=$(mktemp)
timeout 5 "$BIN/harnessctl" watch "$SID7" --from-seq 0 > "$WATCH7" 2>&1 || true
W7OUT=$(cat "$WATCH7")
rm -f "$WATCH7"
assert_contains "$W7OUT" "tool_call_succeeded" "Tool call event emitted (isError handled)"

# ── Phase 8: MCP server crash + auto-reconnect ──
log "Phase 8: MCP server crashes mid-run, harness reconnects"

start_server "--drop-after-n-calls,1" "--mock-latency=0s"
pass "Server restarted with drop-after-n-calls=1"

SID8=$(new_session)
RUN8=$(send_msg "$SID8" "Check machine CNC-001 and create a work order if needed")
if wait_run_done "$RUN8" 30; then
    assert_status "$RUN8" "succeeded" "Run succeeded after MCP reconnect"
else
    fail "Run did not finish (MCP reconnect phase)"
fi

# ── Phase 9: Crash recovery (kill -9 mid-run, restart, assert interrupted + event) ──
log "Phase 9: Crash recovery (kill -9 mid-run, restart, verify interrupted)"

start_server "" "--mock-latency=5s"
pass "Server started for crash recovery (5s latency)"

SID9=$(new_session)
RUN9=$(send_msg "$SID9" "Check machine CNC-001")
sleep 1

kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
pass "Server killed with SIGKILL mid-run"

restart_server "" "--mock-latency=0s"
pass "Server restarted (same DB, 0s latency)"

if wait_run_done "$RUN9" 10; then
    assert_status "$RUN9" "interrupted" "Crashed run marked interrupted on restart"
else
    fail "Crashed run not resolved after restart"
fi

WATCH9=$(mktemp)
timeout 5 "$BIN/harnessctl" watch "$SID9" --from-seq 0 > "$WATCH9" 2>&1 || true
W9OUT=$(cat "$WATCH9")
rm -f "$WATCH9"
assert_contains "$W9OUT" "run_interrupted" "run_interrupted event emitted after crash recovery"

RUN9B=$(send_msg "$SID9" "Create work order WO-999")
if wait_run_done "$RUN9B" 20; then
    assert_status "$RUN9B" "succeeded" "New run succeeds after crash recovery"
else
    fail "New run did not finish after crash recovery"
fi

# ── Summary ──
echo ""
log "Results: $PASS passed, $FAIL failed"
if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
