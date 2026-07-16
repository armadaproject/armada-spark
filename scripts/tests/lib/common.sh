#!/bin/bash

set -uo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)"
TESTS_DIR="$(cd "$LIB_DIR/.."; pwd)"
SCRIPTS_DIR="$(cd "$TESTS_DIR/.."; pwd)"
SUBMIT="$SCRIPTS_DIR/submitArmadaSpark.sh"

if [ -e "$SCRIPTS_DIR/config.sh" ]; then
    source "$SCRIPTS_DIR/config.sh" || true
    set +e
fi

# GNU coreutils `timeout`, or `gtimeout` on macOS (brew install coreutils). If
# neither is present, timed commands run unguarded and their hang-detection
# (rc=124) branches never fire.
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_BIN="gtimeout"
else
    TIMEOUT_BIN=""
fi

TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0
FAILED_NAMES=()

if [ -t 1 ]; then
    C_RESET=$'\033[0m'; C_GREEN=$'\033[32m'; C_RED=$'\033[31m'
    C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'; C_BOLD=$'\033[1m'
else
    C_RESET=""; C_GREEN=""; C_RED=""; C_YELLOW=""; C_BLUE=""; C_BOLD=""
fi

log()  { echo "${C_BLUE}[test]${C_RESET} $*"; }
pass() { TESTS_PASSED=$((TESTS_PASSED + 1)); echo "${C_GREEN}  PASS${C_RESET} $*"; }
skip() { TESTS_SKIPPED=$((TESTS_SKIPPED + 1)); echo "${C_YELLOW}  SKIP${C_RESET} $*"; }
fail() {
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_NAMES+=("${1:-unnamed}")
    echo "${C_RED}  FAIL${C_RESET} $*"
}

start_case() { echo; echo "${C_BOLD}>>> $*${C_RESET}"; }

run_submit() {
    LAST_OUTPUT="$(mktemp)"
    local timeout_secs="${SUBMIT_TIMEOUT:-900}"
    log "submit: $* (timeout ${timeout_secs}s)"
    if [ -n "$TIMEOUT_BIN" ]; then
        "$TIMEOUT_BIN" "${timeout_secs}" "$SUBMIT" "$@" >"$LAST_OUTPUT" 2>&1
    else
        "$SUBMIT" "$@" >"$LAST_OUTPUT" 2>&1
    fi
    LAST_RC=$?
    return $LAST_RC
}

output_matches() {
    grep -Eiq "$1" "$LAST_OUTPUT"
}

driver_succeeded() {
    [ "${LAST_RC:-1}" -eq 0 ] && output_matches "Driver job .* SUCCEEDED"
}

is_cluster_mode() {
    local prev="" a
    for a in "$@"; do
        if [ "$prev" = "-M" ] || [ "$prev" = "--mode" ]; then
            [ "$a" = "cluster" ] && return 0 || return 1
        fi
        prev="$a"
    done
    # No -M/--mode flag: fall back to DEPLOY_MODE (default cluster), matching how
    # submitArmadaSpark.sh resolves the mode. Without this, a DEPLOY_MODE=client
    # run with no flag would be treated as cluster and assert on the wrong
    # sentinel, producing a false FAIL.
    [ "${DEPLOY_MODE:-cluster}" = "cluster" ]
}

dump_tail() {
    echo "${C_YELLOW}    --- last ${1:-20} lines of submit output ---${C_RESET}"
    tail -n "${1:-20}" "$LAST_OUTPUT" | sed 's/^/    /'
}

require_infra() {
    local name="$1"; shift
    local missing=() var
    for var in "$@"; do
        [ -n "${!var:-}" ] || missing+=("$var")
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        skip "$name (no reachable $name configured; set ${missing[*]})"
        return 0
    fi
    return 1
}

print_summary() {
    echo
    echo "${C_BOLD}================ summary ================${C_RESET}"
    echo "  ${C_GREEN}passed:  $TESTS_PASSED${C_RESET}"
    echo "  ${C_RED}failed:  $TESTS_FAILED${C_RESET}"
    echo "  ${C_YELLOW}skipped: $TESTS_SKIPPED${C_RESET}"
    if [ "$TESTS_FAILED" -gt 0 ]; then
        echo "  failed cases: ${FAILED_NAMES[*]}"
        return 1
    fi
    return 0
}
