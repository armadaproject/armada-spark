#!/bin/bash

set -uo pipefail

TESTS_DIR="$(cd "$(dirname "$0")"; pwd)"

# Optional first argument selects what to run: a suite name (db), a category
# (integration), or a category/suite path (integration/db). Omit it to run
# everything. Any remaining arguments are forwarded to each suite, and on to
# submitArmadaSpark.sh.
SELECT=""
if [ "$#" -gt 0 ] && [ "${1#-}" = "$1" ]; then
    SELECT="$1"
    shift
fi

# matches CATEGORY SUITE -> true if SELECT is empty or names this suite.
matches() {
    [ -z "$SELECT" ] && return 0
    case "$SELECT" in
        "$2" | "$1" | "$1/$2") return 0 ;;
        *) return 1 ;;
    esac
}

overall_rc=0
ran=0

# Suites live at <category>/<suite>/<suite>_test.sh.
for script in "$TESTS_DIR"/*/*/*_test.sh; do
    [ -e "$script" ] || continue
    suite_dir="$(dirname "$script")"
    suite="$(basename "$suite_dir")"
    category="$(basename "$(dirname "$suite_dir")")"

    matches "$category" "$suite" || continue

    if [ ! -x "$script" ]; then
        echo "error: $category/$suite: $script is present but not executable"
        overall_rc=1
        continue
    fi

    ran=$((ran + 1))
    echo
    echo "########################################################"
    echo "# suite: $category/$suite"
    echo "########################################################"
    "$script" "$@" || overall_rc=1
done

echo
if [ "$ran" -eq 0 ]; then
    echo "NO SUITES RAN (selector: '${SELECT}')"
    exit 1
fi
if [ "$overall_rc" -eq 0 ]; then
    echo "ALL SUITES PASSED ($ran run)"
else
    echo "ONE OR MORE SUITES FAILED"
fi
exit "$overall_rc"
