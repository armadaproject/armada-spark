#!/bin/bash

set -euo pipefail

TESTS_DIR="$(cd "$(dirname "$0")"; pwd)"
ROOT="$(cd "$TESTS_DIR/../.."; pwd)"
DEST="$ROOT/extraFiles/jobs"

mkdir -p "$DEST"

# Each suite keeps its payloads in <category>/<suite>/jobs/. Stage them into a
# per-suite image subdir (extraFiles/jobs/<suite>/) so payloads from different
# suites never collide once flattened onto the image.
staged=0
for jobs_dir in "$TESTS_DIR"/*/*/jobs; do
    [ -d "$jobs_dir" ] || continue
    suite="$(basename "$(dirname "$jobs_dir")")"

    shopt -s nullglob
    payloads=("$jobs_dir"/*.py)
    shopt -u nullglob
    [ "${#payloads[@]}" -gt 0 ] || continue

    mkdir -p "$DEST/$suite"
    cp "${payloads[@]}" "$DEST/$suite"/
    staged=$((staged + 1))
    echo "staged $suite jobs -> extraFiles/jobs/$suite/"
done

if [ "$staged" -eq 0 ]; then
    echo "no suite jobs found under $TESTS_DIR/*/*/jobs/"
    exit 0
fi

echo
echo "now rebuild the image so they land at /opt/spark/extraFiles/jobs/:"
echo "  ./scripts/createImage.sh"
