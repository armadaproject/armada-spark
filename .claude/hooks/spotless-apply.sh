#!/bin/bash
# Auto-format Scala files after Edit/Write using Spotless
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Only run for build-relevant files
if [[ "$FILE_PATH" != *.scala && "$FILE_PATH" != *.java && "$FILE_PATH" != */pom.xml ]]; then
  exit 0
fi

# Debounce: skip if Spotless ran within the last 30 seconds
STAMP_FILE="/tmp/.claude-spotless-last-run"
if [ -f "$STAMP_FILE" ]; then
  LAST_RUN=$(cat "$STAMP_FILE")
  NOW=$(date +%s)
  ELAPSED=$((NOW - LAST_RUN))
  if [ "$ELAPSED" -lt 30 ]; then
    exit 0
  fi
fi

date +%s > "$STAMP_FILE"

RESULT=$(cd "$CLAUDE_PROJECT_DIR" && mvn spotless:apply -q 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "{\"systemMessage\": \"Spotless formatting applied successfully\"}"
else
  echo "{\"systemMessage\": \"Spotless formatting failed: $RESULT\"}"
fi
