#!/bin/bash
# Auto-format Scala files after Edit/Write using Spotless
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Only run for Scala source files
if [[ "$FILE_PATH" != *.scala ]]; then
  exit 0
fi

RESULT=$(cd "$CLAUDE_PROJECT_DIR" && mvn spotless:apply -q 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "{\"systemMessage\": \"Spotless formatting applied successfully\"}"
else
  echo "{\"systemMessage\": \"Spotless formatting failed: $RESULT\"}"
fi
