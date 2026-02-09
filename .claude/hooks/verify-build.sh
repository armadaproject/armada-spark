#!/bin/bash
# Verify the project compiles before Claude stops
INPUT=$(cat)
STOP_HOOK_ACTIVE=$(echo "$INPUT" | jq -r '.stop_hook_active // false')

# Prevent infinite loops - skip if already triggered by a Stop hook
if [ "$STOP_HOOK_ACTIVE" = "true" ]; then
  echo '{"systemMessage": "Skipped build verification (stop hook already active)"}'
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"
RESULT=$(mvn compile -q 2>&1)
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  ESCAPED_RESULT=$(echo "$RESULT" | head -20 | jq -Rs .)
  echo "{\"systemMessage\": \"Build failed. Fix compilation errors before finishing: ${ESCAPED_RESULT}\"}" >&2
  exit 2
fi

echo '{"systemMessage": "Build verification passed"}'
exit 0
