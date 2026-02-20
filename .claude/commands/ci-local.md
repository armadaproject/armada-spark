Run the full CI pipeline locally to verify everything passes before pushing.

This mirrors what GitHub Actions runs. Execute these steps in order, stopping on first failure:

1. **Lint**: `mvn spotless:check`
   - If it fails, ask the user if they want to auto-fix with `mvn spotless:apply`

2. **Compile**: `mvn compile`
   - Report any compilation errors clearly

3. **Test**: `mvn test`
   - Summarize test results (total, passed, failed, skipped)

4. **Package**: `mvn package -DskipTests`
   - Confirm the JAR was built successfully

Report a final summary:
```
CI Local Results:
  Lint:    PASS/FAIL
  Compile: PASS/FAIL
  Test:    PASS/FAIL (X passed, Y failed)
  Package: PASS/FAIL
```

Do NOT continue to the next step if any step fails — report the failure and stop.
