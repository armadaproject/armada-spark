Check and fix code formatting using Spotless/Scalafmt.

Steps:
1. Run `mvn spotless:check` to see if there are formatting violations
2. If violations are found, run `mvn spotless:apply` to auto-fix them
3. After applying, run `git diff --stat` to show what files were reformatted
4. Summarize the changes (which files, what kind of formatting was fixed)

If no violations are found, say so and stop.

Do NOT commit the formatting changes — just apply and report.
