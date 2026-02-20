Build the project.

If argument is "quick" or "fast", skip tests:
```
mvn clean package -DskipTests
```

Otherwise run the full build with tests:
```
mvn clean package
```

Rules:
1. Before building, run `mvn spotless:check` first — if formatting fails, run `mvn spotless:apply` and report which files were fixed
2. If the build fails, read the error output and provide a clear summary of what went wrong
3. Do NOT automatically fix build errors — report them and let the user decide
4. Show the final artifact path on success (target/*.jar)
