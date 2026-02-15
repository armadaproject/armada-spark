---
paths:
  - "src/main/scala-spark-*/**"
---

# Version-Specific SparkSubmit Rules

Three versions exist: 3.3, 3.5, 4.1. All override `prepareSubmitEnvironment()`.
**Any change to submission logic must be applied to ALL three versions.**

## Critical Differences

| Concern | Spark 3.3 | Spark 3.5 | Spark 4.1 |
|---------|-----------|-----------|-----------|
| Master arg | `args.master` (String) | `args.maybeMaster` (Option) | `args.maybeMaster` (Option) |
| Mesos | Included in CM enum | Removed | Removed |
| Remote/connect | N/A | `maybeRemote` + `spark.remote` | `maybeRemote` + `spark.remote` |
| Jar downloads | Simple download+unpack | `downloadResourcesToCurrentDirectory()` helper | Selective download with `avoidDownload` callback |
| Logging | Plain string (`logInfo`) | Plain string (`logInfo`) | Structured logging (`log"..."`, `MDC`, `LogKeys`) |
| ChildArgs | `childArgs += (res, cls)` tuple | `childArgs += res += cls` chained | `childArgs += res += cls` chained |
| ProxyUser cleanup | None | `FileSystem.closeAllForUGI` in finally | `FileSystem.closeAllForUGI` in finally |

## Checklist When Editing

1. Identify which version you're editing
2. Determine if the change is version-specific or shared logic
3. If shared: apply the equivalent change to all 3 files, adapting syntax per version
4. Build-helper only compiles ONE version per build (`spark.binary.version`), so compile errors in other versions won't show until CI matrix runs
