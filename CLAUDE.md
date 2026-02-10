# CLAUDE.md - armada-spark

## Project Overview

Apache Spark plugin that integrates with [Armada](https://armadaproject.io/), a multi-cluster Kubernetes batch scheduler. Implements Spark's `ExternalClusterManager` SPI to submit and manage Spark jobs via Armada's gRPC API.

**Default development target:** Spark 3.5.5 + Scala 2.13.8 + Java 17. Always build/test against this unless asked to work on a different version.

## Build & Run

```bash
# Build
mvn clean package

# Run tests
mvn test

# Lint check / auto-fix
mvn spotless:check
mvn spotless:apply

# Set Spark/Scala versions (e.g., Spark 3.5.5, Scala 2.13.8)
./scripts/set-version.sh 3 5 5 2 13 8
```

**Stack:** Scala 2.13 | Maven | Spark 3.5 | Java 17 | Fabric8 Kubernetes Client | gRPC/Protobuf (via armada-scala-client)

## Project Structure

```
src/main/scala/org/apache/spark/
├── deploy/armada/              # Configuration & job submission
│   ├── Config.scala            # All spark.armada.* config entries (ConfigBuilder API)
│   ├── DeploymentModeHelper.scala
│   ├── submit/                 # Job submission pipeline
│   │   ├── ArmadaClientApplication.scala   # Main submission logic
│   │   ├── PodSpecConverter.scala          # Fabric8 <-> Protobuf conversion
│   │   ├── PodMerger.scala                 # JSON deep merge for pod specs
│   │   └── ...
│   └── validators/K8sValidator.scala
└── scheduler/cluster/armada/   # Cluster manager & scheduling
    ├── ArmadaClusterManager.scala          # ExternalClusterManager SPI entry point
    ├── ArmadaClusterManagerBackend.scala    # Executor lifecycle management
    ├── ArmadaEventWatcher.scala            # gRPC event stream processing
    └── ArmadaExecutorAllocator.scala       # Dynamic allocation
```

Version-specific sources live in `src/main/scala-spark-{version}/`.

## Architecture

```
User submits Spark job
  └─> ArmadaClusterManager (SPI entry: registers "armada://" master scheme)
        ├─> TaskSchedulerImpl (Spark core task scheduling)
        └─> ArmadaClusterManagerBackend (executor lifecycle, state tracking)
              ├─> ArmadaExecutorAllocator (polls demand vs supply, submits batch jobs)
              │     ├─> KubernetesExecutorBuilder.buildExecutorPod()
              │     ├─> PodSpecConverter.fabric8ToProtobuf()
              │     └─> ArmadaClient.submitJobs()
              └─> ArmadaEventWatcher (daemon thread, gRPC event stream)
                    └─> Handles: JobSubmitted, JobQueued, JobPending, JobRunning,
                         JobSucceeded, JobFailed, JobCancelled, JobPreempted

Cluster-mode submission (separate path):
  └─> ArmadaClientApplication (SparkApplication SPI)
        ├─> KubernetesDriverBuilder.buildDriverPod()
        ├─> PodSpecConverter, PodMerger, ConfigGenerator
        └─> ArmadaClient.submitJobs()
```

**Key classes:**
- **ArmadaClusterManager** — SPI entry point; creates TaskSchedulerImpl + Backend
- **ArmadaClusterManagerBackend** — Tracks executors via ConcurrentHashMaps (executorToJobId, jobIdToExecutor, pendingExecutors, terminalExecutors)
- **ArmadaExecutorAllocator** — Runs on ScheduledExecutorService; respects `ARMADA_ALLOCATION_BATCH_SIZE`, `ARMADA_MAX_PENDING_JOBS`, `ARMADA_ALLOCATION_CHECK_INTERVAL`
- **ArmadaEventWatcher** — Long-lived daemon thread with `volatile running` flag; 5s join timeout on shutdown
- **PodSpecConverter** — Bidirectional Fabric8 <-> Protobuf; hardcodes None/empty for version-incompatible fields (dnsConfig, ephemeralContainers, hostUsers, os, schedulingGates)
- **Config** — 100+ entries via Spark's `ConfigBuilder` API; all prefixed `spark.armada.*`

## Common Pitfalls

- **Version-specific SparkSubmit:** `src/main/scala-spark-{3.3,3.5,4.1}/` each override `prepareSubmitEnvironment()`. Spark 3.3 uses `args.master = ...`; Spark 3.5+ uses `args.maybeMaster = Option(...)`. Changes to submission logic likely need updates in all versions.
- **Shade relocations:** All Armada transitive deps (gRPC, Netty, Protobuf, Jackson, ScalaPB) are relocated to `io.armadaproject.shaded.*`. At runtime, import paths are rewritten — never rely on unshaded `io.grpc.*` or `com.google.protobuf.*` classes from the plugin JAR.
- **Provided scope:** `spark-core` and `spark-kubernetes` are `provided` — they exist at runtime but are NOT in the fat JAR. Do not add their transitive deps as compile-scope dependencies. Spark 4.x additionally needs `spark-common-utils` (activated by profile).
- **Netty conflict:** Spark's `spark-core` bundles its own Netty. The plugin explicitly excludes `io.netty:*` from spark-core and ships its own shaded Netty. Never add unshaded Netty deps.
- **Mockito + Scala:** Use `mock(classOf[Type])` (Java-style), not `mock[Type]`. Scala companion objects cannot be mocked directly — mock the trait/interface instead. Wrap backend calls in tests with `try/catch { case _: NullPointerException => }` because Spark's RPC layer throws NPEs in unit test contexts.
- **SparkConf in tests:** Always use `new SparkConf(false)` to avoid loading system properties. Mock SparkContext/SparkEnv/RpcEnv but use real `ScheduledExecutorService`.
- **Build-helper sources:** Maven's `build-helper-maven-plugin` adds `src/main/scala-spark-${spark.binary.version}` at compile time. Only one version-specific directory is active per build.

## Code Style

- **Formatter:** Scalafmt 3.9.5 (enforced by Spotless Maven plugin)
- **Max line length:** 100 columns
- **Alignment:** `align.preset = more`
- **Dialect:** scala213
- Always run `mvn spotless:apply` before committing

### Naming Conventions

- Classes/traits: `PascalCase` (e.g., `ArmadaClusterManager`)
- Methods/variables: `camelCase`
- Config constants: `UPPER_SNAKE_CASE` (e.g., `ARMADA_JOB_QUEUE`)
- Test files: `{ClassName}Suite.scala`

### Scala Patterns Used

- **Scoped visibility:** `private[spark]` for package-private classes, `private[submit]` / `private[armada]` for internal APIs
- **Case classes** for data types (e.g., `ClientArguments`, `CLIConfig`, `ResourceConfig`)
- **Companion objects** for factory methods and constants
- **Option/Try monads** over null/exceptions; `NonFatal` for catch blocks
- **For-comprehensions** for chained Option/Try operations
- **Call-by-name parameters** (`=> Option[T]`) for lazy evaluation
- **`scala.jdk.CollectionConverters._`** for Java/Scala interop (`.asScala` / `.asJava`)
- **Spark's `Logging` trait** for all logging (`logInfo`, `logWarning`, `logDebug`)
- **Spark's `ConfigBuilder` API** for all configuration entries in `Config.scala`

### Import Order

1. Java/javax imports
2. Scala stdlib imports
3. Third-party imports (io.armadaproject, io.fabric8, com.fasterxml)
4. Spark imports (org.apache.spark)

### License Header

All source files must include the Apache 2.0 license header (see any existing file).

## Testing Standards

- **Framework:** ScalaTest 3.2.16 (`AnyFunSuite` style exclusively)
- **Mocking:** Mockito 5.12 (`mock(classOf[...])`, `when(...).thenReturn(...)`)
- **Assertions:** ScalaTest matchers (`shouldBe`, `shouldEqual`, `should contain`)

### Test Patterns

```scala
// Standard test class structure
class FooSuite extends AnyFunSuite with BeforeAndAfter with Matchers {
  before { /* setup */ }
  after  { /* cleanup */ }
  test("description of behavior") { /* assertions */ }
}

// Table-driven property tests (preferred for parameterized cases)
class BarSuite extends AnyFunSuite with TableDrivenPropertyChecks with Matchers {
  test("validates multiple inputs") {
    val testCases = Table(("input", "expected"), ("a", true), ("", false))
    forAll(testCases) { (input, expected) =>
      validate(input) shouldBe expected
    }
  }
}
```

- Use `BeforeAndAfter` or `BeforeAndAfterEach` for fixtures (temp files, mocks)
- Use `TableDrivenPropertyChecks` for parameterized/data-driven tests
- Mock SparkContext/SparkConf rather than creating real Spark sessions
- Clean up temp files in `after` blocks
- No shared base test class; use trait composition
- E2E tests tagged with custom `E2ETest` ScalaTest tag (excluded from `mvn test`)

## Agent Workflow

**The main agent must act as an orchestrator.** Never do work inline that can be delegated to a subagent.

- **Delegate everything:** Use the Task tool with specialized subagents for all research, code exploration, code writing, testing, and analysis. The main agent should plan, coordinate, and summarize — not do the work itself.
- **Maximize parallelism:** Launch multiple subagents concurrently whenever their tasks are independent. For example, when exploring code patterns AND analyzing tests AND checking dependencies, spawn all three agents in a single message rather than sequentially. Always send independent Task calls in a **single message** with multiple tool-use blocks.
- **Use the right agent type:** Pick `Explore` for codebase search/understanding, `Plan` for architecture decisions, `Bash` for commands, and specialized agents (e.g., `code-reviewer`, `test-automator`, `debugger`) when they match the task.
- **Keep the main context clean:** Offload large file reads, multi-file searches, and deep analysis to subagents so the main conversation stays focused on coordination and user communication.
- **Hooks run automatically — use subagents to respond:** When a hook (Spotless, build verification, code review, or simplification) reports an issue, delegate the fix to a subagent rather than doing it inline. If multiple hooks fail simultaneously, spawn parallel subagents to address each issue concurrently.

## CI/CD

GitHub Actions with matrix builds across Spark 3.3/3.5/4.1 and Scala 2.12/2.13. Pipeline: lint -> build -> e2e tests.
