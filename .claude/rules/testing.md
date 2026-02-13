---
paths:
  - "src/test/**/*.scala"
---

# Testing Rules

## Mock Setup

```scala
// Always prevent system property loading
sparkConf = new SparkConf(false)

// Java-style mocking only (not mock[Type])
sc = mock(classOf[SparkContext])
env = mock(classOf[SparkEnv])
rpcEnv = mock(classOf[RpcEnv])

// Wire the mock chain
when(sc.conf).thenReturn(sparkConf)
when(sc.env).thenReturn(env)
when(env.rpcEnv).thenReturn(rpcEnv)

// ResourceProfileManager is required for backend tests
val rpm = mock(classOf[ResourceProfileManager])
val drp = mock(classOf[ResourceProfile])
when(sc.resourceProfileManager).thenReturn(rpm)
when(rpm.defaultResourceProfile).thenReturn(drp)
```

Use real `ScheduledExecutorService` (don't mock it). Mock companion objects' traits, not the objects themselves.

## NPE Wrapping for Backend Calls

Spark's RPC layer throws NPE in unit tests because `driverEndpoint` is null. Wrap calls that trigger RPC:

```scala
private def ignoreRpcErrors(block: => Unit): Unit = {
  try { block }
  catch { case _: NullPointerException => }
}
```

## Cleanup

```scala
after {
  if (backend != null) {
    try { backend.stop() }
    catch { case NonFatal(_) => }
  }
}
```

## E2E Tests

Tag with the custom `E2ETest` tag object (excluded from `mvn test`):

```scala
test("e2e scenario description", E2ETest) { ... }
```
