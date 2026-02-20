---
paths:
  - "**/Config.scala"
---

# Config Entry Rules

All entries use Spark's `ConfigBuilder` API, prefixed `spark.armada.*`, with `UPPER_SNAKE_CASE` constant names.

## Pattern by Type

```scala
// Optional string with file-path validation
val ARMADA_FOO: OptionalConfigEntry[String] =
  ConfigBuilder("spark.armada.foo")
    .doc("Description of config entry.")
    .stringConf
    .checkValue(path => path.nonEmpty && isValidFilePath(path),
      "Must be a valid local file path, file://, http:// or https:// URL")
    .createOptional

// Boolean with default
val ARMADA_BAR_ENABLED: ConfigEntry[Boolean] =
  ConfigBuilder("spark.armada.bar.enabled")
    .doc("Description.")
    .booleanConf
    .createWithDefault(false)

// Time duration (parsed from string, stored as Long)
val ARMADA_TIMEOUT: ConfigEntry[Long] =
  ConfigBuilder("spark.armada.timeout")
    .doc("Description.")
    .timeConf(TimeUnit.SECONDS)
    .checkValue(_ > 0, "Timeout must be positive.")
    .createWithDefaultString("300s")

// Integer with range check
val ARMADA_BATCH_SIZE: ConfigEntry[Int] =
  ConfigBuilder("spark.armada.batchSize")
    .doc("Description.")
    .intConf
    .checkValue(_ > 0, "Must be positive")
    .createWithDefault(10)
```

## Available Validators

- `isValidFilePath(path)` -- accepts `file://`, `http://`, `https://`, or bare local paths
- `k8sLabelListValidator` / `k8sAnnotationListValidator` -- validates comma-separated `k=v` lists via `K8sValidator`
- Inline predicates: `_ > 0`, `_.nonEmpty`, custom lambdas

## Creation Methods

| Method | Returns | Use when |
|--------|---------|----------|
| `createOptional` | `OptionalConfigEntry[T]` | No sensible default; caller checks `Option` |
| `createWithDefault(v)` | `ConfigEntry[T]` | Typed default value |
| `createWithDefaultString(s)` | `ConfigEntry[T]` | Default needs parsing (e.g., `"300s"` for timeConf) |
