# All About armada-spark

## Running `armada-spark`

`armada-spark`'s main entry point is a class called `ArmadaSparkSubmit`. It's
designed to be a drop-in replacement for `spark-submit`. However, due to
limitations in upstream integration, we load it via the `spark-class` helper
program instead of using `spark-submit` directly.

Here's an example of calling `ArmadaSparkSubmit`:

```bash
 $ /opt/spark/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
    --master armada://localhost:50051 --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.Executor.instances=4 \
    --conf spark.kubernetes.container.image=armada-spark \
    --conf spark.armada.lookouturl=http://localhost:30000 \
    --conf spark.armada.clusterSelectors="armada-spark=true" \
    "local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar"
```

Quite a handful, but let's break down some of the important bits:

The first argument to `spark-class` is the fully-qualified java classpath to
`ArmadaSparkSubmit`. This will load and execute `ArmadaSparkSubmit.main` which
will be behave almost exactly like the base `spark-submit`, but
`AramdaSparkSubmit` knows how to parse and handle `armada` protocol URLs.

The `--master` flag specifies the master URL for spark to use to submit work.
If we break down the URL we have the protocol `armada` and a (hostname, port)
tuple, in this case `localhost:50051`. So `ArmadaSparkSubmit` will try to submit
armada jobs to `localhost` on port `50051`.

The next important flag comes from `--class` which tells `ArmadaSparkSubmit` to
load `org.apache.spark.examples.SparkPi` as the main workload program.

Then comes several configuration options specified with `--conf` flags:
- `--conf spark.Executor.instances=4` The number of Executor jobs/instances to
  launch. `armada-spark` only supports static Executors at the moment.
- `--conf spark.kubernetes.container.image=armada-spark` lets us know what
  container image `ArmadaSparkSubmit` should use for the Driver and Executors.
- `--conf spark.armada.lookouturl=http://localhost:30000` gives the lookout
    URL so we can easily find the results of jobs submitted to Armada.
- `--conf spark.armada.clusterSelectors="armada-spark=true"` helps Armada pick
    a specific kubernetes cluster to use in order to shedule Executors and
    Drivers to the same cluster.

And last, but not least: `"local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar"`
A jar which contains the Java class the Driver and Executors will load. This
will be located within the container image specified earlier.

Once called, `armada-spark` will submit a Driver job and a job for each of the
Executors. Through some magic we'll discuss a bit later, the Driver and
Executors will coordinate to accomplish whatever work is laid out in the loaded
`--class`. Here's outpout from a run of `ArmadaSparkSubmit`:

```
Armada selected as cluster manager.
ArmadaClientApplication.start() called!
host is localhost, port is 50051
Submit health good!
Executor JobID: 01jt6bmrszmnk4apxby6e895d8  Error: None
Executor JobID: 01jt6bmrszmnk4apxby6xrwer6  Error: None
Executor JobID: 01jt6bmrt0bvv5dh3k5k1qc8ta  Error: None
Executor JobID: 01jt6bmrt0bvv5dh3k5mqkeqms  Error: None
Driver JobID: 01jt6bmrthrtzgdwn7tad307nk  Error: None
Lookout URL for this job is http://localhost:30000/?page=0&sort[id]=jobId&sort[desc]=true&ps=50&sb=01jt6bmrthrtzgdwn7tad307nk&active=false&refresh=true
25/05/01 16:42:27 INFO ShutdownHookManager: Shutdown hook called
25/05/01 16:42:27 INFO ShutdownHookManager: Deleting directory /tmp/spark-a5c32faa-d3e4-4d5d-82a6-cdb79bac7984
```

Note the lookout URL. This will let us easily view results for the Driver and
its Executors. If all goes well the jobs will succeed and produce a result!

## Configuration Options

`armada-spark` has a slew of configuration options that can be set with the
`--conf` flag:

- `spark.armada.Executor.trackerPollingInterval` - A `String` value specifying
    the interval between polls to check the state of Executors.
- `spark.armada.Executor.trackerTimeout` - A `String` specifying the time to
    wait for the minimum number of Executors.
- `spark.armada.lookouturl` - A `String` value that gives the base URL for
    Armada's Lookout service.
- `spark.armada.health.checkTimeout` - Time to wait for Armada's health check
    to succeed. A `String` value specifying seconds to wait.
- `spark.armada.clusterSelectors` - A comma separated list of kubernetes
    label selectors (in key=value format) to ensure the spark Driver and
    its Executors are deployed to the same cluster. `String` value.
- `spark.armada.scheduling.nodeUniformityLabel` - A single kubernetes label to
    apply to both Driver and Executors.
- `spark.armada.DriverServiceNamePrefix` - Defines the Driver's service name
    prefix within Armada. A `String` consistenting of lowercase a-z and
    '-' characters only.
- `spark.armada.global.labels` - A comma separated list of kubernetes labels
    (in key=value format) to be added to all both the Driver and Executor pods.
- `spark.armada.Driver.labels` - A comma separated list of kubernetes labels
    (in key=value format) to be added to the Driver pod.
- `spark.armada.Executor.labels` A comma separated list of kubernetes labels
    (in key=value format) to be added to all Executor pods.

# Building `armada-spark`

## Scala Source

`armada-spark` can be built from source using maven. See the [README](https://github.com/armadaproject/armada-spark/blob/master/README.md) for more
information on building `armada-spark`.

## Docker image

`armada-spark` uses docker to produce container images that serve as spark
Drivers and Executors. See the [README](https://github.com/armadaproject/armada-spark/blob/master/README.md)
for more information on how to build these images.

# Architecture & Design

## Overview

Fundamentally, `armada-spark`'s architecture and design is closely related to
the existing Spark Kubernetes cluster manager, but with the added layer of
Armada and it's job system on top.

A great overview of what a "cluster" is from Spark's perspective can be found
[here](https://spark.apache.org/docs/latest/cluster-overview.html).

```
// TODO: armada-spark Architecture overview diagram.
```

The basic flow of an `armada-spark` execution is as follows:

1. End-user calls `ArmadaSparkSubmit.main` via `spark-submit` or `spark-class`.
2. `ArmadaSparkSubmit`:
    - Constructs an `ArmadaClient`.
    - Verifies connectivity and service health of specified Armada URL.
    - Submits a Driver job to Armada.
    - Submits `n` Executor jobs to Armada.
3. After job submissions, `ArmadaSparkSubmit` prints a Lookout link and exits.
4. Armada schedules Driver and Executor jobs submitted by `ArmadaSparkSubmit`.
    - Each job submits a pod with a single container.
    - By using a common label, we can guarantee Driver and Executor jobs are
    scheduled to the same cluster. This guarantees network connectivity and has
    the added benefit of ensuring Drivers and Executors run relatively close to
    each other, reducing network latency and overhead.
5. Asynchronously:
    - Driver starts up and waits for each expected Executor to establish
    connections.
    - Each Executor starts and establishes connections to the Driver.
6. Driver and Executors proceed as any normal Spark program would.
7. Driver and Executors finish their work.

## Integrating with Spark, or "What is a Cluster Manager?"

A "Cluster Manger" is a concept within Spark which allows Spark to submit and
manage Drivers and Executors to a compute cluster like YARN, Mesos,
Kubernetes, and now Armada. Confusingly, cluster managers are also sometimes
referred to as "resource-managers" within the Spark codebase.

To define a new cluster manager within Spark one would inherit the `trait`
[`ExternalClusterManager`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/ExternalClusterManager.scala)
and implement the required methods. Mostly they center around the creation,
management, and deletion of Spark Executors tied to a particular Driver program.

// TODO: Talk about spark-submit and integration there.

## Using Armada as a "cluster"

While Armada can be treated as a cluster for the purposes of Spark, some care
and consideration must be taken in order to meet most, if not all, of Spark's
expectations.

### Driver and Executor (and Cluster Manager) Network Communications

One major requirement that **must** be met is network communication between
Drivers and Executors. This allows Drivers to submit tasks to the Executors
and collate their results. `armada-spark` achieves this through the use of
[kubernetes labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/).
Used correctly, these labels induce the Armada scheulder to schedule Drivers
and their accompanying Executors to the same kubernetes cluster managed by
Armada. Kubernetes cluster networking topology guarantees that nodes within the
cluster will be able to communicate. Scheduling to the same cluster also has
the benefit of increasing the probability of compute resources being
"clustered" closely together. Perhaps servers on the same rack, or even a single
server.

If you refer back to the ["Cluster Manager Overview"](https://spark.apache.org/docs/latest/cluster-overview.html),
the first image you'll find is a diagram showing a complete, directed graph between the:

- Driver Program
- Worker Node / Executors
- Cluster Manager

and while the diagram appears to separate the "Cluster Manager" out as a separate
program or node, in reality it tends to "live" in the same process as the
Driver. So by ensuring connectivity between the Driver and Executors, we also
get connectivity with the "Cluster Manager".

### Driver and Executors as Armada Jobs

Armada, being a multi-cluster Kubernetes manager, takes on the role of
scheduling "jobs" to the clusters it oversees. For now, it makes the most sense
to separate each Driver and Executor into their own Armada job. There are 
several considerations that must be observed when submitting these jobs.

#### Armada Gang Scheduling

`armada-spark` uses gang scheduling annotations to schedule the driver and 
its executors together. By using the gang scheduling “Node Uniformity Label” 
we can guarantee that a driver and its executors are scheduled to the same
cluster if Kubernetes node labels are applied correctly.

#### Kubernetes Node Labels

Any Kubernetes worker nodes should have a common label, such as "armada-spark", 
and each cluster should have a unique value assigned to that label.

#### Custom Service Names

A unique service name is assigned to each Spark job’s driver when submitting 
a Spark job to Armada. This service name acts as a DNS name within Kubernetes 
and allows Spark Executors to connect to their corresponding Drivers.

### Driver and Executor as JVM programs

// TODO: Is this section necessary?

# `armada-spark` Deployment Considerations

## Necessary Kubernetes Configuration

### Kubernetes Cluster Nodes

All nodes in a particular Kubernetes cluster must be labeled by a common
`(label, value)` tuple. The `label` must be an agreed upon name which shows
the cluster will accept `armada-spark` jobs. Something like `armada-spark`.
The `value` must be unique per cluster to ensure individual `armada-spark` jobs
are scheduled to the same cluster. Cross-cluster jobs are not currently supported
by `armada-spark`. This should match an entry in the `trackedNodeLabels` and
`indexedNodeLabels` configuration options mentioned below.

## Necessary Armada Configuration

### Armada Scheduler

The Armada Scheduler indexes whatever label is assigned to cluster nodes that
denote they accept `armada-spark` jobs through their `scheduling.indexedNodeLabels`
configuration option. Gang scheduling will only consider indexed node labels!

### Armada Executor

Armada Executors must track whatever label is assigned to cluster nodes
that denote they accept `armada-spark` jobs through their `kubernetes.trackedNodeLabels`
configuration option.

### Armada Server

The Armada Server must enable custom service names through the
`submission.allowCustomServiceNames` configuration option. `armada-spark`
relies on custom service names to provide a unique DNS name for each
individual Driver.

### Example Configuration

An example configuration of Armada services with all the necessary options
configured can be found by applying this [patch](https://github.com/armadaproject/armada-spark/blob/master/e2e/armada-operator.patch)
to the [Armada Operator Dev Quickstart Config](https://github.com/armadaproject/armada-operator/blob/main/dev/quickstart/armada-crs.yaml)

# Conclusion

```
// TODO
```
