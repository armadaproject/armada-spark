# Armada-spark

**Run Apache Spark workloads on [Armada](https://github.com/armadaproject/armada), a multi-cluster Kubernetes batch scheduler.**

**armada-spark** is an open-source integration designed to streamline deployment and management of Apache Spark workloads on Armada.
It provides preconfigured Docker images, tooling for efficient image management, and example workflows to simplify local and production deployments.

## Getting Started

### Prerequisites

- **Java** 11 or 17
- **Apache Maven** 3.9.6+
- _(Optional)_ [kind](https://kind.sigs.k8s.io/) for local clusters
- An accessible Armada server and Lookout endpoint — see the [Armada Operator Quickstart](https://github.com/armadaproject/armada-operator) to set one up

### Build

The default build targets **Spark 3.5.5** and **Scala 2.13.8**:

```bash
mvn clean package
```

To target a different Spark/Scala version:

```bash
./scripts/set-version.sh 3.3.4 2.12.15   # Spark 3.3.4, Scala 2.12.15
mvn clean package
```

### Supported Version Matrix

| Spark   | Scala  | Java |
|---------|--------|------|
| 3.3.4   | 2.12.15 | 11  |
| 3.3.4   | 2.13.8  | 11  |
| 3.5.5   | 2.12.18 | 17  |
| 3.5.5   | 2.13.8  | 17  |

### Build Docker Images

```bash
./scripts/createImage.sh [-i image-name] [-m armada-master-url] [-q armada-queue] [-l armada-lookout-url]
```

| Flag | Description        | Example                    |
|------|--------------------|----------------------------|
| `-i` | Docker image name  | `spark:armada`             |
| `-m` | Armada master URL  | `armada://localhost:30002` |
| `-q` | Armada queue       | `default`                  |
| `-l` | Armada Lookout URL | `http://localhost:30000`   |
| `-p` | Include Python     |                            |
| `-h` | Display help       |                            |

You can store defaults in `scripts/config.sh`:

```bash
export IMAGE_NAME="spark:armada"
export ARMADA_MASTER="armada://localhost:30002"
export ARMADA_QUEUE="default"
export ARMADA_LOOKOUT_URL="http://localhost:30000"
export INCLUDE_PYTHON=true
export USE_KIND=true
```

For **client mode**, set additional values:

```bash
export SPARK_DRIVER_HOST="172.18.0.1"                    # Required for client mode
export SPARK_DRIVER_PORT="7078"                          # Required for client mode
```

### Deploy to a Local Cluster

We recommend [kind](https://kind.sigs.k8s.io/) for local testing. If you are using the [Armada Operator Quickstart](https://github.com/armadaproject/armada-operator), it is already based on kind.

```bash
kind load docker-image $IMAGE_NAME --name armada
```

## Running Example Workloads

### SparkPi

```bash
# Cluster mode + dynamic allocation
./scripts/submitArmadaSpark.sh -M cluster -A dynamic 100

# Cluster mode + static allocation
./scripts/submitArmadaSpark.sh -M cluster -A static 100

# Client mode + dynamic allocation
./scripts/submitArmadaSpark.sh -M client -A dynamic 100

# Client mode + static allocation
./scripts/submitArmadaSpark.sh -M client -A static 100
```

Run `./scripts/submitArmadaSpark.sh -h` for all available options. The script reads `ARMADA_MASTER`, `ARMADA_QUEUE`, and `ARMADA_LOOKOUT_URL` from `scripts/config.sh`.

### Jupyter Notebook

The Docker image includes Jupyter support (requires `INCLUDE_PYTHON=true`):

```bash
./scripts/runJupyter.sh
```

Opens at `http://localhost:8888`. Override the port with `JUPYTER_PORT` in `scripts/config.sh`. Example notebooks from `example/jupyter/notebooks` are mounted at `/home/spark/workspace/notebooks`.

## Architecture

```
User submits Spark job
  └─> ArmadaClusterManager (SPI entry: registers "armada://" master scheme)
        ├─> TaskSchedulerImpl (Spark core task scheduling)
        └─> ArmadaClusterManagerBackend (executor lifecycle, state tracking)
              ├─> ArmadaExecutorAllocator (demand vs supply, batch job submission)
              └─> ArmadaEventWatcher (daemon thread, gRPC event stream)

Cluster-mode submission:
  └─> ArmadaClientApplication (SparkApplication SPI)
        ├─> KubernetesDriverBuilder → PodSpecConverter → PodMerger
        └─> ArmadaClient.submitJobs()
```

Key source directories:

```
src/main/scala/org/apache/spark/
├── deploy/armada/              # Configuration & job submission
│   ├── Config.scala            # spark.armada.* config entries
│   ├── submit/                 # Job submission pipeline
│   └── validators/             # Kubernetes validation
└── scheduler/cluster/armada/   # Cluster manager & scheduling
    ├── ArmadaClusterManager.scala
    ├── ArmadaClusterManagerBackend.scala
    ├── ArmadaEventWatcher.scala
    └── ArmadaExecutorAllocator.scala
```

Version-specific sources live in `src/main/scala-spark-{3.3,3.5,4.1}/`.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development guide, including commit conventions, coding standards, and how to use Claude Code with this project.

Quick reference:

```bash
mvn test                 # Run unit tests
mvn spotless:check       # Check formatting
mvn spotless:apply       # Auto-fix formatting
scripts/dev-e2e.sh       # Run E2E tests (requires a running Armada cluster)
```
