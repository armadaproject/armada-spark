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
| 3.5.5   | 2.12.18 | 17  |
| 3.5.5   | 2.13.8  | 17  |

### Planned Future Version Support

| Spark   | Scala  | Java |
|---------|--------|------|
| 3.3.4   | 2.12.15 | 11  |
| 3.3.4   | 2.13.8  | 11  |

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

### Running a remote Armada server using Armada Operator
The default Armada Operator setup allows only localhost access.  You can quickly set up a local Armada server
configured to allow external access from other hosts, useful for client development and testing. For this
configuration:

- Copy the file `e2e/kind-config-external-access.yaml` in this repository to `hack/kind-config.yaml`
in your `armada-operator` repository.

- Edit the newly-copied `hack/kind-config.yaml` as noted in the beginning comments of that file.

- Run the armada-operator setup commands (usually `make kind-all`) to create and start your Armada instance.

- Copy the `$HOME/.kube/config` and `$HOME/.armadactl.yaml` (that Armada Operator will generate) from the Armada
server host to your `$HOME` directory on the client (local) host. Then edit the local `.kube/config` and on
the line that has `server: https://0.0.0.0:6443`, change the `0.0.0.0` address to the IP address or hostname
of the remote Armada server system.

- Generate a copy of the client TLS key, cert, and CA-cert files: (1) go into the `e2e` subdirectory, and
run `./extract-kind-cert.sh` - it will generate `client.crt`, `client.key`, and `ca.crt`, from the output
of `kubectl config view`. These files can be left in this directory.

- Copy the `$HOME/.armadactl.yaml` from the Armada server host to your home directory on your client system.

- You should then be able to run `kubectl get pods -A` and see a list of the running pods on the remote
Armada server, as well as running `armadactl get queues`.

- Verify the functionality of your setup by editing `scripts/config.sh` and changing the following line:
```
ARMADA_MASTER=armada://192.168.12.135:30002
```
to the IP address or hostname of your Armada server. You should not need to change the port number.

Also, set the location of the three TLS certificate files by adding/setting:
```
export CLIENT_CERT_FILE=e2e/client.crt
export CLIENT_KEY_FILE=e2e/client.key
export CLUSTER_CA_FILE=e2e/ca.crt
```

-  You should be able to now verify the armada-spark configuration by running the E2E tests:
```
$ ./scripts/dev-e2e.sh
```
This will save its output to `e2e-test.log` for further debugging.

---

## Development

Before submitting a pull request, please ensure that your code adheres to the project's coding standards and passes all tests.

### Testing

To run the unit tests, use the following command:

```bash
mvn test
```

To run the E2E tests, run Armada using the [Operator Quickstart](https://github.com/armadaproject/armada-operator?tab=readme-ov-file#quickstart) guide, then execute:

```bash
scripts/test-e2e.sh
```
### Linting

To check the code for linting issues, use the following command:

```bash
mvn spotless:check
```

To automatically apply linting fixes, use:

```bash
mvn spotless:apply
```

### E2E

Make sure that the [SparkPi](#sparkpi-example) job successfully runs on your Armada cluster before submitting a pull request.

---

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

### Spark History Server

View event logs from completed jobs:

```bash
./scripts/runHistoryServer.sh
```

Requires S3 credentials and `spark.eventLog.enabled=true`. UI at `http://localhost:18080`.

### File Distribution (`--files`)

See `scripts/filesParameterExample.sh` for a working example of distributing local files to driver and executor pods using Spark's `--files` parameter and `SparkFiles.get()`.

### Benchmarks (TPC-DS)

```bash
./scripts/benchmark.sh           # Run against Armada
./scripts/benchmark.sh -K        # Run against native Kubernetes
```

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
│   ├── DeploymentModeHelper.scala  # Gang scheduling strategy per deploy mode
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
