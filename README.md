# armada-spark

**Run Apache Spark workloads seamlessly on [Armada](https://github.com/armadaproject/armada), a multi-cluster Kubernetes batch scheduler**

---

## Overview

**armada-spark** is an open-source integration designed to streamline deployment and management of Apache Spark workloads on Armada.
It provides preconfigured Docker images, tooling for efficient image management, and example workflows to simplify local and production deployments.

## Getting Started

### Prerequisites

- **Java** 8/11/17
- **Scala** 2.12/2.13
- **Apache Maven** 3.9.6+
- _(Optional)_ [kind](https://kind.sigs.k8s.io/) for local clusters
- An accessible Armada Server and Lookout endpoint (check [Armada Operator](https://github.com/armadaproject/armada-operator) for the Quickstart guide)

### Versions

By default, the project targets `Spark 3.5.3` and `Scala 2.13.15`. To change versions:

```bash
./scripts/set-version.sh <spark-version> <scala-version>
```

Example:
```bash
./scripts/set-version.sh 3.5.3 2.13.15
```

### Building Armada Spark

After setting your desired Spark and Scala versions, build the Armada Spark project with Maven by running the following command:

```bash
mvn clean package
```

### Building Docker Images

Once your project is built, create the Docker image using:

```bash
./scripts/createImage.sh [-i image-name] [-m armada-master-url] [-q armada-queue] [-l armada-lookout-url]
```

**Options:**

| Flag | Description        | Example                    |
|------|--------------------|----------------------------|
| `-i` | Docker image name  | `spark:armada`             |
| `-m` | Armada master URL  | `armada://localhost:30002` |
| `-q` | Armada queue       | `default`                  |
| `-l` | Armada Lookout URL | `http://localhost:30000`   |
| `-p` | Include python     |                            |
| `-h` | Display help       |                            |


To simplify, you may store these values in `scripts/config.sh`:

```bash
export IMAGE_NAME="spark:armada"
export ARMADA_MASTER="armada://localhost:30002"
export ARMADA_QUEUE="default"
export ARMADA_LOOKOUT_URL="http://localhost:30000"
export INCLUDE_PYTHON=true
export USE_KIND=true
```

**Note:** For client mode, you need to set additional configuration:

```bash
export ARMADA_MASTER="local://armada://localhost:30002"  # Add "local://" prefix
export SPARK_DRIVER_HOST="172.18.0.1"                    # Required for client mode
export SPARK_DRIVER_PORT="7078"                          # Required for client mode
```

### Deployment

We recommend using [kind](https://kind.sigs.k8s.io/) for local testing.
If you are using the [Armada Operator](https://github.com/armadaproject/armada-operator) Quickstart, it is already based on `kind`.

Run the following command to load the Armada Spark image into your local kind cluster:
```
kind load docker-image $IMAGE_NAME --name armada
```

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

### SparkPi Example

The project includes a ready-to-use Spark job to test your setup:

```bash
# Cluster mode + Dynamic allocation
./scripts/submitArmadaSpark.sh -M cluster -A dynamic 100

# Cluster mode + Static allocation
./scripts/submitArmadaSpark.sh -M cluster -A static 100

# Client mode + Dynamic allocation
./scripts/submitArmadaSpark.sh -M client -A dynamic 100

# Client mode + Static allocation
./scripts/submitArmadaSpark.sh -M client -A static 100
```

This job leverages the same configuration parameters (`ARMADA_MASTER`, `ARMADA_QUEUE`, `ARMADA_LOOKOUT_URL`) as the `scripts/config.sh` script.

Use the -h option to see what other options are available.

### Jupyter Notebook

The Docker image includes Jupyter support. Run Jupyter with the example notebooks:

```bash
./scripts/runJupyter.sh
```

This will start a Jupyter notebook server at `http://localhost:8888` (or the port specified by `JUPYTER_PORT` in `scripts/config.sh`). 
The example notebooks from `example/jupyter/notebooks` are mounted in the container at `/home/spark/workspace/notebooks`.

**Configuration:**
- Override the Jupyter port if required by setting `JUPYTER_PORT` in `scripts/config.sh`
- The script uses the same configuration (`ARMADA_MASTER`, `ARMADA_QUEUE`, `SPARK_DRIVER_HOST`, etc.) as other scripts
- `SPARK_DRIVER_HOST` should be set to the local machines IP address.
