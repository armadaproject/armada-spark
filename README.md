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

### Running a remote Armada server using Armada Operator
The default Armada Operator setup allows only localhost access.  You can quickly set up a local Armada server
configured to allow external access from other hosts, useful for client development and testing. For this
configuration:

- Copy the file `e2e/kind-config-external-access.yaml` in this repository to `hack/kind-config.yaml`
in your `armada-operator` repository.

- Edit the newly-copied `hack/kind-config.yaml` as noted in the beginning comments of that file.

- Run the armada-operator setup commands (usually `make kind-all`) to create and start your Armada instance.

- Copy the `$HOME/.kube/config` and `$HOME/.armadctl.yaml` (that Armada Operator will generate) from the Armada
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
CLIENT_CERT_FILE=e2e/client.crt
CLIENT_KEY_FILE=e2e/client.key
CLUSTER_CA_FILE=e2e/ca.crt
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

**Note:** The Docker image must be built with `INCLUDE_PYTHON=true` for Jupyter to work.

This will start a Jupyter notebook server at `http://localhost:8888` (or the port specified by `JUPYTER_PORT` in `scripts/config.sh`). 
The example notebooks from `example/jupyter/notebooks` are mounted in the container at `/home/spark/workspace/notebooks`.

**Configuration:**
- **Required:** `SPARK_DRIVER_HOST`
- Override the Jupyter port if required by setting `JUPYTER_PORT` in `scripts/config.sh`
- The script uses the same configuration (`ARMADA_MASTER`, `ARMADA_QUEUE`, `SPARK_DRIVER_HOST`, etc.) as other scripts

---

## Working with Claude Code

Project settings (`.claude/settings.json`) include permissions, hooks (auto-formatting via Spotless, build verification), and slash commands out of the box.

**Optional third-party plugins:** Community plugins for code review, testing, and debugging are available but not enabled by default. To opt in:

```bash
/plugin marketplace add wshobson/agents
cp .claude/settings.local.example.json .claude/settings.local.json
# Restart Claude Code
```
