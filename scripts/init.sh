#!/bin/bash

# Utility functions

root="$(cd "$(dirname "$0")/.."; pwd)"
scripts="$(cd "$(dirname "$0")"; pwd)"

if [ -e "$scripts/config.sh" ]; then
    source "$scripts/config.sh"
fi

# Default values for deploy mode and allocation
DEPLOY_MODE="${DEPLOY_MODE:-cluster}"
ALLOCATION_MODE="${ALLOCATION_MODE:-dynamic}"

# s3
ARMADA_S3_BUCKET_NAME=${ARMADA_S3_BUCKET_NAME:-kafka-s3}
ARMADA_S3_BUCKET_ENDPOINT=${ARMADA_S3_BUCKET_ENDPOINT:-http://192.168.59.6}
ARMADA_S3_USER_DIR=${ARMADA_USER_DIR:-s3a://$ARMADA_S3_BUCKET_NAME/$USER}

# benchmark
ARMADA_BENCHMARK_JAR=${ARMADA_BENCHMARK_JAR:-local:///opt/spark/jars/eks-spark-benchmark-assembly-1.0.jar}
ARMADA_BENCHMARK_DATA=${ARMADA_BENCHMARK_DATA:-s3a://kafka-s3/data/benchmark/data/10t}
ARMADA_BENCHMARK_CLASS=${ARMADA_BENCHMARK_CLASS:-com.amazonaws.eks.tpcds.BenchmarkSQL}
ARMADA_BENCHMARK_TOOLS=${ARMADA_BENCHMARK_TOOLS:-/opt/tools/tpcds-kit/tools}

# Parse long options (--mode, --allocation) before getopts
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            DEPLOY_MODE="$2"
            shift 2
            ;;
        --allocation)
            ALLOCATION_MODE="$2"
            shift 2
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

# Restore remaining arguments for getopts
set -- "${ARGS[@]+"${ARGS[@]}"}"

print_usage () {
    echo ' Usage:'
    echo '   -h  help'
    echo '   -k  "use kind cluster"'
    echo '   -p  "build image with python"'
    echo '   -M, --mode <client|cluster>     "deploy mode (default: cluster)"'
    echo '   -A, --allocation <static|dynamic> "allocation type (default: dynamic)"'
    echo '   -i  <image-name>'
    echo '   -m  <armada-master-url>'
    echo '   -q  <armada-queue>'
    echo '   -P  <python script to run>'
    echo '   -s  <scala class to run>'
    echo '   -c  <class path to use>'
    echo '   -e  running e2e tests'
    echo ''
    echo 'Examples:'
    echo '   --mode cluster --allocation dynamic'
    echo '   --mode cluster --allocation static'
    echo '   --mode client --allocation dynamic'
    echo '   --mode client --allocation static'
    echo ''
    echo 'You also can specify those parameters in scripts/config.sh, like so:'
    echo '   IMAGE_NAME=spark:armada'
    echo '   ARMADA_MASTER=armada://localhost:30002'
    echo '   ARMADA_QUEUE=test'
    echo '   USE_KIND=true'
    echo '   INCLUDE_PYTHON=true'
    echo '   DEPLOY_MODE=cluster'
    echo '   ALLOCATION_MODE=dynamic'
    echo '   PYTHON_SCRIPT=/opt/spark/examples/src/main/python/pi.py'
    echo '   SCALA_CLASS=org.apache.spark.examples.SparkPi'
    echo '   CLASS_PATH=local:///opt/spark/extraFiles/spark-examples_2.12-3.5.3.jar'
    echo '   # Auth: Set ARMADA_AUTH_SCRIPT_PATH for authentication'
    exit 1
}

while getopts "hekpi:m:P:s:c:q:M:A:ef" opt; do
  case "$opt" in
    h) print_usage ;;
    k) USE_KIND=true ;;
    p) INCLUDE_PYTHON=true ;;
    i) IMAGE_NAME=$OPTARG ;;
    m) ARMADA_MASTER=$OPTARG ;;
    q) ARMADA_QUEUE=$OPTARG ;;
    P) PYTHON_SCRIPT=$OPTARG ;;
    s) SCALA_CLASS=$OPTARG ;;
    c) CLASSPATH=$OPTARG ;;
    e) RUNNING_E2E_TESTS=true ;;
    M) DEPLOY_MODE=$OPTARG ;;
    A) ALLOCATION_MODE=$OPTARG ;;
    f) USE_FALLBACK_STORAGE=true ;;
  esac
done

export INCLUDE_PYTHON="${INCLUDE_PYTHON:-false}"
export USE_KIND="${USE_KIND:-false}"
export IMAGE_NAME="${IMAGE_NAME:-spark:armada}"
export ARMADA_MASTER="${ARMADA_MASTER:-armada://localhost:30002}"
export ARMADA_INTERNAL_URL="${ARMADA_INTERNAL_URL:-armada://armada-server.armada:50051}"
export ARMADA_QUEUE="${ARMADA_QUEUE:-test}"
export ARMADA_AUTH_TOKEN=${ARMADA_AUTH_TOKEN:-}
export ARMADA_AUTH_SCRIPT_PATH=${ARMADA_AUTH_SCRIPT_PATH:-}
export ARMADA_EVENT_WATCHER_USE_TLS=${ARMADA_EVENT_WATCHER_USE_TLS:-false}
export SPARK_BLOCK_MANAGER_PORT=${SPARK_BLOCK_MANAGER_PORT:-}
export SCALA_CLASS="${SCALA_CLASS:-org.apache.spark.examples.SparkPi}"
export RUNNING_E2E_TESTS="${RUNNING_E2E_TESTS:-false}"
export USE_FALLBACK_STORAGE="${USE_FALLBACK_STORAGE:-false}"
export SPARK_SECRET_KEY="${SPARK_SECRET_KEY:-armada-secret}"

ARMADA_AUTH_ARGS=()
# Add auth script path if configured
if [ "$ARMADA_AUTH_SCRIPT_PATH" != "" ]; then
    ARMADA_AUTH_ARGS+=("--conf" "spark.armada.auth.script.path=$ARMADA_AUTH_SCRIPT_PATH")
fi

if [ "$ARMADA_EVENT_WATCHER_USE_TLS" != "" ]; then
    ARMADA_AUTH_ARGS+=("--conf" "spark.armada.eventWatcher.useTls=$ARMADA_EVENT_WATCHER_USE_TLS")
fi

# Build deploy-mode specific arguments array
DEPLOY_MODE_ARGS=()
if [ "$DEPLOY_MODE" = "client" ]; then
    DEPLOY_MODE_ARGS=(
        --conf spark.driver.host=$SPARK_DRIVER_HOST
        --conf spark.driver.port=$SPARK_DRIVER_PORT
        --conf spark.driver.bindAddress=0.0.0.0
    )
else
    DEPLOY_MODE_ARGS=(
        --conf spark.armada.internalUrl=$ARMADA_INTERNAL_URL
    )
fi

# Add block manager port if configured
if [ "$SPARK_BLOCK_MANAGER_PORT" != "" ]; then
    DEPLOY_MODE_ARGS+=("--conf" "spark.blockManager.port=$SPARK_BLOCK_MANAGER_PORT")
fi

DOCKER_ENV_ARGS=(-e SPARK_PRINT_LAUNCH_COMMAND=true)
if [ "$ARMADA_AUTH_TOKEN" != "" ]; then
    DOCKER_ENV_ARGS+=(-e "ARMADA_AUTH_TOKEN=$ARMADA_AUTH_TOKEN")
fi

# Validation

if [[ "$DEPLOY_MODE" != "client" && "$DEPLOY_MODE" != "cluster" ]]; then
    echo "Error: --mode/-M must be either 'client' or 'cluster'"
    exit 1
fi
export DEPLOY_MODE

if [[ "$ALLOCATION_MODE" != "static" && "$ALLOCATION_MODE" != "dynamic" ]]; then
    echo "Error: --allocation/-A must be either 'static' or 'dynamic'"
    exit 1
fi
export ALLOCATION_MODE

if [ "$ALLOCATION_MODE" = "static" ]; then
    STATIC_MODE=true
else
    STATIC_MODE=false
fi
export STATIC_MODE

if [ -z "${PYTHON_SCRIPT:-}" ]; then
    PYTHON_SCRIPT="/opt/spark/examples/src/main/python/pi.py"
else
    INCLUDE_PYTHON=true
fi

# derive Scala and Spark versions from pom.xml, set via ./scripts/set-version.sh
if [[ -z "${SCALA_VERSION:-}" ]]; then
  export SCALA_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=scala.version -q -DforceStdout)
  export SCALA_BIN_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=scala.binary.version -q -DforceStdout)
fi
if [[ -z "${SPARK_VERSION:-}" ]]; then
  export SPARK_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=spark.version -q -DforceStdout)
  export SPARK_BIN_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=spark.binary.version -q -DforceStdout)
fi

export CLASS_PATH="${CLASS_PATH:-local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar}"

# check the Spark version is supported
if ! [ -e "$root/src/main/scala-spark-$SPARK_BIN_VERSION" ]; then
    echo "This tool does not support Spark version ${SPARK_VERSION}."
    exit 1
fi

if [ "$USE_FALLBACK_STORAGE" = "true" ]; then
    if [[ "$SPARK_VERSION" != "3.5.3" || "$SCALA_BIN_VERSION" != "2.12" ]]; then
        echo fallback storage currently only supported for spark 3.5.3/scala 2.12
        echo current version is $SPARK_VERSION $SCALA_BIN_VERSION
        exit 1
    fi
fi

shift $((OPTIND - 1))
FINAL_ARGS=("${@:-}")   

if [ ${#FINAL_ARGS[@]} -eq 0 ]; then
    FINAL_ARGS+=("100")
fi

if [ "$INCLUDE_PYTHON" == "true" ]; then WITH_PYTHON="-python3"; else WITH_PYTHON=""; fi
image_tag="$SPARK_VERSION-scala$SCALA_BIN_VERSION-java${JAVA_VERSION:-17}$WITH_PYTHON-ubuntu"
