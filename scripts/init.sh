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
    echo '   # Auth options (use either token or signin binary):'
    echo '   ARMADA_AUTH_TOKEN=<token>'
    echo '   # OR use signin binary to obtain token:'
    echo '   ARMADA_AUTH_SIGNIN_BINARY=/path/to/signin'
    echo '   ARMADA_AUTH_SIGNIN_ARGS="access-token -c client.config client-name"'
    exit 1
}

while getopts "hekpi:a:m:P:s:c:q:M:A:e" opt; do
  case "$opt" in
    h) print_usage ;;
    k) USE_KIND=true ;;
    p) INCLUDE_PYTHON=true ;;
    a) ARMADA_AUTH_TOKEN=$OPTARG ;;
    i) IMAGE_NAME=$OPTARG ;;
    m) ARMADA_MASTER=$OPTARG ;;
    q) ARMADA_QUEUE=$OPTARG ;;
    P) PYTHON_SCRIPT=$OPTARG ;;
    s) SCALA_CLASS=$OPTARG ;;
    c) CLASSPATH=$OPTARG ;;
    e) RUNNING_E2E_TESTS=true ;;
    M) DEPLOY_MODE=$OPTARG ;;
    A) ALLOCATION_MODE=$OPTARG ;;
  esac
done

export INCLUDE_PYTHON="${INCLUDE_PYTHON:-false}"
export USE_KIND="${USE_KIND:-false}"
export IMAGE_NAME="${IMAGE_NAME:-spark:armada}"
export ARMADA_MASTER="${ARMADA_MASTER:-armada://localhost:30002}"
export ARMADA_QUEUE="${ARMADA_QUEUE:-test}"
export ARMADA_AUTH_TOKEN=${ARMADA_AUTH_TOKEN:-}
export ARMADA_AUTH_SIGNIN_BINARY=${ARMADA_AUTH_SIGNIN_BINARY:-}
export ARMADA_AUTH_SIGNIN_ARGS=${ARMADA_AUTH_SIGNIN_ARGS:-}
export ARMADA_EVENT_WATCHER_USE_TLS=${ARMADA_EVENT_WATCHER_USE_TLS:-false}
export SPARK_BLOCK_MANAGER_PORT=${SPARK_BLOCK_MANAGER_PORT:-}
export SCALA_CLASS="${SCALA_CLASS:-org.apache.spark.examples.SparkPi}"
export RUNNING_E2E_TESTS="${RUNNING_E2E_TESTS:-false}"

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


shift $((OPTIND - 1))
FINAL_ARGS=("${@:-}")   

if [ ${#FINAL_ARGS[@]} -eq 0 ]; then
    FINAL_ARGS+=("100")
fi

if [ "$INCLUDE_PYTHON" == "true" ]; then WITH_PYTHON="-python3"; else WITH_PYTHON=""; fi
image_tag="$SPARK_VERSION-scala$SCALA_BIN_VERSION-java${JAVA_VERSION:-17}$WITH_PYTHON-ubuntu"
