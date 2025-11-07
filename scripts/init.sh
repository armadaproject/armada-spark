#!/bin/bash

# Utility functions

root="$(cd "$(dirname "$0")/.."; pwd)"
scripts="$(cd "$(dirname "$0")"; pwd)"

if [ -e "$scripts/config.sh" ]; then
    source "$scripts/config.sh"
fi

print_usage () {
    echo ' Usage:'
    echo '   -h  help'
    echo '   -k  "use kind cluster"'
    echo '   -p  "build image with python"'
    echo '   -i  <image-name>'
    echo '   -m  <armada-master-url>'
    echo '   -q  <armada-queue>'
    echo '   -P  <python script to run>'
    echo '   -s  <scala class to run>'
    echo '   -c  <class path to use>'
    echo '   -e  running e2e tests'
    echo ''
    echo 'You also can specify those parameters in scripts/config.sh, like so:'
    echo '   IMAGE_NAME=spark:armada'
    echo '   ARMADA_MASTER=armada://localhost:30002'
    echo '   ARMADA_QUEUE=test'
    echo '   USE_KIND=true'
    echo '   INCLUDE_PYTHON=true'
    echo '   PYTHON_SCRIPT=/opt/spark/extraFiles/test-basic-shuffle.py'
    echo '   SCALA_CLASS=org.apache.spark.examples.SparkPi'
    echo '   CLASS_PATH=local:///opt/spark/extraFiles/spark-examples_2.12-3.5.3.jar'
    exit 1
}

while getopts "hekpi:a:m:P:s:c:q:" opt; do
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
  esac
done

export INCLUDE_PYTHON="${INCLUDE_PYTHON:-false}"
export USE_KIND="${USE_KIND:-false}"
export IMAGE_NAME="${IMAGE_NAME:-spark:armada}"
export ARMADA_MASTER="${ARMADA_MASTER:-armada://localhost:30002}"
export ARMADA_QUEUE="${ARMADA_QUEUE:-test}"
export ARMADA_AUTH_TOKEN=${ARMADA_AUTH_TOKEN:-}
export SCALA_CLASS="${SCALA_CLASS:-org.apache.spark.examples.EnricoTest}"
export RUNNING_E2E_TESTS="${RUNNING_E2E_TESTS:-false}"

if [ -z "${PYTHON_SCRIPT:-}" ]; then
    PYTHON_SCRIPT="/opt/spark/conf/next7.py"
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
