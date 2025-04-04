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
    echo '   -i  <image-name>'
    echo '   -m  <armada-master-url>'
    echo '   -q  <armada-queue>'
    echo '   -l  <armada-lookout-url>'
    echo ''
    echo 'You also can specify those parameters in scripts/config.sh, like so:'
    echo '   IMAGE_NAME=spark:armada'
    echo '   ARMADA_MASTER=armada://localhost:30002'
    echo '   ARMADA_QUEUE=test'
    echo '   ARMADA_LOOKOUT_URL=http://localhost:30000'
    exit 1
}

while getopts "hs:i:l:m:q" opt; do
  case "$opt" in
    h) print_usage ;;
    i) IMAGE_NAME=$OPTARG ;;
    m) ARMADA_MASTER=$OPTARG ;;
    q) ARMADA_QUEUE=$OPTARG ;;
    l) ARMADA_LOOKOUT_URL=$OPTARG ;;
  esac
done

export IMAGE_NAME="${IMAGE_NAME:-spark:armada}"
export ARMADA_MASTER="${ARMADA_MASTER:-armada://localhost:30002}"
export ARMADA_QUEUE="${ARMADA_QUEUE:-test}"
export ARMADA_LOOKOUT_URL="${ARMADA_LOOKOUT_URL:-http://localhost:30000}"

# derive Scala and Spark versions from pom.xml, set via ./scripts/set-version.sh
if [[ -z "${SCALA_VERSION:-}" ]]; then
  export SCALA_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=scala.version -q -DforceStdout)
  export SCALA_BIN_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=scala.binary.version -q -DforceStdout)
fi
if [[ -z "${SPARK_VERSION:-}" ]]; then
  export SPARK_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=spark.version -q -DforceStdout)
  export SPARK_BIN_VERSION=$(cd "$scripts/.."; mvn help:evaluate -Dexpression=spark.binary.version -q -DforceStdout)
fi

# check the Spark version is supported
if ! [ -e "$root/src/main/scala-spark-$SPARK_BIN_VERSION" ]; then
    echo "This tool does not support Spark version ${SPARK_VERSION}."
    exit 1
fi
