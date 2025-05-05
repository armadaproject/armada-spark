#!/bin/bash
set -euo pipefail

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

if [ "${INCLUDE_PYTHON}" == "false" ]; then
    echo Running Scala Spark
    NAME=spark-pi
    CLASS_PROMPT="--class"
    CLASS_ARG=org.apache.spark.examples.SparkPi
    FIRST_ARG="local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar"
else
    echo Running Python Spark
    NAME=python-pi
    CLASS_PROMPT=""
    CLASS_ARG=""
    FIRST_ARG=/opt/spark/examples/src/main/python/pi.py
fi

# Ensure queue exists on Armada
if ! armadactl get queue $ARMADA_QUEUE >& /dev/null; then
  armadactl create queue $ARMADA_QUEUE
fi

# needed by kind load docker-image (if docker is installed via snap)
# https://github.com/kubernetes-sigs/kind/issues/2535
export TMPDIR="$scripts/.tmp"
mkdir -p "$TMPDIR"
kind load docker-image $IMAGE_NAME --name armada

# run spark pi example via Docker image
docker run --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
    --master $ARMADA_MASTER --deploy-mode cluster \
    --name $NAME \
    $CLASS_PROMPT $CLASS_ARG \
    --conf spark.executor.instances=4 \
    --conf spark.kubernetes.container.image=$IMAGE_NAME \
    --conf spark.armada.lookouturl=$ARMADA_LOOKOUT_URL \
    --conf spark.armada.clusterSelectors="armada-spark=true" \
    $FIRST_ARG 100
