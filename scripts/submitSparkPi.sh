#!/bin/bash
set -euo pipefail

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

JOBSET="${JOBSET:-armada-spark}"

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
    --conf spark.armada.internalUrl=armada-server.armada:50051 \
    --conf spark.armada.queue=$ARMADA_QUEUE \
    --conf spark.armada.jobSetId="$JOBSET" \
    --conf spark.executor.instances=2 \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.armada.lookouturl=$ARMADA_LOOKOUT_URL \
    --conf spark.armada.scheduling.nodeUniformity=armada-spark \
    --conf spark.armada.scheduling.nodeSelectors=armada-spark=true \
    --conf spark.armada.pod.labels=foo=bar \
    --conf spark.armada.driver.limit.cores=200m \
    --conf spark.armada.driver.limit.memory=450Mi \
    --conf spark.armada.driver.request.cores=200m \
    --conf spark.armada.driver.request.memory=450Mi \
    --conf spark.armada.executor.limit.cores=100m \
    --conf spark.armada.executor.limit.memory=510Mi \
    --conf spark.armada.executor.request.cores=100m \
    --conf spark.armada.executor.request.memory=510Mi \
    $FIRST_ARG 100
