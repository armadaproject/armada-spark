#!/bin/bash
set -euo pipefail

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

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
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.container.image=$IMAGE_NAME \
    --conf spark.armada.lookouturl=$ARMADA_LOOKOUT_URL \
    local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar 100