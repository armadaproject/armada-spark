#!/bin/bash
set -euo pipefail

echo Submitting spark job to Armada.

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

JOBSET="${JOBSET:-armada-spark}"

if [ "${INCLUDE_PYTHON}" == "false" ]; then
    NAME=spark-pi
    CLASS_PROMPT="--class"
    CLASS_ARG="$SCALA_CLASS"
    FIRST_ARG="$CLASS_PATH"
    echo Running Scala Spark: $CLASS_ARG $FIRST_ARG "${FINAL_ARGS[@]}"
else
    NAME=python-pi
    CLASS_PROMPT=""
    CLASS_ARG=""
    FIRST_ARG="$PYTHON_SCRIPT"
    echo Running Python Spark: $FIRST_ARG "${FINAL_ARGS[@]}"
fi

if [ "${USE_KIND}" == "true" ]; then
    # Ensure queue exists on Armada
    if ! armadactl get queue $ARMADA_QUEUE >& /dev/null; then
        armadactl create queue $ARMADA_QUEUE
    fi

    # needed by kind load docker-image (if docker is installed via snap)
    # https://github.com/kubernetes-sigs/kind/issues/2535
    export TMPDIR="$scripts/.tmp"
    mkdir -p "$TMPDIR"
    kind load docker-image $IMAGE_NAME --name armada
fi

if [ "$ARMADA_AUTH_TOKEN" != "" ]; then
    AUTH_ARG=" --conf spark.armada.auth.token=$ARMADA_AUTH_TOKEN"
else
    AUTH_ARG=""
fi

# Disable config maps until this is fixed: https://github.com/G-Research/spark/issues/109
DISABLE_CONFIG_MAP=true

# Run Armada Spark via docker image
docker run -v $scripts/../conf:/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
    --master $ARMADA_MASTER --deploy-mode cluster \
    --name $NAME \
    $CLASS_PROMPT $CLASS_ARG \
    $AUTH_ARG \
    --conf spark.home=/opt/spark \
    --conf spark.executor.instances=2 \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.armada.jobSetId="$JOBSET" \
    --conf spark.armada.scheduling.nodeUniformity=armada-spark \
    --conf spark.kubernetes.container.image=$IMAGE_NAME \
    --conf spark.kubernetes.file.upload.path=/tmp \
    --conf spark.kubernetes.executor.disableConfigMap=$DISABLE_CONFIG_MAP \
    --conf spark.local.dir=/tmp \
    $FIRST_ARG  "${FINAL_ARGS[@]}"
