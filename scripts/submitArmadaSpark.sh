#!/bin/bash
set -euo pipefail

echo Submitting spark job to Armada.

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

echo "Deploy Mode: $DEPLOY_MODE"
echo "Allocation Mode: $ALLOCATION_MODE"

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
# Build configuration based on allocation mode
EXTRA_CONF=(
    --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    --conf spark.executor.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
)
if [ "$STATIC_MODE" = true ]; then
    echo running static mode
    EXTRA_CONF+=("${STATIC_ALLOC_CONF[@]}")
else
    echo running dynamic mode
    EXTRA_CONF+=("${DYNAMIC_ALLOC_CONF[@]}")
fi

# Run Armada Spark via docker image
SPARK_SUBMIT_ARGS=(
    --master $ARMADA_MASTER
    --deploy-mode $DEPLOY_MODE
    --name $NAME
    $CLASS_PROMPT $CLASS_ARG
    "${S3_CONF[@]}" \
    "${ARMADA_COMMON_CONF[@]}" \
)

# Add fallback storage / decommission conf if enabled
SPARK_SUBMIT_ARGS+=("${FALLBACK_STORAGE_CONF[@]}")

# Add deploy mode args
SPARK_SUBMIT_ARGS+=("${DEPLOY_MODE_ARGS[@]}")

# Add auth args
SPARK_SUBMIT_ARGS+=("${ARMADA_AUTH_ARGS[@]}")

# Add event log conf
SPARK_SUBMIT_ARGS+=("${EVENT_LOG_CONF[@]}")

# Add extra conf
SPARK_SUBMIT_ARGS+=("${EXTRA_CONF[@]}")

# Add application and final args
SPARK_SUBMIT_ARGS+=($FIRST_ARG "${FINAL_ARGS[@]}")

docker run "${DOCKER_ENV_ARGS[@]}" -v $scripts/../conf:/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}"
