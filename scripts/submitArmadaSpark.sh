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
# Disable config maps until this is fixed: https://github.com/G-Research/spark/issues/109
DISABLE_CONFIG_MAP=true

# Set memory limits
EXECUTOR_MEMORY_LIMIT="${EXECUTOR_MEMORY_LIMIT:-1Gi}"
DRIVER_MEMORY_LIMIT="${DRIVER_MEMORY_LIMIT:-1Gi}"

# Build configuration based on allocation mode
if [ "$STATIC_MODE" = true ]; then
    echo running static mode
    # Static mode: fixed executor count
    EXTRA_CONF=(
        --conf spark.executor.instances=2
        --conf spark.armada.executor.limit.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.executor.request.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.driver.limit.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.armada.driver.request.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        --conf spark.executor.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    )
else
    echo running dynamic mode
    # Dynamic mode: dynamic allocation with debug options
    EXTRA_CONF=(
        --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        --conf spark.executor.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        --conf spark.armada.scheduling.namespace=${ARMADA_NAMESPACE:-default}
        --conf spark.armada.executor.limit.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.executor.request.memory=$EXECUTOR_MEMORY_LIMIT
        --conf spark.armada.driver.limit.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.armada.driver.request.memory=$DRIVER_MEMORY_LIMIT
        --conf spark.default.parallelism=10
        --conf spark.executor.instances=1
        --conf spark.sql.shuffle.partitions=5
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.dynamicAllocation.minExecutors=1
        --conf spark.dynamicAllocation.maxExecutors=4
        --conf spark.dynamicAllocation.initialExecutors=1
        --conf spark.dynamicAllocation.executorIdleTimeout=5
        --conf spark.dynamicAllocation.schedulerBacklogTimeout=5
        --conf spark.decommission.enabled=true
        --conf spark.storage.decommission.enabled=true
        --conf spark.storage.decommission.shuffleBlocks.enabled=true
    )
fi

# Run Armada Spark via docker image
SPARK_SUBMIT_ARGS=(
    --master $ARMADA_MASTER
    --deploy-mode $DEPLOY_MODE
    --name $NAME
    $CLASS_PROMPT $CLASS_ARG
    --conf spark.home=/opt/spark
    --conf spark.armada.container.image=$IMAGE_NAME
    --conf spark.armada.queue=$ARMADA_QUEUE
    --conf spark.armada.lookouturl=${ARMADA_LOOKOUT_URL:-http://localhost:30000}
    --conf spark.kubernetes.file.upload.path=/tmp
    --conf spark.kubernetes.executor.disableConfigMap=$DISABLE_CONFIG_MAP
    --conf spark.local.dir=/tmp
    --conf spark.storage.decommission.fallbackStorage.path=$ARMADA_S3_USER_DIR/shuffle/
)

# Add deploy mode args
SPARK_SUBMIT_ARGS+=("${DEPLOY_MODE_ARGS[@]}")

# Add auth args
SPARK_SUBMIT_ARGS+=("${ARMADA_AUTH_ARGS[@]}")

# Add extra conf
SPARK_SUBMIT_ARGS+=("${EXTRA_CONF[@]}")

# Add application and final args
SPARK_SUBMIT_ARGS+=($FIRST_ARG "${FINAL_ARGS[@]}")

docker run "${DOCKER_ENV_ARGS[@]}" -v $scripts/../conf:/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}"
