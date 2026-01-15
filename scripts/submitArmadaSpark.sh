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

if [ "$ARMADA_AUTH_TOKEN" != "" ]; then
    AUTH_ARG=" --conf spark.armada.auth.token=$ARMADA_AUTH_TOKEN"
else
    AUTH_ARG=""
fi

# Disable config maps until this is fixed: https://github.com/G-Research/spark/issues/109
DISABLE_CONFIG_MAP=true

# Set memory limits based on deploy mode
EXECUTOR_MEMORY_LIMIT="1Gi"
DRIVER_MEMORY_LIMIT="1Gi"

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

# Build deploy-mode specific arguments array
DEPLOY_MODE_ARGS=()
if [ "$DEPLOY_MODE" = "client" ]; then
    DEPLOY_MODE_ARGS=(
        --conf spark.driver.host=$SPARK_DRIVER_HOST
        --conf spark.driver.port=$SPARK_DRIVER_PORT
        --conf spark.driver.bindAddress=0.0.0.0
    )
else
    export ARMADA_INTERNAL_URL="${ARMADA_INTERNAL_URL:-armada://armada-server.armada:50051}"
    DEPLOY_MODE_ARGS=(
        --conf spark.armada.internalUrl=$ARMADA_INTERNAL_URL
    )
fi

# Run Armada Spark via docker image
docker run -e SPARK_PRINT_LAUNCH_COMMAND=true -v $scripts/../conf:/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-submit \
    --master $ARMADA_MASTER --deploy-mode $DEPLOY_MODE \
    --name $NAME \
    $CLASS_PROMPT $CLASS_ARG \
    $AUTH_ARG \
    --conf spark.home=/opt/spark \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.kubernetes.file.upload.path=/tmp \
    --conf spark.kubernetes.executor.disableConfigMap=$DISABLE_CONFIG_MAP \
    --conf spark.local.dir=/tmp \
    "${DEPLOY_MODE_ARGS[@]}" \
    "${EXTRA_CONF[@]}" \
    $FIRST_ARG  "${FINAL_ARGS[@]}"
