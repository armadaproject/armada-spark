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
    "${APP_ID_CONF[@]}"
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
SPARK_SUBMIT_ARGS+=("${DISTRIBUTED_SHUFFLE_STORAGE_CONF[@]}")

# Add deploy mode args
SPARK_SUBMIT_ARGS+=("${DEPLOY_MODE_ARGS[@]}")

# Add auth args
SPARK_SUBMIT_ARGS+=("${ARMADA_AUTH_ARGS[@]}")

# Add OAuth conf (when OAUTH_ENABLED=true)
if [ "${#OAUTH_CONF[@]}" -gt 0 ]; then
    SPARK_SUBMIT_ARGS+=("${OAUTH_CONF[@]}")
fi

# Add event log conf
SPARK_SUBMIT_ARGS+=("${EVENT_LOG_CONF[@]}")

# Add extra conf
SPARK_SUBMIT_ARGS+=("${EXTRA_CONF[@]}")

# Add application and final args
SPARK_SUBMIT_ARGS+=($FIRST_ARG "${FINAL_ARGS[@]}")

# Submit the spark job and print "<jobSetId> <driverJobId>" on stdout.
# All other output (docker stdout/stderr, status messages) is routed to stderr.
submit_job() {
    local submit_log
    submit_log=$(mktemp)
    trap "rm -f $submit_log" EXIT

    docker run "${DOCKER_ENV_ARGS[@]}" -v $scripts/../conf:/opt/spark/conf --rm --network host $IMAGE_NAME \
        /opt/spark/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}" \
        1>&2 2> >(tee "$submit_log" >&2)

    # Format: "Submitted driver job with ID: <jobSetId>:<jobId>"
    local job_info
    job_info=$(grep -oP "Submitted driver job with ID: \K[^\s,]+" "$submit_log" | tail -1)
    if [ -z "$job_info" ]; then
        echo "ERROR: Could not extract job ID from spark-submit output" >&2
        exit 1
    fi

    echo "${job_info%%:*} ${job_info##*:}"
}

# Watch the given job-set until the driver job reaches a terminal state.
# The watch passes iff the driver job specifically succeeds — executor jobs
# in the same job-set may fail/cancel independently and that's not our concern.
# Args: $1 = job_set_id, $2 = driver_job_id
watch_job() {
    local job_set_id="$1"
    local driver_job_id="$2"
    echo "Watching driver job $driver_job_id (job-set $job_set_id) on queue $ARMADA_QUEUE..." >&2

    local armada_url="${ARMADA_MASTER#armada://}"
    local max_retries=12
    local retry_delay=5
    local attempt watch_log
    for attempt in $(seq 1 $max_retries); do
        watch_log=$(mktemp)
        armadactl watch "$ARMADA_QUEUE" "$job_set_id" \
            --armadaUrl "$armada_url" \
            --exit-if-inactive 2>&1 | tee "$watch_log" >&2 || true

        # armadactl watch event lines look like:
        #   JobSucceededEvent, job id: 01ksn... | ...
        #   JobFailedEvent,    job id: 01ksn... | ...
        # Filter to the driver's job id only — ignore executor terminal events.
        local driver_events
        driver_events=$(grep -E "Job(Succeeded|Failed|Cancelled)Event.*job id: $driver_job_id" "$watch_log" || true)

        if grep -qE "Job(Succeeded|Failed|Cancelled)Event" <<< "$driver_events"; then
            if grep -qE "JobSucceededEvent" <<< "$driver_events"; then
                echo "Driver job $driver_job_id SUCCEEDED" >&2
                rm -f "$watch_log"
                exit 0
            else
                echo "Driver job $driver_job_id FAILED" >&2
                rm -f "$watch_log"
                exit 1
            fi
        fi
        rm -f "$watch_log"

        if [ "$attempt" -lt "$max_retries" ]; then
            echo "Driver job not yet terminal (attempt $attempt/$max_retries), retrying in ${retry_delay}s..." >&2
            sleep $retry_delay
        fi
    done

    echo "ERROR: Timed out waiting for driver job $driver_job_id to terminate" >&2
    exit 1
}

# Dispatch on deploy mode: cluster mode submits and watches; client mode just runs.
run() {
    if [ "$DEPLOY_MODE" = "cluster" ]; then
        local job_set_id driver_job_id
        read -r job_set_id driver_job_id < <(submit_job)
        watch_job "$job_set_id" "$driver_job_id"
    else
        docker run "${DOCKER_ENV_ARGS[@]}" -v $scripts/../conf:/opt/spark/conf --rm --network host $IMAGE_NAME \
            /opt/spark/bin/spark-submit "${SPARK_SUBMIT_ARGS[@]}"
    fi
}

run
