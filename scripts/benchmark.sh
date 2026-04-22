#!/bin/bash
set -euo pipefail

# Parse -K, (Kubernetes benchmark,) flag before sourcing init.sh (which consumes its own flags)
USE_K8S=false
for arg in "$@"; do
    if [ "$arg" = "-K" ]; then
        USE_K8S=true
        break
    fi
done
# Remove -K from args so init.sh doesn't see it
filtered_args=()
for arg in "$@"; do
    if [ "$arg" != "-K" ]; then
        filtered_args+=("$arg")
    fi
done
# Replace $@ with filtered args (minus -K) for init.sh;
set -- "${filtered_args[@]+"${filtered_args[@]}"}"

SPARK_CONF_DIR=/opt/spark/conf
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

if [ "$USE_K8S" = true ]; then
    echo "Submitting spark benchmark job to Kubernetes."
    BENCHMARK_MASTER=$K8S_MASTER
    BENCHMARK_RESULTS_DIR=$ARMADA_S3_USER_DIR/benchmark/kresults
    DOCKER_ENV_ARGS+=(-e "KUBECONFIG=/opt/spark/conf/kubeconfig")
    K8S_CONF=(
        --conf spark.kubernetes.authenticate.driver.serviceAccountName="${K8S_SERVICE_ACCOUNT:-spark-sa}"
        --conf spark.kubernetes.container.image=$IMAGE_NAME
    )
else
    echo "Submitting spark benchmark job to Armada."
    BENCHMARK_MASTER=$ARMADA_MASTER
    BENCHMARK_RESULTS_DIR=$ARMADA_S3_USER_DIR/benchmark/results
    K8S_CONF=()
fi

QUICK_TEST="q41-v2.4"
LONGER_TEST="q7-v2.4"
MULTIPLE_TESTS="q41-v2.4,q10-v2.4,q12-v2.4,q13-v2.4,q14a-v2.4,q14b-v2.4,q11-v2.4,q15-v2.4,q16-v2.4,q17-v2.4,q18-v2.4,q19-v2.4,q20-v2.4,q21-v2.4,q22-v2.4,q23a-v2.4" 
ALL_TESTS=""
TESTS_TO_RUN=$QUICK_TEST
#TESTS_TO_RUN=$MULTIPLE_TESTS

JOBSET="${JOBSET:-armada-spark-benchmark}"

docker run "${DOCKER_ENV_ARGS[@]}" -v $scripts/../benchmark:/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-submit \
    --master $BENCHMARK_MASTER --deploy-mode cluster \
    --name spark-benchmark \
    --class $ARMADA_BENCHMARK_CLASS \
    "${S3_CONF[@]}" \
    "${ARMADA_AUTH_ARGS[@]}" \
    "${K8S_CONF[@]}" \
    "${DYNAMIC_ALLOC_CONF[@]}" \
    "${FALLBACK_STORAGE_CONF[@]}" \
    --conf spark.sql.shuffle.partitions=1000 \
    --conf spark.dynamicAllocation.executorIdleTimeout=60 \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=1 \
    --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=240 \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=40 \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.hadoop.fs.s3a.bucket.$ARMADA_S3_BUCKET_NAME.endpoint=$ARMADA_S3_BUCKET_ENDPOINT \
    --conf spark.armada.queue=$ARMADA_QUEUE \
    --conf spark.armada.internalUrl=$ARMADA_INTERNAL_URL \
    $ARMADA_BENCHMARK_JAR \
    $ARMADA_BENCHMARK_DATA \
    $BENCHMARK_RESULTS_DIR \
    $ARMADA_BENCHMARK_TOOLS \
    parquet \
    1g \
    1 \
    false \
    $TESTS_TO_RUN \
    false true
