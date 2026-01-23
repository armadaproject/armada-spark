#!/bin/bash
set -euo pipefail

echo Submitting spark benchmark job to Armada.

unset SPARK_CONF_DIR
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

QUICK_TEST="q41-v2.4" 
LONGER_TEST="q7-v2.4"  
MULTIPLE_TESTS="q41-v2.4,q10-v2.4,q12-v2.4,q13-v2.4,q14a-v2.4,q14b-v2.4,q11-v2.4,q15-v2.4,q16-v2.4,q17-v2.4,q18-v2.4,q19-v2.4,q20-v2.4,q21-v2.4,q22-v2.4,q23a-v2.4" 
ALL_TESTS=""
TESTS_TO_RUN=$MULTIPLE_TESTS

JOBSET="${JOBSET:-armada-spark-benchmark}"
    EXTRA_CONF=(
        --conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        --conf spark.executor.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
        --conf spark.armada.executor.limit.memory=60Gi
        --conf spark.armada.executor.request.memory=60Gi
        --conf spark.armada.driver.limit.memory=60Gi
        --conf spark.armada.driver.request.memory=60Gi
        --conf spark.default.parallelism=10
        --conf spark.executor.instances=1
        --conf spark.sql.shuffle.partitions=5
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.dynamicAllocation.minExecutors=1
        --conf spark.dynamicAllocation.maxExecutors=40
        --conf spark.dynamicAllocation.initialExecutors=1
        --conf spark.dynamicAllocation.executorIdleTimeout=10
        --conf spark.dynamicAllocation.schedulerBacklogTimeout=1
        --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=10
        --conf spark.decommission.enabled=true
        --conf spark.storage.decommission.enabled=true
        --conf spark.storage.decommission.shuffleBlocks.enabled=true
        --conf spark.storage.decommission.shuffleBlocks.maxDiskSize=0
        --conf spark.storage.decommission.fallbackStorage.cleanUp=true
        --conf spark.storage.decommission.fallbackStorage.proactive.enabled=true
        --conf spark.storage.decommission.fallbackStorage.proactive.reliable=true
    )

S3_CONF=()
if [[ $ARMADA_S3_ACCESS_KEY != "" ]]; then
    S3_CONF=(
        --conf spark.hadoop.fs.s3a.access.key=$ARMADA_S3_ACCESS_KEY
        --conf spark.hadoop.fs.s3a.secret.key=$ARMADA_S3_SECRET_KEY
    )
else if [[ $ARMADA_SPARK_SECRET_KEY != "" ]]; then
    S3_CONF=(
        --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=$ARMADA_SPARK_SECRET_KEY:secret_key
        --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=$ARMADA_SPARK_SECRET_KEY:secret_key
        --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=$ARMADA_SPARK_SECRET_KEY:access_key
        --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=$ARMADA_SPARK_SECRET_KEY:access_key
    )
     fi
fi
# Run Armada Spark via docker image
docker run --user 185 -v $scripts/../benchmark:/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-submit \
    --master $ARMADA_MASTER --deploy-mode cluster \
    --name spark-benchmark \
    --class $ARMADA_BENCHMARK_CLASS \
    "${EXTRA_CONF[@]}" \
    "${S3_CONF[@]}" \
    $AUTH_ARG \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.storage.decommission.fallbackStorage.path=$ARMADA_S3_USER_DIR/shuffle/ \
    --conf spark.hadoop.fs.s3a.bucket.$ARMADA_S3_BUCKET_NAME.endpoint=$ARMADA_S3_BUCKET_ENDPOINT \
    --conf spark.home=/opt/spark \
    --conf spark.armada.queue=$ARMADA_QUEUE \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.kubernetes.file.upload.path=/tmp \
    --conf spark.kubernetes.executor.disableConfigMap=true \
    --conf spark.local.dir=/tmp \
    --conf spark.armada.internalUrl=$ARMADA_INTERNAL_URL \
    $ARMADA_BENCHMARK_JAR \
    $ARMADA_BENCHMARK_DATA \
    $ARMADA_S3_USER_DIR/benchmark/results \
    $ARMADA_BENCHMARK_TOOLS \
    parquet \
    1g \
    1 \
    false \
    $TESTS_TO_RUN \
    false |& tee /tmp/output
