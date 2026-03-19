#!/bin/bash
set -euo pipefail

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

if [ -z "${ARMADA_EVENT_LOG_DIR:-}" ]; then
    echo "Error: ARMADA_EVENT_LOG_DIR is not set. Configure S3 credentials to enable event logging."
    exit 1
fi

HISTORY_OPTS="-Dspark.history.fs.logDirectory=${ARMADA_EVENT_LOG_DIR}"
HISTORY_OPTS+=" -Dspark.hadoop.fs.s3a.endpoint=${ARMADA_S3_BUCKET_ENDPOINT}"
HISTORY_OPTS+=" -Dspark.hadoop.fs.s3a.path.style.access=true"
if [[ ${AWS_ACCESS_KEY_ID:-} != "" ]]; then
    HISTORY_OPTS+=" -Dspark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}"
    HISTORY_OPTS+=" -Dspark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}"
fi

echo "Starting Spark History Server"
echo "  Log directory: ${ARMADA_EVENT_LOG_DIR}"
echo "  UI: http://localhost:18080"

docker run --name armada-spark-history-server "${DOCKER_ENV_ARGS[@]}" \
    -e "SPARK_HISTORY_OPTS=${HISTORY_OPTS}" \
    -v "$(pwd)/conf":/opt/spark/conf --rm --network host $IMAGE_NAME \
    /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer 2>&1 | {
    while IFS= read -r line; do
        echo "$line"
        if echo "$line" | grep -q "Log directory specified does not exist"; then
            echo ""
            echo "========================================="
            echo "Error: Event log directory does not exist: ${ARMADA_EVENT_LOG_DIR}"
            docker stop armada-spark-history-server 2>/dev/null
            exit 1
        fi
    done
}
