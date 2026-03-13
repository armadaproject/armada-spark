#!/bin/bash
set -euo pipefail

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

docker run -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY  -v $(pwd)/conf:/opt/spark/conf --rm --network host $IMAGE_NAME /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
