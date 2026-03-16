#!/bin/bash
set -euo pipefail

echo test --files parameter

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

EXAMPLE_FILES_DIR="example/files"
mkdir -p "$EXAMPLE_FILES_DIR"

RUN_UUID=$(uuidgen)
echo "Run UUID: $RUN_UUID"

cat > "$EXAMPLE_FILES_DIR/lookup.csv" <<EOF
$RUN_UUID,line1
$RUN_UUID,line2
EOF

cat > "$EXAMPLE_FILES_DIR/read_lines.py" <<EOF
import time

from pyspark import SparkFiles
from pyspark.sql import SparkSession


def read_line(line_num):
    path = SparkFiles.get("lookup.csv")
    with open(path) as f:
        # when we reach this executors line print it out
        for i, line in enumerate(f):
            if i == line_num:
                print('executor uuid is: ' + line.rstrip())
                time.sleep(10)
                return


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LookupLineReader").getOrCreate()
    sc = spark.sparkContext

    uuid = '$RUN_UUID'
    print('starting uuid: ' + uuid)


    with open(SparkFiles.get("lookup.csv")) as f:
        num_lines = sum(1 for _ in f)

    # send out a line number for each line
    sc.parallelize(range(num_lines), num_lines).foreach(read_line)

    spark.stop()
EOF

docker run \
  -v "$(pwd)/$EXAMPLE_FILES_DIR":/opt/files \
  "${DOCKER_ENV_ARGS[@]}" \
  --rm --network host $IMAGE_NAME \
                          \
  /opt/spark/bin/spark-submit \
    --master $ARMADA_MASTER \
    --deploy-mode cluster \
    --name files-paramater-example \
    --conf spark.armada.queue=$ARMADA_QUEUE \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.kubernetes.file.upload.path=$ARMADA_S3_USER_DIR/tmp \
    --conf spark.hadoop.fs.s3a.endpoint=$ARMADA_S3_BUCKET_ENDPOINT \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    "${S3_CONF[@]}" \
    "${ARMADA_AUTH_ARGS[@]}" \
    --files /opt/files/lookup.csv \
    /opt/files/read_lines.py
