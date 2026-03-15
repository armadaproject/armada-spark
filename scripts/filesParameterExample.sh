#!/bin/bash
set -euo pipefail

echo test --files parameter

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

EXAMPLE_FILES_DIR="example/files"
mkdir -p "$EXAMPLE_FILES_DIR"

cat > "$EXAMPLE_FILES_DIR/lookup.csv" <<'EOF'
bdeee1ec-9f0f-4280-91ca-ade78f37c1fa,executor1-1,executor1-2,executor1-3
bdeee1ec-9f0f-4280-91ca-ade78f37c1fa,executor2-1,executor2-2,executor2-3
EOF

cat > "$EXAMPLE_FILES_DIR/read_lines.py" <<'EOF'
import time

from pyspark import SparkFiles
from pyspark.sql import SparkSession


def read_line(line_num):
    path = SparkFiles.get("lookup.csv")
    with open(path) as f:
        for i, line in enumerate(f):
            if i == line_num:
                print(line.rstrip())
                time.sleep(10)
                return


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LookupLineReader").getOrCreate()
    sc = spark.sparkContext

    with open(SparkFiles.get("lookup.csv")) as f:
        num_lines = sum(1 for _ in f)

    sc.parallelize(range(num_lines), num_lines).foreach(read_line)

    spark.stop()
EOF

docker run \
  -v "$(pwd)/$EXAMPLE_FILES_DIR":/opt/files \
  -v ~/incoming/conf:/opt/spark/conf \
  "${DOCKER_ENV_ARGS[@]}" \
  --rm --network host $IMAGE_NAME \
                          \
  /opt/spark/bin/spark-submit \
    --master $ARMADA_MASTER \
    --deploy-mode cluster \
    --name python-pi \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --conf spark.kubernetes.file.upload.path=$ARMADA_S3_USER_DIR/tmp \
    --files /opt/files/lookup.csv \
    /opt/files/read_lines.py
