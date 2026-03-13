#!/bin/bash
set -euo pipefail

echo test --files parameter

source "scripts/config.gr.sh"

docker run \
  -v ~/incoming/files:/opt/files \
  -v ~/incoming/conf:/opt/spark/conf \
  -e ARMADA_AUTH_TOKEN=$ARMADA_AUTH_TOKEN \
  --rm --network host $IMAGE_NAME \
                          \
  /opt/spark/bin/spark-submit \
    --master $ARMADA_MASTER \
    --deploy-mode cluster \
    --name python-pi \
    --conf spark.armada.container.image=$IMAGE_NAME \
    --files /opt/files/lookup.csv \
    /opt/files/read_lines.py


