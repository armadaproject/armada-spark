#!/bin/bash

set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts"/init.sh

if docker image inspect "spark-py:$image_tag" >/dev/null 2>/dev/null; then
     image_prefix=spark-py
else
     image_prefix=apache/spark
fi

#Generate data for extraFiles directory
rm -rf $scripts/../extraFiles/*
echo Copying extrafiles from $image_prefix:$image_tag
id=$(docker create $image_prefix:$image_tag)
docker cp $id:/opt/spark/examples/jars  $scripts/../extraFiles
docker cp $id:/opt/spark/examples/src  $scripts/../extraFiles
docker rm -v $id
