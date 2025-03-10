#!/bin/bash
set -e

# generate armada docker image
#  run like so: scripts/createImage.sh /home/gbj/incoming/spark testing 2.13
SPARK_ROOT=$1
IMAGE_NAME=$2

# Get the dependencies to be copied into docker image
mvn --batch-mode package

# Make the image
cd $SPARK_ROOT
./bin/docker-image-tool.sh -t $IMAGE_NAME build
