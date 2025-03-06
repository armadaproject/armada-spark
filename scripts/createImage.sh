#!/bin/bash
set -e

# generate armada docker image
#  run like so: scripts/createImage.sh /home/gbj/incoming/spark testing 2.13
SPARK_ROOT=$1
IMAGE_NAME=$2
SCALA_BIN_VERSION=$3

# Get the dependencies to be copied into docker image
mvn --batch-mode package dependency:copy-dependencies
dependencies=(
    target/armada-cluster-manager_${SCALA_BIN_VERSION}-1.0.0-SNAPSHOT.jar
    target/dependency/lenses_${SCALA_BIN_VERSION}-0.11.13.jar
    target/dependency/scalapb-runtime_${SCALA_BIN_VERSION}-0.11.13.jar
    target/dependency/scalapb-runtime-grpc_${SCALA_BIN_VERSION}-0.11.13.jar
    target/dependency/armada-scala-client_${SCALA_BIN_VERSION}-0.1.0-SNAPSHOT.jar
)

# Copy dependencies to the docker image directory
cp "${dependencies[@]}" $SPARK_ROOT/assembly/target/scala-${SCALA_BIN_VERSION}/jars/

# Make the image
cd $SPARK_ROOT
./bin/docker-image-tool.sh -t $IMAGE_NAME build
