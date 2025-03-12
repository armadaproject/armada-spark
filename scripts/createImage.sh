#!/bin/bash
set -e

# generate armada docker image

source scripts/init.sh

# Set this project's Spark and Scala version according to the Spark sources in $SPARK_HOME
./scripts/set-version.sh $SPARK_VERSION $SCALA_VERSION

# Get the dependencies to be copied into docker image
mvn --batch-mode clean package dependency:copy-dependencies
dependencies=(
    target/armada-cluster-manager_${SCALA_BIN_VERSION}-1.0.0-SNAPSHOT.jar
    target/dependency/lenses_${SCALA_BIN_VERSION}-0.11.13.jar
    target/dependency/scalapb-runtime_${SCALA_BIN_VERSION}-0.11.13.jar
    target/dependency/scalapb-runtime-grpc_${SCALA_BIN_VERSION}-0.11.13.jar
    target/dependency/armada-scala-client_${SCALA_BIN_VERSION}-0.1.0-SNAPSHOT.jar
)

spark_v3_dependencies=(
    target/dependency/grpc-api-1.47.1.jar
    target/dependency/grpc-core-1.47.1.jar
    target/dependency/grpc-netty-1.47.1.jar
    target/dependency/grpc-protobuf-1.47.1.jar
    target/dependency/grpc-context-1.47.1.jar
    target/dependency/grpc-stub-1.47.1.jar
    target/dependency/guava-31.0.1-android.jar
    target/dependency/failureaccess-1.0.1.jar
    target/dependency/perfmark-api-0.25.0.jar
)

spark_v33_dependencies=(
    target/dependency/netty-codec-http2-4.1.72.Final.jar
    target/dependency/netty-codec-http-4.1.72.Final.jar
    target/dependency/protobuf-java-3.19.6.jar
)

if [[ $SPARK_VERSION == 3* ]]; then
    dependencies+=("${spark_v3_dependencies[@]}")
fi

if [[ $SPARK_VERSION == "3.3."* ]]; then
    dependencies+=("${spark_v33_dependencies[@]}")
fi


# These jar version don't work with the Armada client.  Replace them with newer versions below.
if [ -e  $SPARK_HOME/assembly/target/scala-${SCALA_BIN_VERSION}/jars/guava-14.0.1.jar ]; then
    # will be replaced with: guava-31.0.1-android.jar
    rm $SPARK_HOME/assembly/target/scala-${SCALA_BIN_VERSION}/jars/guava-14.0.1.jar
fi

if [ -e  $SPARK_HOME/assembly/target/scala-${SCALA_BIN_VERSION}/jars/protobuf-java-2.5.0.jar ]; then
    # will be replaced with: protobuf-java-3.19.6.jar
    rm $SPARK_HOME/assembly/target/scala-${SCALA_BIN_VERSION}/jars/protobuf-java-2.5.0.jar
fi


# Copy dependencies to the docker image directory
cp "${dependencies[@]}" $SPARK_HOME/assembly/target/scala-${SCALA_BIN_VERSION}/jars/


# Make the image
cd $SPARK_HOME
./bin/docker-image-tool.sh -t $IMAGE_NAME build
