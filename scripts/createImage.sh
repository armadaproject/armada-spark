#/bin/bash
set -e
SPARK_ROOT=$1
IMAGE_NAME=$2

mvn --batch-mode package dependency:copy-dependencies
dependencies=(
    target/armada-cluster-manager-1.0.0-SNAPSHOT.jar
    target/dependency/lenses_2.13-0.11.13.jar
    target/dependency/scalapb-runtime_2.13-0.11.13.jar
    target/dependency/scalapb-runtime-grpc_2.13-0.11.13.jar
    target/dependency/scala-armada-client_2.13-0.1.0-SNAPSHOT.jar
)

# Copy dependencies to the docker image directory
for d in ${dependencies[@]}; do
    cp $d $SPARK_ROOT/assembly/target/scala-2.13/jars
done

cp scripts/entrypoint.sh $SPARK_ROOT/resource-managers/kubernetes/docker/src/main/dockerfiles/spark
cd $SPARK_ROOT
./bin/docker-image-tool.sh -t $IMAGE_NAME build
kind load docker-image spark:$IMAGE_NAME --name armada
