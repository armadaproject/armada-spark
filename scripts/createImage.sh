#/bin/bash
set -e

# generate armada docker image
#  run like so: scripts/createImage.sh /home/gbj/incoming/spark testing
SPARK_ROOT=$1
IMAGE_NAME=$2

# Get the dependencies to be copied into docker image
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

#Update entrypoint.sh if needed
MD5SUM_NEW_ENTRYPOINT=7cde88baa3c931f2dc16b46fdae296f2
MD5SUM_OLD_ENTRYPOINT=4d8ef1e0bfe568d30112406f2169028b
MD5SUM_CURRENT_ENTRYPOINT=`md5sum  $SPARK_ROOT/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh  | awk '{print $1}'`
if [ "$MD5SUM_CURRENT_ENTRYPOINT" != "$MD5SUM_NEW_ENTRYPOINT" ]; then
    if [ "$MD5SUM_CURRENT_ENTRYPOINT" = "$MD5SUM_OLD_ENTRYPOINT" ]; then
        cp scripts/entrypoint.sh $SPARK_ROOT/resource-managers/kubernetes/docker/src/main/dockerfiles/spark
    else
        echo Unexpected entrypoint found.  Exiting.
        exit 1
    fi
fi

# Make the image
cd $SPARK_ROOT
./bin/docker-image-tool.sh -t $IMAGE_NAME build

# Load the image into kind cluster
kind load docker-image spark:$IMAGE_NAME --name armada
