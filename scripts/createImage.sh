#/bin/bash
set -e
SPARK_ROOT=$1
IMAGE_NAME=$2
MD5SUM_NEW_ENTRYPOINT=5ca946a98b408e2475e86f8c506f449e
MD5SUM_OLD_ENTRYPOINT=4d8ef1e0bfe568d30112406f2169028b

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
MD5SUM_CURRENT_ENTRYPOINT=`md5sum  $SPARK_ROOT/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh  | awk '{print $1}'`
if [ "$MD5SUM_CURRENT_ENTRYPOINT" != "$MD5SUM_NEW_ENTRYPOINT" ]; then
    if [ "$MD5SUM_CURRENT_ENTRYPOINT" = "$MD5SUM_OLD_ENTRYPOINT" ]; then
        cp scripts/entrypoint.sh $SPARK_ROOT/resource-managers/kubernetes/docker/src/main/dockerfiles/spark
    else
        echo Unexpected entrypoint found.  Exiting.
        exit 1
    fi
fi

cd $SPARK_ROOT
./bin/docker-image-tool.sh -t $IMAGE_NAME build
kind load docker-image spark:$IMAGE_NAME --name armada
