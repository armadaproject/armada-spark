#/bin/bash
set -e
SPARK_ROOT=$1
IMAGE_NAME=$2
mvn --batch-mode package
cp target/armada-cluster-manager-1.0.0-SNAPSHOT.jar /home/gbj/incoming/spark/assembly/target/scala-2.13/jars
cp ~/.m2/repository/com/thesamet/scalapb/lenses_2.13/0.11.17/lenses_2.13-0.11.17.jar /home/gbj/incoming/spark/assembly/target/scala-2.13/jars
cp ~/.m2/repository/com/thesamet/scalapb/scalapb-runtime_2.13/0.11.17/scalapb-runtime_2.13-0.11.17.jar /home/gbj/incoming/spark/assembly/target/scala-2.13/jars
cp ~/.m2/repository/com/thesamet/scalapb/scalapb-runtime-grpc_2.13/0.11.13/scalapb-runtime-grpc_2.13-0.11.13.jar /home/gbj/incoming/spark/assembly/target/scala-2.13/jars
cp ~/.m2/repository/io/armadaproject/armada/scala-armada-client_2.13/0.1.0-SNAPSHOT/scala-armada-client_2.13-0.1.0-SNAPSHOT.jar /home/gbj/incoming/spark/assembly/target/scala-2.13/jars
cp scripts/entrypoint.sh $SPARK_ROOT/resource-managers/kubernetes/docker/src/main/dockerfiles/spark
cd $SPARK_ROOT
./bin/docker-image-tool.sh -t $IMAGE_NAME build
kind load docker-image spark:$IMAGE_NAME --name armada
