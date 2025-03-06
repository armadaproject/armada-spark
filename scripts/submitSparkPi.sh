#!/bin/bash

#command should be run like so:
#  examples/submitSparkPi.sh armada://localhost:30002 ~/incoming/spark

source scripts/config.sh
ARMADA_SPARK_ROOT=`pwd`
# Ensure our "test" queue exists on Armada
if ! armadactl get queue $ARMADA_QUEUE >& /dev/null; then
  armadactl create queue $ARMADA_QUEUE
fi

# run spark pi app on armada.  Note we are using the "ArmadaSparkSubmit" class below
#  instead of the standard "SparkSubmit".  The rest of the parameters are standard spark
$SPARK_ROOT/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
  --master $ARMADA_MASTER --deploy-mode cluster \
  --jars $ARMADA_SPARK_ROOT/target/armada-cluster-manager-1.0.0-SNAPSHOT.jar \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.container.image=spark:$IMAGE_NAME \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.3.4.jar 100
