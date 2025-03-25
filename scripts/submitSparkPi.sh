#!/bin/bash

source scripts/init.sh

# Ensure queue exists on Armada
if ! armadactl get queue $ARMADA_QUEUE >& /dev/null; then
  armadactl create queue $ARMADA_QUEUE
fi

# run spark pi app on armada.  Note we are using the "ArmadaSparkSubmit" class below
#  instead of the standard "SparkSubmit".  The rest of the parameters are standard spark
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
  --master $ARMADA_MASTER --deploy-mode cluster \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.container.image=spark:$IMAGE_NAME \
  local:///opt/spark/examples/jars/spark-examples_${SCALA_BIN_VERSION}-${SPARK_VERSION}.jar 100
