#!/bin/bash

#command should be run like so:
#  examples/submitSparkPi.sh armada://localhost:30002 ~/incoming/spark

ARMADA_MASTER=$1
SPARK_ROOT=$2
ARMADA_SPARK_ROOT=`pwd`
# run spark pi app on armada
$SPARK_ROOT/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
  --master $ARMADA_MASTER --deploy-mode cluster \
  --jars $ARMADA_SPARK_ROOT/target/armada-cluster-manager-1.0.0-SNAPSHOT.jar  --name spark-pi --class org.apache.spark.examples.SparkPi --conf spark.executor.instances=2 --conf spark.kubernetes.container.image=spark:testing local:///opt/spark/examples/jars/spark-examples.jar 100
