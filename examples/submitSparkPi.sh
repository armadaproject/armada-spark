#!/bin/bash

#command should be run like so:
#  examples/submitSparkPi.sh local://armada://localhost:30002 ~/incoming/spark

ARMADA_MASTER=$1
SPARK_ROOT=$2
ARMADA_SPARK_ROOT=`pwd`
# run spark pi app on armada
SPARK_DRIVER_BIND_ADDRESS=localhost  $SPARK_ROOT/bin/spark-class org.apache.spark.deploy.ArmadaSparkSubmit \
  --master $ARMADA_MASTER \
  --jars $ARMADA_SPARK_ROOT/target/armada-cluster-manager-1.0.0-SNAPSHOT.jar   --driver-class-path  $ARMADA_SPARK_ROOT/target/armada-cluster-manager-1.0.0-SNAPSHOT.jar  --name spark-pi --class org.apache.spark.examples.SparkPi --conf spark.executor.instances=2 --conf spark.kubernetes.container.image=spark:testing $SPARK_ROOT/examples/target/scala-2.13/jars/spark-examples_2.13-4.1.0-SNAPSHOT.jar 100
