#!/bin/bash

# run spark pi app on armada
bin/spark-submit --master armada://localhost:30002 --deploy-mode cluster --name spark-pi --class org.apache.spark.examples.SparkPi --conf spark.executor.instances=2 --conf spark.kubernetes.container.image=spark:testing local:///opt/spark/examples/jars/spark-examples.jar 100
