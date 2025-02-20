#!/bin/bash

# Start up the driver, get it's ip address, then start the executor with it
set -e

echo
echo starting SparkPi driver
armadactl submit examples/spark-pi-driver.yaml > /tmp/jobid.txt
JOB_ID=`cat /tmp/jobid.txt | awk  '{print $5}'`
cat /tmp/jobid.txt
echo


echo waiting for SparkPi driver to start
sleep 20

echo
echo SparkPi driver ip addr:
IP_ADDR=`kubectl get pod "armada-$JOB_ID-0" -o jsonpath='{.status.podIP}'`
echo     $IP_ADDR
echo

echo passing drivers ip addr to executor and starting it
IP_ADDR=$IP_ADDR envsubst < examples/spark-pi-executor.yaml > /tmp/ex.yaml
armadactl submit /tmp/ex.yaml
echo

echo SparkPi driver/executor started