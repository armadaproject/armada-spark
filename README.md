# armada-spark  - Running Spark Jobs on Armada

## Introduction

This repo allows you build docker images with spark and armada support and to load those images as armada jobs.  It requires you specify a spark repo which is used as the baseline config for those docker images.

## Building the project

First, set the Spark and Scala version that you want to build for (unless the default works for you), e.g.:
```
./scripts/set-version.sh 3.5.3 2.13.15
```

Then you can build the project:
```
mvn package
```

## Building Docker Images

To build the images, run the "scripts/createImage.sh" script.

That script requires config similiar to the following:
```
SPARK_HOME=../spark
IMAGE_NAME=testing
ARMADA_MASTER=armada://localhost:30002
ARMADA_QUEUE=test
```

You can specify those parameters in scripts/config.sh, or on the command line with "-s", "-i", "-m" and "-q".

This script will set Spark and Scala versions of this project according to the version set in $SPARK_HOME.

Before running the command you will need to build the spark code like so:
```
cd $SPARK_HOME
git checkout origin/master
./build/sbt -Pkubernetes clean package
```
Spark versions "3.3.4" and "3.5.3" are also supported.

The spark/scala versions used are the ones specificied in the spark repo, here: $SPARK_HOME/pom.xml.

## KIND Armada clusters
If you are using "kind", you can load the image like so:
```
kind load docker-image spark:$IMAGE_NAME --name armada
```

## Running the sparkPi application
It can be run like so:
```
scripts/submitSparkPi.sh
```
It uses the same config parameters as createImage.sh





