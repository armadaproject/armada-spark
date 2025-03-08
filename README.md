# armada-spark  - Running Spark Jobs on Armada

## Introduction

This repo allows you build docker images with spark and armada support and to load those images as armada jobs.  It requires you specify a spark repo which is used as the baseline config for those docker images.

## Building the Armada Client
Before you can build the docker images you need to build the Armada client:
```
cd $ARMADA_ROOT
mage buildScala
cd client/scala/armada-scala-client
sbt publishM2
```

Spark versions, older than spark4, use scala 2.12, so you will need to build that version of the client as well.  To do so modify $ARMADA_ROOT/client/scala/armada-scala-client/build.sbt like so:

```
-val scala2Version = "2.13.15"
+val scala2Version = "2.12.20"
```
## Building Docker Images

To build the images, run the "scripts/createImage.sh" script.

That script requires the following config:
```
SPARK_HOME=../spark
IMAGE_NAME=testing
ARMADA_MASTER=armada://localhost:30002
ARMADA_QUEUE=test
```

You can specify those parameters in scripts/config.sh, or on the command line with "-s", "-i", "-m" and "-q".

Before running the command you will need to build the spark code like so:
```
cd $SPARK_HOME
git checkout origin/master
./build/sbt -Pkubernetes clean package
```
createImage.sh also supports spark versions "3.3.4" and "3.5.3" so you can build those as well.

Whichever spark/scala versions are used in the $SPARK_HOME/pom.xml are the versions that will be built for Armada.

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





