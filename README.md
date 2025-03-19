# armada-spark  - Running Spark Jobs on Armada

## Introduction

This repo allows you build docker images with spark and armada support and to load those images as armada jobs.  It requires you specify a spark repo which is used as the baseline config for those docker images.

## Building the project

First, set the Spark and Scala version that you want to build for (unless the default works for you), e.g.:
```bash
./scripts/set-version.sh 3.5.3 2.13.15
```

Then you can build the project:
```bash
mvn package
```

## Building Docker Images

To build the image, first build the project as described above.

Then, build the image with:
```bash
./scripts/createImage.sh
```

This script provides options to confgure your build:
```bash
 Usage:
   -h  help
   -i  <image-name>
   -m  <armada-master-url>
   -q  <armada-queue>

You also can specify those parameters in scripts/config.sh, like so:
   IMAGE_NAME=spark:armada
   ARMADA_MASTER=armada://localhost:30002
   ARMADA_QUEUE=test
```

Spark versions "3.3.4" and "3.5.3" are supported.

## KIND Armada clusters
If you are using "kind", you can load the image like so:
```
kind load docker-image $IMAGE_NAME --name armada
```

## Running the sparkPi application
It can be run like so:
```
scripts/submitSparkPi.sh
```
It uses the same config parameters as `createImage.sh`.

