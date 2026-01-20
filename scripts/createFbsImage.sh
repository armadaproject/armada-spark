#!/bin/bash
set -e
repo=${ARMADA_FALLBACK_STORAGE_REPO:-https://github.com/G-Research/spark}
repo_dir=`basename $repo`
branch=${ARMADA_FALLBACK_STORAGE_BRANCH:-fallback-storage-v3.5.3}
# create the spark image with fallback storage support
git clone $repo
cd $repo_dir
git checkout $branch
#override setting
export SPARK_HOME=`pwd`
./build/mvn clean install --batch-mode -Dscalastyle.skip=true -DskipTests  -Pkubernetes -Pscala-2.12
./bin/docker-image-tool.sh -u 185 -t "spark.fbs.img2"  -p ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile build
