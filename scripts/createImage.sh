#!/bin/bash
set -e

echo Generating armada spark docker image.

root="$(cd "$(dirname "$0")/.."; pwd)"
scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

image_prefix=apache/spark


# There are no Docker images for Spark 3 and Scala 2.13, as well as for Spark 3.3.4 and any Scala
if [[ "$SPARK_VERSION" == "3."* ]] && ( [[ "$SCALA_BIN_VERSION" == "2.13" ]] || [[ "$SPARK_VERSION" == "3.3.4" ]] ); then
  echo Checking for images for spark: $SPARK_VERSION scala: $SCALA_BIN_VERSION
  if [ "${INCLUDE_PYTHON}" == "false" ]; then
      echo Building image without Python.
      image_prefix=spark
      extra_build_params=""
  else
      echo Building Python image.
      image_prefix=spark-py
      extra_build_params=" -p ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile "
  fi
  if ! docker image inspect "$image_prefix:$image_tag" >/dev/null 2>/dev/null; then
    echo "There are no Docker images released for Spark $SPARK_VERSION and Scala $SCALA_BIN_VERSION."
    echo "A Docker image has to be built from Spark sources locally."
    if [[ ! -d ".spark-$SPARK_VERSION" ]]; then
      echo "Checking out Spark sources for tag v$SPARK_VERSION."
      git clone https://github.com/apache/spark --branch v$SPARK_VERSION --depth 1 --no-tags ".spark-$SPARK_VERSION"
    fi
    echo "Building Spark Docker image $image_prefix:$image_tag."
    cd ".spark-$SPARK_VERSION"
    # Spark 3.3.4 does not compile without this fix
    if [[ "$SPARK_VERSION" == "3.3.4" ]]; then
      sed -i -e "s%<scala.version>2.13.8</scala.version>%<scala.version>2.13.6</scala.version>%" pom.xml
    fi
    ./dev/change-scala-version.sh $SCALA_BIN_VERSION
    # by packaging the assembly project specifically, jars of all depending Spark projects are fetch from Maven
    # spark-examples jars are not released, so we need to build these from sources
    ./build/mvn --batch-mode clean
    ./build/mvn --batch-mode package -pl examples
    ./build/mvn --batch-mode package -Pkubernetes -Pscala-$SCALA_BIN_VERSION -pl assembly
    ./bin/docker-image-tool.sh -t "$image_tag" $extra_build_params build
    cd ..
  fi
fi

source "$scripts/prepExtraFiles.sh"

docker build \
  --tag $IMAGE_NAME \
  --build-arg spark_base_image_prefix=$image_prefix \
  --build-arg spark_base_image_tag=$image_tag \
  --build-arg scala_binary_version=$SCALA_BIN_VERSION \
  --build-arg include_python=$INCLUDE_PYTHON \
  -f "$root/docker/Dockerfile" \
  "$root"
