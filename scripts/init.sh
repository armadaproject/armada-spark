#!/bin/bash

# Utility functions

if [ -e "scripts/config.sh" ]; then
    source scripts/config.sh
fi

print_usage () {
    echo ' Usage:'
    echo '   -h  help'
    echo '   -s  <spark-home>'
    echo '   -i  <image-name>'
    echo '   -m  <armada-master-url>'
    echo '   -q  <armada-queue>'
    echo ''
    echo 'You also can specify those parameters in scripts/config.sh, like so:'
    echo '   SPARK_HOME=../spark'
    echo '   IMAGE_NAME=testing'
    echo '   ARMADA_MASTER=armada://localhost:30002'
    echo '   ARMADA_QUEUE=test'
    exit 1
}

while getopts "hs:i:m:q" opt; do
  case "$opt" in
    h) print_usage ;;
    s) SPARK_HOME=$OPTARG ;;
    i) IMAGE_NAME=$OPTARG ;;
    m) ARMADA_MASTER=$OPTARG ;;
    q) ARMADA_QUEUE=$OPTARG ;;
  esac
done

get_scala_version () {
    version=$(grep '<scala.version>' $SPARK_HOME/pom.xml | head -1  | grep -oP '(?<=<scala.version>).*?(?=</scala.version>)')
    bin_version=$(grep '<scala.binary.version>' $SPARK_HOME/pom.xml | head -1  | grep -oP '(?<=<scala.binary.version>).*?(?=</scala.binary.version>)')
    echo "$version $bin_version"
}

get_spark_version () {
    version=$(grep -A1 spark-parent $SPARK_HOME/pom.xml | tail -1   | grep -oP '(?<=<version>).*?(?=</version>)')
    patch=${version/*./}
    bin_version=${version%.${patch}}
    echo "$version $bin_version"
}

read SCALA_VERSION SCALA_BIN_VERSION <<< $(get_scala_version)
read SPARK_VERSION SPARK_BIN_VERSION <<< $(get_spark_version)

if ! [ -e src/main/scala-spark-$SPARK_BIN_VERSION ]; then
    echo This tool does not support Spark version $SPARK_VERSION . Exiting
    exit 1
fi
