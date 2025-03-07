#!/bin/bash

# Utility functions

source scripts/config.sh

get_scala_bin_version () {
    grep '<scala.binary.version>' $SPARK_HOME/pom.xml | head -1  | grep -oP '(?<=<scala.binary.version>).*?(?=</scala.binary.version>)'
}

get_spark_version () {
    grep -A1 spark-parent $SPARK_HOME/pom.xml | tail -1   | grep -oP '(?<=<version>).*?(?=</version>)'
}

SCALA_BIN_VERSION=`get_scala_bin_version`
SPARK_VERSION=`get_spark_version`
ARMADA_SPARK_ROOT=`pwd`