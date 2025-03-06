#!/bin/bash

# Utility functions

get_scala_bin_version () {
    SCALA_HOME=$1
    grep '<scala.binary.version>' $SCALA_HOME/pom.xml | head -1  | grep -oP '(?<=<scala.binary.version>).*?(?=</scala.binary.version>)'
}

get_spark_version () {
    SCALA_HOME=$1
    grep -A1 spark-parent $SCALA_HOME/pom.xml | tail -1   | grep -oP '(?<=<version>).*?(?=</version>)'
}
