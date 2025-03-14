#!/bin/bash

root="$(cd "$(dirname "$0")/.."; pwd)"

if [ $# -eq 2 ]
then
    spark=$1
    scala=$2

    spark_compat=${spark%.*}
    scala_compat=${scala%.*}

    spark_major=${spark_compat%.*}
    scala_major=${scala_compat%.*}

    spark_minor=${spark_compat/*./}
    scala_minor=${scala_compat/*./}

    spark_patch=${spark/*./}
    scala_patch=${scala/*./}

    echo "setting spark=$spark and scala=$scala"
    sed -i -E \
        -e "s%^(    <artifactId>)([^_]+)[_0-9.]+(</artifactId>)$%\1\2_${scala_compat}\3%" \
        -e "s%^(        <scala.major.version>).+(</scala.major.version>)$%\1${scala_major}\2%" \
        -e "s%^(        <scala.minor.version>).+(</scala.minor.version>)$%\1${scala_minor}\2%" \
        -e "s%^(        <scala.patch.version>).+(</scala.patch.version>)$%\1${scala_patch}\2%" \
        -e "s%^(        <spark.major.version>).+(</spark.major.version>)$%\1${spark_major}\2%" \
        -e "s%^(        <spark.minor.version>).+(</spark.minor.version>)$%\1${spark_minor}\2%" \
        -e "s%^(        <spark.patch.version>).+(</spark.patch.version>)$%\1${spark_patch}\2%" \
        "$root/pom.xml"
else
    echo "Provide the Spark and Scala version to set"
    exit 1
fi

