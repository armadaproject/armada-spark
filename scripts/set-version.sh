#!/bin/bash

root="$(cd "$(dirname "$0")/.." || exit; pwd)"
SED="sed"
OS=$(uname -s)

# The sed that macOS ships does not understand all the regex patterns (which
# we use) that GNU sed does, so look for 'gsed' and use that, if available.
if [ "$OS" = 'Darwin' ]; then
    sed_location=$(type -p $SED)
    if [ "$sed_location" = '/usr/bin/sed' ]; then
        type -p gsed > /dev/null
        if [ $? -eq 0 ]; then
            SED=gsed
        else
            echo "$0: the version of sed on this system ($sed_location) does not handle" > /dev/stderr
            echo "all the GNU sed extensions needed. Please install 'gsed' and re-run this script" > /dev/stderr
            exit 1
        fi
    fi
fi

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

    if [ "$spark_major" == "3" ]; then
        if [ "$spark_minor" == "3" ]; then
            kubernetes_client_version=5.12.2
            jackson_version=2.13.4
        else
            kubernetes_client_version=6.7.2
            jackson_version=2.15.2
        fi
    else
        kubernetes_client_version=7.1.0
        jackson_version=2.15.2
    fi

    echo "setting spark=$spark and scala=$scala"
    $SED -i -E \
        -e "s%^(    <artifactId>)([^_]+)[_0-9.]+(</artifactId>)$%\1\2_${scala_compat}\3%" \
        -e "s%^(        <scala.major.version>).+(</scala.major.version>)$%\1${scala_major}\2%" \
        -e "s%^(        <scala.minor.version>).+(</scala.minor.version>)$%\1${scala_minor}\2%" \
        -e "s%^(        <scala.patch.version>).+(</scala.patch.version>)$%\1${scala_patch}\2%" \
        -e "s%^(        <spark.major.version>).+(</spark.major.version>)$%\1${spark_major}\2%" \
        -e "s%^(        <spark.minor.version>).+(</spark.minor.version>)$%\1${spark_minor}\2%" \
        -e "s%^(        <spark.patch.version>).+(</spark.patch.version>)$%\1${spark_patch}\2%" \
        -e "s%^(        <kubernetes-client.version>).+(</kubernetes-client.version>)$%\1${kubernetes_client_version}\2%" \
        -e "s%^(        <jackson.version>).+(</jackson.version>)$%\1${jackson_version}\2%" \
        "$root/pom.xml"
else
    echo "Provide the Spark and Scala version to set; for example:"
    echo "  $0 3.5.5 2.13.5"
    exit 1
fi

