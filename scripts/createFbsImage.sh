#!/bin/bash
set -e

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts/init.sh"

# these utilities are needed
# sudo apt-get install gcc make flex bison byacc git

# Check if the 'lex' command exists
if ! command -v lex &> /dev/null; then
    echo "Error: 'lex' command not found."
    echo 'you need to run "apt-get install gcc make flex bison byacc git"'
    exit 1
fi

tmp_dir=$(mktemp -d)
echo Creating temp dir: $tmp_dir
cd $tmp_dir

repo=${ARMADA_FALLBACK_STORAGE_REPO:-https://github.com/G-Research/spark}
repo_dir=`basename $repo`
branch=${ARMADA_FALLBACK_STORAGE_BRANCH:-fallback-storage-v3.5.3}

# create the spark image with fallback storage support
git clone $repo
cd $repo_dir
git checkout $branch

export SPARK_HOME=`pwd`
./build/mvn clean install --batch-mode -Dscalastyle.skip=true -DskipTests  -Pkubernetes -Pscala-2.12
./bin/docker-image-tool.sh -u 185 -t "spark.fbs.img"  -p ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile build
cd ..

# build the benchmarking tools
mkdir docker
cd docker
databricks_repo=${DATABRICKS_REPO:-https://github.com/databricks/tpcds-kit}
git clone $databricks_repo tpcds-kit
pushd tpcds-kit/tools
make
popd


# get the benchmark jar files
mkdir jars
if [ `basename $ARMADA_BENCHMARK_JAR` == "eks-spark-benchmark-assembly-1.0.jar" ]; then
    # this was built from https://github.com/EnricoMi/eks-spark-benchmark
    wget --no-check-certificate 'https://docs.google.com/uc?export=download&id=1g97dUxbZboI5jq_EjUXaOijtjDBcGsci' \
         -O  jars/eks-spark-benchmark-assembly-1.0.jar
fi

# Copy the cert file into the local dir unless we are skipping
# The cert file is need for clusters using TLS with self signed certs
# e.g. To use our tig cluster, retrieve the cert from the jumpbox here:
#  /usr/local/share/ca-certificates/testkube-ca.crt
ARMADA_SKIP_CERT=${ARMADA_SKIP_CERT:-""}
if [[ $ARMADA_SKIP_CERT != "true" ]]; then
    if [[ $1 == "" ]]; then
        echo "need to pass the cert arg, (unless ARMADA_SKIP_CERT is set)."
    fi
    echo copying $1
    cp $1 ca.crt
    CRT_COMMANDS="COPY ca.crt /tmp/ca.crt
RUN keytool -importcert -file /tmp/ca.crt -keystore /opt/java/openjdk/lib/security/cacerts -alias mycert -storepass changeit -noprompt"
else
    CRT_COMMANDS=""
fi

cat <<EOF > Dockerfile
FROM spark-py:spark.fbs.img
WORKDIR /

# Reset to root to run installation tasks
USER 0

RUN mkdir /opt/tools
COPY tpcds-kit /opt/tools/tpcds-kit
COPY jars/* /opt/spark/jars
$CRT_COMMANDS
EOF

docker build --tag spark-py:spark.fbs.img2 .

