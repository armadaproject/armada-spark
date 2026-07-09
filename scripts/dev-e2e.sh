#!/bin/bash

set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts"/init.sh

# AOHOME is retained because run-test adds $AOHOME/bin/tooling to PATH.
AOHOME="$scripts/../../armada-operator"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log() {
  echo -e "${GREEN}$1${NC}"
}

err() {
  echo -e "${RED}$1${NC}" >&2
}

run-test() {
  echo "Running Scala E2E test suite..."

  # Add armadactl to PATH so the e2e framework can access it
  PATH="$scripts:$AOHOME/bin/tooling/:$PATH"
  export PATH

  # Change to armada-spark directory
  cd "$scripts/.."

  tls_args=()
  test -n "${CLIENT_CERT_FILE:-}" && tls_args+=( -Dclient_cert_file="$CLIENT_CERT_FILE" )
  test -n "${CLIENT_KEY_FILE:-}" && tls_args+=( -Dclient_key_file="$CLIENT_KEY_FILE" )
  test -n "${CLUSTER_CA_FILE:-}" && tls_args+=( -Dcluster_ca_file="$CLUSTER_CA_FILE" )

  # Run the Scala E2E test suite
  env KUBERNETES_TRUST_CERTIFICATES=true \
  mvn ${MVN_OFFLINE-} -e scalatest:test -Dsuites="org.apache.spark.deploy.armada.e2e.ArmadaSparkE2E" \
    -Dcontainer.image="$IMAGE_NAME" \
    -Dscala.version="$SCALA_VERSION" \
    -Dscala.binary.version="$SCALA_BIN_VERSION" \
    -Dspark.version="$SPARK_VERSION" \
    -Darmada.queue="$ARMADA_QUEUE" \
    -Darmada.master="$ARMADA_MASTER" \
    -Darmada.lookout.url="$ARMADA_LOOKOUT_URL" \
    -Darmadactl.path="$scripts/armadactl" \
    ${tls_args[@]:-} 2>&1 | tee e2e-test.log
  TEST_EXIT_CODE=${PIPESTATUS[0]}

  if [ "$TEST_EXIT_CODE" -ne 0 ]; then
    err "E2E tests failed with exit code $TEST_EXIT_CODE"
    exit "$TEST_EXIT_CODE"
  fi

  log "E2E tests completed successfully"
}

main() {
    "$scripts"/create-cluster.sh
    run-test
}

main
