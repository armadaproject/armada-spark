#!/bin/bash

set -euo pipefail

scripts="$(cd "$(dirname "$0")"; pwd)"
source "$scripts"/init.sh

STATUSFILE="$(mktemp)"
AOREPO='https://github.com/armadaproject/armada-operator.git'
AOHOME="$scripts/../../armada-operator"
ARMADACTL_VERSION='0.20.23'

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

trap 'rm -f -- "$STATUSFILE"' EXIT

os=$(uname -s)
arch=$(uname -m)

if [ "$os" = 'Darwin' ]; then
  sed_inplace=(sed -I .bak)
else
  sed_inplace=(sed -i.bak)
fi

log() {
  echo -e "${GREEN}$1${NC}"
}

err() {
  echo -e "${RED}$1${NC}" >&2
}

log_group() {
  if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then echo -n "::group::"; fi
  echo "$@"
  cat -
  if [ "${GITHUB_ACTIONS:-false}" == "true" ]; then echo; echo "::endgroup::"; fi
}

fetch-armadactl() {
  dl_url='https://github.com/armadaproject/armada/releases/download'

  if [ "$os" = "Darwin" ]; then
    dl_os='darwin'
    dl_arch='all'  # Darwin releases use 'all' for universal binaries
  elif [ "$os" = "Linux" ]; then
    dl_os='linux'
    if [ "$arch" = 'arm64' ]; then
      dl_arch='arm64'
    elif [ "$arch" = 'x86_64' ]; then
      dl_arch='amd64'
    else
      err "fetch-armadactl(): sorry, architecture $arch not supported; exiting now"
      exit 1
    fi
  else
    err "fetch-armadactl(): sorry, operating system $os not supported; exiting now"
    exit 1
  fi

  dl_file=armadactl_${ARMADACTL_VERSION}_${dl_os}_${dl_arch}.tar.gz

  curl --silent --location "${dl_url}/v${ARMADACTL_VERSION}/$dl_file" | tar -C "$scripts" -xzf - armadactl
}

# `kind load docker-image` fails when a locally-tagged image resolves to a
# multi-platform manifest index (e.g. anything pulled by tag under Docker's
# containerd-backed image store, which keeps the full index as the tag's
# descriptor even though only the host platform's content was downloaded).
# kind issues `ctr images import --all-platforms` against it, which then
# fails looking for blobs of platforms that were never pulled:
#   "ctr: content digest sha256:...: not found"
# Resolve the image to the single-platform manifest digest for the host
# platform and re-tag locally so `docker image inspect` no longer reports
# an index, avoiding the all-platforms import path entirely.
pin-single-platform-image() {
  local image="$1"

  if ! command -v jq &> /dev/null; then
    err "'jq' command not found; cannot verify $image is a single-platform image."
    err 'Install it (e.g. "apt-get install jq" or "brew install jq") and re-run this script.'
    exit 1
  fi

  local descriptor_media_type
  descriptor_media_type=$(docker image inspect "$image" --format '{{json .Descriptor.mediaType}}' 2>/dev/null || echo '""')

  case "$descriptor_media_type" in
    *manifest.list*|*image.index*) ;;
    *) return 0 ;; # already a single-platform manifest; nothing to do
  esac

  local goarch
  case "$arch" in
    x86_64) goarch='amd64' ;;
    aarch64|arm64) goarch='arm64' ;;
    *) goarch="$arch" ;;
  esac
  local goos
  goos=$(echo "$os" | tr '[:upper:]' '[:lower:]')

  # `docker manifest inspect` only succeeds for images backed by a registry;
  # locally-built images (e.g. IMAGE_NAME from `docker createImage.sh`) can
  # also report an index descriptor (build attestations) but have all their
  # content already present locally, so kind load works fine as-is. Guard
  # the pipeline with `|| true` so a lookup failure here (under `set -e -o
  # pipefail`) doesn't abort the whole script.
  local digest
  digest=$( (docker manifest inspect "$image" 2>/dev/null \
    | jq -r --arg os "$goos" --arg arch "$goarch" \
      '.manifests[]? | select(.platform.os == $os and .platform.architecture == $arch and ((.platform.variant // "") == "")) | .digest') || true)
  digest=$(echo "$digest" | head -n1)

  if [ -z "$digest" ]; then
    echo "Could not resolve a $goos/$goarch manifest for $image via registry lookup (may be a locally-built image); leaving as-is."
    return 0
  fi

  local repo="${image%%:*}"
  repo="${repo%%@*}"

  if ! docker pull "$repo@$digest"; then
    err "Could not pull $repo@$digest; please try running"
    err "  docker pull $repo@$digest"
    err "then run this script again"
    exit 1
  fi

  if ! docker tag "$repo@$digest" "$image"; then
    err "Error running"
    err "  docker tag $repo@$digest $image"
    err "Please try it manually, then run this script again"
    exit 1
  fi
}

armadactl-retry() {
  for attempt in {1..10}; do
    if "$scripts"/armadactl "$@" > /dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  err "Running \"armadactl $*\" failed after $attempt attempts" >&2
  return 1
}

start-armada() {
  if [ -d "$AOHOME" ]; then
    PRINTABLE_AOHOME=$(realpath "$AOHOME")
    echo "Using existing armada-operator repo at $PRINTABLE_AOHOME"
  else
    if ! git clone "$AOREPO" "$AOHOME"; then
      err "There was a problem cloning the armada-operator repo; exiting now"
      exit 1
    fi

    log "Patching armada-operator"
    if ! patch -p1 -d "$AOHOME/" < "$scripts/../e2e/armada-operator.patch"; then
      err "There was an error patching the repo copy $AOHOME"
      exit 1
    fi
  fi

  # Get IP address of the Armada servers first network interface that
  # is not loopback or a K8S internal network interface and patch it
  # into the Kind config so that it binds to a valid address instead
  # of the hardcoded placeholder (192.168.12.135).
  external_ip=$(ifconfig -a| grep -w 'inet'  | grep -v 'inet 127\.0\.0' | grep -v 'inet 172\.' | awk '{print $2}' | sed -ne '1p')

  if [ -z "$external_ip" ]; then
    err "Unable to find any IP addresses on an external interface on this system; exiting now"
    exit 1
  fi

  if ! "${sed_inplace[@]}" -e "s/192.168.12.135/$external_ip/" "$AOHOME/hack/kind-config.yaml"; then
    err "There was an error modifying $AOHOME/hack/kind-config.yaml"
    exit 1
  fi

  echo "Running 'make kind-all' to install and start Armada; this may take up to 6 minutes"
  if ! (cd "$AOHOME"; make kind-all 2>&1) | tee armada-start.txt; then
    echo ""
    err "There was a problem starting Armada; exiting now"
    exit 1
  fi

  echo "Extracting TLS client certificate files from Kind cluster"
  if ! e2e/extract-kind-cert.sh; then
    err "There was a problem extracting the certificates"
    exit 1
  fi
}

init-cluster() {
  IMG_REGEX='^[[:alnum:]_./-]+:[[:alnum:]_-]+$'

  if ! (echo "$IMAGE_NAME" | grep -Eq "$IMG_REGEX"); then
    err "IMAGE_NAME is not defined. Please set it in $scripts/config.sh, for example:"
    err "IMAGE_NAME=spark:testing"
    exit 1
  fi

  if ! (echo "$INIT_CONTAINER_IMAGE" | grep -Eq "$IMG_REGEX"); then
    err "INIT_CONTAINER_IMAGE is not defined. Please set it in $scripts/config.sh, for example:"
    err "INIT_CONTAINER_IMAGE=busybox:latest"
    exit 1
  fi

  if [ -z "$ARMADA_QUEUE" ]; then
    err "ARMADA_QUEUE is not defined. Please set it in $scripts/config.sh, for example:"
    err "ARMADA_QUEUE=spark-test"
    exit 1
  fi
  echo "Checking for armadactl .."
  if ! test -x "$scripts"/armadactl; then
    fetch-armadactl 2>&1 | log_group "Fetching armadactl"
  fi


  echo "Checking if image $IMAGE_NAME is available"
  if ! docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
    err "Image $IMAGE_NAME not found in local Docker instance."
    err "Rebuild the image (cd '$scripts/..' && mvn ${MVN_OFFLINE-} test-compile && mvn ${MVN_OFFLINE-} clean package && ./scripts/createImage.sh -p), and re-run this script"
    exit 1
  fi

  echo "Checking if image $INIT_CONTAINER_IMAGE is available"
  if ! docker image inspect "$INIT_CONTAINER_IMAGE" > /dev/null 2>&1; then
    echo "Image $INIT_CONTAINER_IMAGE not found in local Docker instance; pulling it from Docker Hub."

    if ! docker pull "$INIT_CONTAINER_IMAGE"; then
      err "Could not pull $INIT_CONTAINER_IMAGE; please try running"
      err "  docker pull $INIT_CONTAINER_IMAGE"
      err "then run this script again"
      exit 1
    fi
  fi

  echo "Checking to see if Armada cluster is available ..."

  if ! "$scripts"/armadactl get queues > "$STATUSFILE" 2>&1 ; then
    if grep -q 'connection refused' "$STATUSFILE"; then
      start-armada 2>&1 | log_group "Using armada-operator to start Armada; this may take up to 5 minutes"
      sleep 10
      armadactl-retry get queues
    else
      err "FAILED: output is "
      cat "$STATUSFILE"
      echo ""
      err "Armada cluster appears to be running, but an unknown error has happened; exiting now"
      exit 1
    fi
  else
    log "Armada is available"
  fi

  log "Armada Cluster Nodes"
  kubectl get nodes

  mkdir -p "$scripts/.tmp"

  if [[ "$ARMADA_MASTER" == *"//localhost"* || "$ARMADA_MASTER" == *"//host.docker.internal"* ]] ; then
    for IMG in "$IMAGE_NAME" "$INIT_CONTAINER_IMAGE"; do
      pin-single-platform-image "$IMG"
      TMPDIR="$scripts/.tmp" "$AOHOME/bin/tooling/kind" load docker-image "$IMG" --name armada 2>&1 \
        | log_group "Loading Docker image $IMG into Armada (Kind) cluster";
    done
  fi

  # configure the defaults for the e2e test
  cp "$scripts/../e2e/spark-defaults.conf" "$scripts/../conf/spark-defaults.conf"

  # If using a remote Armada server, assume it is already running and ready
  if [[ "$ARMADA_MASTER" == *"//localhost"* || "$ARMADA_MASTER" == *"//host.docker.internal"* ]] ; then
    log "Waiting 60 seconds for Armada to stabilize ..."
    sleep 60
  fi
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
    init-cluster
    run-test
}

main
