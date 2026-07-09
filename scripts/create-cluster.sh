#!/usr/bin/env bash
#
# create-cluster.sh — bring up the local kind + Armada cluster (idempotent).
# Shared by scripts/dev-e2e.sh and scripts/test/preemption/run.sh so cluster
# creation is decoupled from running the e2e test suite.
#
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

# Detect sed flavour by capability, not OS: GNU sed (which may shadow BSD sed
# on macOS via homebrew's gnu-sed) supports --version; BSD sed does not.
if sed --version >/dev/null 2>&1; then
  sed_inplace=(sed -i.bak)
else
  sed_inplace=(sed -I .bak)
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
    # Reset the patch's target files back to their committed state so the current
    # patch always re-applies cleanly. Without this the setup is not idempotent:
    # an updated patch would be skipped, and re-running `patch` over an already-
    # patched tree fails with "previously applied". We only `checkout` (restore
    # tracked files the patch/sed touch) rather than `git clean`, to preserve the
    # repo's untracked build tooling (bin/tooling) that a later step relies on.
    log "Resetting existing armada-operator repo to a clean checkout"
    if ! git -C "$AOHOME" checkout -- .; then
      err "There was a problem resetting the existing repo copy $AOHOME; exiting now"
      exit 1
    fi
  else
    if ! git clone "$AOREPO" "$AOHOME"; then
      err "There was a problem cloning the armada-operator repo; exiting now"
      exit 1
    fi
  fi

  # Always (re)apply the patch — on a fresh clone or a freshly-reset existing repo.
  log "Patching armada-operator"
  if ! patch -p1 -d "$AOHOME/" < "$scripts/../e2e/armada-operator.patch"; then
    err "There was an error patching the repo copy $AOHOME"
    exit 1
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

    # On MacOS systems with arch64 CPUs, we must specify pulling the arm64-specific manifest by
    # its platform digest (not the multi-arch index), then re-tag it locally as busybox:latest
    # otherwise the subsequent `kind load docker-image busybox:latest` step fails.
    ARM64_BUSYBOX_IMG='busybox@sha256:c4e5b27bf840ba1ebd5568b6b914f6926f3559b2ad4f505b1f37aae483b907d6'
    if [ "$os" = "Darwin" ] && [ "$arch" = 'arm64' ]; then
      if ! docker pull "$ARM64_BUSYBOX_IMG"; then
        err "Could not pull $ARM64_BUSYBOX_IMG; please try running"
        err "  docker pull $ARM64_BUSYBOX_IMG"
        err "then run this script again"
        exit 1
      fi

      if ! docker tag "$ARM64_BUSYBOX_IMG" "$INIT_CONTAINER_IMAGE"; then
        err "Error running"
        err "  docker tag $ARM64_BUSYBOX_IMG $INIT_CONTAINER_IMAGE"
        err "Please try it manually, then run this script again"
        exit 1
      fi

    # Linux systems and MacOS systems on amd64 processors
    elif ! docker pull "$INIT_CONTAINER_IMAGE"; then
      err "Could not pull $INIT_CONTAINER_IMAGE; please try running"
      err "  docker pull $INIT_CONTAINER_IMAGE"
      err "then run this script again"
      exit 1
    fi
  fi

  echo "Checking to see if Armada cluster is available ..."

  # Whether we booted Armada from scratch this run. A fresh `make kind-all` needs a
  # stabilization pause afterwards; an already-running cluster does not.
  local armada_started=false

  if ! "$scripts"/armadactl get queues > "$STATUSFILE" 2>&1 ; then
    if grep -q 'connection refused' "$STATUSFILE"; then
      start-armada 2>&1 | log_group "Using armada-operator to start Armada; this may take up to 5 minutes"
      sleep 10
      armadactl-retry get queues
      armada_started=true
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
      TMPDIR="$scripts/.tmp" "$AOHOME/bin/tooling/kind" load docker-image "$IMG" --name armada 2>&1 \
        | log_group "Loading Docker image $IMG into Armada (Kind) cluster";
    done
  fi

  # configure the defaults for the e2e test
  cp "$scripts/../e2e/spark-defaults.conf" "$scripts/../conf/spark-defaults.conf"

  # Only pause to let Armada settle when we just booted it this run. An
  # already-running cluster (the common dev-loop / re-test case) is skipped, saving 60s.
  if [ "$armada_started" = "true" ] \
     && [[ "$ARMADA_MASTER" == *"//localhost"* || "$ARMADA_MASTER" == *"//host.docker.internal"* ]] ; then
    log "Waiting 60 seconds for Armada to stabilize ..."
    sleep 60
  fi
}

# True if the `armada` kind cluster exists and every node reports Ready.
cluster_is_up() {
  kind get clusters 2>/dev/null | grep -qx armada || return 1
  kubectl --context kind-armada get nodes >/dev/null 2>&1 || return 1
  # A NotReady node's STATUS is "NotReady" (no leading-space " Ready"); a Ready
  # node's line contains " Ready". If any node line lacks " Ready", not up.
  ! kubectl --context kind-armada get nodes --no-headers 2>/dev/null | grep -qv ' Ready'
}

main() {
  if cluster_is_up; then
    log "armada kind cluster already running — ensuring armadactl/images/config are current"
  else
    log "armada kind cluster not detected — creating it"
  fi
  # init-cluster is itself idempotent about the expensive path: it runs
  # start-armada / `make kind-all` ONLY when Armada is unreachable (connection
  # refused), but ALWAYS (re)loads the images and copies spark-defaults.conf.
  # Those per-run syncs are what the e2e dev loop (rebuild image -> re-test) and
  # the preemption harness rely on, so we always run init-cluster — never skip it
  # just because the cluster already exists.
  init-cluster
}

main
