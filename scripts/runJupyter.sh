#!/bin/bash
set -euo pipefail

# init environment variables
scripts="$(cd "$(dirname "$0")"; pwd)"

# Jupyter always runs in client mode; set defaults before sourcing init.sh
# so that DEPLOY_MODE_ARGS is built with the correct values.
DEPLOY_MODE=client
JUPYTER_PORT="${JUPYTER_PORT:-8888}"
SPARK_BLOCK_MANAGER_PORT="${SPARK_BLOCK_MANAGER_PORT:-10061}"
SPARK_DRIVER_PORT="${SPARK_DRIVER_PORT:-7078}"

source "$scripts/init.sh"

# SPARK_DRIVER_HOST is required - must be reachable from Kubernetes executors
if [ -z "${SPARK_DRIVER_HOST:-}" ]; then
    echo "Error: SPARK_DRIVER_HOST must be set."
    echo ""
    exit 1
fi

if [ "${USE_KIND}" == "true" ]; then
    # Ensure queue exists on Armada
    if ! armadactl get queue $ARMADA_QUEUE >& /dev/null; then
        armadactl create queue $ARMADA_QUEUE
    fi

    # needed by kind load docker-image (if docker is installed via snap)
    # https://github.com/kubernetes-sigs/kind/issues/2535
    export TMPDIR="$scripts/.tmp"
    mkdir -p "$TMPDIR"
    kind load docker-image $IMAGE_NAME --name armada
fi

# Setup workspace directory
root="$(cd "$scripts/.."; pwd)"
notebooks_dir="$root/example/jupyter/notebooks"
workspace_dir="$root/example/jupyter/workspace"

# Create workspace directory if it doesn't exist
mkdir -p "$workspace_dir"

# Copy example notebooks to workspace only if they don't already exist
if [ -d "$notebooks_dir" ]; then
    for notebook in "$notebooks_dir"/*.ipynb; do
        [ -f "$notebook" ] || break
        notebook_name=$(basename "$notebook")
        if [ ! -f "$workspace_dir/$notebook_name" ]; then
            echo "Copying $notebook_name to workspace..."
            cp "$notebook" "$workspace_dir/"
        fi
    done
fi

# Remove existing container if it exists
if docker ps -a --format '{{.Names}}' | grep -q "^armada-jupyter$"; then
    echo "Removing existing armada-jupyter container..."
    docker rm -f armada-jupyter >/dev/null 2>&1 || true
fi

# Run Jupyter container
docker run -d \
  --name armada-jupyter \
  -p ${JUPYTER_PORT}:8888 \
  -p ${SPARK_BLOCK_MANAGER_PORT}:${SPARK_BLOCK_MANAGER_PORT} \
  -p ${SPARK_DRIVER_PORT}:${SPARK_DRIVER_PORT} \
  -e ARMADA_MASTER=${ARMADA_MASTER} \
  -e ARMADA_COMMON_CONF="${ARMADA_COMMON_CONF[*]:-}" \
  -e ARMADA_AUTH_ARGS="${ARMADA_AUTH_ARGS[*]:-}" \
  -e DEPLOY_MODE_ARGS="${DEPLOY_MODE_ARGS[*]:-}" \
  -e DYNAMIC_ALLOC_CONF="${DYNAMIC_ALLOC_CONF[*]:-}" \
  -e STATIC_ALLOC_CONF="${STATIC_ALLOC_CONF[*]:-}" \
  -e FALLBACK_STORAGE_CONF="${FALLBACK_STORAGE_CONF[*]:-}" \
  -e ALLOCATION_MODE=${ALLOCATION_MODE} \
  -v "$workspace_dir:/home/spark/workspace" \
  -v "$root/conf:/opt/spark/conf:ro" \
  --rm \
  ${IMAGE_NAME} \
  /opt/spark/bin/jupyter-entrypoint.sh

# Wait for Jupyter server to be reachable
for i in {1..10}; do
    if curl -s -f -o /dev/null "http://localhost:${JUPYTER_PORT}" 2>/dev/null; then
        echo "Jupyter notebook is running at http://localhost:${JUPYTER_PORT}"
        echo "Workspace is available in the container at /home/spark/workspace"
        echo "Notebooks are persisted in $workspace_dir"
        exit 0
    fi
    sleep 1
done

echo "Error: Jupyter server is not reachable. The container may have exited."
echo "This likely means Python/Jupyter is not installed in the image (INCLUDE_PYTHON=false)."
exit 1
