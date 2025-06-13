#!/bin/bash
# E2E Test Runner
# Usage: ./scripts/test-e2e.sh

set -euo pipefail

echo "Running Comprehensive Armada Spark E2E Framework..."

# Get script directory
scripts="$(cd "$(dirname "$0")"; pwd)"

echo "Setting Spark and Scala versions..."
"$scripts/set-version.sh" 3.5.3 2.13.16

echo "Creating Docker image..."
"$scripts/createImage.sh"

echo "Loading Docker image into kind cluster..."
kind load docker-image spark:armada --name armada

echo "Running E2E tests..."
mvn scalatest:test -Dsuites="org.apache.spark.deploy.armada.e2e.ArmadaSparkE2E"