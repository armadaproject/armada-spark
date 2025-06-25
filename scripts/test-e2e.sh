#!/bin/bash
# E2E Test Runner
# Usage: ./scripts/test-e2e.sh [-s|--spark-version VERSION] [-c|--scala-version VERSION]

set -euo pipefail

# Default versions
SPARK_VERSION="3.5.3"
SCALA_VERSION="2.13.8"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--spark-version)
      SPARK_VERSION="$2"
      shift 2
      ;;
    -c|--scala-version)
      SCALA_VERSION="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  -s, --spark-version VERSION   Spark version to use (default: 3.5.3)"
      echo "  -c, --scala-version VERSION   Scala version to use (default: 2.13.8)"
      echo "  -h, --help                    Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                                  # Use default versions"
      echo "  $0 -s 3.4.0 -c 2.12.17              # Use Spark 3.4.0 and Scala 2.12.17"
      echo "  $0 --spark-version 3.5.1            # Use Spark 3.5.1 with default Scala"
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      echo "Use -h or --help for usage information"
      exit 1
      ;;
  esac
done

echo "Running Comprehensive Armada Spark E2E Framework..."
echo "Spark version: $SPARK_VERSION"
echo "Scala version: $SCALA_VERSION"

echo "Compiling Scala code..."
mvn clean package -DskipTests

# Get script directory
scripts="$(cd "$(dirname "$0")"; pwd)"

echo "Setting Spark and Scala versions..."
"$scripts/set-version.sh" "$SPARK_VERSION" "$SCALA_VERSION"

echo "Creating Docker image..."
"$scripts/createImage.sh"

echo "Loading Docker image into kind cluster..."
kind load docker-image spark:armada --name armada

echo "Running E2E tests..."
mvn scalatest:test -Dsuites="org.apache.spark.deploy.armada.e2e.ArmadaSparkE2E"