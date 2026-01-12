#!/bin/bash
set -e

root="$(cd "$(dirname "$0")/.."; pwd)"
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

root="$(cd "$(dirname "$0")/.."; pwd)"
databricks_repo=${DATABRICS_REPO:-https://github.com/databricks/tpcds-kit}
cd extraFiles
git clone $databricks_repo tpcds-kit
cd tpcds-kit/tools
make
