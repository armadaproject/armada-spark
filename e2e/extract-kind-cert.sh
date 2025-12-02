#!/bin/bash

CONTEXT="kind-armada"

E2E_DIR=$(realpath "$0" | xargs dirname)

cd "$E2E_DIR" || (echo "Error: could not cd to $E2E_DIR"; exit 1)

# What These Files Are
#  -  client.crt: Your user (client) certificate
#  -  client.key: The private key associated with the certificate
#  -  ca.crt: The CA certificate used by the Kubernetes API server (for verifying client and server certs)

# Extract the client certificate
kubectl config view --raw -o json | jq -r \
  ".users[] | select(.name == \"${CONTEXT}\") | .user.[\"client-certificate-data\"]" | base64 -d > client.crt

# Extract the client key
kubectl config view --raw -o json | jq -r \
  ".users[] | select(.name == \"${CONTEXT}\") | .user.[\"client-key-data\"]" | base64 -d > client.key

# Extract the cluster CA certificate
kubectl config view --raw -o json | jq -r \
  ".clusters[] | select(.name == \"${CONTEXT}\") | .cluster.[\"certificate-authority-data\"]" | base64 -d > ca.crt

