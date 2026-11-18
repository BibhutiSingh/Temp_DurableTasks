#!/usr/bin/env bash
set -euo pipefail

: "${AZURE_LOCATION:=centralindia}"
: "${ENVIRONMENT_NAME:=djp-dev}"

az deployment sub create \
  --location "$AZURE_LOCATION" \
  --template-file infra/main.bicep \
  --parameters environmentName="$ENVIRONMENT_NAME" location="$AZURE_LOCATION" resourceGroupName="rg-$ENVIRONMENT_NAME"
