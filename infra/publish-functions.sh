#!/usr/bin/env bash
set -euo pipefail

: "${RESOURCE_GROUP:=rg-djp-dev}"
: "${FUNCTION_APP_NAME:=}"

if [[ -z "$FUNCTION_APP_NAME" ]]; then
  FUNCTION_APP_NAME="$(az functionapp list --resource-group "$RESOURCE_GROUP" --query "[0].name" -o tsv)"
fi

if [[ -z "$FUNCTION_APP_NAME" ]]; then
  echo "No Function App found in $RESOURCE_GROUP. Set FUNCTION_APP_NAME explicitly." >&2
  exit 1
fi

rm -rf artifacts/functions
dotnet publish samples/DurableJobPipeline.Functions/DurableJobPipeline.Functions.csproj -c Release -o artifacts/functions
rm -f artifacts/DurableJobPipeline.Functions.zip
( cd artifacts/functions && zip -qr ../DurableJobPipeline.Functions.zip . )
az functionapp deployment source config-zip \
  --resource-group "$RESOURCE_GROUP" \
  --name "$FUNCTION_APP_NAME" \
  --src artifacts/DurableJobPipeline.Functions.zip

echo "Deployed $FUNCTION_APP_NAME"
