#!/usr/bin/env bash
set -euo pipefail

REDIRECT_URI="${1:-http://localhost:5173}"
APP_NAME="OneDrive Project Dashboard"
TENANT=$(az account show --query tenantId -o tsv)

echo "Creating app registration '${APP_NAME}' in tenant ${TENANT}..."
APP_ID=$(az ad app create --display-name "$APP_NAME" --sign-in-audience "AzureADMyOrg" --query appId -o tsv)

echo "Adding SPA redirect URI ${REDIRECT_URI} ..."
az ad app update --id "$APP_ID" --web-redirect-uris "${REDIRECT_URI}" --enable-id-token-issuance true

# Microsoft Graph resource appId
GRAPH_APP_ID="00000003-0000-0000-c000-000000000000"

# Delegated permissions: User.Read, Files.Read.All
echo "Adding Graph delegated permissions (User.Read, Files.Read.All) ..."
az ad app permission add --id "$APP_ID" --api "$GRAPH_APP_ID" --api-permissions "User.Read=Scope" "Files.Read.All=Scope"

echo "Attempting admin consent (requires directory admin)..."
set +e
az ad app permission admin-consent --id "$APP_ID"
set -e

echo
echo "Done."
echo "CLIENT_ID: $APP_ID"
echo "Add this to your .env as VITE_AZURE_AD_CLIENT_ID"
