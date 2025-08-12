param(
  [string]$RedirectUri = "http://localhost:5173"
)

$AppName = "OneDrive Project Dashboard"
$tenant = (az account show --query tenantId -o tsv)

Write-Host "Creating app registration '$AppName' in tenant $tenant ..."
$appId = az ad app create --display-name "$AppName" --sign-in-audience "AzureADMyOrg" --query appId -o tsv

Write-Host "Adding SPA redirect URI $RedirectUri ..."
az ad app update --id $appId --web-redirect-uris $RedirectUri --enable-id-token-issuance true

# Microsoft Graph
$graphAppId = "00000003-0000-0000-c000-000000000000"

Write-Host "Adding Graph delegated permissions (User.Read, Files.Read.All) ..."
az ad app permission add --id $appId --api $graphAppId --api-permissions "User.Read=Scope" "Files.Read.All=Scope"

Write-Host "Attempting admin consent (requires directory admin) ..."
try {
  az ad app permission admin-consent --id $appId
} catch {
  Write-Warning "Admin consent failed or requires admin approval. Proceed in the Entra portal."
}

Write-Host ""
Write-Host "Done."
Write-Host "CLIENT_ID: $appId"
Write-Host "Add this to your .env as VITE_AZURE_AD_CLIENT_ID"
