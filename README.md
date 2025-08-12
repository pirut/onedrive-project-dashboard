# OneDrive Project Dashboard

A minimal React (Vite) app that lists subfolders in a chosen OneDrive folder and lets you export Name + Link to CSV.

## Quick Start

### 1) Create an Azure AD App (Microsoft Entra)

You can do this in the portal or via CLI.

**Portal (fastest):**

1. Go to https://entra.microsoft.com > Applications > App registrations > **New registration**
2. Name: `OneDrive Project Dashboard` (any name)
3. Supported account types: Single tenant (or multi-tenant if you prefer)
4. Register.
5. Authentication > **Add a platform** > **Single-page application (SPA)**. Add redirect URI: `http://localhost:5173` and `https://cstonedash.jrbussard.com`
6. API permissions > **Add a permission** > **Microsoft Graph** > **Delegated**: add `User.Read` and `Files.Read.All`. Grant admin consent if needed.

Copy the **Application (client) ID**.

**CLI (optional):**

```bash
# Requires: az login
./scripts/azure-setup.sh "http://localhost:5173"
# or PowerShell:
# ./scripts/azure-setup.ps1 "http://localhost:5173"
```

This attempts to create an app, add the SPA redirect URI, and request Graph delegated permissions. Admin consent may still be required.

### 2) Configure the app

Create a `.env` and fill in values (file is ignored by git):

```
VITE_AZURE_AD_CLIENT_ID=YOUR_AZURE_AD_APP_CLIENT_ID
VITE_AZURE_AD_TENANT_ID=common  # or your tenant id, e.g. contoso.onmicrosoft.com

# Optional: FastField sync
VITE_FASTFIELD_SYNC_WEBHOOK_URL=
VITE_FASTFIELD_TABLE_NAME=CornerstoneProjects

# Webhook server (Node 18+)
TENANT_ID=your-tenant-id-or-domain
MSAL_CLIENT_ID=your-server-app-client-id
MSAL_CLIENT_SECRET=your-server-app-client-secret
MS_GRAPH_SCOPE=https://graph.microsoft.com/.default
DEFAULT_SITE_URL=https://YOURTENANT.sharepoint.com/sites/work
DEFAULT_LIBRARY=Documents/Cornerstone Jobs
PORT=3001
# Allow your front-end origin
CORS_ORIGIN=https://cstonedash.jrbussard.com
```

### 3) Install & run

```bash
npm install
npm run dev
```

## Webhook (optional)

You can run a webhook that accepts file uploads and saves them to the correct SharePoint folder using Microsoft Graph.

1. Add to `.env`:

```
TENANT_ID=your-tenant-id-or-domain
MSAL_CLIENT_ID=your-server-app-client-id
MSAL_CLIENT_SECRET=your-server-app-client-secret
MS_GRAPH_SCOPE=https://graph.microsoft.com/.default
DEFAULT_SITE_URL=https://YOURTENANT.sharepoint.com/sites/work
DEFAULT_LIBRARY=Documents/Cornerstone Jobs
# For production front-end hosted at https://cstonedash.jrbussard.com
CORS_ORIGIN=https://cstonedash.jrbussard.com
```

2. Run:

```bash
npm run webhook
```

POST `multipart/form-data` to `http://localhost:3001/upload` (or your deployed webhook URL) with fields:

-   `folderName`: target folder name (must exist under the configured library)
-   `file`: file blob to upload (can be multiple `file` fields)

Open the printed local URL (usually `http://localhost:5173`). Click **Sign in**, enter a folder path (e.g. `/Projects/Active`), then **Load projects**. Click **Export CSV** to save the list.

### Production

-   Front-end: build and deploy the Vite app to the host serving `https://cstonedash.jrbussard.com`.
    -   Ensure SPA redirect URI `https://cstonedash.jrbussard.com` is added to your Entra app registration.
-   Webhook: either deploy `server/webhook.js` as a Node 18+ service or use Vercel Serverless Functions included in this repo.
    -   Vercel functions: `api/health.js`, `api/upload.js`. Configure env vars in Vercel (`TENANT_ID`, `MSAL_CLIENT_ID`, `MSAL_CLIENT_SECRET`, `MS_GRAPH_SCOPE`, `DEFAULT_SITE_URL`, `DEFAULT_LIBRARY`, `CORS_ORIGIN=https://cstonedash.jrbussard.com`).
    -   Frontend envs must be set in Vercel (build-time): `VITE_AZURE_AD_CLIENT_ID`, `VITE_AZURE_AD_TENANT_ID`.

## Notes

-   The app lists only **subfolders** under the path you provide.
-   Increase page size or implement paging if you expect more than 999 entries.
-   To include files, remove the `filter((it) => it.folder)` line in `src/App.jsx`.
-   If you run into `interaction_required` errors, the popup sign-in/consent will handle it.
