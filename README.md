# OneDrive Project Webhook

Minimal webhook endpoints to upload files to SharePoint/OneDrive via Microsoft Graph.

## Quick Start

### 1) Create an Azure AD App (Microsoft Entra)

You can do this in the portal or via CLI.

**Portal:**

1. Go to https://entra.microsoft.com > Applications > App registrations > **New registration**
2. Name: `OneDrive Project Dashboard` (any name)
3. Supported account types: Single tenant (or multi-tenant if you prefer)
4. Register.
5. API permissions > **Add a permission** > **Microsoft Graph** > **Application permissions**: add `Sites.ReadWrite.All` (or `Sites.Selected` with site assignment). Grant admin consent.

Copy the **Application (client) ID**.

**CLI (optional):**

```bash
# Requires: az login
./scripts/azure-setup.sh "http://localhost:5173"
# or PowerShell:
# ./scripts/azure-setup.ps1 "http://localhost:5173"
```

This attempts to create an app, add the SPA redirect URI, and request Graph delegated permissions. Admin consent may still be required.

### 2) Configure environment

Create a `.env` and fill in values (file is ignored by git):

```
VITE_AZURE_AD_CLIENT_ID=YOUR_AZURE_AD_APP_CLIENT_ID
VITE_AZURE_AD_TENANT_ID=common  # or your tenant id, e.g. contoso.onmicrosoft.com

# Webhook server (Node 18+)
TENANT_ID=your-tenant-id-or-domain
MSAL_CLIENT_ID=your-server-app-client-id
MSAL_CLIENT_SECRET=your-server-app-client-secret
MS_GRAPH_SCOPE=https://graph.microsoft.com/.default
DEFAULT_SITE_URL=https://YOURTENANT.sharepoint.com/sites/work
DEFAULT_LIBRARY=Documents/Cornerstone Jobs
PORT=3001
CORS_ORIGIN=*
```

### 3) Install & run locally

```bash
npm install
npm run webhook
```

## Webhook endpoints

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

POST `multipart/form-data` to `http://localhost:3001/upload` (or your deployed URL) with fields:

-   `folderName`: target folder name (must exist under the configured library)
-   `file`: file blob to upload (can be multiple `file` fields)

### Handling large PDFs with UploadThing

Vercel’s serverless runtime rejects uploads bigger than ~4.5 MB. To ingest larger PDFs:

1. Upload the file to UploadThing (or any host that gives you a direct HTTPS link). The URL should look like `https://utfs.io/f/<file-key>`.
2. Call `POST /api/ingest-pdf` _without_ a file and include metadata instead:

   - `siteUrl` and `libraryPath` (same as before)
   - `uploadthingUrl` (or `fileUrl`/`remoteUrl`): the HTTPS link to the hosted PDF
   - `uploadthingFilename` (optional): original file name if you want to override the name detected from the URL

   You can send these fields as `multipart/form-data` or as a JSON body (`Content-Type: application/json`).

The API downloads the remote PDF on-demand, streams it into OneDrive, and logs the extra steps (`remote:download:*`) so you can trace the hand-off in the admin dashboard.

### FastField staging webhook

- Configure FastField to drop completed Job Walk PDFs into a staging folder (e.g. SharePoint `Cornerstone/JOB WALKS`).
- Point the FastField webhook at `/api/fastfield-webhook`. The payload must include the filename (FastField’s default JSON format already does).
- Set these environment variables:
  - `FASTFIELD_STAGING_SITE_URL` – site URL containing the staging folder (defaults to `DEFAULT_SITE_URL`).
  - `FASTFIELD_STAGING_LIBRARY_PATH` – drive/folder path for the staging drop (e.g. `Cornerstone/JOB WALKS`).
  - `FASTFIELD_WEBHOOK_SECRET` – optional shared secret validated against the `x-webhook-secret` header.
  - `FASTFIELD_STAGING_WAIT_MS`, `FASTFIELD_STAGING_INITIAL_DELAY_MS`, `FASTFIELD_STAGING_WAIT_INTERVAL_MS` – optional wait tuning (defaults: quick check, then 5 min pause, then 10 s polling for up to 10 min).
- When the webhook fires, the server locates the PDF in the staging folder and moves it into the proper job’s `Job Walks` subfolder, preserving the filename.
- Every run is logged to KV with type `pdf_ingest` and source `fastfield_move`; you can inspect the step-by-step trace in the admin dashboard.
- If the destination already contains a file with the same name, the webhook automatically appends ` (1)`, ` (2)`, etc., to avoid overwriting.
- To drain existing staging files manually, run `npm run process-staging` (or `node scripts/process-staging-job-walks.js`) after setting the same environment variables locally.

Open the printed local URL (usually `http://localhost:5173`). Click **Sign in**, enter a folder path (e.g. `/Projects/Active`), then **Load projects**. Click **Export CSV** to save the list.

### Production (Vercel)

- Only serverless functions are deployed. `vercel.json` builds `api/*`.
- Set envs in Vercel: `TENANT_ID`, `MSAL_CLIENT_ID`, `MSAL_CLIENT_SECRET`, `MS_GRAPH_SCOPE`, `DEFAULT_SITE_URL`, `DEFAULT_LIBRARY`, `CORS_ORIGIN`.
- Test: `GET /api/health`, `POST /api/upload`.

## Notes

-   The app lists only **subfolders** under the path you provide.
-   Increase page size or implement paging if you expect more than 999 entries.
-   To include files, remove the `filter((it) => it.folder)` line in `src/App.jsx`.
-   If you run into `interaction_required` errors, the popup sign-in/consent will handle it.
