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
  - `FASTFIELD_STAGING_WAIT_MS`, `FASTFIELD_STAGING_INITIAL_DELAY_MS`, `FASTFIELD_STAGING_WAIT_INTERVAL_MS` – optional wait tuning (defaults: quick check, then 1 min pause, then 10 s polling for up to 3 min total).
- When the webhook fires, the server locates the PDF in the staging folder and moves it into the proper job’s `Job Walks` subfolder, preserving the filename.
- Every run is logged to KV with type `pdf_ingest` and source `fastfield_move`; you can inspect the step-by-step trace in the admin dashboard.
- If the destination already contains a file with the same name, the webhook automatically appends ` (1)`, ` (2)`, etc., to avoid overwriting.
- To drain existing staging files manually, run `npm run process-staging` (or `node scripts/process-staging-job-walks.js`) after setting the same environment variables locally.

Open the printed local URL (usually `http://localhost:5173`). Click **Sign in**, enter a folder path (e.g. `/Projects/Active`), then **Load projects**. Click **Export CSV** to save the list.

### Production (Vercel)

- Only serverless functions are deployed. `vercel.json` builds `api/*`.
- Set envs in Vercel: `TENANT_ID`, `MSAL_CLIENT_ID`, `MSAL_CLIENT_SECRET`, `MS_GRAPH_SCOPE`, `DEFAULT_SITE_URL`, `DEFAULT_LIBRARY`, `CORS_ORIGIN`.
- Test: `GET /api/health`, `POST /api/upload`.

## Planner Sync

Production-ready two-way sync between Business Central Project Tasks and Microsoft Planner.

### Environment variables

```
# Business Central
BC_TENANT_ID=
BC_ENVIRONMENT=
BC_COMPANY_ID=
BC_CLIENT_ID=
BC_CLIENT_SECRET=
BC_API_BASE=https://api.businesscentral.dynamics.com/v2.0
BC_API_PUBLISHER=cornerstone
BC_API_GROUP=plannerSync
BC_API_VERSION=v1.0

# Microsoft Graph
GRAPH_TENANT_ID=
GRAPH_CLIENT_ID=
GRAPH_CLIENT_SECRET=
GRAPH_SUBSCRIPTION_CLIENT_STATE=
GRAPH_NOTIFICATION_URL=

# Planner
PLANNER_GROUP_ID=
PLANNER_DEFAULT_PLAN_ID=
PLANNER_TENANT_DOMAIN=
PLANNER_WEB_BASE=

# Sync settings
SYNC_MODE=perProjectPlan
SYNC_POLL_MINUTES=10
SYNC_TIMEZONE=America/New_York
SYNC_ALLOW_DEFAULT_PLAN_FALLBACK=true
SYNC_LOCK_TIMEOUT_MINUTES=30
SYNC_PREFER_BC=true
SYNC_BC_MODIFIED_GRACE_MS=2000
SYNC_USE_PLANNER_DELTA=true
SYNC_USE_SMART_POLLING=false
PLANNER_DELTA_SELECT=id,planId,title,bucketId

# Optional persistence overrides
PLANNER_SUBSCRIPTIONS_FILE=.planner-subscriptions.json
PLANNER_PROJECT_SYNC_FILE=.planner-project-sync.json
PLANNER_DELTA_FILE=.planner-delta.json
BC_PROJECT_CHANGES_FILE=.bc-project-changes.json
KV_REST_API_URL=
KV_REST_API_TOKEN=
```

Notes:
- `PLANNER_DEFAULT_PLAN_ID` is required when `SYNC_MODE=singlePlan`.
- For `SYNC_MODE=perProjectPlan`, plan creation failures fall back to `PLANNER_DEFAULT_PLAN_ID` (task titles are prefixed with `projectNo`).
- Set `PLANNER_TENANT_DOMAIN` or `PLANNER_WEB_BASE` to generate clickable plan URLs in sync responses (defaults to the new Planner web UI).
- Set `GRAPH_NOTIFICATION_URL` (or `PLANNER_NOTIFICATION_URL`) to force the subscription webhook endpoint.
- Graph change notifications must use HTTPS in production. Point the subscription to `/api/webhooks/graph/planner`.
- Webhook notifications are queued in Vercel KV/Upstash if configured; otherwise they use an in-memory queue for local dev.
- If both BC and Planner changed since `lastSyncAt`, Planner wins; otherwise BC changes take precedence when they exist.
- `SYNC_BC_MODIFIED_GRACE_MS` ignores BC modified timestamps within this window after `lastSyncAt` (defaults to 2000ms) to avoid treating sync metadata updates as user changes.
- `SYNC_USE_SMART_POLLING=true` enables BC project change feed + Planner delta queries so only affected projects run BC → Planner sync.
- Smart polling expects a BC change feed endpoint at `/projectChanges` returning `sequenceNo` and `projectNo`; a 404 falls back to Planner-only polling.
- Use `POST /api/sync/projects` to disable sync for specific projects or delete plans (prevents re-creation after deletion).

### Admin endpoints

- `POST /api/sync/run-bc-to-planner` (optional JSON: `{ "projectNo": "P-100" }`) - runs BC <-> Planner sync
- `GET /api/sync/projects` (list Planner projects + sync state)
- `POST /api/sync/projects` (toggle per-project sync or delete plan)
- `POST /api/sync/subscriptions/create`
- `POST /api/sync/subscriptions/renew`
- `POST /api/webhooks/graph/planner` (Graph notification receiver)

### Example curl commands

```bash
# Run full sync (BC <-> Planner) for a single project
curl -X POST http://localhost:3000/api/sync/run-bc-to-planner \\
  -H 'Content-Type: application/json' \\
  -d '{\"projectNo\":\"P-100\"}'

# Create Graph subscriptions
curl -X POST https://your-domain.com/api/sync/subscriptions/create

# Test webhook validation locally
curl -i -X POST \"http://localhost:3000/api/webhooks/graph/planner?validationToken=test123\"

```

## Notes

-   The app lists only **subfolders** under the path you provide.
-   Increase page size or implement paging if you expect more than 999 entries.
-   To include files, remove the `filter((it) => it.folder)` line in `src/App.jsx`.
-   If you run into `interaction_required` errors, the popup sign-in/consent will handle it.
