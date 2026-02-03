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

# Webhook server (Node 18+)
TENANT_ID=your-tenant-id-or-domain
MSAL_CLIENT_ID=your-server-app-client-id
MICROSOFT_CLIENT_SECRET=your-server-app-client-secret
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
MICROSOFT_CLIENT_SECRET=your-server-app-client-secret
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
- Set envs in Vercel: `TENANT_ID`, `MSAL_CLIENT_ID`, `MICROSOFT_CLIENT_SECRET`, `MS_GRAPH_SCOPE`, `DEFAULT_SITE_URL`, `DEFAULT_LIBRARY`, `CORS_ORIGIN`.
- Test: `GET /api/health`, `POST /api/upload`.

## Planner Premium Sync

Bi-directional sync between Business Central Project Tasks and Planner Premium (Dataverse / Project for the Web).

### Environment variables

```
# Shared tenant + secret (Graph/BC/Dataverse)
TENANT_ID=
MICROSOFT_CLIENT_SECRET=

# Business Central
BC_ENVIRONMENT=
BC_COMPANY_ID=
BC_CLIENT_ID=
BC_API_BASE=https://api.businesscentral.dynamics.com/v2.0
BC_API_PUBLISHER=cornerstone
BC_API_GROUP=plannerSync
BC_API_VERSION=v1.0
BC_PROJECT_CHANGES_ENTITY_SET=projectChanges

# Dataverse (Planner Premium)
DATAVERSE_BASE_URL=https://yourorg.api.crm.dynamics.com
DATAVERSE_API_VERSION=v9.2
DATAVERSE_CLIENT_ID=
DATAVERSE_RESOURCE_SCOPE=https://yourorg.api.crm.dynamics.com/.default
DATAVERSE_NOTIFICATION_URL=
DATAVERSE_WEBHOOK_SECRET=
DATAVERSE_AUTH_MODE=client_credentials
DATAVERSE_AUTH_CLIENT_ID=
DATAVERSE_AUTH_SCOPES=https://yourorg.api.crm.dynamics.com/user_impersonation offline_access
DATAVERSE_AUTH_REDIRECT_URI=
DATAVERSE_AUTH_STATE_SECRET=
DATAVERSE_AUTH_SUCCESS_REDIRECT=
DATAVERSE_TOKEN_ENCRYPTION_SECRET=
DATAVERSE_REFRESH_TOKEN_FILE=.dataverse-refresh-token.json
PREMIUM_PROJECT_WEB_BASE=
PREMIUM_PROJECT_URL_TEMPLATE=

# Dataverse mapping (override to match your schema)
DATAVERSE_PROJECT_ENTITY_SET=msdyn_projects
DATAVERSE_TASK_ENTITY_SET=msdyn_projecttasks
DATAVERSE_PROJECT_ID_FIELD=msdyn_projectid
DATAVERSE_TASK_ID_FIELD=msdyn_projecttaskid
DATAVERSE_PROJECT_TITLE_FIELD=msdyn_subject
DATAVERSE_TASK_TITLE_FIELD=msdyn_subject
DATAVERSE_TASK_PROJECT_LOOKUP_FIELD=msdyn_project
DATAVERSE_TASK_PROJECT_ID_FIELD=_msdyn_project_value
DATAVERSE_BC_PROJECT_NO_FIELD=
DATAVERSE_BC_TASK_NO_FIELD=
DATAVERSE_TASK_START_FIELD=msdyn_start
DATAVERSE_TASK_FINISH_FIELD=msdyn_finish
DATAVERSE_TASK_PERCENT_FIELD=msdyn_percentcomplete
DATAVERSE_TASK_DESCRIPTION_FIELD=
DATAVERSE_TASK_MODIFIED_FIELD=modifiedon
DATAVERSE_ALLOW_PROJECT_CREATE=false
DATAVERSE_ALLOW_TASK_CREATE=true
DATAVERSE_ALLOW_TASK_DELETE=false
DATAVERSE_PERCENT_SCALE=1
DATAVERSE_PERCENT_MIN=0
DATAVERSE_PERCENT_MAX=100

# Sync settings
SYNC_PREFER_BC=true
SYNC_BC_MODIFIED_GRACE_MS=2000
SYNC_LOCK_TIMEOUT_MINUTES=30
SYNC_MAX_PROJECTS_PER_RUN=0
PREMIUM_DELETE_BEHAVIOR=clearLink
PREMIUM_POLL_PAGE_SIZE=200
PREMIUM_POLL_MAX_PAGES=10

# Optional persistence overrides
PREMIUM_PROJECT_SYNC_FILE=.premium-project-sync.json
DATAVERSE_DELTA_FILE=.dataverse-delta.json
BC_PROJECT_CHANGES_FILE=.bc-project-changes.json
BC_WEBHOOK_STORE_FILE=.bc-webhook-store.json
KV_REST_API_URL=
KV_REST_API_TOKEN=

# Webhooks + Cron
BC_WEBHOOK_NOTIFICATION_URL=
BC_WEBHOOK_SHARED_SECRET=
CRON_SECRET=
```

Notes:
- Dataverse change tracking uses `Prefer: odata.track-changes` and stores delta links in `DATAVERSE_DELTA_FILE` (or KV).
- Configure `DATAVERSE_BC_PROJECT_NO_FIELD` and `DATAVERSE_BC_TASK_NO_FIELD` to match your custom columns for stable ID mapping.
- `SYNC_BC_MODIFIED_GRACE_MS` ignores BC modified timestamps within this window after `lastSyncAt` (defaults to 2000ms).
- If `SYNC_PREFER_BC=true` and BC changed since `lastSyncAt`, Premium → BC updates are skipped to avoid overwrites.
- Use `POST /api/sync/projects` to disable sync or clear Premium IDs for specific projects.
- Set `DATAVERSE_ALLOW_PROJECT_CREATE=true` to auto-create a Premium plan when a BC project appears, even before any tasks exist.
- To show Premium project links in the admin cleanup list, set `PREMIUM_PROJECT_URL_TEMPLATE` (use `{projectId}` and optional `{tenantId}` / `{orgId}` placeholders). Example:
  `https://planner.cloud.microsoft/webui/premiumplan/{projectId}/org/{orgId}?tid={tenantId}`.
  If you don't set it, the app defaults to that format when it can resolve `OrganizationId` from Dataverse.
- Percent complete auto-detects 0–100 input when `DATAVERSE_PERCENT_MAX=1` (it divides by 100).

### Dataverse delegated auth (recommended for Schedule API)

Planner Premium schedule updates require a licensed user context. Set `DATAVERSE_AUTH_MODE=delegated` and run the OAuth flow once to store a refresh token.

1) Add a redirect URI to your Entra app:
   - `https://<your-domain>/api/auth/dataverse/callback` (prod)
   - `http://localhost:3000/api/auth/dataverse/callback` (local)
2) Set env vars:
   - `DATAVERSE_AUTH_CLIENT_ID`, `MICROSOFT_CLIENT_SECRET`
   - `DATAVERSE_AUTH_REDIRECT_URI` (must match the redirect above)
   - `DATAVERSE_AUTH_SCOPES` (default: `<baseUrl>/user_impersonation offline_access`)
   - `DATAVERSE_AUTH_STATE_SECRET` (recommended for state validation)
   - `DATAVERSE_TOKEN_ENCRYPTION_SECRET` (optional, encrypts refresh token at rest)
3) If your app is **public client** (no secret), `MICROSOFT_CLIENT_SECRET` can be empty (Graph/BC app-only features require a secret).

4) Visit:
   - `GET /api/auth/dataverse/login`
4) Confirm:
   - `GET /api/sync/premium-test` returns `ok: true`.

### Dataverse change tracking + webhooks

Preferred path is Dataverse change tracking (delta links). Optionally register Dataverse webhooks on the task entity for real-time triggers.

- Delta polling endpoint: `POST /api/sync/premium-to-bc` (legacy: `POST /api/sync/premium-change/poll`)
- Webhook receiver: `POST /api/webhooks/dataverse` (runs auto sync decision; set `DATAVERSE_NOTIFICATION_URL` if you need a custom URL)
- If using webhooks, configure your Dataverse service endpoint to send notifications to the webhook URL and include the shared secret header (`x-dataverse-secret`) matching `DATAVERSE_WEBHOOK_SECRET`.
- Optional team auto-add: set `PLANNER_GROUP_RESOURCE_IDS` (comma-separated bookable resource IDs) or `PLANNER_GROUP_ID` (AAD group id, if supported by your Dataverse schema).

### Admin endpoints

- `POST /api/sync/bc-to-premium` (optional JSON: `{ \"projectNo\": \"P-100\" }`, set `includePremiumChanges: true` to run both)
- `POST /api/sync/premium-to-bc`
- `POST /api/sync/auto` (decides direction by most recent changes)
- `POST /api/sync/premium-change/poll` (legacy)
- `GET /api/sync/projects` (list Premium projects + sync state)
- `GET /api/sync/premium-project-link` (resolve a Premium plan link by projectNo/projectId)
- `POST /api/sync/projects` (toggle per-project sync or clear links)
- `GET /api/sync/debug-operation-sets` (list Dataverse schedule API operation sets)
- `POST /api/sync/clear-operation-sets` (delete Dataverse schedule API operation sets)
- `POST /api/sync/clear-bc-sync-lock` (clear syncLock for a BC task)
- `POST /api/sync/register-dataverse-webhook` (register Dataverse webhook + steps)
- `POST /api/webhooks/dataverse` (Dataverse notification receiver)
- `POST /api/webhooks/bc` (Business Central notification receiver)
- `POST /api/sync/bc-subscriptions/create`
- `POST /api/sync/bc-subscriptions/renew`
- `POST /api/sync/bc-subscriptions/delete`
- `POST /api/sync/bc-jobs/process`

### Example curl commands

```bash
# Run BC → Premium for a single project
curl -X POST http://localhost:3000/api/sync/bc-to-premium \\
  -H 'Content-Type: application/json' \\
  -d '{\"projectNo\":\"P-100\"}'

# Run Premium → BC (Dataverse delta)
curl -X POST https://your-domain.com/api/sync/premium-to-bc

# Auto sync (decides by most recent changes)
curl -X POST https://your-domain.com/api/sync/auto

# Resolve Premium plan link for a BC project (JSON)
curl -X GET \"https://your-domain.com/api/sync/premium-project-link?projectNo=P-100\"

# Resolve Premium plan link and redirect the browser
curl -i \"https://your-domain.com/api/sync/premium-project-link?projectNo=P-100&redirect=1\"

# List Dataverse schedule API operation sets
curl -X GET \"https://your-domain.com/api/sync/debug-operation-sets\"

# Delete operation sets older than 60 minutes
curl -X POST \"https://your-domain.com/api/sync/clear-operation-sets\" \\
  -H 'Content-Type: application/json' \\
  -d '{\"olderThanMinutes\":60}'

# Clear syncLock for a BC task
curl -X POST \"https://your-domain.com/api/sync/clear-bc-sync-lock\" \\
  -H 'Content-Type: application/json' \\
  -d '{\"projectNo\":\"P-100\",\"taskNo\":\"1200\"}'

# Clear syncLock for all tasks in a project
curl -X POST \"https://your-domain.com/api/sync/clear-bc-sync-lock\" \\
  -H 'Content-Type: application/json' \\
  -d '{\"projectNo\":\"P-100\"}'

# Register Dataverse webhook + steps (Create/Update/Delete on msdyn_projecttask)
curl -X POST \"https://your-domain.com/api/sync/register-dataverse-webhook\" \\
  -H 'Content-Type: application/json' \\
  -d '{\"webhookUrl\":\"https://your-domain.com/api/webhooks/dataverse\"}'

# Ping webhook locally
curl -i http://localhost:3000/api/webhooks/dataverse
```

### Business Central Webhooks (Vercel)

BC webhooks let Business Central changes enqueue targeted BC → Premium sync jobs instead of relying on polling.

1) Set env vars:
- `BC_WEBHOOK_NOTIFICATION_URL` (optional) to force the webhook URL (defaults to your deployment URL + `/api/webhooks/bc`).
- `BC_WEBHOOK_SHARED_SECRET` (optional) is sent as `clientState` and validated on receipt.
- `CRON_SECRET` (required) for scheduled renewals + job processing.
- Ensure KV/Upstash (`KV_REST_API_URL`/`KV_REST_API_TOKEN`) is configured for durable queues.

2) Expose locally (optional): use ngrok/cloudflared and set `BC_WEBHOOK_NOTIFICATION_URL` to the HTTPS tunnel URL.

3) Create the subscription:

```bash
curl -X POST https://your-domain.com/api/sync/bc-subscriptions/create \\
  -H 'Content-Type: application/json' \\
  -d '{\"entitySets\":[\"projectTasks\"]}'
```

4) Validate handshake:

```bash
curl -i -X POST \"http://localhost:3000/api/webhooks/bc?validationToken=test123\"
```

5) Process queued jobs (cron or manual):

```bash
curl -X POST https://your-domain.com/api/sync/bc-jobs/process?cronSecret=YOUR_SECRET
```

6) Renew subscriptions (cron or manual):

```bash
curl -X POST https://your-domain.com/api/sync/bc-subscriptions/renew?cronSecret=YOUR_SECRET
```

Vercel Cron will call `/api/sync/bc-subscriptions/renew` daily and `/api/sync/bc-jobs/process` every few minutes. If using Vercel Cron, append `?cronSecret=...` to the cron paths (or send the `x-cron-secret` header) to satisfy the auth check.
Cron auth now protects `/api/sync/auto` and `/api/sync-folders-cron` as well, so include the same secret there.

## Notes

-   The app lists only **subfolders** under the path you provide.
-   Increase page size or implement paging if you expect more than 999 entries.
-   To include files, remove the `filter((it) => it.folder)` line in `src/App.jsx`.
-   If you run into `interaction_required` errors, the popup sign-in/consent will handle it.
