# OneDrive Project Dashboard — Architecture & Dev Notes

This repository contains two runtime surfaces:

1) Local Express webhook server (for simple uploads during development)
   - Entry: `server/webhook.js`
   - Endpoints:
     - `GET /health` — health check
     - `POST /upload` — `multipart/form-data` file upload with fields: `folderName`, optional `siteUrl`, `libraryPath`
   - Auth: Microsoft Graph app-only via `@azure/msal-node` client credentials
   - CORS: controlled by `CORS_ORIGIN` (comma-separated list or `*`)

2) Vercel serverless API endpoints (for production)
   - Files: `api/*.js` (each file exports a default handler)
   - Notable endpoints:
     - `api/health` — base health
     - `api/upload` — production upload endpoint (multipart via Busboy)
     - `api/ingest` — JSON ingest logging to KV
     - `api/dashboard` — HTML dashboard of recent submissions (reads KV)
     - `api/submissions` — JSON list of recent submissions
     - `api/kv-diag` and `api/kv-write-test` — KV connectivity diagnostics
     - `api/sync-projects` — folder listing to JSON or forward to FastField
     - `api/sync-folders-cron` — cron job target (see `vercel.json`)
   - Deployed by Vercel; headers and a cron schedule are configured in `vercel.json`.

Support libraries and scripts:

- KV utilities: `lib/kv.js` (Upstash REST via `@upstash/redis`)
- Diagnostic and integration scripts: `scripts/*.js` (permission checks, folder sync, FastField integration, etc.). These call external services — use carefully.

Front‑end:

- Built assets under `dist/` (e.g., `dist/index.html` and `dist/assets/*`). The source `src/` is not included here; the `dist` bundle is suitable for static hosting.

Environment variables:

- Examples: `.env.example` (front‑end sign‑in values) and `env.example` (server / Graph / FastField). For production, set envs in Vercel.
- Core server vars:
  - `TENANT_ID`, `MSAL_CLIENT_ID`, `MSAL_CLIENT_SECRET`, `MS_GRAPH_SCOPE`
  - `DEFAULT_SITE_URL`, `DEFAULT_LIBRARY`
  - `CORS_ORIGIN`
- KV vars (Vercel KV via Upstash REST):
  - `KV_REST_API_URL`, `KV_REST_API_TOKEN` (or `KV_REST_API_READ_ONLY_TOKEN`)
- FastField vars (if using sync features):
  - `FASTFIELD_API_URL`, `FASTFIELD_AUTH_HEADER`, `FASTFIELD_TABLE_ID`, `FASTFIELD_TABLE_NAME`

Local development:

- Install deps: `npm install` (Node 18+)
- Run webhook server: `npm run webhook`
  - Health: `curl http://localhost:${PORT:-3001}/health`
  - Upload example:
    ```bash
    curl -sSf -X POST \
      -F "folderName=Your Target Folder" \
      -F "file=@/path/to/file.pdf" \
      "http://localhost:${PORT:-3001}/upload"
    ```
- Serverless endpoints run on Vercel (`vercel dev`) or in production.

Vercel deploy notes:

- `vercel.json` sets CORS headers for `/api/*` and schedules the cron for `/api/sync-folders-cron`.
- Set the required envs in your Vercel project (Graph, KV, and FastField if applicable).

Operational considerations:

- File uploads use Graph simple upload (suitable for small files). For large uploads, add chunked upload session logic.
- Library path format: `"<DriveName>/<Optional/Subfolder>"`. The code resolves the drive by name, then optionally navigates to the subpath.
- Folder selection is case‑insensitive; `(archive)` folders are skipped in some endpoints.
- KV is optional. If not configured, logging falls back to console (best effort).

Safety flags (recommended):

- Scripts under `scripts/` perform real Graph/FastField operations. Run only after confirming envs target a safe tenant/site and have appropriate permissions.
- Protect sensitive endpoints with API keys where appropriate. `api/sync-projects` already enforces an API key if configured via `API_KEYS`.

Quick file map (high‑value):

- Webhook server: `server/webhook.js`
- API endpoints: `api/*.js`
- KV helpers: `lib/kv.js`
- Vercel config: `vercel.json`
- Examples/env: `.env.example`, `env.example`
- Scripts: `scripts/*.js`

If anything in this doc looks off for your setup, let me know and I’ll adjust.

