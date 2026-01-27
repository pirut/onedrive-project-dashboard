# Onedrive Project Dashboard - Agent Notes

Quick context
- This repo hosts an admin dashboard + serverless API for OneDrive + Business Central + Planner Premium (Dataverse) sync.
- Planner Premium sync lives in `lib/premium-sync/` and `lib/dataverse-client.*`.
- BC utilities remain in `lib/planner-sync/`.
- Vercel deploys `api/*` serverless functions; `app/api/*` are Next routes used locally/when running the app framework.
- Always push changes to the remote after making edits.

Key behaviors (Premium sync)
- Task titles use description only (task number removed).
- Heading mapping from BC task numbers still used to segment work:
  - `JOB NAME` -> `Pre-Construction`
  - `INSTALLATION` -> `Installation`
  - `CHANGE ORDER(S)` -> `Change Orders`
  - `REVENUE` section is skipped
- `syncLock` is respected but auto-clears when stale:
  - `SYNC_LOCK_TIMEOUT_MINUTES` (default 30) clears stale locks.

Important endpoints
- Sync: `POST /api/sync/bc-to-premium` (optional `{ "projectNo": "PR00001" }`, runs BC → Premium and optionally Premium → BC)
- Premium change poll: `POST /api/sync/premium-change/poll`
- Debug: `GET /api/debug`
- Webhook (Dataverse): `POST /api/webhooks/dataverse`
- Admin helpers:
  - Inspect BC task: `POST /api/sync/debug-bc-task` (body: `{ projectNo, taskNo }`)
  - Webhook snapshot: `GET /api/sync/webhook-log`
  - Webhook stream: `GET /api/sync/webhook-log-stream?include=1`

Admin dashboard (UI)
- `api/admin.js` renders the dashboard.
- Premium panel includes:
  - Run Sync
  - Run Sync (PR00001) quick test
  - Poll Premium changes
  - Inspect BC Task (enter Project No + Task No)
  - Webhook debug: Start/Stop feed, Clear, Snapshot
- Premium Projects panel includes per-project sync toggles and clear-links action.

Dataverse notes
- Planner Premium data is stored in Dataverse (`msdyn_projects`, `msdyn_projecttasks` by default).
- Change tracking uses `Prefer: odata.track-changes` and delta links stored in KV or `DATAVERSE_DELTA_FILE`.
- Optional Dataverse webhooks should target `/api/webhooks/dataverse` and include `x-dataverse-secret` if configured.

BC notes
- Custom API is `cornerstone/plannerSync/v1.0` with entity sets `projects` and `projectTasks`.
- BC permissions must include:
  - API pages (e.g., Page 50335)
  - TableData for custom tables (e.g., 50326 "SIT Project Task Sync Setup")

Common pitfalls
- Webhook live feed on Vercel is best-effort; SSE can land on a different instance than the webhook.
- Missing Dataverse mapping fields (`DATAVERSE_BC_PROJECT_NO_FIELD`, `DATAVERSE_BC_TASK_NO_FIELD`) can lead to duplicate tasks.

Environment variables (Premium/BC)
- BC: `BC_TENANT_ID`, `BC_ENVIRONMENT`, `BC_COMPANY_ID`, `BC_CLIENT_ID`, `BC_CLIENT_SECRET`,
  `BC_API_BASE`, `BC_API_PUBLISHER`, `BC_API_GROUP`, `BC_API_VERSION`
- Dataverse: `DATAVERSE_BASE_URL`, `DATAVERSE_TENANT_ID`, `DATAVERSE_CLIENT_ID`, `DATAVERSE_CLIENT_SECRET`,
  `DATAVERSE_RESOURCE_SCOPE`
- Sync: `SYNC_PREFER_BC`, `SYNC_BC_MODIFIED_GRACE_MS`, `SYNC_LOCK_TIMEOUT_MINUTES`, `SYNC_MAX_PROJECTS_PER_RUN`
- Webhooks: `DATAVERSE_NOTIFICATION_URL`, `DATAVERSE_WEBHOOK_SECRET`, `BC_WEBHOOK_NOTIFICATION_URL`, `BC_WEBHOOK_SHARED_SECRET`, `CRON_SECRET`

Recent changes to remember
- Added Dataverse client + premium sync layer in `lib/premium-sync/`.
- Added `/api/webhooks/dataverse` and `/api/sync/premium-change/poll` routes.
- Admin UI now targets Planner Premium endpoints and logs.
