# Onedrive Project Dashboard - Agent Notes

Quick context
- This repo hosts an admin dashboard + serverless API for OneDrive + Business Central + Microsoft Planner sync.
- BC <-> Planner sync lives in `lib/planner-sync/`.
- Vercel deploys `api/*` serverless functions; `app/api/*` are Next routes used locally/when running the app framework.
- Always push changes to the remote after making edits.

Key behaviors (Planner sync)
- `SYNC_MODE=perProjectPlan` creates one plan per project; `singlePlan` uses `PLANNER_DEFAULT_PLAN_ID`.
- Plan names: `ProjectNo - Description` (fallback: `ProjectNo`).
- Task titles use description only (task number removed).
- Bucket mapping from BC heading rows:
  - `JOB NAME` -> `Pre-Construction`
  - `INSTALLATION` -> `Installation`
  - `CHANGE ORDER(S)` -> `Change Orders`
  - `REVENUE` section is skipped
- `syncLock` is respected but auto-clears when stale:
  - `SYNC_LOCK_TIMEOUT_MINUTES` (default 30) clears stale locks.

Important endpoints
- Sync: `POST /api/sync/run-bc-to-planner` (optional `{ "projectNo": "PR00001" }`)
- Poll: `POST /api/sync/run-poll`
- Debug: `GET /api/debug`
- Webhook (Graph): `POST /api/webhooks/graph/planner`
- Webhook validation (GET): `.../api/webhooks/graph/planner?validationToken=ping`
- Subscriptions:
  - Create: `POST /api/sync/subscriptions/create`
  - Renew: `POST /api/sync/subscriptions/renew`
  - List: `GET /api/sync/subscriptions/list`
- Admin helpers:
  - Inspect BC task: `POST /api/sync/debug-bc-task` (body: `{ projectNo, taskNo }`)
  - Webhook snapshot: `GET /api/sync/webhook-log`
  - Webhook stream: `GET /api/sync/webhook-log-stream?include=1`

Admin dashboard (UI)
- `api/admin.js` renders the dashboard.
- Planner panel includes:
  - Run BC -> Planner
  - Run BC -> Planner (PR00001) quick test
  - Inspect BC Task (enter Project No + Task No)
  - Webhook debug: Start/Stop feed, Clear, Snapshot, List subscriptions

Graph / Planner notes
- Plan creation must use `POST /planner/plans` with `{ title, owner: groupId }`.
- Graph subscriptions created for each plan: resource `/planner/plans/{planId}/tasks`.
- `GRAPH_NOTIFICATION_URL` (or `PLANNER_NOTIFICATION_URL`) overrides webhook notification URL.

BC notes
- Custom API is `cornerstone/plannerSync/v1.0` with entity sets `projects` and `projectTasks`.
- BC permissions must include:
  - API pages (e.g., Page 50335)
  - TableData for custom tables (e.g., 50326 "SIT Project Task Sync Setup")

Common pitfalls
- Webhook validation returns "Method not allowed" if GET isn’t routed; now handled in:
  - `app/api/webhooks/graph/planner/route.ts`
  - `api/webhooks/graph/planner.js`
- Webhook live feed on Vercel is best-effort; SSE can land on a different instance than the webhook.
- If plan creation fails, `SYNC_ALLOW_DEFAULT_PLAN_FALLBACK=false` surfaces the Graph error.

Environment variables (Planner/BC/Graph)
- BC: `BC_TENANT_ID`, `BC_ENVIRONMENT`, `BC_COMPANY_ID`, `BC_CLIENT_ID`, `BC_CLIENT_SECRET`,
  `BC_API_BASE`, `BC_API_PUBLISHER`, `BC_API_GROUP`, `BC_API_VERSION`
- Graph: `GRAPH_TENANT_ID`, `GRAPH_CLIENT_ID`, `GRAPH_CLIENT_SECRET`, `GRAPH_SUBSCRIPTION_CLIENT_STATE`
- Planner: `PLANNER_GROUP_ID`, `PLANNER_DEFAULT_PLAN_ID`
- Sync: `SYNC_MODE`, `SYNC_POLL_MINUTES`, `SYNC_TIMEZONE`, `SYNC_ALLOW_DEFAULT_PLAN_FALLBACK`, `SYNC_LOCK_TIMEOUT_MINUTES`
- URLs: `GRAPH_NOTIFICATION_URL` (preferred), `PLANNER_NOTIFICATION_URL` (fallback),
  `PLANNER_TENANT_DOMAIN` or `PLANNER_WEB_BASE` for plan links

Recent changes to remember
- Added debug endpoint for BC task inspection: `api/sync/debug-bc-task.js`
- Added webhook feed + snapshot + subscriptions list in admin UI
- Plan URL responses now use Planner web UI format by default
- Added Graph diagnostics in `/api/debug`
