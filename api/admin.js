import "isomorphic-fetch";
import crypto from "crypto";
import { kvDiagnostics, listSubmissions } from "../lib/kv.js";

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

// Admin auth configuration
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || ""; // required in production
const ADMIN_SESSION_SECRET = process.env.ADMIN_SESSION_SECRET || ""; // required in production

function signSession(data) {
    if (!ADMIN_SESSION_SECRET) return null;
    const h = crypto.createHmac("sha256", ADMIN_SESSION_SECRET);
    h.update(data);
    return h.digest("hex");
}

function safeEqual(a = "", b = "") {
    const ab = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    if (ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

function parseCookies(req) {
    const hdr = req.headers["cookie"];
    const out = {};
    if (!hdr) return out;
    String(hdr)
        .split(";")
        .map((s) => s.trim())
        .forEach((pair) => {
            const eq = pair.indexOf("=");
            if (eq === -1) return;
            const k = decodeURIComponent(pair.slice(0, eq).trim());
            const v = decodeURIComponent(pair.slice(eq + 1).trim());
            out[k] = v;
        });
    return out;
}

async function readFormBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    const params = new URLSearchParams(text);
    const obj = {};
    for (const [k, v] of params) obj[k] = v;
    return obj;
}

function sessionCookie(username) {
    const ts = Date.now().toString();
    const raw = `${username}|${ts}`;
    const sig = signSession(raw);
    if (!sig) return null;
    const value = Buffer.from(`${raw}|${sig}`).toString("base64url");
    const maxAge = 7 * 24 * 60 * 60; // 7 days
    const secure = process.env.NODE_ENV === "production";
    const cookie = [`admin_session=${value}`, `Path=/`, `HttpOnly`, `SameSite=Lax`, `Max-Age=${maxAge}`, secure ? "Secure" : null].filter(Boolean).join("; ");
    return cookie;
}

function verifySession(cookieVal) {
    try {
        if (!cookieVal) return false;
        const decoded = Buffer.from(cookieVal, "base64url").toString("utf8");
        const parts = decoded.split("|");
        if (parts.length !== 3) return false;
        const [u, ts, sig] = parts;
        const raw = `${u}|${ts}`;
        const expect = signSession(raw);
        if (!expect) return false;
        if (!safeEqual(sig, expect)) return false;
        // Optional: expire after 7 days
        const ageMs = Date.now() - Number(ts);
        if (!Number.isFinite(ageMs) || ageMs > 7 * 24 * 60 * 60 * 1000) return false;
        return { username: u };
    } catch {
        return false;
    }
}

function htmlEscape(s) {
    return String(s).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}

function layout(title, bodyHtml) {
    return `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>${htmlEscape(title)}</title>
<style>
  :root{--bg:#0b1220;--panel:#0f172a;--muted:#8b98b8;--fg:#e6ecff;--ok:#10b981;--bad:#ef4444;--warn:#f59e0b}
  body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto}
  .wrap{max-width:1100px;margin:32px auto;padding:0 16px}
  h1{font-size:20px;margin:0 0 16px 0}
  .panel{background:var(--panel);border:1px solid #1f2a44;border-radius:12px;padding:16px;margin:0 0 16px 0}
  details.panel{padding:0}
  details.panel > summary{list-style:none;cursor:pointer;padding:14px 16px;font-weight:600;display:flex;align-items:center;justify-content:space-between;gap:8px}
  details.panel > summary::-webkit-details-marker{display:none}
  details.panel > summary::after{content:"+";color:var(--muted);font-weight:700}
  details.panel[open] > summary::after{content:"-"}
  details.panel[open] > summary{border-bottom:1px solid #1f2a44}
  .panel-body{padding:16px}
  .summary-meta{color:var(--muted);font-size:12px;font-weight:400}
  table{width:100%;border-collapse:collapse}
  th,td{padding:8px;border-top:1px solid #1f2a44;text-align:left;vertical-align:top}
  th{color:#b9c2da}
  .muted{color:var(--muted)}
  .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
  .badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#16213a;color:#9fb1d9;border:1px solid #1f2a44}
  input,button{font:inherit}
  input{background:#0b1220;border:1px solid #1f2a44;border-radius:8px;color:var(--fg);padding:8px;width:100%}
  button{background:#2b61d1;color:#fff;border:0;border-radius:8px;padding:8px 12px;cursor:pointer}
  form .row{display:grid;grid-template-columns:1fr;gap:8px;margin:8px 0}
  .right{float:right}
  .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
  .small{font-size:12px}
  .log-extras{margin-top:6px;display:flex;flex-direction:column;gap:6px}
  details.log{margin-top:6px;border:1px solid #1f2a44;border-radius:8px;background:#0b1220}
  details.log summary{cursor:pointer;padding:6px 8px;font-weight:600}
  details.log[open] summary{border-bottom:1px solid #1f2a44;margin-bottom:6px}
  .step-list{list-style:none;margin:0;padding:0}
  .step-list li{margin:4px 0;padding:4px 8px;border-radius:6px;background:#111b2f}
  .step-list li .meta{display:block;color:var(--muted);margin-top:2px;word-break:break-word}
  pre.log-block{background:#111b2f;border-radius:6px;padding:8px;white-space:pre-wrap;word-break:break-word;margin:6px 0 0 0;color:#c8d5ff;font-size:12px}
</style>
</head><body><div class="wrap">${bodyHtml}</div></body></html>`;
}

function loginView(msg = null) {
    const warn =
        !ADMIN_PASSWORD || !ADMIN_SESSION_SECRET
            ? `<div class="panel"><div class="bad">Admin password/secret not configured. Set <span class="mono">ADMIN_PASSWORD</span> and <span class="mono">ADMIN_SESSION_SECRET</span>.</div></div>`
            : "";
    const inner = `
  <h1>Admin Login</h1>
  ${warn}
  ${msg ? `<div class="panel"><div class="bad">${htmlEscape(msg)}</div></div>` : ""}
  <div class="panel">
    <form method="POST" action="">
      <div class="row">
        <label>Username</label>
        <input name="username" autocomplete="username" value="${htmlEscape(ADMIN_USERNAME)}" />
      </div>
      <div class="row">
        <label>Password</label>
        <input name="password" type="password" autocomplete="current-password" />
      </div>
      <div class="row">
        <button type="submit">Sign in</button>
      </div>
      <div class="small muted">Only you should know this password.</div>
    </form>
  </div>`;
    return layout("Login", inner);
}

async function ping(url, opts = {}) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), opts.timeoutMs || 1500);
    try {
        const res = await fetch(url, { signal: controller.signal, headers: { "cache-control": "no-cache" } });
        clearTimeout(t);
        return { ok: res.ok, status: res.status };
    } catch (e) {
        clearTimeout(t);
        return { ok: false, error: e?.message || String(e) };
    }
}

function envPresence(name) {
    return Boolean(process.env[name]);
}

async function dashboardView(req) {
    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
    const notificationUrlDefault = process.env.GRAPH_NOTIFICATION_URL || process.env.PLANNER_NOTIFICATION_URL || `${origin}/api/webhooks/graph/planner`;

    // Gather health info
    const endpoints = [
        { name: "/api/health", url: `${origin}/api/health`, desc: "Base health" },
        { name: "/api/submissions", url: `${origin}/api/submissions?limit=5`, desc: "Recent logs (KV)" },
        { name: "/api/kv-diag", url: `${origin}/api/kv-diag`, desc: "KV diagnostics" },
    ];
    const checks = await Promise.all(endpoints.map(async (e) => ({ ...e, result: await ping(e.url) })));

    const graphEnvOk = ["TENANT_ID", "MSAL_CLIENT_ID", "MSAL_CLIENT_SECRET", "DEFAULT_SITE_URL", "DEFAULT_LIBRARY"].every(envPresence);
    const plannerEnvRequired = [
        "BC_TENANT_ID",
        "BC_ENVIRONMENT",
        "BC_COMPANY_ID",
        "BC_CLIENT_ID",
        "BC_CLIENT_SECRET",
        "GRAPH_TENANT_ID",
        "GRAPH_CLIENT_ID",
        "GRAPH_CLIENT_SECRET",
        "GRAPH_SUBSCRIPTION_CLIENT_STATE",
        "PLANNER_GROUP_ID",
        "SYNC_MODE",
    ];
    const plannerEnvMissing = plannerEnvRequired.filter((name) => !envPresence(name));
    const plannerEnvOk = plannerEnvMissing.length === 0;
    const kvDiag = await kvDiagnostics();
    const items = await listSubmissions(500);

    const formatStep = (step) => {
        if (!step) return "";
        const clone = { ...step };
        const ts = clone.ts ? `<span class="mono small">${htmlEscape(clone.ts)}</span>` : "";
        const msg = clone.msg ? htmlEscape(clone.msg) : "";
        delete clone.ts;
        delete clone.msg;
        const metaEntries = Object.entries(clone).filter(([_, v]) => v !== undefined && v !== null && v !== "");
        const metaStr = metaEntries.length
            ? `<span class="meta">${htmlEscape(
                  metaEntries
                      .map(([k, v]) => {
                          let val = typeof v === "object" ? JSON.stringify(v) : String(v);
                          if (val.length > 240) val = `${val.slice(0, 240)}…`;
                          return `${k}=${val}`;
                      })
                      .join(" · ")
              )}</span>`
            : "";
        return `<li>${ts}${ts && msg ? " " : ""}${msg}${metaStr}</li>`;
    };

    const endpointsTableRows = checks
        .map((c) => {
            const status = c.result.ok ? `<span class="ok">OK</span>` : `<span class="bad">FAIL</span>`;
            const extra = c.result.status ? `${c.result.status}` : c.result.error ? htmlEscape(c.result.error) : "";
            return `<tr><td><a class="mono" href="${htmlEscape(c.url)}" target="_blank" rel="noreferrer">${htmlEscape(c.name)}</a></td><td>${htmlEscape(
                c.desc
            )}</td><td>${status}</td><td class="mono small">${extra}</td></tr>`;
        })
        .join("");

    const itemsRows = items
        .map((it) => {
            const status = (it.status || "").toLowerCase();
            const statusCls = status === "ok" ? "ok" : status === "error" ? "bad" : status ? "warn" : "muted";
            const files = Array.isArray(it.files) ? it.files : [];
            const filesCount = files.length ? `${files.length} file${files.length === 1 ? "" : "s"}` : "";
            const filesLinks = files
                .map((f) => `<a href="${htmlEscape(f.url || "")}" target="_blank" rel="noreferrer">${htmlEscape(f.filename || f.url || "file")}</a>`)
                .join(", ");
            const detailsParts = [
                it.folderName ? `folder: <span class=\"mono\">${htmlEscape(it.folderName)}</span>` : null,
                it.phase ? `phase: <span class=\"mono\">${htmlEscape(it.phase)}</span>` : null,
                it.traceId ? `trace: <span class=\"mono\">${htmlEscape(it.traceId)}</span>` : null,
                it.errorStatus ? `status: <span class=\"mono\">${htmlEscape(String(it.errorStatus))}</span>` : null,
                it.errorContentRange ? `range: <span class=\"mono\">${htmlEscape(it.errorContentRange)}</span>` : null,
                it.reason ? `reason: ${htmlEscape(it.reason)}` : null,
                it.error ? `error: ${htmlEscape(it.error)}` : null,
            ].filter(Boolean);
            const mainDetails = detailsParts.join(" · ");
            const stepsBlock =
                Array.isArray(it.steps) && it.steps.length
                    ? `<details class=\"log\"><summary>Logs (${it.steps.length})</summary><ul class=\"step-list\">${it.steps
                          .map((step) => formatStep(step))
                          .join("")}</ul></details>`
                    : "";
            const responseBlock = it.errorResponse
                ? `<details class=\"log\"><summary>Error response</summary><pre class=\"log-block\">${htmlEscape(
                      it.errorResponse.length > 2000 ? `${it.errorResponse.slice(0, 2000)}…` : it.errorResponse
                  )}</pre></details>`
                : "";
            const stackBlock = it.errorStack
                ? `<details class=\"log\"><summary>Error stack</summary><pre class=\"log-block\">${htmlEscape(
                      it.errorStack.length > 4000 ? `${it.errorStack.slice(0, 4000)}…` : it.errorStack
                  )}</pre></details>`
                : "";
            const extras = [stepsBlock, responseBlock, stackBlock].filter(Boolean).join("");
            const details = `${mainDetails}${extras ? `<div class=\"log-extras\">${extras}</div>` : ""}`;
            return `<tr>
              <td class="mono small">${htmlEscape(it.loggedAt || "")}</td>
              <td>${htmlEscape(it.type || "")}</td>
              <td><span class="badge ${statusCls}">${htmlEscape(it.status || "") || "n/a"}</span></td>
              <td>${details || '<span class="muted">—</span>'}</td>
              <td style="text-align:center">${htmlEscape(String(it.uploaded ?? ""))}</td>
              <td>${filesCount}${filesLinks ? `<div class=\"small\">${filesLinks}</div>` : ""}</td>
            </tr>`;
        })
        .join("");

    const inner = `
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:16px">
    <h1 style="margin:0">Project Dashboard — Admin</h1>
    <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center">
      <a href="/api/projects-kanban" style="background:#2b61d1;color:#fff;text-decoration:none;border-radius:8px;padding:8px 16px;font:inherit;display:inline-block">Kanban Board</a>
      <button type="button" id="export-active-csv" style="background:#1f2a44;color:#e6ecff;border:1px solid #1f2a44;border-radius:8px;padding:8px 12px;font:inherit;cursor:pointer">Export Active Projects CSV</button>
      <button type="button" id="export-active-json" style="background:#1f2a44;color:#e6ecff;border:1px solid #1f2a44;border-radius:8px;padding:8px 12px;font:inherit;cursor:pointer">Export Active Projects JSON</button>
    </div>
  </div>
  <div class="grid">
    <details class="panel">
      <summary>Graph env</summary>
      <div class="panel-body">
        <div>${graphEnvOk ? '<span class="ok">Configured</span>' : '<span class="bad">Missing</span>'}</div>
        <div class="small mono">TENANT_ID, MSAL_CLIENT_ID, MSAL_CLIENT_SECRET, DEFAULT_SITE_URL, DEFAULT_LIBRARY</div>
      </div>
    </details>
    <details class="panel">
      <summary>KV status</summary>
      <div class="panel-body">
        <div>${kvDiag.ok ? '<span class="ok">OK</span>' : '<span class="bad">ERROR</span>'}</div>
        <div class="small mono">${htmlEscape(kvDiag.info?.provider || "unknown")} · url=${kvDiag.info?.urlPresent ? "yes" : "no"} · token=${
        kvDiag.info?.tokenPresent ? "yes" : "no"
    }</div>
        ${kvDiag.ok ? "" : `<div class="small muted" style="margin-top:6px">${htmlEscape(kvDiag.error || "not configured")}</div>`}
      </div>
    </details>
    <details class="panel">
      <summary>Admin auth</summary>
      <div class="panel-body">
        <div>${ADMIN_PASSWORD && ADMIN_SESSION_SECRET ? '<span class="ok">Enabled</span>' : '<span class="warn">Setup needed</span>'}</div>
        <div class="small mono">ADMIN_USERNAME=${htmlEscape(ADMIN_USERNAME)}</div>
      </div>
    </details>
    <details class="panel">
      <summary>Planner sync env</summary>
      <div class="panel-body">
        <div>${plannerEnvOk ? '<span class="ok">Configured</span>' : '<span class="bad">Missing</span>'}</div>
        <div class="small mono">${plannerEnvOk ? "All required vars present" : htmlEscape(plannerEnvMissing.join(", "))}</div>
      </div>
    </details>
  </div>

  <details class="panel">
    <summary>Endpoint Health</summary>
    <div class="panel-body">
      <div class="small muted" style="margin-bottom:8px">Pings via ${htmlEscape(origin)}</div>
      <table>
        <thead><tr><th>Endpoint</th><th>Description</th><th>Status</th><th class="small">Info</th></tr></thead>
        <tbody>${endpointsTableRows || '<tr><td colspan="4" class="muted">No checks</td></tr>'}</tbody>
      </table>
    </div>
  </details>

  <details class="panel">
    <summary>USPS Address Formatter</summary>
    <div class="panel-body">
      <div class="small muted" style="margin-bottom:8px">Uploads → USPS Addresses 3.0</div>
      <form id="usps-form">
        <div class="row">
          <label for="usps-file">CSV file</label>
          <input id="usps-file" name="file" type="file" accept=".csv,text/csv" />
        </div>
        <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
          <button type="submit" id="usps-submit">Format addresses</button>
          <button type="button" id="usps-reset" style="background:#1f2a44;color:#e6ecff">Reset</button>
          <button type="button" id="usps-verify-btn" style="background:#0f8b4c;color:#fff">Verify USPS connection</button>
        </div>
        <div class="small muted">Header row required. Include <span class="mono">Address1</span> and either <span class="mono">City + State</span> or <span class="mono">Zip</span>.</div>
      </form>
      <div id="usps-status" class="small muted" style="margin-top:8px">Choose a CSV to start.</div>
      <div id="usps-summary" class="small" style="display:none;margin-top:6px"></div>
      <div id="usps-download-wrap" style="margin-top:8px;display:none">
        <a id="usps-download" class="badge" href="#" download="addresses-standardized.csv">Download standardized CSV</a>
      </div>
      <div id="usps-pending-download-wrap" style="margin-top:8px;display:none">
        <a id="usps-pending-download" class="badge" href="#" download="addresses-pending.csv">Download pending CSV</a>
      </div>
      <div id="usps-preview" class="small muted" style="margin-top:12px">Preview will show first rows after processing.</div>
    </div>
  </details>

  <details class="panel">
    <summary>Planner Sync</summary>
    <div class="panel-body">
      <div style="margin-bottom:8px">
        <a class="badge" href="/api/admin-debug">Open Debug Console</a>
      </div>
      <div class="row">
        <label for="planner-project-no">Project No (optional)</label>
        <input id="planner-project-no" placeholder="P-100" />
      </div>
      <div class="row">
        <label for="planner-debug-task-no">Task No (debug)</label>
        <input id="planner-debug-task-no" placeholder="1100" />
      </div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
        <button type="button" id="planner-run-bc">Run Sync</button>
        <button type="button" id="planner-run-bc-pr00001" style="background:#2b61d1;color:#fff">Run Sync (PR00001)</button>
        <button type="button" id="planner-debug-task" style="background:#0f8b4c;color:#fff">Inspect BC Task</button>
        <button type="button" id="planner-debug-bc-timestamps" style="background:#1f2a44;color:#e6ecff">Inspect BC timestamps</button>
        <button type="button" id="planner-debug-decision" style="background:#1f2a44;color:#e6ecff">Inspect sync decision</button>
      </div>
      <div id="planner-status" class="small muted" style="margin-top:8px">Ready.</div>
      <pre id="planner-output" class="log-block" style="display:none"></pre>
      <div class="small muted" style="margin-top:8px">Request log</div>
      <ul id="planner-log" class="step-list"></ul>
      <div class="small muted" style="margin-top:12px">Planner sync logs</div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
        <button type="button" id="planner-sync-log-load" style="background:#1f2a44;color:#e6ecff">Load logs</button>
        <button type="button" id="planner-sync-log-clear" style="background:#1f2a44;color:#e6ecff">Clear view</button>
      </div>
      <div id="planner-sync-log-status" class="small muted" style="margin-top:6px">Idle.</div>
      <pre id="planner-sync-log-output" class="log-block" style="display:none;max-height:360px;overflow:auto"></pre>
    </div>
  </details>

  <details class="panel">
    <summary>Planner Projects</summary>
    <div class="panel-body">
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
        <button type="button" id="planner-projects-refresh" style="background:#1f2a44;color:#e6ecff">Refresh list</button>
        <span class="small muted">Sync settings update per project</span>
      </div>
      <div class="row" style="margin:8px 0 12px 0">
        <input id="planner-projects-filter" placeholder="Filter by project no, status, or plan..." />
      </div>
      <div id="planner-projects-status" class="small muted" style="margin-bottom:8px">Loading planner projects…</div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px">
        <button type="button" id="planner-projects-select-all" style="background:#1f2a44;color:#e6ecff">Select all (filtered)</button>
        <button type="button" id="planner-projects-clear" style="background:#1f2a44;color:#e6ecff">Clear selection</button>
        <button type="button" id="planner-projects-bulk-enable" style="background:#2b61d1;color:#fff">Enable sync</button>
        <button type="button" id="planner-projects-bulk-disable" style="background:#ef4444;color:#fff">Disable sync</button>
        <button type="button" id="planner-projects-bulk-sync" style="background:#0f8b4c;color:#fff">Run sync</button>
        <span id="planner-projects-count" class="small muted">No projects selected</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>Project</th>
            <th>Description</th>
            <th>Planner Plan</th>
            <th>Last sync</th>
            <th>Sync</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="planner-projects-tbody">
          <tr><td colspan="7" class="muted">Loading…</td></tr>
        </tbody>
      </table>
      <div id="planner-orphan-plans" class="small muted" style="margin-top:8px"></div>
      <div class="small muted" style="margin-top:8px">Disable sync to prevent plan recreation after deleting a plan.</div>
      <div class="small muted" style="margin-top:6px">Delete plan disables sync and clears Planner links in BC.</div>
    </div>
  </details>

  <details class="panel">
    <summary>API Debugging</summary>
    <div class="panel-body">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px">
        <button type="button" id="debug-refresh-btn" onclick="if(window.__debugAction){window.__debugAction('refresh');}else{document.getElementById('debug-status').textContent='Debug JS not loaded';}">Refresh Debug Info</button>
        <div class="small muted">Test routes and view request details</div>
      </div>
      <div class="row" style="margin:8px 0 12px 0">
        <button type="button" id="debug-test-btn" onclick="if(window.__debugAction){window.__debugAction('test');}else{document.getElementById('debug-status').textContent='Debug JS not loaded';}" style="background:#2b61d1;color:#fff">Test Debug Endpoint</button>
        <button type="button" id="debug-clear-btn" onclick="if(window.__debugAction){window.__debugAction('clear');}else{document.getElementById('debug-status').textContent='Debug JS not loaded';}" style="background:#1f2a44;color:#e6ecff">Clear</button>
      </div>
      <div id="debug-status" class="small muted" style="margin-top:8px">Click "Test Debug Endpoint" to fetch debug information.</div>
      <pre id="debug-output" class="log-block" style="display:none;max-height:400px;overflow:auto"></pre>
      <div id="debug-routes" style="margin-top:12px;display:none">
        <div style="font-weight:600;margin-bottom:8px">Available API Routes:</div>
        <table>
          <thead><tr><th>Method</th><th>Route</th><th>Description</th><th>Action</th></tr></thead>
          <tbody id="debug-routes-tbody"></tbody>
        </table>
      </div>
    </div>
  </details>

  <details class="panel">
    <summary>Submissions</summary>
    <div class="panel-body">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px">
        <button type="button" id="refresh-btn">Refresh</button>
        <div class="small muted">Use Refresh to update · Showing latest ${items.length}</div>
      </div>
      <div class="row" style="margin:8px 0 12px 0">
        <input id="filter-input" placeholder="Filter by text (type, folder, status, error, trace)..." />
      </div>
      <table>
        <thead><tr><th>Time</th><th>Type</th><th>Status</th><th>Details</th><th>Uploaded</th><th>Files</th></tr></thead>
        <tbody id="subs-tbody">${itemsRows || '<tr><td colspan="6" class="muted">No submissions yet.</td></tr>'}</tbody>
      </table>
      <div class="small muted" id="last-updated" style="margin-top:6px">Last updated: pending</div>
    </div>
  </details>

  <script src="/api/admin-runtime.js" defer></script>`;

    return layout("Admin Dashboard", inner);
}

export default async function handler(req, res) {
    // Serve HTML only; disable caching
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");

    const cookies = parseCookies(req);
    const sess = verifySession(cookies["admin_session"]);

    if (req.method === "GET") {
        if (sess) {
            const html = await dashboardView(req);
            res.status(200).send(html);
            return;
        }
        res.status(200).send(loginView());
        return;
    }

    if (req.method === "POST") {
        // Parse form and validate
        const form = await readFormBody(req);
        const u = String(form.username || "").trim();
        const p = String(form.password || "").trim();
        const userOk = u === ADMIN_USERNAME;
        const passOk = safeEqual(p, ADMIN_PASSWORD);
        if (!userOk || !passOk || !ADMIN_SESSION_SECRET) {
            res.status(200).send(loginView("Invalid credentials or admin secret not configured"));
            return;
        }
        const cookie = sessionCookie(u);
        if (!cookie) {
            res.status(200).send(loginView("Admin session secret not configured"));
            return;
        }
        res.setHeader("Set-Cookie", cookie);
        const html = await dashboardView(req);
        res.status(200).send(html);
        return;
    }

    res.status(405).send(layout("Method Not Allowed", `<div class=\"wrap\"><div class=\"panel\">Method not allowed</div></div>`));
}
