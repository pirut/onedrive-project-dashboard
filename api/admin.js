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
    <div class="panel">
      <div class="muted small">Graph env</div>
      <div>${graphEnvOk ? '<span class="ok">Configured</span>' : '<span class="bad">Missing</span>'}</div>
      <div class="small mono">TENANT_ID, MSAL_CLIENT_ID, MSAL_CLIENT_SECRET, DEFAULT_SITE_URL, DEFAULT_LIBRARY</div>
    </div>
    <div class="panel">
      <div class="muted small">KV status</div>
      <div>${kvDiag.ok ? '<span class="ok">OK</span>' : '<span class="bad">ERROR</span>'}</div>
      <div class="small mono">${htmlEscape(kvDiag.info?.provider || "unknown")} · url=${kvDiag.info?.urlPresent ? "yes" : "no"} · token=${
        kvDiag.info?.tokenPresent ? "yes" : "no"
    }</div>
      ${kvDiag.ok ? "" : `<div class="small muted" style="margin-top:6px">${htmlEscape(kvDiag.error || "not configured")}</div>`}
    </div>
    <div class="panel">
      <div class="muted small">Admin auth</div>
      <div>${ADMIN_PASSWORD && ADMIN_SESSION_SECRET ? '<span class="ok">Enabled</span>' : '<span class="warn">Setup needed</span>'}</div>
      <div class="small mono">ADMIN_USERNAME=${htmlEscape(ADMIN_USERNAME)}</div>
    </div>
    <div class="panel">
      <div class="muted small">Planner sync env</div>
      <div>${plannerEnvOk ? '<span class="ok">Configured</span>' : '<span class="bad">Missing</span>'}</div>
      <div class="small mono">${plannerEnvOk ? "All required vars present" : htmlEscape(plannerEnvMissing.join(", "))}</div>
    </div>
  </div>

  <div class="panel">
    <div style="display:flex;align-items:center;gap:8px;justify-content:space-between">
      <div style="font-weight:600">Endpoint Health</div>
      <div class="small muted">Pings via ${htmlEscape(origin)}</div>
    </div>
    <table>
      <thead><tr><th>Endpoint</th><th>Description</th><th>Status</th><th class="small">Info</th></tr></thead>
      <tbody>${endpointsTableRows || '<tr><td colspan="4" class="muted">No checks</td></tr>'}</tbody>
    </table>
  </div>

  <div class="panel">
    <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;flex-wrap:wrap">
      <div style="font-weight:600">USPS Address Formatter</div>
      <div class="small muted">Uploads → USPS Addresses 3.0</div>
    </div>
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

  <div class="panel">
    <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;flex-wrap:wrap">
      <div style="font-weight:600">Planner Sync</div>
      <div class="small muted">Run sync + subscriptions</div>
    </div>
    <div class="row">
      <label for="planner-project-no">Project No (optional)</label>
      <input id="planner-project-no" placeholder="P-100" />
    </div>
    <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
      <button type="button" id="planner-run-bc">Run BC → Planner</button>
      <button type="button" id="planner-run-poll" style="background:#1f2a44;color:#e6ecff">Run polling</button>
    </div>
    <div class="row">
      <label for="planner-notify-url">Notification URL (optional)</label>
      <input id="planner-notify-url" placeholder="https://your-domain.com/api/webhooks/graph/planner" />
    </div>
    <div class="row">
      <label for="planner-plan-ids">Plan IDs (optional, comma-separated)</label>
      <input id="planner-plan-ids" placeholder="planId1, planId2" />
    </div>
    <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
      <button type="button" id="planner-create-subs" style="background:#0f8b4c;color:#fff">Create subscriptions</button>
      <button type="button" id="planner-renew-subs" style="background:#1f2a44;color:#e6ecff">Renew subscriptions</button>
      <button type="button" id="planner-test-webhook" style="background:#1f2a44;color:#e6ecff">Test webhook validation</button>
    </div>
    <div id="planner-status" class="small muted" style="margin-top:8px">Ready.</div>
    <pre id="planner-output" class="log-block" style="display:none"></pre>
  </div>

  <div class="panel">
    <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;flex-wrap:wrap">
      <div style="font-weight:600">API Debugging</div>
      <div style="display:flex;align-items:center;gap:8px">
        <button type="button" id="debug-refresh-btn">Refresh Debug Info</button>
        <div class="small muted">Test routes and view request details</div>
      </div>
    </div>
    <div class="row" style="margin:8px 0 12px 0">
      <button type="button" id="debug-test-btn" style="background:#2b61d1;color:#fff">Test Debug Endpoint</button>
      <button type="button" id="debug-clear-btn" style="background:#1f2a44;color:#e6ecff">Clear</button>
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

  <div class="panel">
    <div style="display:flex;align-items:center;gap:8px;justify-content:space-between;flex-wrap:wrap">
      <div style="font-weight:600">Submissions</div>
      <div style="display:flex;align-items:center;gap:8px">
        <button type="button" id="refresh-btn">Refresh</button>
        <div class="small muted">Use Refresh to update · Showing latest ${items.length}</div>
      </div>
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

  <script>
    (function(){
      var tbody = document.getElementById('subs-tbody');
      var input = document.getElementById('filter-input');
      var last = document.getElementById('last-updated');
      var refreshBtn = document.getElementById('refresh-btn');
      var uspsForm = document.getElementById('usps-form');
      var uspsFileInput = document.getElementById('usps-file');
      var uspsStatusEl = document.getElementById('usps-status');
      var uspsSummaryEl = document.getElementById('usps-summary');
      var uspsDownloadWrap = document.getElementById('usps-download-wrap');
      var uspsDownloadEl = document.getElementById('usps-download');
      var uspsPendingDownloadWrap = document.getElementById('usps-pending-download-wrap');
      var uspsPendingDownloadEl = document.getElementById('usps-pending-download');
      var uspsPreviewEl = document.getElementById('usps-preview');
      var uspsResetBtn = document.getElementById('usps-reset');
      var uspsVerifyBtn = document.getElementById('usps-verify-btn');
      var exportCsvBtn = document.getElementById('export-active-csv');
      var exportJsonBtn = document.getElementById('export-active-json');
      var plannerProjectInput = document.getElementById('planner-project-no');
      var plannerNotifyInput = document.getElementById('planner-notify-url');
      var plannerPlanIdsInput = document.getElementById('planner-plan-ids');
      var plannerRunBcBtn = document.getElementById('planner-run-bc');
      var plannerRunPollBtn = document.getElementById('planner-run-poll');
      var plannerCreateSubsBtn = document.getElementById('planner-create-subs');
      var plannerRenewSubsBtn = document.getElementById('planner-renew-subs');
      var plannerTestWebhookBtn = document.getElementById('planner-test-webhook');
      var plannerStatusEl = document.getElementById('planner-status');
      var plannerOutputEl = document.getElementById('planner-output');
      var uspsLoading = false;
      var uspsDownloadUrl = null;
      var uspsPendingDownloadUrl = null;
      var cache = [];
      var refreshing = false;

      function htmlEscape(s){
        return String(s)
          .replaceAll('&','&amp;')
          .replaceAll('<','&lt;')
          .replaceAll('>','&gt;')
          .replaceAll('"','&quot;')
          .replaceAll("'","&#39;");
      }
      function statusCls(s){
        s = String(s||'').toLowerCase();
        if(s==='ok' || s==='success' || s==='validated') return 'ok';
        if(s==='error' || s==='failed' || s==='fail') return 'bad';
        if(s==='warn' || s==='warning') return 'warn';
        return s ? 'warn' : 'muted';
      }
      function setUspsStatus(text, tone){
        if(!uspsStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        uspsStatusEl.textContent = text;
        uspsStatusEl.className = 'small ' + toneClass;
      }
      function setPlannerStatus(text, tone){
        if(!plannerStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        plannerStatusEl.textContent = text;
        plannerStatusEl.className = 'small ' + toneClass;
      }
      function renderPlannerOutput(payload){
        if(!plannerOutputEl) return;
        if(payload == null){
          plannerOutputEl.style.display = 'none';
          plannerOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        plannerOutputEl.textContent = text;
        plannerOutputEl.style.display = 'block';
      }
      function renderUspsPreview(rows){
        if(!uspsPreviewEl) return;
        if(!Array.isArray(rows) || !rows.length){
          uspsPreviewEl.className = 'small muted';
          uspsPreviewEl.textContent = 'Preview will show first rows after processing.';
          return;
        }
        var total = rows.length;
        var sample = rows.slice(0, Math.min(total, 5));
        var items = sample.map(function(row){
          var status = row && row.status ? String(row.status) : (row && row.error ? 'error' : (row && (row.address1 || row.zip5) ? 'success' : 'pending'));
          var cls = statusCls(status);
          var addressParts = [];
          if(row && row.address1) addressParts.push(row.address1);
          if(row && row.address2) addressParts.push(row.address2);
          var cityState = [];
          if(row && row.city) cityState.push(row.city);
          if(row && row.state) cityState.push(row.state);
          if(cityState.length) addressParts.push(cityState.join(', '));
          var zip = '';
          if(row){
            var zip5 = (row.zip5 || '').toString().trim();
            var zip4 = (row.zip4 || '').toString().trim();
            if(zip5) zip = zip5 + (zip4 ? '-' + zip4 : '');
          }
          if(zip) addressParts.push(zip);
          if(row && row.country) addressParts.push(row.country);
          var displayAddress = addressParts.join(', ');
          if(!displayAddress) displayAddress = '(no standardized match)';
          var inputParts = [];
          if(row && row.input_address1) inputParts.push(row.input_address1);
          if(row && row.input_address2) inputParts.push(row.input_address2);
          var inputCityState = [];
          if(row && row.input_city) inputCityState.push(row.input_city);
          if(row && row.input_state) inputCityState.push(row.input_state);
          if(inputCityState.length) inputParts.push(inputCityState.join(', '));
          if(row){
            var inputZip = '';
            if(row.input_zip5) inputZip = String(row.input_zip5);
            if(row.input_zip4) inputZip = inputZip ? inputZip + '-' + row.input_zip4 : String(row.input_zip4);
            if(inputZip) inputParts.push(inputZip);
          }
          if(row && row.input_country) inputParts.push(row.input_country);
          var metaParts = [];
          if(row && row.row != null) metaParts.push('row ' + row.row);
          if(inputParts.length) metaParts.push('input: ' + inputParts.join(', '));
          if(row && row.dpvConfirmation) metaParts.push('dpv: ' + row.dpvConfirmation);
          var meta = metaParts.map(function(part){ return htmlEscape(String(part)); }).join(' · ');
          var errorBlock = row && row.error ? '<div class="bad small">' + htmlEscape(String(row.error)) + '</div>' : '';
          var notesBlock = row && row.footnotes ? '<div class="muted small">' + htmlEscape('Notes: ' + row.footnotes) + '</div>' : '';
          return '<li>'
            + '<span class="badge ' + cls + '">' + htmlEscape(String(status)) + '</span> '
            + htmlEscape(displayAddress)
            + (meta ? '<span class="meta">' + meta + '</span>' : '')
            + errorBlock
            + notesBlock
            + '</li>';
        }).join('');
        uspsPreviewEl.className = 'small';
        uspsPreviewEl.innerHTML = '<div class="muted small">Preview (' + sample.length + ' of ' + total + ')</div><ul class="step-list">' + items + '</ul>';
      }
      function resetUspsState(options){
        if(uspsDownloadUrl){
          URL.revokeObjectURL(uspsDownloadUrl);
          uspsDownloadUrl = null;
        }
        if(uspsPendingDownloadUrl){
          URL.revokeObjectURL(uspsPendingDownloadUrl);
          uspsPendingDownloadUrl = null;
        }
        if(uspsSummaryEl){
          uspsSummaryEl.style.display = 'none';
          uspsSummaryEl.textContent = '';
        }
        if(uspsDownloadWrap){
          uspsDownloadWrap.style.display = 'none';
        }
        if(uspsPendingDownloadWrap){
          uspsPendingDownloadWrap.style.display = 'none';
        }
        renderUspsPreview([]);
        if(options && options.clearFile && uspsFileInput){
          uspsFileInput.value = '';
        }
        if(!options || options.keepStatus !== true){
          setUspsStatus('Choose a CSV to start.', 'muted');
        }
      }
      resetUspsState();
      function formatStep(step){
        if(!step) return '';
        var clone = Object.assign({}, step);
        var ts = clone.ts ? '<span class="mono small">' + htmlEscape(clone.ts) + '</span>' : '';
        var msg = clone.msg ? htmlEscape(clone.msg) : '';
        delete clone.ts;
        delete clone.msg;
        var metaEntries = Object.keys(clone).filter(function(k){ return clone[k] !== undefined && clone[k] !== null && clone[k] !== ''; });
        var meta = '';
        if(metaEntries.length){
          var metaStr = metaEntries.map(function(k){
            var v = clone[k];
            if(typeof v === 'object'){ try { v = JSON.stringify(v); } catch(e){ v = String(v); } }
            v = String(v);
            if(v.length > 240) v = v.slice(0,240) + '…';
            return k + '=' + v;
          }).join(' · ');
          meta = '<span class="meta">' + htmlEscape(metaStr) + '</span>';
        }
        return '<li>' + (ts ? ts + (msg ? ' ' : '') : '') + msg + meta + '</li>';
      }
      function buildDetails(it){
        var parts = [];
        if(it.folderName) parts.push('folder: <span class="mono">' + htmlEscape(it.folderName) + '</span>');
        if(it.phase) parts.push('phase: <span class="mono">' + htmlEscape(it.phase) + '</span>');
        if(it.traceId) parts.push('trace: <span class="mono">' + htmlEscape(it.traceId) + '</span>');
        if(it.errorStatus != null) parts.push('status: <span class="mono">' + htmlEscape(String(it.errorStatus)) + '</span>');
        if(it.errorContentRange) parts.push('range: <span class="mono">' + htmlEscape(it.errorContentRange) + '</span>');
        if(it.reason) parts.push('reason: ' + htmlEscape(it.reason));
        if(it.error) parts.push('error: ' + htmlEscape(it.error));
        var main = parts.join(' · ');
        var extras = [];
        if(Array.isArray(it.steps) && it.steps.length){
          extras.push('<details class="log"><summary>Logs (' + it.steps.length + ')</summary><ul class="step-list">' + it.steps.map(formatStep).join('') + '</ul></details>');
        }
        if(typeof it.errorResponse === 'string' && it.errorResponse){
          var resp = it.errorResponse.length > 2000 ? it.errorResponse.slice(0,2000) + '…' : it.errorResponse;
          extras.push('<details class="log"><summary>Error response</summary><pre class="log-block">' + htmlEscape(resp) + '</pre></details>');
        }
        if(typeof it.errorStack === 'string' && it.errorStack){
          var stack = it.errorStack.length > 4000 ? it.errorStack.slice(0,4000) + '…' : it.errorStack;
          extras.push('<details class="log"><summary>Error stack</summary><pre class="log-block">' + htmlEscape(stack) + '</pre></details>');
        }
        if(!main && !extras.length) return '<span class="muted">—</span>';
        return main + (extras.length ? '<div class="log-extras">' + extras.join('') + '</div>' : '');
      }
      function render(items){
        var q = (input.value||'').trim().toLowerCase();
        var filtered = !q ? items : items.filter(function(it){
          var hay = [
            it.loggedAt,
            it.type,
            it.status,
            it.folderName,
            it.phase,
            it.reason,
            it.error,
            it.traceId,
            it.errorStatus,
            it.errorContentRange,
            it.errorResponse,
            (it.uploaded==null?'':String(it.uploaded))
          ].join(' ').toLowerCase();
          return hay.indexOf(q) !== -1;
        });
        var rows = filtered.map(function(it){
          var files = Array.isArray(it.files) ? it.files : [];
          var filesCount = files.length ? (files.length + ' file' + (files.length===1?'':'s')) : '';
          var links = files.map(function(f){
            return '<a href="' + htmlEscape(f.url||'') + '" target="_blank" rel="noreferrer">' + htmlEscape(f.filename||f.url||'file') + '</a>';
          }).join(', ');
          var details = buildDetails(it);
          return '<tr>'
            + '<td class="mono small">' + htmlEscape(it.loggedAt||'') + '</td>'
            + '<td>' + htmlEscape(it.type||'') + '</td>'
            + '<td><span class="badge ' + statusCls(it.status) + '">' + (htmlEscape(it.status||'')||'n/a') + '</span></td>'
            + '<td>' + details + '</td>'
            + '<td style="text-align:center">' + htmlEscape(it.uploaded==null?'':String(it.uploaded)) + '</td>'
            + '<td>' + filesCount + (links?'<div class="small">' + links + '</div>':'') + '</td>'
            + '</tr>';
        }).join('');
        tbody.innerHTML = rows || '<tr><td colspan="6" class="muted">No submissions yet.</td></tr>';
        last.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
      }
      async function fetchAndRender(){
        try{
          var res = await fetch('/api/submissions?limit=500', { headers: { 'cache-control': 'no-cache' } });
          if(!res.ok) return;
          var data = await res.json();
          cache = Array.isArray(data.items)? data.items : [];
          render(cache);
          if(last) last.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
        }catch(e){
          if(last) last.textContent = 'Last updated: failed (' + (e && e.message ? e.message : 'error') + ')';
        }
      }
      if(uspsResetBtn){
        uspsResetBtn.addEventListener('click', function(){
          resetUspsState({ clearFile: true });
        });
      }
      if(uspsVerifyBtn){
        uspsVerifyBtn.addEventListener('click', async function(){
          if(uspsLoading) return;
          uspsLoading = true;
          var originalText = uspsVerifyBtn.textContent;
          uspsVerifyBtn.disabled = true;
          uspsVerifyBtn.textContent = 'Verifying…';
          setUspsStatus('Verifying USPS credentials…', 'muted');
          try{
            var res = await fetch('/api/usps-verify');
            var payload;
            var ct = res.headers.get('content-type') || '';
            if(ct.indexOf('application/json') !== -1){
              payload = await res.json();
            } else {
              var raw = await res.text();
              try{ payload = JSON.parse(raw); }
              catch(e){ throw new Error(raw || ('HTTP ' + res.status)); }
            }
            if(!res.ok || (payload && payload.ok === false)){
              var message = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(message);
            }
            var expires = payload && payload.expiresIn ? ('Token expires in ' + Math.round(payload.expiresIn) + 's.') : 'Token verified.';
            var preview = payload && payload.tokenPreview ? ' Preview: ' + payload.tokenPreview : '';
            setUspsStatus('USPS credentials verified. ' + expires + preview, 'ok');
          }catch(err){
            setUspsStatus('Verification failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          }finally{
            uspsLoading = false;
            uspsVerifyBtn.disabled = false;
            uspsVerifyBtn.textContent = originalText;
          }
        });
      }
      if(uspsFileInput){
        uspsFileInput.addEventListener('change', function(){
          if(uspsFileInput.files && uspsFileInput.files.length){
            resetUspsState({ keepStatus: true });
            setUspsStatus('Ready to format ' + uspsFileInput.files[0].name + '.', 'muted');
          } else {
            resetUspsState();
          }
        });
      }
      if(uspsForm){
        uspsForm.addEventListener('submit', async function(ev){
          ev.preventDefault();
          if(uspsLoading) return;
          if(!uspsFileInput || !uspsFileInput.files || !uspsFileInput.files[0]){
            setUspsStatus('Choose a CSV file first.', 'warn');
            return;
          }
          uspsLoading = true;
          resetUspsState({ keepStatus: true });
          setUspsStatus('Formatting addresses…', 'muted');
          try{
            var fd = new FormData();
            var file = uspsFileInput.files[0];
            fd.append('file', file, file.name);
            var res = await fetch('/api/usps-format', { method: 'POST', body: fd });
            var payload;
            var ct = res.headers.get('content-type') || '';
            if(ct.indexOf('application/json') !== -1){
              payload = await res.json();
            } else {
              var rawText = await res.text();
              try { payload = JSON.parse(rawText); }
              catch(parseErr){ throw new Error(rawText || ('HTTP ' + res.status)); }
            }
            if(!res.ok || (payload && payload.ok === false) || (payload && payload.error)){
              var errMessage = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(errMessage);
            }
            if(uspsSummaryEl && payload && payload.summary){
              var summary = payload.summary || {};
              var processed = summary.processed != null ? summary.processed : (summary.success != null ? summary.success : 0);
              var total = summary.total != null ? summary.total : 0;
              var pendingCount = summary.pending != null ? summary.pending : 0;
              var errorCount = summary.errors != null ? summary.errors : 0;
              var requestsUsed = summary.requestsUsed;
              var maxRequests = summary.maxRequests;
              var uniqueRequests = summary.uniqueRequests;
              var summaryParts = [];
              summaryParts.push('Processed ' + processed + ' of ' + total + ' rows.');
              if(pendingCount){ summaryParts.push(pendingCount + ' pending (quota limit).'); }
              if(errorCount){ summaryParts.push(errorCount + ' errors.'); }
              if(requestsUsed != null && maxRequests != null){ summaryParts.push('USPS requests used: ' + requestsUsed + '/' + maxRequests + '.'); }
              if(uniqueRequests != null){ summaryParts.push('Unique addresses this run: ' + uniqueRequests + '.'); }
              uspsSummaryEl.style.display = 'block';
              uspsSummaryEl.className = 'small';
              uspsSummaryEl.textContent = summaryParts.join(' ');
            }
            if(uspsDownloadWrap && uspsDownloadEl){
              if(payload && payload.csv){
                if(uspsDownloadUrl){ URL.revokeObjectURL(uspsDownloadUrl); }
                uspsDownloadUrl = URL.createObjectURL(new Blob([payload.csv], { type: 'text/csv;charset=utf-8;' }));
                uspsDownloadEl.href = uspsDownloadUrl;
                uspsDownloadEl.download = (payload && payload.filename) || 'addresses-standardized.csv';
                uspsDownloadWrap.style.display = 'block';
              } else {
                if(uspsDownloadUrl){ URL.revokeObjectURL(uspsDownloadUrl); uspsDownloadUrl = null; }
                uspsDownloadWrap.style.display = 'none';
              }
            }
            if(uspsPendingDownloadWrap && uspsPendingDownloadEl){
              if(payload && payload.pendingCsv){
                if(uspsPendingDownloadUrl){ URL.revokeObjectURL(uspsPendingDownloadUrl); }
                uspsPendingDownloadUrl = URL.createObjectURL(new Blob([payload.pendingCsv], { type: 'text/csv;charset=utf-8;' }));
                uspsPendingDownloadEl.href = uspsPendingDownloadUrl;
                uspsPendingDownloadEl.download = (payload && payload.pendingFilename) || 'addresses-pending.csv';
                uspsPendingDownloadWrap.style.display = 'block';
              } else {
                if(uspsPendingDownloadUrl){ URL.revokeObjectURL(uspsPendingDownloadUrl); uspsPendingDownloadUrl = null; }
                uspsPendingDownloadWrap.style.display = 'none';
              }
            }
            if(payload && Array.isArray(payload.rows)){
              renderUspsPreview(payload.rows);
            } else {
              renderUspsPreview([]);
            }
            var sum = payload && payload.summary ? payload.summary : {};
            var sumProcessed = sum.processed != null ? sum.processed : (sum.success != null ? sum.success : 0);
            var sumPending = sum.pending != null ? sum.pending : 0;
            var sumErrors = sum.errors != null ? sum.errors : 0;
            var tone = 'ok';
            var statusMsg = 'Formatting complete.';
            if(sumPending){
              tone = 'warn';
              statusMsg = 'Processed ' + sumProcessed + ' rows; ' + sumPending + ' pending due to quota. Download the pending list to resume later.';
            } else if(sumErrors){
              tone = 'warn';
              statusMsg = 'Processed ' + sumProcessed + ' rows with ' + sumErrors + ' errors. Review the preview for details.';
            } else if(payload && (!payload.csv || sumProcessed === 0)){
              statusMsg = 'No rows processed.';
            } else {
              statusMsg += ' Download is ready.';
            }
            setUspsStatus(statusMsg, tone);
          }catch(err){
            resetUspsState({ keepStatus: true });
            setUspsStatus('Error: ' + (err && err.message ? err.message : 'Unable to format addresses.'), 'bad');
          }finally{
            uspsLoading = false;
          }
        });
      }
      input.addEventListener('input', function(){ render(cache); });
      async function runPlannerRequest(options){
        var url = options.url;
        var body = options.body;
        var method = options.method || 'POST';
        setPlannerStatus(options.label + '…', 'muted');
        renderPlannerOutput(null);
        try{
          var res = await fetch(url, {
            method: method,
            headers: body ? { 'Content-Type': 'application/json' } : undefined,
            body: body ? JSON.stringify(body) : undefined
          });
          var ct = res.headers.get('content-type') || '';
          var payload;
          if(ct.indexOf('application/json') !== -1){
            payload = await res.json();
          } else {
            payload = await res.text();
          }
          if(!res.ok || (payload && payload.ok === false)){
            var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
            throw new Error(msg);
          }
          setPlannerStatus(options.label + ' complete.', 'ok');
          renderPlannerOutput(payload);
          return;
        }catch(err){
          setPlannerStatus(options.label + ' failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          renderPlannerOutput(err && err.message ? err.message : err);
        }
      }
      if(plannerRunBcBtn){
        plannerRunBcBtn.addEventListener('click', function(){
          var projectNo = plannerProjectInput && plannerProjectInput.value ? plannerProjectInput.value.trim() : '';
          var body = projectNo ? { projectNo: projectNo } : undefined;
          runPlannerRequest({ label: 'BC → Planner sync', url: '/api/sync/run-bc-to-planner', body: body });
        });
      }
      if(plannerRunPollBtn){
        plannerRunPollBtn.addEventListener('click', function(){
          runPlannerRequest({ label: 'Polling sync', url: '/api/sync/run-poll' });
        });
      }
      if(plannerCreateSubsBtn){
        plannerCreateSubsBtn.addEventListener('click', function(){
          var notificationUrl = plannerNotifyInput && plannerNotifyInput.value ? plannerNotifyInput.value.trim() : '';
          var planIdsRaw = plannerPlanIdsInput && plannerPlanIdsInput.value ? plannerPlanIdsInput.value : '';
          var planIds = planIdsRaw.split(',').map(function(id){ return id.trim(); }).filter(Boolean);
          var body = {};
          if(notificationUrl) body.notificationUrl = notificationUrl;
          if(planIds.length) body.planIds = planIds;
          runPlannerRequest({ label: 'Create subscriptions', url: '/api/sync/subscriptions/create', body: Object.keys(body).length ? body : undefined });
        });
      }
      if(plannerRenewSubsBtn){
        plannerRenewSubsBtn.addEventListener('click', function(){
          runPlannerRequest({ label: 'Renew subscriptions', url: '/api/sync/subscriptions/renew' });
        });
      }
      if(plannerTestWebhookBtn){
        plannerTestWebhookBtn.addEventListener('click', function(){
          var token = 'test_' + Math.random().toString(36).slice(2, 10);
          runPlannerRequest({
            label: 'Webhook validation',
            url: '/api/webhooks/graph/planner?validationToken=' + encodeURIComponent(token),
            method: 'POST'
          });
        });
      }

      async function downloadActiveProjects(format){
        var target = format === 'json' ? exportJsonBtn : exportCsvBtn;
        if(!target) return;
        try {
          console.log('[Export] Starting download for format:', format);
        } catch(_) {}
        var originalText = target.textContent;
        target.disabled = true;
        target.textContent = (format === 'json' ? 'Exporting JSON…' : 'Exporting CSV…');
        try{
          var res = await fetch('/api/projects-kanban/export?format=' + encodeURIComponent(format || 'csv'), {
            headers: { 'cache-control': 'no-cache' }
          });
          try {
            console.log('[Export] Response status:', res.status, 'ok:', res.ok);
          } catch(_) {}
          if(!res.ok){
            var errPayload = null;
            try { errPayload = await res.json(); } catch(parseErr){
              try { console.log('[Export] Failed to parse error JSON:', parseErr); } catch(__){}
            }
            var msg = (errPayload && errPayload.error) ? errPayload.error : ('HTTP ' + res.status);
            try {
              console.log('[Export] Error payload:', errPayload);
            } catch(_) {}
            throw new Error(msg);
          }
          var ct = res.headers.get('content-type') || '';
          try {
            console.log('[Export] Content-Type:', ct);
          } catch(_) {}
          var filename = (format === 'json') ? 'active-projects.json' : 'active-projects.csv';
          var blob;
          if(ct.indexOf('application/json') !== -1 || format === 'json'){
            var text = await res.text();
            try {
              console.log('[Export] JSON text length:', text && text.length);
            } catch(_) {}
            blob = new Blob([text], { type: 'application/json;charset=utf-8;' });
          } else {
            var textCsv = await res.text();
            try {
              console.log('[Export] CSV text length:', textCsv && textCsv.length);
            } catch(_) {}
            blob = new Blob([textCsv], { type: 'text/csv;charset=utf-8;' });
          }
          var url = URL.createObjectURL(blob);
          var a = document.createElement('a');
          a.href = url;
          a.download = filename;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
        } catch(e){
          try {
            console.error('[Export] Export failed:', e);
          } catch(_) {}
          alert('Export failed: ' + (e && e.message ? e.message : 'error'));
        } finally {
          target.disabled = false;
          target.textContent = originalText;
        }
      }

      if(exportCsvBtn){
        exportCsvBtn.addEventListener('click', function(){
          downloadActiveProjects('csv');
        });
      }
      if(exportJsonBtn){
        exportJsonBtn.addEventListener('click', function(){
          downloadActiveProjects('json');
        });
      }
      async function handleRefresh(){
        if(refreshing) return;
        refreshing = true;
        if(refreshBtn){ refreshBtn.disabled = true; refreshBtn.textContent = 'Refreshing…'; }
        try{
          await fetchAndRender();
        } finally {
          refreshing = false;
          if(refreshBtn){ refreshBtn.disabled = false; refreshBtn.textContent = 'Refresh'; }
        }
      }
      if(refreshBtn){ refreshBtn.addEventListener('click', handleRefresh); }

      // Debug panel handlers
      var debugTestBtn = document.getElementById('debug-test-btn');
      var debugClearBtn = document.getElementById('debug-clear-btn');
      var debugRefreshBtn = document.getElementById('debug-refresh-btn');
      var debugStatusEl = document.getElementById('debug-status');
      var debugOutputEl = document.getElementById('debug-output');
      var debugRoutesEl = document.getElementById('debug-routes');
      var debugRoutesTbody = document.getElementById('debug-routes-tbody');
      
      function setDebugStatus(text, tone){
        if(!debugStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        debugStatusEl.textContent = text;
        debugStatusEl.className = 'small ' + toneClass;
      }
      
      function renderDebugOutput(payload){
        if(!debugOutputEl) return;
        if(payload == null){
          debugOutputEl.style.display = 'none';
          debugOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        debugOutputEl.textContent = text;
        debugOutputEl.style.display = 'block';
      }
      
      function renderDebugRoutes(routes){
        if(!debugRoutesTbody || !debugRoutesEl) return;
        if(!routes || typeof routes !== 'object'){
          debugRoutesEl.style.display = 'none';
          return;
        }
        
        var rows = Object.keys(routes).map(function(route){
          var desc = routes[route];
          var parts = route.split(' ');
          var method = parts[0] || 'GET';
          var path = parts.slice(1).join(' ') || route;
          var testBtn = '<button type="button" onclick="testRoute(\'' + htmlEscape(route) + '\')" style="background:#2b61d1;color:#fff;border:0;border-radius:4px;padding:4px 8px;cursor:pointer;font-size:11px">Test</button>';
          return '<tr>' +
            '<td><span class="mono small">' + htmlEscape(method) + '</span></td>' +
            '<td><span class="mono small">' + htmlEscape(path) + '</span></td>' +
            '<td>' + htmlEscape(desc) + '</td>' +
            '<td>' + testBtn + '</td>' +
            '</tr>';
        }).join('');
        
        debugRoutesTbody.innerHTML = rows || '<tr><td colspan="4" class="muted">No routes available</td></tr>';
        debugRoutesEl.style.display = 'block';
      }
      
      window.testRoute = function(route){
        var parts = route.split(' ');
        var method = parts[0] || 'GET';
        var path = parts.slice(1).join(' ') || route;
        setDebugStatus('Testing ' + route + '…', 'muted');
        renderDebugOutput(null);
        fetch(path, {
          method: method,
          headers: method === 'POST' ? { 'Content-Type': 'application/json' } : {}
        })
        .then(function(res){
          return res.text().then(function(text){
            var payload;
            try { payload = JSON.parse(text); }
            catch(e){ payload = text; }
            return { ok: res.ok, status: res.status, payload: payload };
          });
        })
        .then(function(result){
          if(result.ok){
            setDebugStatus('Route test successful (HTTP ' + result.status + ')', 'ok');
          } else {
            setDebugStatus('Route test failed (HTTP ' + result.status + ')', 'bad');
          }
          renderDebugOutput(result);
        })
        .catch(function(err){
          setDebugStatus('Route test error: ' + (err && err.message ? err.message : 'error'), 'bad');
          renderDebugOutput({ error: err && err.message ? err.message : String(err) });
        });
      };
      
      if(debugTestBtn){
        debugTestBtn.addEventListener('click', async function(){
          setDebugStatus('Fetching debug information…', 'muted');
          renderDebugOutput(null);
          try{
            var res = await fetch('/api/debug', {
              method: 'GET',
              headers: { 'cache-control': 'no-cache' }
            });
            var payload;
            var ct = res.headers.get('content-type') || '';
            if(ct.indexOf('application/json') !== -1){
              payload = await res.json();
            } else {
              var raw = await res.text();
              try{ payload = JSON.parse(raw); }
              catch(e){ throw new Error(raw || ('HTTP ' + res.status)); }
            }
            if(!res.ok || (payload && payload.ok === false)){
              var message = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(message);
            }
            setDebugStatus('Debug information retrieved successfully', 'ok');
            renderDebugOutput(payload);
            if(payload && payload.routes){
              renderDebugRoutes(payload.routes);
            }
          }catch(err){
            setDebugStatus('Failed to fetch debug info: ' + (err && err.message ? err.message : 'error'), 'bad');
            renderDebugOutput({ error: err && err.message ? err.message : String(err) });
          }
        });
      }
      
      if(debugClearBtn){
        debugClearBtn.addEventListener('click', function(){
          setDebugStatus('Click "Test Debug Endpoint" to fetch debug information.', 'muted');
          renderDebugOutput(null);
          if(debugRoutesEl) debugRoutesEl.style.display = 'none';
        });
      }
      
      if(debugRefreshBtn){
        debugRefreshBtn.addEventListener('click', function(){
          if(debugTestBtn) debugTestBtn.click();
        });
      }

      fetchAndRender();
    })();
  </script>`;

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
