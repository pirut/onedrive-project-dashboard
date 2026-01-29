function htmlEscape(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function layout(title, bodyHtml) {
    return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${htmlEscape(title)}</title>
    <style>
      @import url("https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500&family=Space+Grotesk:wght@400;500;600&display=swap");
      :root{
        --bg:#0b1220;
        --panel:#0f172a;
        --muted:#8b98b8;
        --fg:#e6ecff;
        --ok:#10b981;
        --bad:#ef4444;
        --warn:#f59e0b;
        --accent:#2b61d1;
      }
      *{box-sizing:border-box}
      body{
        margin:0;
        background:radial-gradient(1200px 600px at 10% -20%, #1b2541 0%, var(--bg) 45%) fixed;
        color:var(--fg);
        font:14px/1.6 "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
      }
      h1,h2,h3,summary{font-family:"Space Grotesk", "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;}
      .wrap{max-width:1140px;margin:28px auto;padding:0 16px}
      h1{font-size:22px;margin:0 0 6px 0}
      .panel{background:var(--panel);border:1px solid #1f2a44;border-radius:14px;padding:16px;margin:0 0 16px 0;box-shadow:0 10px 30px rgba(5,10,25,.25)}
      details.panel{padding:0}
      details.panel > summary{list-style:none;cursor:pointer;padding:14px 16px;font-weight:600;display:flex;align-items:center;justify-content:space-between;gap:8px}
      details.panel > summary::-webkit-details-marker{display:none}
      details.panel > summary::after{content:"+";color:var(--muted);font-weight:700}
      details.panel[open] > summary::after{content:"-"}
      details.panel[open] > summary{border-bottom:1px solid #1f2a44}
      .panel-body{padding:16px}
      .summary-meta{color:var(--muted);font-size:12px;font-weight:400}
      .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}
      .muted{color:var(--muted)}
      .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
      .badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#16213a;color:#9fb1d9;border:1px solid #1f2a44}
      nav.jump{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
      nav.jump a{color:#c9d6ff;text-decoration:none;background:#111b2f;border:1px solid #1f2a44;border-radius:999px;padding:4px 10px;font-size:12px}
      table{width:100%;border-collapse:collapse}
      th,td{padding:8px;border-top:1px solid #1f2a44;text-align:left;vertical-align:top}
      th{color:#b9c2da}
      input,button{font:inherit}
      input{background:#0b1220;border:1px solid #1f2a44;border-radius:8px;color:var(--fg);padding:8px;width:100%}
      button{background:var(--accent);color:#fff;border:0;border-radius:8px;padding:8px 12px;cursor:pointer}
      form .row{display:grid;grid-template-columns:1fr;gap:8px;margin:8px 0}
      .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
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
      .hero{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}
      .hero-actions{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
      .hero-actions a, .hero-actions button{display:inline-flex;align-items:center;gap:6px}
      .link-button{background:#1f2a44;color:#e6ecff;text-decoration:none;border-radius:8px;padding:8px 12px;border:1px solid #1f2a44}
    </style>
  </head>
  <body>
    <div class="wrap">${bodyHtml}</div>
  </body>
</html>`;
}

function renderLoginPage({ adminUsername, adminConfigured, message }) {
    const warn = !adminConfigured
        ? `<div class="panel"><div class="bad">Admin password/secret not configured. Set <span class="mono">ADMIN_PASSWORD</span> and <span class="mono">ADMIN_SESSION_SECRET</span>.</div></div>`
        : "";
    const inner = `
  <h1>Admin Login</h1>
  ${warn}
  ${message ? `<div class="panel"><div class="bad">${htmlEscape(message)}</div></div>` : ""}
  <div class="panel">
    <form method="POST" action="">
      <div class="row">
        <label>Username</label>
        <input name="username" autocomplete="username" value="${htmlEscape(adminUsername)}" />
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

function renderPanel({ id, title, meta, body, open = false }) {
    const metaHtml = meta ? `<span class="summary-meta">${htmlEscape(meta)}</span>` : "";
    return `
  <details class="panel" id="${htmlEscape(id)}"${open ? " open" : ""}>
    <summary>${htmlEscape(title)}${metaHtml ? ` ${metaHtml}` : ""}</summary>
    <div class="panel-body">
      ${body}
    </div>
  </details>`;
}

function renderSummaryCards({ graphEnvOk, premiumEnvOk, premiumEnvMissing, kvDiag, adminConfigured, adminUsername }) {
    return `
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
        <div>${adminConfigured ? '<span class="ok">Enabled</span>' : '<span class="warn">Setup needed</span>'}</div>
        <div class="small mono">ADMIN_USERNAME=${htmlEscape(adminUsername)}</div>
      </div>
    </details>
    <details class="panel">
      <summary>Premium sync env</summary>
      <div class="panel-body">
        <div>${premiumEnvOk ? '<span class="ok">Configured</span>' : '<span class="bad">Missing</span>'}</div>
        <div class="small mono">${premiumEnvOk ? "All required vars present" : htmlEscape(premiumEnvMissing.join(", "))}</div>
      </div>
    </details>
  </div>`;
}

function renderEndpointHealth({ origin, checks }) {
    const rows = (checks || [])
        .map((c) => {
            const status = c.result.ok ? `<span class="ok">OK</span>` : `<span class="bad">FAIL</span>`;
            const extra = c.result.status ? `${c.result.status}` : c.result.error ? htmlEscape(c.result.error) : "";
            return `<tr><td><a class="mono" href="${htmlEscape(c.url)}" target="_blank" rel="noreferrer">${htmlEscape(c.name)}</a></td><td>${htmlEscape(
                c.desc
            )}</td><td>${status}</td><td class="mono small">${extra}</td></tr>`;
        })
        .join("");
    return `
  <div class="small muted" style="margin-bottom:8px">Pings via ${htmlEscape(origin)}</div>
  <table>
    <thead><tr><th>Endpoint</th><th>Description</th><th>Status</th><th class="small">Info</th></tr></thead>
    <tbody>${rows || '<tr><td colspan="4" class="muted">No checks</td></tr>'}</tbody>
  </table>`;
}

function renderUspsSection() {
    return `
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
  <div id="usps-preview" class="small muted" style="margin-top:12px">Preview will show first rows after processing.</div>`;
}

function renderPlannerSyncSection() {
    return `
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
    <button type="button" id="planner-run-bc">Run Premium Sync</button>
    <button type="button" id="planner-run-bc-pr00001" style="background:#2b61d1;color:#fff">Run Sync (PR00001)</button>
    <button type="button" id="planner-poll-premium" style="background:#0f8b4c;color:#fff">Poll Premium Changes</button>
    <button type="button" id="planner-debug-task" style="background:#0f8b4c;color:#fff">Inspect BC Task</button>
    <button type="button" id="planner-debug-bc-timestamps" style="background:#1f2a44;color:#e6ecff">Inspect BC timestamps</button>
    <button type="button" id="planner-debug-decision" style="background:#1f2a44;color:#e6ecff">Inspect sync decision</button>
  </div>
  <div id="planner-status" class="small muted" style="margin-top:8px">Ready.</div>
  <pre id="planner-output" class="log-block" style="display:none"></pre>
  <div class="small muted" style="margin-top:8px">Request log</div>
  <ul id="planner-log" class="step-list"></ul>
  <div class="small muted" style="margin-top:12px">Premium sync logs</div>
  <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
    <button type="button" id="planner-sync-log-load" style="background:#1f2a44;color:#e6ecff">Load logs</button>
    <button type="button" id="planner-sync-log-clear" style="background:#1f2a44;color:#e6ecff">Clear view</button>
  </div>
  <div id="planner-sync-log-status" class="small muted" style="margin-top:6px">Idle.</div>
  <pre id="planner-sync-log-output" class="log-block" style="display:none;max-height:360px;overflow:auto"></pre>`;
}

function renderWebhookSection({ notificationUrlDefault, bcNotificationUrlDefault }) {
    return `
  <div class="grid">
    <div class="panel">
      <div style="font-weight:600">Dataverse Webhooks (Premium → BC)</div>
      <div class="small muted" style="margin-top:4px">Dataverse service endpoint notifications + webhook logs</div>
      <div class="row">
        <label for="planner-notify-url">Notification URL</label>
        <input id="planner-notify-url" value="${htmlEscape(notificationUrlDefault)}" />
      </div>
      <div class="row">
        <label for="planner-webhook-secret">Shared secret</label>
        <input id="planner-webhook-secret" type="password" placeholder="DATAVERSE_WEBHOOK_SECRET" />
      </div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
        <button type="button" id="planner-test-webhook" style="background:#0f8b4c;color:#fff">Ping webhook</button>
        <button type="button" id="planner-webhook-log" style="background:#1f2a44;color:#e6ecff">Load webhook log</button>
        <button type="button" id="planner-webhook-stream" style="background:#0f8b4c;color:#fff">Start live feed</button>
        <button type="button" id="planner-webhook-stop" style="background:#1f2a44;color:#e6ecff">Stop feed</button>
        <button type="button" id="planner-webhook-clear" style="background:#1f2a44;color:#e6ecff">Clear feed</button>
      </div>
      <div class="small muted">Webhook log shows recent Dataverse notifications (if any).</div>
      <pre id="planner-webhook-output" class="log-block" style="display:none;max-height:220px;overflow:auto"></pre>
      <pre id="planner-webhook-feed" class="log-block" style="display:none;max-height:220px;overflow:auto;margin-top:8px"></pre>
    </div>

    <div class="panel">
      <div style="font-weight:600">Business Central Webhooks (BC → Premium)</div>
      <div class="small muted" style="margin-top:4px">BC subscriptions + enqueue + process</div>
      <div class="row">
        <label for="bc-notify-url">Notification URL</label>
        <input id="bc-notify-url" value="${htmlEscape(bcNotificationUrlDefault)}" />
      </div>
      <div class="row">
        <label for="bc-entity-sets">Entity sets (comma)</label>
        <input id="bc-entity-sets" value="projectTasks" />
      </div>
      <div class="row">
        <label for="bc-cron-secret">Cron secret</label>
        <input id="bc-cron-secret" type="password" placeholder="CRON_SECRET" />
      </div>
      <div class="row">
        <label for="bc-resource">Webhook resource (optional)</label>
        <input id="bc-resource" placeholder="projectTasks(systemId) or full resource path" />
      </div>
      <div class="row">
        <label for="bc-system-id">System ID (for test)</label>
        <input id="bc-system-id" placeholder="c23f4835-84fb-f011-8405-7ced8ded633e" />
      </div>
      <div class="row">
        <label for="bc-change-type">Change type</label>
        <input id="bc-change-type" value="updated" />
      </div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap">
        <button type="button" id="bc-subs-list" style="background:#1f2a44;color:#e6ecff">List BC subscriptions</button>
        <button type="button" id="bc-subs-create">Create subscription</button>
        <button type="button" id="bc-subs-renew" style="background:#1f2a44;color:#e6ecff">Renew subscription</button>
        <button type="button" id="bc-subs-delete" style="background:#ef4444;color:#fff">Delete subscription</button>
        <button type="button" id="bc-webhook-validate" style="background:#0f8b4c;color:#fff">Validate webhook</button>
        <button type="button" id="bc-webhook-test" style="background:#2b61d1;color:#fff">Send test webhook</button>
        <button type="button" id="bc-jobs-process" style="background:#1f2a44;color:#e6ecff">Process BC jobs</button>
        <button type="button" id="bc-webhook-log" style="background:#1f2a44;color:#e6ecff">Load webhook log</button>
        <button type="button" id="bc-webhook-stream" style="background:#0f8b4c;color:#fff">Start live feed</button>
        <button type="button" id="bc-webhook-stop" style="background:#1f2a44;color:#e6ecff">Stop feed</button>
        <button type="button" id="bc-webhook-clear" style="background:#1f2a44;color:#e6ecff">Clear feed</button>
      </div>
      <div class="small muted">Webhook log shows recent BC notifications (if any).</div>
      <div id="bc-webhook-status" class="small muted">Idle.</div>
      <pre id="bc-webhook-output" class="log-block" style="display:none;max-height:220px;overflow:auto"></pre>
      <pre id="bc-webhook-feed" class="log-block" style="display:none;max-height:220px;overflow:auto;margin-top:8px"></pre>
    </div>
  </div>`;
}

function renderPlannerProjectsSection() {
    return `
  <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
    <button type="button" id="planner-projects-refresh" style="background:#1f2a44;color:#e6ecff">Refresh list</button>
    <span class="small muted">Sync settings update per project</span>
  </div>
  <div class="row" style="margin:8px 0 12px 0">
    <input id="planner-projects-filter" placeholder="Filter by project no, status, or premium id..." />
  </div>
  <div id="planner-projects-status" class="small muted" style="margin-bottom:8px">Loading premium projects…</div>
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
        <th>Premium Project</th>
        <th>Last sync</th>
        <th>Sync</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody id="planner-projects-tbody">
      <tr><td colspan="7" class="muted">Loading…</td></tr>
    </tbody>
  </table>
  <div class="small muted" style="margin-top:8px">Disable sync to prevent changes flowing to Planner Premium.</div>
  <div class="small muted" style="margin-top:6px">Clear links disables sync and removes Premium IDs from BC.</div>`;
}

function renderPlannerCleanupSection() {
    return `
  <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
    <button type="button" id="planner-assets-refresh" style="background:#1f2a44;color:#e6ecff">Refresh lists</button>
    <button type="button" id="planner-assets-convert" style="background:#2b61d1;color:#fff">Convert selected Graph plans</button>
    <button type="button" id="planner-assets-delete-graph" style="background:#ef4444;color:#fff">Delete selected Graph plans</button>
    <button type="button" id="planner-assets-delete-dv" style="background:#ef4444;color:#fff">Delete selected Premium projects</button>
  </div>
  <div class="row" style="margin:8px 0 12px 0">
    <input id="planner-assets-filter" placeholder="Filter by title, ID, or BC project no..." />
  </div>
  <div id="planner-assets-status" class="small muted" style="margin-bottom:12px">Idle.</div>
  <div class="grid">
    <div>
      <div style="font-weight:600;margin-bottom:6px">Graph Plans (classic)</div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
        <button type="button" id="planner-assets-graph-select-all" style="background:#1f2a44;color:#e6ecff">Select all (filtered)</button>
        <button type="button" id="planner-assets-graph-clear" style="background:#1f2a44;color:#e6ecff">Clear</button>
        <span id="planner-assets-graph-count" class="small muted">No plans selected</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>Title</th>
            <th>Plan ID</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody id="planner-assets-graph-tbody">
          <tr><td colspan="4" class="muted">Loading…</td></tr>
        </tbody>
      </table>
    </div>
    <div>
      <div style="font-weight:600;margin-bottom:6px">Premium Projects (Dataverse)</div>
      <div class="row" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
        <button type="button" id="planner-assets-dv-select-all" style="background:#1f2a44;color:#e6ecff">Select all (filtered)</button>
        <button type="button" id="planner-assets-dv-clear" style="background:#1f2a44;color:#e6ecff">Clear</button>
        <span id="planner-assets-dv-count" class="small muted">No projects selected</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>Project</th>
            <th>BC No</th>
            <th>Project ID</th>
          </tr>
        </thead>
        <tbody id="planner-assets-dv-tbody">
          <tr><td colspan="4" class="muted">Loading…</td></tr>
        </tbody>
      </table>
    </div>
  </div>
  <div class="small muted" style="margin-top:8px">Convert creates a new Premium project using the plan title (and BC No if found).</div>`;
}

function renderDebugSection() {
    return `
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
  </div>`;
}

function renderSubmissionsSection({ submissions }) {
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

    const rows = (submissions || [])
        .map((it) => {
            const status = (it.status || "").toLowerCase();
            const statusCls = status === "ok" ? "ok" : status === "error" ? "bad" : status ? "warn" : "muted";
            const files = Array.isArray(it.files) ? it.files : [];
            const filesCount = files.length ? `${files.length} file${files.length === 1 ? "" : "s"}` : "";
            const filesLinks = files
                .map((f) => `<a href="${htmlEscape(f.url || "")}" target="_blank" rel="noreferrer">${htmlEscape(f.filename || f.url || "file")}</a>`)
                .join(", ");
            const detailsParts = [
                it.folderName ? `folder: <span class="mono">${htmlEscape(it.folderName)}</span>` : null,
                it.phase ? `phase: <span class="mono">${htmlEscape(it.phase)}</span>` : null,
                it.traceId ? `trace: <span class="mono">${htmlEscape(it.traceId)}</span>` : null,
                it.errorStatus ? `status: <span class="mono">${htmlEscape(String(it.errorStatus))}</span>` : null,
                it.errorContentRange ? `range: <span class="mono">${htmlEscape(it.errorContentRange)}</span>` : null,
                it.reason ? `reason: ${htmlEscape(it.reason)}` : null,
                it.error ? `error: ${htmlEscape(it.error)}` : null,
            ].filter(Boolean);
            const mainDetails = detailsParts.join(" · ");
            const stepsBlock =
                Array.isArray(it.steps) && it.steps.length
                    ? `<details class="log"><summary>Logs (${it.steps.length})</summary><ul class="step-list">${it.steps
                          .map((step) => formatStep(step))
                          .join("")}</ul></details>`
                    : "";
            const responseBlock = it.errorResponse
                ? `<details class="log"><summary>Error response</summary><pre class="log-block">${htmlEscape(
                      it.errorResponse.length > 2000 ? `${it.errorResponse.slice(0, 2000)}…` : it.errorResponse
                  )}</pre></details>`
                : "";
            const stackBlock = it.errorStack
                ? `<details class="log"><summary>Error stack</summary><pre class="log-block">${htmlEscape(
                      it.errorStack.length > 4000 ? `${it.errorStack.slice(0, 4000)}…` : it.errorStack
                  )}</pre></details>`
                : "";
            const extras = [stepsBlock, responseBlock, stackBlock].filter(Boolean).join("");
            const details = `${mainDetails}${extras ? `<div class="log-extras">${extras}</div>` : ""}`;
            return `<tr>
              <td class="mono small">${htmlEscape(it.loggedAt || "")}</td>
              <td>${htmlEscape(it.type || "")}</td>
              <td><span class="badge ${statusCls}">${htmlEscape(it.status || "") || "n/a"}</span></td>
              <td>${details || '<span class="muted">—</span>'}</td>
              <td style="text-align:center">${htmlEscape(String(it.uploaded ?? ""))}</td>
              <td>${filesCount}${filesLinks ? `<div class="small">${filesLinks}</div>` : ""}</td>
            </tr>`;
        })
        .join("");

    return `
  <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px">
    <button type="button" id="refresh-btn">Refresh</button>
    <div class="small muted">Use Refresh to update · Showing latest ${submissions?.length || 0}</div>
  </div>
  <div class="row" style="margin:8px 0 12px 0">
    <input id="filter-input" placeholder="Filter by text (type, folder, status, error, trace)..." />
  </div>
  <table>
    <thead><tr><th>Time</th><th>Type</th><th>Status</th><th>Details</th><th>Uploaded</th><th>Files</th></tr></thead>
    <tbody id="subs-tbody">${rows || '<tr><td colspan="6" class="muted">No submissions yet.</td></tr>'}</tbody>
  </table>
  <div class="small muted" id="last-updated" style="margin-top:6px">Last updated: pending</div>`;
}

function renderDashboardPage(data) {
    const sections = [
        { id: "overview", title: "Overview", meta: "Status cards", body: renderSummaryCards(data), open: true },
        { id: "endpoint-health", title: "Endpoint Health", meta: "Health checks", body: renderEndpointHealth(data) },
        { id: "usps", title: "USPS Address Formatter", meta: "CSV cleanup", body: renderUspsSection() },
        { id: "planner-sync", title: "Premium Sync", meta: "BC ↔ Planner Premium", body: renderPlannerSyncSection() },
        { id: "webhooks", title: "Webhook Debugging", meta: "Dataverse + BC", body: renderWebhookSection(data) },
        { id: "planner-projects", title: "Premium Projects", meta: "Per-project sync", body: renderPlannerProjectsSection() },
        { id: "planner-assets", title: "Planner Cleanup", meta: "Graph + Premium", body: renderPlannerCleanupSection() },
        { id: "api-debugging", title: "API Debugging", meta: "Route checks", body: renderDebugSection() },
        { id: "submissions", title: "Submissions", meta: "Uploads + logs", body: renderSubmissionsSection(data) },
    ];

    const jumpLinks = sections
        .map((section) => `<a href="#${htmlEscape(section.id)}">${htmlEscape(section.title)}</a>`)
        .join("");

    const body = `
  <div class="hero">
    <div>
      <h1>Project Dashboard — Admin</h1>
      <div class="small muted">Operational controls for OneDrive, Planner Premium, and Business Central</div>
      <nav class="jump">${jumpLinks}</nav>
    </div>
    <div class="hero-actions">
      <a href="/api/projects-kanban" class="link-button">Kanban Board</a>
      <button type="button" id="export-active-csv" class="link-button">Export Active Projects CSV</button>
      <button type="button" id="export-active-json" class="link-button">Export Active Projects JSON</button>
    </div>
  </div>
  ${sections.map((section) => renderPanel(section)).join("")}
  <script src="/api/admin-runtime.js" defer></script>`;

    return layout("Admin Dashboard", body);
}

export { renderDashboardPage, renderLoginPage };
