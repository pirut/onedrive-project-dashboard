import crypto from "crypto";

const ADMIN_SESSION_SECRET = process.env.ADMIN_SESSION_SECRET || "";

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
        const ageMs = Date.now() - Number(ts);
        if (!Number.isFinite(ageMs) || ageMs > 7 * 24 * 60 * 60 * 1000) return false;
        return { username: u };
    } catch {
        return false;
    }
}

function htmlEscape(s) {
    return String(s)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function layout(title, bodyHtml) {
    return `<!doctype html>
<html lang="en"><head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${htmlEscape(title)}</title>
  <style>
    @import url("https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Space+Grotesk:wght@400;500;600&display=swap");
    :root{
      --bg:#0b1220;
      --panel:#0f172a;
      --panel-2:#111b2f;
      --muted:#8b98b8;
      --fg:#e6ecff;
      --ok:#10b981;
      --bad:#ef4444;
      --warn:#f59e0b;
      --accent:#60a5fa;
      --accent-2:#22d3ee;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      background:radial-gradient(1200px 600px at 10% -20%, #1b2541 0%, var(--bg) 45%) fixed;
      color:var(--fg);
      font:14px/1.6 "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
    }
    h1,h2,h3{font-family:"Space Grotesk", "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;}
    .wrap{max-width:1200px;margin:28px auto;padding:0 16px}
    .topbar{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:16px}
    .title{font-size:22px;font-weight:600}
    .subtitle{color:var(--muted);font-size:12px}
    .actions{display:flex;gap:8px;flex-wrap:wrap}
    .btn, .btn-ghost, .btn-danger{
      border-radius:10px;border:1px solid #1f2a44;padding:8px 12px;cursor:pointer;
      text-decoration:none;display:inline-flex;gap:6px;align-items:center;color:var(--fg);
    }
    .btn{background:linear-gradient(135deg, var(--accent), var(--accent-2)); color:#07101f; border:none}
    .btn-ghost{background:var(--panel); color:var(--fg)}
    .btn-danger{background:#ef4444;color:#fff;border:none}
    .grid{display:grid;gap:12px}
    .grid.stats{grid-template-columns:repeat(auto-fit,minmax(220px,1fr));}
    .card{background:var(--panel);border:1px solid #1f2a44;border-radius:16px;padding:14px 16px;box-shadow:0 14px 40px rgba(5,10,25,.25)}
    .card h3{margin:0 0 6px 0;font-size:14px}
    .card .value{font-size:22px;font-weight:600}
    .card .meta{color:var(--muted);font-size:12px}
    .panel{background:var(--panel);border:1px solid #1f2a44;border-radius:16px;padding:14px 16px;margin-top:16px}
    .panel h2{margin:0 0 8px 0;font-size:16px}
    .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    input,button{font:inherit}
    input{
      background:var(--panel-2);
      border:1px solid #1f2a44;
      border-radius:10px;
      color:var(--fg);
      padding:8px 10px;
      min-width:240px;
    }
    table{width:100%;border-collapse:collapse;margin-top:10px}
    th,td{padding:8px;border-top:1px solid #1f2a44;text-align:left;vertical-align:top}
    th{color:#b9c2da}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
    .pill{display:inline-flex;gap:6px;align-items:center;border:1px solid #1f2a44;border-radius:999px;padding:2px 8px;color:var(--muted);font-size:12px}
    .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
    pre.log{
      background:var(--panel-2);
      border:1px solid #1f2a44;
      border-radius:12px;
      padding:10px;
      color:#c8d5ff;
      font:12px/1.5 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      white-space:pre-wrap;
      word-break:break-word;
      max-height:260px;
      overflow:auto;
      margin:8px 0 0 0;
    }
    .muted{color:var(--muted)}
    .small{font-size:12px}
  </style>
</head>
<body>
  <div class="wrap">
    ${bodyHtml}
  </div>
  <script src="/api/plans-dashboard-runtime.js" defer></script>
</body></html>`;
}

function renderPage() {
    return layout(
        "Plan Manager",
        `
    <div class="topbar">
      <div>
        <div class="title">Plan Manager</div>
        <div class="subtitle">Track and reset Standard (Graph) and Premium (Dataverse) plans</div>
      </div>
      <div class="actions">
        <a class="btn-ghost" href="/api/admin">Back to Admin</a>
        <button class="btn" id="plans-refresh">Refresh</button>
      </div>
    </div>

    <div class="grid stats">
      <div class="card">
        <h3>Standard Plans</h3>
        <div class="value" id="graph-count">—</div>
        <div class="meta" id="graph-meta">—</div>
      </div>
      <div class="card">
        <h3>Premium Projects</h3>
        <div class="value" id="premium-count">—</div>
        <div class="meta" id="premium-meta">—</div>
      </div>
      <div class="card">
        <h3>Auto-create From BC</h3>
        <div class="value" id="auto-create-status">—</div>
        <div class="meta">DATAVERSE_ALLOW_PROJECT_CREATE</div>
      </div>
      <div class="card">
        <h3>Planner Group</h3>
        <div class="value" id="group-status">—</div>
        <div class="meta" id="group-meta">PLANNER_GROUP_ID</div>
      </div>
    </div>

    <div class="panel">
      <h2>Recreate Premium Projects</h2>
      <div class="row">
        <button class="btn" id="recreate-all">Recreate All From BC</button>
        <button class="btn-danger" id="reset-all">Delete All + Recreate</button>
      </div>
      <div class="small muted" style="margin-top:6px">
        Recreate runs a full BC → Premium sync across every BC project and forces project creation when missing.
      </div>
      <div id="action-status" class="small muted" style="margin-top:8px">Idle.</div>
      <pre id="action-output" class="log" style="display:none"></pre>
    </div>

    <div class="panel">
      <h2>BC Projects Sync Status</h2>
      <div class="row">
        <input id="bc-projects-filter" placeholder="Filter by project no, description, or status..." />
      </div>
      <div class="row" style="margin-top:8px">
        <button class="btn" id="bc-projects-sync">Sync selected (BC → Premium)</button>
        <button class="btn-ghost" id="bc-projects-select-all">Select all (filtered)</button>
        <button class="btn-ghost" id="bc-projects-clear">Clear</button>
        <span class="small muted" id="bc-projects-count">0 selected</span>
      </div>
      <div id="bc-projects-status" class="small muted" style="margin-top:8px">Loading BC projects…</div>
      <pre id="bc-projects-output" class="log" style="display:none"></pre>
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>Project</th>
            <th>Description</th>
            <th>Status</th>
            <th>Sync</th>
            <th>Tasks</th>
            <th>Last Sync</th>
            <th>Premium</th>
          </tr>
        </thead>
        <tbody id="bc-projects-tbody">
          <tr><td colspan="8" class="muted">Loading…</td></tr>
        </tbody>
      </table>
      <div class="small muted" style="margin-top:8px">Sync uses BC → Premium with project creation enabled.</div>
    </div>

    <div class="panel">
      <h2>Filter + Bulk Actions</h2>
      <div class="row">
        <input id="plans-filter" placeholder="Filter by title, ID, BC No..." />
      </div>
      <div class="row" style="margin-top:8px">
        <span class="pill">Standard Plans</span>
        <button class="btn-ghost" id="graph-select-all">Select all (filtered)</button>
        <button class="btn-ghost" id="graph-clear">Clear</button>
        <button class="btn-danger" id="graph-delete">Delete selected</button>
        <button class="btn-danger" id="graph-delete-all">Delete ALL</button>
        <span class="small muted" id="graph-count-selected">0 selected</span>
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
        <tbody id="graph-tbody">
          <tr><td colspan="4" class="muted">Loading…</td></tr>
        </tbody>
      </table>

      <div class="row" style="margin-top:16px">
        <span class="pill">Premium Projects</span>
        <button class="btn-ghost" id="premium-select-all">Select all (filtered)</button>
        <button class="btn-ghost" id="premium-clear">Clear</button>
        <button class="btn-danger" id="premium-delete">Delete selected</button>
        <button class="btn-danger" id="premium-delete-all">Delete ALL</button>
        <span class="small muted" id="premium-count-selected">0 selected</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>Project</th>
            <th>BC No</th>
            <th>Open</th>
            <th>Project ID</th>
            <th>Modified</th>
          </tr>
        </thead>
        <tbody id="premium-tbody">
          <tr><td colspan="6" class="muted">Loading…</td></tr>
        </tbody>
      </table>
    </div>
    `
    );
}

export default async function handler(req, res) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");

    const cookies = parseCookies(req);
    const sess = verifySession(cookies["admin_session"]);
    if (!sess) {
        res.status(302).setHeader("Location", "/api/admin").end();
        return;
    }

    res.status(200).send(renderPage());
}
