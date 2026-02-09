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
    h1,h2{font-family:"Space Grotesk", "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;}
    .wrap{max-width:1200px;margin:28px auto;padding:0 16px}
    .topbar{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:16px}
    .title{font-size:22px;font-weight:600}
    .subtitle{color:var(--muted);font-size:12px}
    .actions{display:flex;gap:8px;flex-wrap:wrap}
    .btn, .btn-ghost{
      border-radius:10px;border:1px solid #1f2a44;padding:8px 12px;cursor:pointer;
      text-decoration:none;display:inline-flex;gap:6px;align-items:center;color:var(--fg);
    }
    .btn{background:linear-gradient(135deg, var(--accent), var(--accent-2)); color:#07101f; border:none}
    .btn-ghost{background:var(--panel); color:var(--fg)}
    .panel{background:var(--panel);border:1px solid #1f2a44;border-radius:16px;padding:14px 16px}
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
    .small{font-size:12px}
    .muted{color:var(--muted)}
    .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
    .pill{display:inline-flex;gap:6px;align-items:center;border:1px solid #1f2a44;border-radius:999px;padding:2px 8px;color:var(--muted);font-size:12px}
    pre.log{
      background:var(--panel-2);
      border:1px solid #1f2a44;
      border-radius:12px;
      padding:10px;
      color:#c8d5ff;
      font:12px/1.5 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      white-space:pre-wrap;
      word-break:break-word;
      max-height:280px;
      overflow:auto;
      margin:8px 0 0 0;
    }
  </style>
</head>
<body>
  <div class="wrap">
    ${bodyHtml}
  </div>
  <script src="/api/share-reminders-dashboard-runtime.js" defer></script>
</body></html>`;
}

function renderPage() {
    return layout(
        "Share Project Task Manager",
        `
    <div class="topbar">
      <div>
        <div class="title">Share Project Task Manager</div>
        <div class="subtitle">Bulk create/verify <span class="mono">Share Project</span> tasks and assignment to Connie.</div>
      </div>
      <div class="actions">
        <a class="btn-ghost" href="/api/admin">Back to Admin</a>
        <button class="btn" id="share-reminders-refresh">Refresh</button>
      </div>
    </div>

    <div class="panel">
      <div class="row">
        <input id="share-reminders-filter" placeholder="Filter by project no, description, status, or premium id..." />
      </div>
      <div class="row" style="margin-top:8px">
        <button class="btn-ghost" id="share-reminders-select-all">Select all (filtered)</button>
        <button class="btn-ghost" id="share-reminders-clear">Clear</button>
        <button class="btn" id="share-reminders-ensure-selected">Ensure tasks (selected)</button>
        <button class="btn" id="share-reminders-ensure-filtered">Ensure tasks (all filtered)</button>
        <span id="share-reminders-count" class="small muted">0 selected</span>
      </div>
      <div id="share-reminders-status" class="small muted" style="margin-top:8px">Loading projects…</div>
      <pre id="share-reminders-output" class="log" style="display:none"></pre>
      <table>
        <thead>
          <tr>
            <th>Select</th>
            <th>Project</th>
            <th>Description</th>
            <th>Premium ID</th>
            <th>Sync</th>
            <th>Last Sync</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody id="share-reminders-tbody">
          <tr><td colspan="7" class="muted">Loading…</td></tr>
        </tbody>
      </table>
      <div class="small muted" style="margin-top:8px">
        Standard behavior: each Premium project should have one <span class="mono">Share Project</span> task assigned to Connie.
      </div>
    </div>`
    );
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).send("Method not allowed");
        return;
    }
    const cookies = parseCookies(req);
    const session = verifySession(cookies.admin_session);
    if (!session) {
        res.setHeader("Location", "/api/admin");
        res.status(302).send("Redirecting...");
        return;
    }
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).send(renderPage());
}
