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
    const cookie = [
        `admin_session=${value}`,
        `Path=/` ,
        `HttpOnly`,
        `SameSite=Lax`,
        `Max-Age=${maxAge}`,
        secure ? "Secure" : null,
    ]
        .filter(Boolean)
        .join("; ");
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
    return String(s)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
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
</style>
</head><body><div class="wrap">${bodyHtml}</div></body></html>`;
}

function loginView(msg = null) {
    const warn = (!ADMIN_PASSWORD || !ADMIN_SESSION_SECRET)
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
    const origin = `${(req.headers["x-forwarded-proto"] || "https")}://${req.headers.host}`;

    // Gather health info
    const endpoints = [
        { name: "/api/health", url: `${origin}/api/health`, desc: "Base health" },
        { name: "/api/submissions", url: `${origin}/api/submissions?limit=5`, desc: "Recent logs (KV)" },
        { name: "/api/kv-diag", url: `${origin}/api/kv-diag`, desc: "KV diagnostics" },
    ];
    const checks = await Promise.all(
        endpoints.map(async (e) => ({ ...e, result: await ping(e.url) }))
    );

    const graphEnvOk = ["TENANT_ID", "MSAL_CLIENT_ID", "MSAL_CLIENT_SECRET", "DEFAULT_SITE_URL", "DEFAULT_LIBRARY"].every(envPresence);
    const kvDiag = await kvDiagnostics();
    const items = await listSubmissions(25);

    const endpointsTableRows = checks
        .map((c) => {
            const status = c.result.ok ? `<span class="ok">OK</span>` : `<span class="bad">FAIL</span>`;
            const extra = c.result.status ? `${c.result.status}` : c.result.error ? htmlEscape(c.result.error) : "";
            return `<tr><td><a class="mono" href="${htmlEscape(c.url)}" target="_blank" rel="noreferrer">${htmlEscape(
                c.name
            )}</a></td><td>${htmlEscape(c.desc)}</td><td>${status}</td><td class="mono small">${extra}</td></tr>`;
        })
        .join("");

    const itemsRows = items
        .map((it) => {
            const files = Array.isArray(it.files)
                ? it.files
                      .map((f) => `<div><a href="${htmlEscape(f.url || "")}" target="_blank" rel="noreferrer">${htmlEscape(
                            f.filename || f.url || "file"
                        )}</a></div>`) 
                      .join("")
                : "";
            return `<tr>
              <td>${htmlEscape(it.loggedAt || "")}</td>
              <td>${htmlEscape(it.type || "")}</td>
              <td>${htmlEscape(it.folderName || "")}</td>
              <td style="text-align:center">${htmlEscape(String(it.uploaded ?? ""))}</td>
              <td>${files}</td>
            </tr>`;
        })
        .join("");

    const inner = `
  <h1>Project Dashboard — Admin</h1>
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
    <div style="font-weight:600">Recent Logs</div>
    <div class="small muted" style="margin:4px 0 8px 0">Showing latest ${items.length}</div>
    <table>
      <thead><tr><th>Time</th><th>Type</th><th>Folder</th><th>Uploaded</th><th>Files</th></tr></thead>
      <tbody>${itemsRows || '<tr><td colspan="5" class="muted">No submissions yet.</td></tr>'}</tbody>
    </table>
  </div>`;

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

