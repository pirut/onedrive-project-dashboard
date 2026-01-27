import "isomorphic-fetch";
import crypto from "crypto";

const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
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
    return String(s).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}

function layout(title, bodyHtml) {
    return `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>${htmlEscape(title)}</title>
<style>
  :root{--bg:#0b1220;--panel:#0f172a;--muted:#8b98b8;--fg:#e6ecff;--ok:#10b981;--bad:#ef4444;--warn:#f59e0b}
  body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto}
  .wrap{max-width:1200px;margin:32px auto;padding:0 16px}
  h1{font-size:20px;margin:0}
  .panel{background:var(--panel);border:1px solid #1f2a44;border-radius:12px;padding:16px;margin:0 0 16px 0}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px}
  .muted{color:var(--muted)}
  .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
  input,button{font:inherit}
  input{background:#0b1220;border:1px solid #1f2a44;border-radius:8px;color:var(--fg);padding:8px;width:100%}
  button{background:#2b61d1;color:#fff;border:0;border-radius:8px;padding:8px 12px;cursor:pointer}
  a.button{display:inline-block;background:#1f2a44;color:#e6ecff;border-radius:8px;padding:8px 12px;text-decoration:none}
  .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:8px 0}
  .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
  .small{font-size:12px}
  pre.log{background:#111b2f;border-radius:6px;padding:8px;white-space:pre-wrap;word-break:break-word;margin:6px 0 0 0;color:#c8d5ff;font-size:12px;min-height:80px}
</style>
</head><body><div class="wrap">${bodyHtml}</div></body></html>`;
}

async function debugView(req) {
    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
    const notificationUrlDefault = process.env.DATAVERSE_NOTIFICATION_URL || `${origin}/api/webhooks/dataverse`;

    const inner = `
  <div class="panel" style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap">
    <div>
      <h1>Webhook Debug Console</h1>
      <div class="small muted">Planner Premium + webhook diagnostics.</div>
    </div>
    <div class="row">
      <a class="button" href="/api/admin">Back to Admin</a>
      <button type="button" id="refresh-all">Refresh all</button>
    </div>
  </div>

  <div class="panel">
    <div style="font-weight:600">Notification URL</div>
    <div class="row">
      <input id="notification-url" value="${htmlEscape(notificationUrlDefault)}" readonly />
      <button type="button" id="ping-webhook">Ping</button>
    </div>
    <div id="validation-status" class="small muted">Ping hits <span class="mono">/api/webhooks/dataverse</span></div>
    <pre id="validation-output" class="log" style="display:none"></pre>
  </div>

  <div class="panel">
    <div style="font-weight:600">Quick Actions</div>
    <div class="row">
      <button type="button" id="premium-test-btn">Test Premium API</button>
      <button type="button" id="webhook-snapshot-btn" style="background:#1f2a44;color:#e6ecff">Load Webhook Snapshot</button>
      <button type="button" id="webhook-feed-start" style="background:#0f8b4c;color:#fff">Start Live Feed</button>
      <button type="button" id="webhook-feed-stop" style="background:#1f2a44;color:#e6ecff">Stop Feed</button>
      <button type="button" id="webhook-feed-clear" style="background:#1f2a44;color:#e6ecff">Clear Feed</button>
    </div>
    <div class="small muted">Dataverse webhooks fire on task create/update/delete once registered.</div>
  </div>

  <div class="grid">
    <div class="panel">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div style="font-weight:600">Premium API Test</div>
        <span id="premium-test-status" class="small muted">Idle</span>
      </div>
      <pre id="premium-test-output" class="log"></pre>
    </div>

    <div class="panel">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div style="font-weight:600">Webhook Snapshot</div>
        <span id="webhook-status" class="small muted">Idle</span>
      </div>
      <pre id="webhook-output" class="log"></pre>
    </div>

    <div class="panel">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div style="font-weight:600">Webhook Feed (live)</div>
        <span id="feed-status" class="small muted">Idle</span>
      </div>
      <pre id="webhook-feed" class="log"></pre>
    </div>
  </div>

  <div class="panel">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <div style="font-weight:600">Debug Info</div>
      <span id="debug-status" class="small muted">Idle</span>
    </div>
    <pre id="debug-output" class="log"></pre>
  </div>

  <script>
    (function(){
      function byId(id){ return document.getElementById(id); }
      function renderJson(el, payload){
        if(!el) return;
        if(payload == null){ el.textContent = ''; return; }
        try { el.textContent = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ el.textContent = String(payload); }
      }
      function setStatus(el, text, tone){
        if(!el) return;
        el.textContent = text;
        el.className = 'small ' + (tone || 'muted');
      }
      async function fetchJson(url, opts){
        var res = await fetch(url, opts || {});
        var ct = res.headers.get('content-type') || '';
        var payload = ct.indexOf('application/json') !== -1 ? await res.json() : await res.text();
        return { ok: res.ok, status: res.status, payload: payload };
      }

      var debugOutput = byId('debug-output');
      var debugStatus = byId('debug-status');
      var premiumOutput = byId('premium-test-output');
      var premiumStatus = byId('premium-test-status');
      var webhookOutput = byId('webhook-output');
      var webhookStatus = byId('webhook-status');
      var feedOutput = byId('webhook-feed');
      var feedStatus = byId('feed-status');
      var validationStatus = byId('validation-status');
      var validationOutput = byId('validation-output');
      var feedSource = null;

      async function loadDebug(){
        setStatus(debugStatus, 'Loading...', 'muted');
        var res = await fetchJson('/api/debug', { headers: { 'cache-control': 'no-cache' }});
        renderJson(debugOutput, res.payload);
        setStatus(debugStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
      }

      async function loadPremiumTest(){
        setStatus(premiumStatus, 'Loading...', 'muted');
        var res = await fetchJson('/api/sync/premium-test', { headers: { 'cache-control': 'no-cache' }});
        renderJson(premiumOutput, res.payload);
        setStatus(premiumStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
      }

      async function loadWebhookSnapshot(){
        setStatus(webhookStatus, 'Loading...', 'muted');
        var res = await fetchJson('/api/sync/webhook-log?limit=50', { headers: { 'cache-control': 'no-cache' }});
        renderJson(webhookOutput, res.payload);
        setStatus(webhookStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
      }

      function startFeed(){
        if(feedSource) feedSource.close();
        feedOutput.textContent = '';
        feedSource = new EventSource('/api/sync/webhook-log-stream?include=1');
        setStatus(feedStatus, 'Connected', 'ok');
        feedSource.onmessage = function(ev){
          feedOutput.textContent = (feedOutput.textContent ? feedOutput.textContent + '\n' : '') + ev.data;
        };
        feedSource.onerror = function(){
          setStatus(feedStatus, 'Error', 'bad');
        };
      }

      function stopFeed(){
        if(feedSource) feedSource.close();
        feedSource = null;
        setStatus(feedStatus, 'Stopped', 'muted');
      }

      function clearFeed(){
        feedOutput.textContent = '';
        setStatus(feedStatus, 'Cleared', 'muted');
      }

      async function pingWebhook(){
        setStatus(validationStatus, 'Pinging...', 'muted');
        var res = await fetchJson('/api/webhooks/dataverse', { method: 'GET' });
        renderJson(validationOutput, res.payload);
        validationOutput.style.display = 'block';
        setStatus(validationStatus, res.ok ? 'Ping OK' : 'Ping failed', res.ok ? 'ok' : 'bad');
      }

      byId('refresh-all')?.addEventListener('click', function(){
        loadDebug();
        loadPremiumTest();
        loadWebhookSnapshot();
      });
      byId('premium-test-btn')?.addEventListener('click', loadPremiumTest);
      byId('webhook-snapshot-btn')?.addEventListener('click', loadWebhookSnapshot);
      byId('webhook-feed-start')?.addEventListener('click', startFeed);
      byId('webhook-feed-stop')?.addEventListener('click', stopFeed);
      byId('webhook-feed-clear')?.addEventListener('click', clearFeed);
      byId('ping-webhook')?.addEventListener('click', pingWebhook);

      loadDebug();
      loadPremiumTest();
      loadWebhookSnapshot();
    })();
  </script>
  `;

    return layout("Webhook Debug", inner);
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

    const html = await debugView(req);
    res.status(200).send(html);
}
