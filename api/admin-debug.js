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
  .badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#16213a;color:#9fb1d9;border:1px solid #1f2a44}
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
    const notificationUrlDefault = process.env.GRAPH_NOTIFICATION_URL || process.env.PLANNER_NOTIFICATION_URL || `${origin}/api/webhooks/graph/planner`;

    const inner = `
  <div class="panel" style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap">
    <div>
      <h1>Webhook Debug Console</h1>
      <div class="small muted">All Planner + webhook diagnostics in one place.</div>
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
      <button type="button" id="validate-webhook">Validate</button>
    </div>
    <div id="validation-status" class="small muted">Validation hits <span class="mono">/api/webhooks/graph/planner?validationToken=...</span></div>
    <pre id="validation-output" class="log" style="display:none"></pre>
  </div>

    <div class="panel">
      <div style="font-weight:600">Quick Actions</div>
      <div class="row">
        <button type="button" id="planner-test-btn">Test Planner API</button>
        <button type="button" id="subs-list-btn" style="background:#1f2a44;color:#e6ecff">List Subscriptions</button>
        <button type="button" id="subs-delete-btn" style="background:#ef4444;color:#fff">Delete Planner Subscriptions</button>
        <button type="button" id="webhook-snapshot-btn" style="background:#1f2a44;color:#e6ecff">Load Webhook Snapshot</button>
        <button type="button" id="webhook-feed-start" style="background:#0f8b4c;color:#fff">Start Live Feed</button>
        <button type="button" id="webhook-feed-stop" style="background:#1f2a44;color:#e6ecff">Stop Feed</button>
        <button type="button" id="webhook-feed-clear" style="background:#1f2a44;color:#e6ecff">Clear Feed</button>
      </div>
      <div class="small muted">Planner webhooks only fire on task create/update/delete for subscribed plan IDs.</div>
  </div>

  <div class="grid">
    <div class="panel">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div style="font-weight:600">Planner API Test</div>
        <span id="planner-test-status" class="small muted">Idle</span>
      </div>
      <div id="planner-test-hint" class="small muted" style="margin-top:6px"></div>
      <div class="row" style="margin-top:6px">
        <a id="planner-plan-link" class="button" href="#" target="_blank" rel="noopener" style="display:none">Open Plan</a>
      </div>
      <pre id="planner-test-output" class="log"></pre>
    </div>

    <div class="panel">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div style="font-weight:600">Graph Subscriptions</div>
        <span id="subs-status" class="small muted">Idle</span>
      </div>
      <pre id="subs-output" class="log"></pre>
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
        if(payload == null){
          el.textContent = '';
          return;
        }
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
      function extractPlanHint(payload){
        try {
          var groupId = payload && payload.groupId ? String(payload.groupId) : '';
          var owner = payload && payload.checks && payload.checks.plan && payload.checks.plan.owner ? String(payload.checks.plan.owner) : '';
          if(groupId && owner && groupId !== owner){
            return 'Plan owner does not match PLANNER_GROUP_ID. Check groupId or default plan.';
          }
        } catch(e){}
        return '';
      }

      var debugOutput = byId('debug-output');
      var debugStatus = byId('debug-status');
      var plannerOutput = byId('planner-test-output');
      var plannerStatus = byId('planner-test-status');
      var plannerHint = byId('planner-test-hint');
      var plannerPlanLink = byId('planner-plan-link');
      var subsOutput = byId('subs-output');
      var subsStatus = byId('subs-status');
      var webhookOutput = byId('webhook-output');
      var webhookStatus = byId('webhook-status');
      var feedOutput = byId('webhook-feed');
      var feedStatus = byId('feed-status');
      var validationStatus = byId('validation-status');
      var validationOutput = byId('validation-output');

      async function loadDebug(){
        setStatus(debugStatus, 'Loading…', 'muted');
        try {
          var res = await fetchJson('/api/debug', { headers: { 'cache-control': 'no-cache' }});
          renderJson(debugOutput, res.payload);
          setStatus(debugStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
        } catch(e){
          renderJson(debugOutput, e && e.message ? e.message : String(e));
          setStatus(debugStatus, 'Error', 'bad');
        }
      }

      async function loadPlannerTest(){
        setStatus(plannerStatus, 'Loading…', 'muted');
        plannerHint.textContent = '';
        if(plannerPlanLink){ plannerPlanLink.style.display = 'none'; plannerPlanLink.href = '#'; }
        try {
          var res = await fetchJson('/api/sync/planner-test', { headers: { 'cache-control': 'no-cache' }});
          renderJson(plannerOutput, res.payload);
          setStatus(plannerStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
          var hint = extractPlanHint(res.payload);
          if(hint){ plannerHint.textContent = hint; plannerHint.className = 'small warn'; }
          if(res.payload && res.payload.planUrl && plannerPlanLink){
            plannerPlanLink.href = res.payload.planUrl;
            plannerPlanLink.style.display = 'inline-block';
          }
        } catch(e){
          renderJson(plannerOutput, e && e.message ? e.message : String(e));
          setStatus(plannerStatus, 'Error', 'bad');
        }
      }

      async function loadSubscriptions(){
        setStatus(subsStatus, 'Loading…', 'muted');
        try {
          var res = await fetchJson('/api/sync/subscriptions/list', { headers: { 'cache-control': 'no-cache' }});
          renderJson(subsOutput, res.payload);
          setStatus(subsStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
        } catch(e){
          renderJson(subsOutput, e && e.message ? e.message : String(e));
          setStatus(subsStatus, 'Error', 'bad');
        }
      }

      async function deleteSubscriptions(){
        var ok = window.confirm('Delete all Planner subscriptions? This cannot be undone.');
        if(!ok) return;
        setStatus(subsStatus, 'Deleting…', 'warn');
        try {
          var res = await fetchJson('/api/sync/subscriptions/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ all: true })
          });
          renderJson(subsOutput, res.payload);
          setStatus(subsStatus, res.ok ? 'Deleted' : 'Error', res.ok ? 'ok' : 'bad');
        } catch(e){
          renderJson(subsOutput, e && e.message ? e.message : String(e));
          setStatus(subsStatus, 'Error', 'bad');
        }
      }

      async function loadWebhookSnapshot(){
        setStatus(webhookStatus, 'Loading…', 'muted');
        try {
          var res = await fetchJson('/api/sync/webhook-log?limit=50', { headers: { 'cache-control': 'no-cache' }});
          renderJson(webhookOutput, res.payload);
          setStatus(webhookStatus, res.ok ? 'Loaded' : 'Error', res.ok ? 'ok' : 'bad');
        } catch(e){
          renderJson(webhookOutput, e && e.message ? e.message : String(e));
          setStatus(webhookStatus, 'Error', 'bad');
        }
      }

      async function validateWebhook(){
        var token = 'debug_' + Math.random().toString(36).slice(2, 10);
        setStatus(validationStatus, 'Validating…', 'muted');
        validationOutput.style.display = 'none';
        try {
          var res = await fetch('/api/webhooks/graph/planner?validationToken=' + encodeURIComponent(token), {
            headers: { 'cache-control': 'no-cache' }
          });
          var text = await res.text();
          validationOutput.style.display = 'block';
          validationOutput.textContent = text;
          var ok = res.ok && text.trim() === token;
          setStatus(validationStatus, ok ? 'Validated' : ('Validation failed: ' + res.status), ok ? 'ok' : 'bad');
        } catch(e){
          validationOutput.style.display = 'block';
          validationOutput.textContent = e && e.message ? e.message : String(e);
          setStatus(validationStatus, 'Validation error', 'bad');
        }
      }

      var feedSource = null;
      function startFeed(){
        if(feedSource) return;
        feedOutput.textContent = '';
        feedSource = new EventSource('/api/sync/webhook-log-stream?include=1');
        feedSource.onmessage = function(ev){
          if(!ev || !ev.data) return;
          var line = ev.data;
          try { line = JSON.stringify(JSON.parse(ev.data)); } catch(e){}
          feedOutput.textContent = (feedOutput.textContent ? feedOutput.textContent + '\\n' : '') + line;
        };
        feedSource.onerror = function(){
          setStatus(feedStatus, 'Feed error', 'warn');
          stopFeed();
        };
        setStatus(feedStatus, 'Running', 'ok');
      }
      function stopFeed(){
        if(feedSource){
          feedSource.close();
          feedSource = null;
        }
        setStatus(feedStatus, 'Stopped', 'muted');
      }
      function clearFeed(){
        feedOutput.textContent = '';
      }

      byId('planner-test-btn').addEventListener('click', loadPlannerTest);
      byId('subs-list-btn').addEventListener('click', loadSubscriptions);
      byId('subs-delete-btn').addEventListener('click', deleteSubscriptions);
      byId('webhook-snapshot-btn').addEventListener('click', loadWebhookSnapshot);
      byId('webhook-feed-start').addEventListener('click', startFeed);
      byId('webhook-feed-stop').addEventListener('click', stopFeed);
      byId('webhook-feed-clear').addEventListener('click', clearFeed);
      byId('validate-webhook').addEventListener('click', validateWebhook);
      byId('refresh-all').addEventListener('click', function(){
        loadDebug();
        loadPlannerTest();
        loadSubscriptions();
        loadWebhookSnapshot();
      });

      window.addEventListener('load', function(){
        loadDebug();
        loadPlannerTest();
        loadSubscriptions();
        loadWebhookSnapshot();
      });
    })();
  </script>`;

    return layout("Admin Debug Console", inner);
}

export default async function handler(req, res) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");

    const cookies = parseCookies(req);
    const sess = verifySession(cookies["admin_session"]);

    if (req.method !== "GET") {
        res.status(405).send("Method not allowed");
        return;
    }

    if (!sess) {
        res.status(302).setHeader("Location", "/api/admin").end();
        return;
    }

    const html = await debugView(req);
    res.status(200).send(html);
}
