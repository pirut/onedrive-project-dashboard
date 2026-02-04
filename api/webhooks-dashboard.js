import "isomorphic-fetch";
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
    @import url("https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500&display=swap");
    :root{
      --bg:#0a0f1c;
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
      background:
        radial-gradient(1200px 600px at 10% -20%, #1b2a4a 0%, var(--bg) 45%),
        radial-gradient(1000px 500px at 100% 0%, #0e1d3f 0%, var(--bg) 55%);
      color:var(--fg);
      font:14px/1.6 "Space Grotesk", ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
    }
    .wrap{max-width:1280px;margin:28px auto;padding:0 18px}
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
    .grid{display:grid;gap:12px}
    .grid.stats{grid-template-columns:repeat(auto-fit,minmax(220px,1fr));}
    .card{background:var(--panel);border:1px solid #1f2a44;border-radius:16px;padding:14px 16px;box-shadow:0 14px 40px rgba(5,10,25,.25)}
    .card h3{margin:0 0 6px 0;font-size:14px}
    .card .value{font-size:22px;font-weight:600}
    .card .meta{color:var(--muted);font-size:12px}
    .section{margin-top:16px}
    .panel{background:var(--panel);border:1px solid #1f2a44;border-radius:16px;padding:14px 16px}
    .panel h2{margin:0 0 8px 0;font-size:16px}
    .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
    input,select,button{font:inherit}
    input,select{
      background:var(--panel-2);
      border:1px solid #1f2a44;
      border-radius:10px;
      color:var(--fg);
      padding:8px 10px;
    }
    pre.log{
      background:var(--panel-2);
      border:1px solid #1f2a44;
      border-radius:12px;
      padding:10px;
      color:#c8d5ff;
      font:12px/1.5 "IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      white-space:pre-wrap;
      word-break:break-word;
      max-height:260px;
      overflow:auto;
      margin:8px 0 0 0;
    }
    .pill{display:inline-flex;gap:6px;align-items:center;border:1px solid #1f2a44;border-radius:999px;padding:2px 8px;color:var(--muted);font-size:12px}
    .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
    .two-col{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px}
    .footer-note{color:var(--muted);font-size:11px;margin-top:8px}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div>
        <div class="title">Webhook Control Center</div>
        <div class="subtitle">Unified view of BC + Premium webhooks, PSS errors, and schedule operations</div>
      </div>
      <div class="actions">
        <a class="btn-ghost" href="/api/admin">Back to Admin</a>
        <a class="btn-ghost" href="/api/admin-debug">Open Debug Console</a>
        <button class="btn" id="refresh-all">Refresh Now</button>
      </div>
    </div>

    <div class="grid stats">
      <div class="card">
        <h3>BC Webhooks</h3>
        <div class="value" id="bc-count">—</div>
        <div class="meta">Last: <span id="bc-last">—</span></div>
        <div class="meta">Enqueued: <span id="bc-enqueued">—</span> · Processed: <span id="bc-processed">—</span></div>
        <div class="meta">Deduped: <span id="bc-deduped">—</span></div>
      </div>
      <div class="card">
        <h3>Premium Webhooks</h3>
        <div class="value" id="premium-count">—</div>
        <div class="meta">Last: <span id="premium-last">—</span></div>
        <div class="meta">Notifications: <span id="premium-notifications">—</span></div>
        <div class="meta">Ignored: <span id="premium-ignored">—</span></div>
      </div>
      <div class="card">
        <h3>Operation Sets</h3>
        <div class="value" id="ops-count">—</div>
        <div class="meta">Newest: <span id="ops-last">—</span></div>
      </div>
      <div class="card">
        <h3>PSS Errors</h3>
        <div class="value" id="pss-count">—</div>
        <div class="meta">Latest: <span id="pss-last">—</span></div>
        <div class="meta">Last Error: <span id="pss-last-msg">—</span></div>
      </div>
    </div>

    <div class="section panel">
      <h2>Live Streams</h2>
      <div class="two-col">
        <div>
          <div class="row">
            <span class="pill">BC Stream</span>
            <button class="btn-ghost" id="bc-stream-start">Start</button>
            <button class="btn-ghost" id="bc-stream-stop">Stop</button>
            <button class="btn-ghost" id="bc-stream-clear">Clear</button>
          </div>
          <pre id="bc-stream" class="log"></pre>
        </div>
        <div>
          <div class="row">
            <span class="pill">Premium Stream</span>
            <button class="btn-ghost" id="premium-stream-start">Start</button>
            <button class="btn-ghost" id="premium-stream-stop">Stop</button>
            <button class="btn-ghost" id="premium-stream-clear">Clear</button>
          </div>
          <pre id="premium-stream" class="log"></pre>
        </div>
      </div>
      <div class="footer-note">Streams use server-sent events and include recent history on start.</div>
    </div>

    <div class="section panel">
      <h2>Recent Webhook Logs</h2>
      <div class="two-col">
        <div>
          <div class="row">
            <span class="pill">BC Log</span>
            <button class="btn-ghost" id="bc-log-load">Load</button>
          </div>
          <pre id="bc-log" class="log"></pre>
        </div>
        <div>
          <div class="row">
            <span class="pill">Premium Log</span>
            <button class="btn-ghost" id="premium-log-load">Load</button>
          </div>
          <pre id="premium-log" class="log"></pre>
        </div>
      </div>
    </div>

    <div class="section panel">
      <h2>Operation Sets</h2>
      <div class="row">
        <button class="btn-ghost" id="ops-load">Load</button>
        <span class="pill">Schedule API</span>
      </div>
      <pre id="ops-log" class="log"></pre>
    </div>

    <div class="section panel">
      <h2>PSS Error Logs</h2>
      <div class="row">
        <input id="pss-correlation" placeholder="Filter by correlationId (optional)" />
        <label class="pill"><input type="checkbox" id="pss-include-log" /> Include log payload</label>
        <button class="btn-ghost" id="pss-load">Load</button>
      </div>
      <pre id="pss-log" class="log"></pre>
    </div>
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
      async function fetchJson(url){
        var res = await fetch(url, { headers: { 'cache-control': 'no-cache' }});
        var ct = res.headers.get('content-type') || '';
        var payload = ct.indexOf('application/json') !== -1 ? await res.json() : await res.text();
        return { ok: res.ok, status: res.status, payload: payload };
      }
      function formatTs(value){
        if(!value) return '—';
        try{ return new Date(value).toLocaleString(); }catch(e){ return String(value); }
      }

      var bcStream = byId('bc-stream');
      var premiumStream = byId('premium-stream');
      var bcSource = null;
      var premiumSource = null;

      function startStream(kind){
        if(kind === 'bc'){
          if(bcSource) bcSource.close();
          bcStream.textContent = '';
          bcSource = new EventSource('/api/sync/bc-webhook-log-stream?include=1');
          bcSource.onmessage = function(ev){
            bcStream.textContent = (bcStream.textContent ? bcStream.textContent + '\\n' : '') + ev.data;
          };
        } else {
          if(premiumSource) premiumSource.close();
          premiumStream.textContent = '';
          premiumSource = new EventSource('/api/sync/webhook-log-stream?include=1');
          premiumSource.onmessage = function(ev){
            premiumStream.textContent = (premiumStream.textContent ? premiumStream.textContent + '\\n' : '') + ev.data;
          };
        }
      }
      function stopStream(kind){
        if(kind === 'bc' && bcSource){ bcSource.close(); bcSource = null; }
        if(kind === 'premium' && premiumSource){ premiumSource.close(); premiumSource = null; }
      }

      async function loadBcLog(){
        var res = await fetchJson('/api/sync/bc-webhook-log?limit=50');
        renderJson(byId('bc-log'), res.payload);
        var items = res.payload && res.payload.items ? res.payload.items : [];
        byId('bc-count').textContent = items.length || 0;
        if(items.length){
          var last = items[0];
          byId('bc-last').textContent = formatTs(last.ts);
        }
        var enqueued = 0, processed = 0, deduped = 0;
        items.forEach(function(it){
          if(typeof it.enqueued === 'number') enqueued += it.enqueued;
          if(typeof it.processed === 'number') processed += it.processed;
          if(typeof it.deduped === 'number') deduped += it.deduped;
        });
        byId('bc-enqueued').textContent = enqueued;
        byId('bc-processed').textContent = processed;
        byId('bc-deduped').textContent = deduped;
      }

      async function loadPremiumLog(){
        var res = await fetchJson('/api/sync/webhook-log?limit=50');
        renderJson(byId('premium-log'), res.payload);
        var items = res.payload && res.payload.items ? res.payload.items : [];
        byId('premium-count').textContent = items.length || 0;
        if(items.length){
          var last = items[0];
          byId('premium-last').textContent = formatTs(last.ts);
        }
        var notifications = 0;
        var ignored = 0;
        items.forEach(function(it){
          if(it.type === 'notification') notifications += 1;
          if(Array.isArray(it.ignoredTaskIds)) ignored += it.ignoredTaskIds.length;
        });
        byId('premium-notifications').textContent = notifications;
        byId('premium-ignored').textContent = ignored;
      }

      async function loadOperationSets(){
        var res = await fetchJson('/api/sync/debug-operation-sets?limit=50');
        renderJson(byId('ops-log'), res.payload);
        var items = res.payload && res.payload.items ? res.payload.items : [];
        byId('ops-count').textContent = items.length || 0;
        if(items.length){
          byId('ops-last').textContent = formatTs(items[0].createdon || items[0].modifiedon);
        }
      }

      async function loadPssErrors(){
        var correlation = (byId('pss-correlation').value || '').trim();
        var includeLog = byId('pss-include-log').checked;
        var url = '/api/sync/debug-pss-error-logs?limit=50' + (includeLog ? '&includeLog=1' : '');
        if(correlation) url += '&correlationId=' + encodeURIComponent(correlation);
        var res = await fetchJson(url);
        renderJson(byId('pss-log'), res.payload);
        var items = res.payload && res.payload.items ? res.payload.items : [];
        byId('pss-count').textContent = items.length || 0;
        if(items.length){
          byId('pss-last').textContent = formatTs(items[0].createdon || items[0].modifiedon);
          byId('pss-last-msg').textContent = (items[0].log || items[0].errorCode || '—').toString().slice(0, 60);
        }
      }

      function refreshAll(){
        loadBcLog();
        loadPremiumLog();
        loadOperationSets();
        loadPssErrors();
      }

      byId('refresh-all')?.addEventListener('click', refreshAll);
      byId('bc-log-load')?.addEventListener('click', loadBcLog);
      byId('premium-log-load')?.addEventListener('click', loadPremiumLog);
      byId('ops-load')?.addEventListener('click', loadOperationSets);
      byId('pss-load')?.addEventListener('click', loadPssErrors);
      byId('bc-stream-start')?.addEventListener('click', function(){ startStream('bc'); });
      byId('bc-stream-stop')?.addEventListener('click', function(){ stopStream('bc'); });
      byId('bc-stream-clear')?.addEventListener('click', function(){ bcStream.textContent=''; });
      byId('premium-stream-start')?.addEventListener('click', function(){ startStream('premium'); });
      byId('premium-stream-stop')?.addEventListener('click', function(){ stopStream('premium'); });
      byId('premium-stream-clear')?.addEventListener('click', function(){ premiumStream.textContent=''; });

      refreshAll();
      startStream('bc');
      startStream('premium');
      setInterval(refreshAll, 15000);
    })();
  </script>
</body></html>`;
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

    res.status(200).send(layout("Webhook Control Center", ""));
}
