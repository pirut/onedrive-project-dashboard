import "isomorphic-fetch";
import crypto from "crypto";


// Admin auth configuration
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

function requireAuth(req) {
    const cookies = parseCookies(req);
    const session = verifySession(cookies.admin_session);
    if (!session) {
        return { error: "Unauthorized", status: 401 };
    }
    return { session };
}


function htmlEscape(s) {
    return String(s)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function kanbanDashboardHTML() {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Project Kanban Board</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #0f172a;
      --muted: #8b98b8;
      --fg: #e6ecff;
      --ok: #10b981;
      --bad: #ef4444;
      --warn: #f59e0b;
      --border: #1f2a44;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--fg);
      font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto;
    }
    .header {
      background: var(--panel);
      border-bottom: 1px solid var(--border);
      padding: 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }
    .header h1 { margin: 0; font-size: 20px; }
    .header-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    button {
      background: #2b61d1;
      color: #fff;
      border: 0;
      border-radius: 8px;
      padding: 8px 16px;
      cursor: pointer;
      font: inherit;
    }
    button:hover { background: #1e4ed8; }
    button.secondary {
      background: var(--panel);
      border: 1px solid var(--border);
    }
    button.secondary:hover { background: #1a2332; }
    .kanban-board {
      display: flex;
      gap: 16px;
      padding: 16px;
      overflow-x: auto;
      min-height: calc(100vh - 80px);
    }
    .bucket {
      min-width: 280px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      display: flex;
      flex-direction: column;
    }
    .bucket-header {
      font-weight: 600;
      padding: 8px;
      margin-bottom: 8px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .bucket-count {
      background: var(--bg);
      border-radius: 12px;
      padding: 2px 8px;
      font-size: 12px;
      color: var(--muted);
    }
    .bucket-items {
      flex: 1;
      min-height: 100px;
    }
    .project-card {
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 8px;
      cursor: move;
      transition: transform 0.2s, box-shadow 0.2s;
    }
    .project-card:hover {
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .project-card.dragging {
      opacity: 0.5;
    }
    .project-card.archived {
      border-left: 3px solid var(--muted);
      opacity: 0.8;
    }
    .project-name {
      font-weight: 500;
      margin-bottom: 4px;
      word-break: break-word;
    }
    .project-link {
      color: #60a5fa;
      text-decoration: none;
      font-size: 12px;
    }
    .project-link:hover { text-decoration: underline; }
    .project-meta {
      font-size: 11px;
      color: var(--muted);
      margin-top: 4px;
    }
    .bucket.drag-over {
      background: #1a2332;
      border-color: #3b82f6;
    }
    .loading {
      text-align: center;
      padding: 40px;
      color: var(--muted);
    }
    .error {
      background: var(--panel);
      border: 1px solid var(--bad);
      border-radius: 8px;
      padding: 16px;
      margin: 16px;
      color: var(--bad);
    }
    .back-link {
      color: var(--muted);
      text-decoration: none;
      font-size: 14px;
    }
    .back-link:hover { color: var(--fg); }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <a href="/api/admin" class="back-link">← Back to Admin</a>
      <h1>Project Kanban Board</h1>
    </div>
    <div class="header-actions">
      <button onclick="syncProjects()">Sync Projects</button>
      <button onclick="manageBuckets()" class="secondary">Manage Buckets</button>
    </div>
  </div>
  <div id="kanban-board" class="kanban-board">
    <div class="loading">Loading projects...</div>
  </div>
  <script>
    let projects = [];
    let buckets = [];
    let draggedElement = null;
    let draggedData = null;
    let lastUpdateTimestamp = null;
    let pollInterval = null;

    async function loadData() {
      const board = document.getElementById('kanban-board');
      try {
        board.innerHTML = '<div class="loading">Loading projects...</div>';
        const res = await fetch('/api/projects-kanban/data');
        if (!res.ok) {
          const statusText = res.statusText || '';
          const errorMsg = 'HTTP ' + res.status + (statusText ? ': ' + statusText : '');
          const errorData = await res.json().catch(() => ({ error: errorMsg }));
          throw new Error(errorData.error || errorMsg);
        }
        const data = await res.json();
        projects = data.projects || [];
        buckets = data.buckets || [];
        lastUpdateTimestamp = data.lastUpdate || null;
        console.log('Loaded ' + projects.length + ' projects, ' + buckets.length + ' buckets');
        renderBoard();
      } catch (e) {
        console.error('Error loading data:', e);
        board.innerHTML = 
          '<div class="error">Error loading data: ' + escapeHtml(e.message) + 
          '<br><button onclick="loadData()" style="margin-top: 8px">Retry</button></div>';
      }
    }

    async function checkForUpdates() {
      try {
        const res = await fetch('/api/projects-kanban/timestamp');
        if (!res.ok) return;
        const data = await res.json();
        if (data.timestamp && data.timestamp !== lastUpdateTimestamp) {
          // Timestamp changed, reload data
          await loadData();
        }
      } catch (e) {
        // Silently fail - polling will continue
        console.error('Poll error:', e);
      }
    }

    function startPolling() {
      // Poll every 2 seconds for updates
      if (pollInterval) clearInterval(pollInterval);
      pollInterval = setInterval(checkForUpdates, 2000);
    }

    function stopPolling() {
      if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
      }
    }

    function renderBoard() {
      const board = document.getElementById('kanban-board');
      if (buckets.length === 0) {
        board.innerHTML = '<div class="loading">No buckets configured</div>';
        return;
      }

      const sortedBuckets = [...buckets].sort((a, b) => a.order - b.order);
      board.innerHTML = sortedBuckets.map(bucket => {
        const bucketProjects = projects.filter(p => p.bucketId === bucket.id);
        return '<div class="bucket" data-bucket-id="' + bucket.id + '" ' +
               'ondragover="handleDragOver(event)" ' +
               'ondrop="handleDrop(event)" ' +
               'ondragleave="handleDragLeave(event)">' +
            '<div class="bucket-header" style="border-left: 3px solid ' + bucket.color + '">' +
              '<span>' + escapeHtml(bucket.name) + '</span>' +
              '<span class="bucket-count">' + bucketProjects.length + '</span>' +
            '</div>' +
            '<div class="bucket-items">' +
              bucketProjects.map(p => renderProjectCard(p)).join('') +
            '</div>' +
          '</div>';
      }).join('');
    }

    function renderProjectCard(project) {
      const archivedClass = project.isArchived ? 'archived' : '';
      const webUrlLink = project.webUrl ? '<a href="' + escapeHtml(project.webUrl) + '" target="_blank" class="project-link">Open in SharePoint</a>' : '';
      const archivedMeta = project.isArchived ? '<div class="project-meta">Archived</div>' : '';
      return '<div class="project-card ' + archivedClass + '" ' +
             'draggable="true" ' +
             'data-project-id="' + escapeHtml(project.id) + '" ' +
             'ondragstart="handleDragStart(event)" ' +
             'ondragend="handleDragEnd(event)">' +
          '<div class="project-name">' + escapeHtml(project.name) + '</div>' +
          webUrlLink +
          archivedMeta +
        '</div>';
    }

    function handleDragStart(e) {
      const projectId = e.target.getAttribute('data-project-id') || e.target.closest('[data-project-id]').getAttribute('data-project-id');
      draggedElement = e.target;
      draggedData = { projectId: projectId };
      e.target.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
    }

    function handleDragEnd(e) {
      e.target.classList.remove('dragging');
      draggedElement = null;
      draggedData = null;
      document.querySelectorAll('.bucket').forEach(b => b.classList.remove('drag-over'));
    }

    function handleDragOver(e) {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      e.currentTarget.classList.add('drag-over');
    }

    function handleDragLeave(e) {
      e.currentTarget.classList.remove('drag-over');
    }

    async function handleDrop(e) {
      e.preventDefault();
      e.currentTarget.classList.remove('drag-over');
      
      if (!draggedData) return;
      
      const bucketId = e.currentTarget.dataset.bucketId;
      const projectId = draggedData.projectId;
      
      // Check if project is archived - prevent moving out of archive bucket
      const project = projects.find(p => p.id === projectId);
      if (project && project.isArchived) {
        const archiveBucket = buckets.find(b => b.id === 'archive');
        if (bucketId !== archiveBucket?.id && bucketId !== 'archive') {
          alert('Archived folders must remain in the Archive bucket');
          renderBoard();
          return;
        }
      }
      
      try {
        const res = await fetch('/api/projects-kanban/move', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ projectId, bucketId })
        });
        
        if (!res.ok) {
          const err = await res.json();
          throw new Error(err.error || 'Failed to move project');
        }
        
        // Update local state
        if (project) {
          project.bucketId = bucketId;
        }
        
        renderBoard();
      } catch (err) {
        alert('Error moving project: ' + err.message);
        renderBoard();
      }
    }

    async function syncProjects() {
      const board = document.getElementById('kanban-board');
      try {
        board.innerHTML = '<div class="loading">Syncing projects...</div>';
        const res = await fetch('/api/projects-kanban/sync', { method: 'POST' });
        if (!res.ok) {
          const errorData = await res.json().catch(() => ({ error: 'HTTP ' + res.status }));
          throw new Error(errorData.error || 'Sync failed');
        }
        const data = await res.json();
        console.log('Sync complete: ' + (data.synced || 0) + ' folders synced');
        await loadData();
        alert('Projects synced successfully: ' + (data.synced || 0) + ' folders');
      } catch (e) {
        console.error('Sync error:', e);
        alert('Error syncing: ' + e.message);
        await loadData(); // Try to load existing data
      }
    }

    function manageBuckets() {
      alert('Bucket management coming soon');
    }

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    // Auto-sync on page load, then load data
    async function initDashboard() {
      const board = document.getElementById('kanban-board');
      try {
        board.innerHTML = '<div class="loading">Syncing projects from SharePoint...</div>';
        const syncRes = await fetch('/api/projects-kanban/sync', { method: 'POST' });
        if (!syncRes.ok) {
          const errorData = await syncRes.json().catch(() => ({ error: 'HTTP ' + syncRes.status }));
          console.warn('Sync warning:', errorData.error);
          // Continue anyway - might have existing data
        } else {
          const syncData = await syncRes.json();
          console.log('Sync complete: ' + (syncData.synced || 0) + ' folders synced');
        }
        // Then load the data
        await loadData();
        // Start polling for real-time updates
        startPolling();
      } catch (e) {
        console.error('Init error:', e);
        board.innerHTML = '<div class="error">Error initializing: ' + escapeHtml(e.message) + 
          '<br><button onclick="initDashboard()" style="margin-top: 8px">Retry</button></div>';
        // Still try to load data even if sync fails
        try {
          await loadData();
          startPolling();
        } catch (loadError) {
          console.error('Failed to load data:', loadError);
        }
      }
    }
    
    // Stop polling when page is hidden, resume when visible
    document.addEventListener('visibilitychange', function() {
      if (document.hidden) {
        stopPolling();
      } else {
        startPolling();
        checkForUpdates(); // Check immediately when page becomes visible
      }
    });
    
    initDashboard();
  </script>
</body>
</html>`;
}

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") return res.status(204).end();

    const auth = requireAuth(req);
    if (auth.error) {
        return res.status(auth.status).json({ error: auth.error });
    }

    return res.status(200).setHeader("Content-Type", "text/html").send(kanbanDashboardHTML());
}


