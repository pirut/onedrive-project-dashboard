import { listSubmissions, kvDiagnostics } from "../lib/kv.js";

function escapeHtml(s) {
    return String(s).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}

export default async function handler(_req, res) {
    const [diag, items] = await Promise.all([kvDiagnostics(), listSubmissions(200)]);
    const rows = items
        .map((it) => {
            const files = Array.isArray(it.files)
                ? it.files
                      .map(
                          (f) =>
                              `<div><a href="${escapeHtml(f.url || "")}" target="_blank" rel="noreferrer">${escapeHtml(
                                  f.filename || f.url || "file"
                              )}</a></div>`
                      )
                      .join("")
                : "";
            return `
        <tr>
          <td>${escapeHtml(it.loggedAt || "")}</td>
          <td>${escapeHtml(it.type || "")}</td>
          <td>${escapeHtml(it.folderName || "")}</td>
          <td style="text-align:center">${escapeHtml(String(it.uploaded ?? ""))}</td>
          <td>${files}</td>
        </tr>
      `;
        })
        .join("");

    const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Webhook Submissions</title>
<style>
body{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;padding:24px;background:#f9fafb;color:#111827}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,0.06)}
th,td{padding:12px;border-top:1px solid #f3f4f6;vertical-align:top}
th{background:#f9fafb;text-align:left;color:#374151}
tr:nth-child(even){background:#fcfcfd}
.muted{color:#6b7280}
</style></head>
<body>
  <h1 style="margin:0 0 16px 0; font-size:20px; font-weight:700">Webhook Submissions</h1>
  <div class="muted" style="margin-bottom:8px">KV status: ${diag.ok ? "OK" : "ERROR"} (${escapeHtml(diag.info?.provider || "unknown")})</div>
  ${
      diag.ok
          ? ""
          : `<pre style="white-space:pre-wrap;background:#fff;padding:8px;border:1px solid #eee;border-radius:8px;margin:0 0 16px 0">${escapeHtml(
                diag.error || "not configured"
            )}</pre>`
  }
  <div class="muted" style="margin-bottom:16px">Showing latest ${items.length} events</div>
  <table>
    <thead>
      <tr><th>Time</th><th>Type</th><th>Folder</th><th>Uploaded</th><th>Files</th></tr>
    </thead>
    <tbody>${rows || '<tr><td colspan="5" class="muted">No submissions yet.</td></tr>'}</tbody>
  </table>
</body></html>`;

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).send(html);
}
