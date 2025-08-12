import "isomorphic-fetch";
import { ConfidentialClientApplication } from "@azure/msal-node";

function readEnv(name, required = false) {
  const val = process.env[name];
  if (required && !val) throw new Error(`Missing env ${name}`);
  return val;
}

const TENANT_ID = readEnv("TENANT_ID", true);
const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
const MSAL_CLIENT_SECRET = readEnv("MSAL_CLIENT_SECRET", true);
const MS_GRAPH_SCOPE = readEnv("MS_GRAPH_SCOPE") || "https://graph.microsoft.com/.default";
const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);
const DEFAULT_LIBRARY = readEnv("DEFAULT_LIBRARY", true);

const FASTFIELD_SYNC_URL = process.env.FASTFIELD_SYNC_URL || "";
const FASTFIELD_TABLE_NAME = process.env.FASTFIELD_TABLE_NAME || "CornerstoneProjects";

const msalApp = new ConfidentialClientApplication({
  auth: {
    authority: `https://login.microsoftonline.com/${TENANT_ID}`,
    clientId: MSAL_CLIENT_ID,
    clientSecret: MSAL_CLIENT_SECRET,
  },
});

async function getAppToken() {
  const result = await msalApp.acquireTokenByClientCredential({ scopes: [MS_GRAPH_SCOPE] });
  return result.accessToken;
}

async function graphFetch(path, accessToken, options = {}) {
  const res = await fetch(`https://graph.microsoft.com/v1.0${path}`, {
    method: options.method || "GET",
    headers: { Authorization: `Bearer ${accessToken}`, ...(options.headers || {}) },
    body: options.body,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Graph ${options.method || "GET"} ${path} -> ${res.status}: ${text}`);
  }
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) return res.json();
  return res.arrayBuffer();
}

function encodeDrivePath(path) {
  const trimmed = String(path || "").replace(/^\/+|\/+$/g, "");
  if (!trimmed) return "";
  return trimmed
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}

export default async function handler(req, res) {
  const origin = process.env.CORS_ORIGIN || "*";
  res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key");
  if (req.method === "OPTIONS") return res.status(204).end();

  // API key enforcement
  const configuredKeys = (process.env.API_KEYS || process.env.API_KEY || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (configuredKeys.length > 0) {
    const headerKey = req.headers["x-api-key"];
    const auth = req.headers["authorization"]; // Bearer <key>
    const provided = headerKey || (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ") ? auth.slice(7) : null);
    if (!provided || !configuredKeys.includes(String(provided))) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }

  try {
    const accessToken = await getAppToken();
    const siteUrl = req.query.siteUrl || DEFAULT_SITE_URL;
    const libraryPath = req.query.libraryPath || DEFAULT_LIBRARY;

    // Resolve drive
    const url = new URL(siteUrl);
    const host = url.host;
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
    const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
    const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
    const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
    const [driveName, ...subPathParts] = libPathTrimmed.split("/");
    const drive = (drives.value || []).find((d) => d.name === driveName);
    if (!drive) throw new Error(`Library not found: ${driveName}`);

    // List children (subPath or root)
    let resItems;
    if (subPathParts.length === 0) {
      resItems = await graphFetch(`/drives/${drive.id}/root/children`, accessToken);
    } else {
      const subPath = encodeDrivePath(subPathParts.join("/"));
      resItems = await graphFetch(`/drives/${drive.id}/root:/${subPath}:/children`, accessToken);
    }

    // Keep only folders, exclude name == (ARCHIVE)
    const foldersOnly = (resItems.value || [])
      .filter((it) => it.folder)
      .filter((it) => (it.name || "").trim().toLowerCase() !== "(archive)")
      .map((it) => ({
        id: it.id,
        name: it.name,
        link: it.webUrl || null,
        driveId: it.parentReference?.driveId || null,
        parentPath: it.parentReference?.path || null,
      }));

    const payload = {
      tableName: FASTFIELD_TABLE_NAME,
      generatedAt: new Date().toISOString(),
      siteUrl,
      libraryPath,
      rows: foldersOnly,
    };

    if (FASTFIELD_SYNC_URL) {
      const ff = await fetch(FASTFIELD_SYNC_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!ff.ok) throw new Error(`FastField sync failed: ${ff.status}`);
      return res.status(200).json({ ok: true, synced: foldersOnly.length });
    }

    return res.status(200).json({ ok: true, rows: foldersOnly });
  } catch (e) {
    return res.status(500).json({ error: e.message || String(e) });
  }
}


