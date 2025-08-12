import "isomorphic-fetch";
import Busboy from "busboy";
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

async function resolveDriveAndFolder(accessToken, siteUrl, libraryPath, targetFolderName) {
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

    let childrenPath;
    if (subPathParts.length === 0) {
        childrenPath = `/drives/${drive.id}/root/children`;
    } else {
        const subPath = encodeDrivePath(subPathParts.join("/"));
        childrenPath = `/drives/${drive.id}/root:/${subPath}:/children`;
    }
    const list = await graphFetch(childrenPath, accessToken);
    const target = (list.value || []).find(
        (it) =>
            it.folder &&
            (it.name || "").trim().toLowerCase() ===
                String(targetFolderName || "")
                    .trim()
                    .toLowerCase()
    );
    if (!target) throw new Error(`Target folder not found: ${targetFolderName}`);
    return { driveId: drive.id, folderId: target.id };
}

async function uploadSmallFile(accessToken, driveId, parentItemId, filename, buffer) {
    const path = `/drives/${driveId}/items/${parentItemId}:/${encodeURIComponent(filename)}:/content`;
    await graphFetch(path, accessToken, { method: "PUT", headers: { "Content-Type": "application/octet-stream" }, body: buffer });
}

export const config = { api: { bodyParser: false } };

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") {
        res.status(204).end();
        return;
    }
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

  try {
    let site = DEFAULT_SITE_URL;
    let library = DEFAULT_LIBRARY;
    let inferredFolderName = undefined;
    const files = [];

    await new Promise((resolve, reject) => {
      const bb = new Busboy({ headers: req.headers });
      bb.on("file", (_name, file, info) => {
        const chunks = [];
        file.on("data", (d) => chunks.push(d));
        file.on("end", () => {
          files.push({ filename: info.filename, buffer: Buffer.concat(chunks) });
        });
      });
      bb.on("field", (name, val) => {
        if (name === "folderName") inferredFolderName = val;
        if (name === "siteUrl") site = val;
        if (name === "libraryPath") library = val;
      });
      bb.on("finish", resolve);
      bb.on("error", reject);
      req.pipe(bb);
    });

    if (!inferredFolderName) return res.status(400).json({ error: "Missing folderName" });
    if (!site || !library) return res.status(400).json({ error: "Missing siteUrl or libraryPath" });
    if (files.length === 0) return res.status(400).json({ error: "No files uploaded" });

    const token = await getAppToken();
    const { driveId, folderId } = await resolveDriveAndFolder(token, site, library, inferredFolderName);

    for (const f of files) {
      await uploadSmallFile(token, driveId, folderId, f.filename, f.buffer);
    }

    res.status(200).json({ ok: true, uploaded: files.length, driveId, folderId });
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  }
}
