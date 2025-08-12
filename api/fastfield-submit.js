import "isomorphic-fetch";
import { ConfidentialClientApplication } from "@azure/msal-node";
import { logSubmission } from "../lib/kv.js";

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

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key");
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    try {
        // Flexible body: { folderName, files:[{url,filename}] } or { data: { ... } }
        const body = req.body && typeof req.body === "object" ? req.body : await readJsonBody(req);
        const data = body?.data && typeof body.data === "object" ? body.data : body;
        let folderName = data?.folderName || data?.projectFolderName || data?.folder || null;
        const site = data?.siteUrl || DEFAULT_SITE_URL;
        const library = data?.libraryPath || DEFAULT_LIBRARY;
        const files = normalizeFiles(data);

    if (!folderName) {
      await logSubmission({ type: "fastfield-submit", status: "invalid", reason: "Missing folderName", siteUrl: site, libraryPath: library });
      return res.status(400).json({ error: "Missing folderName in payload" });
    }
    if (!site || !library) {
      await logSubmission({ type: "fastfield-submit", status: "invalid", reason: "Missing siteUrl or libraryPath", folderName });
      return res.status(400).json({ error: "Missing siteUrl or libraryPath" });
    }
    if (files.length === 0) {
      await logSubmission({ type: "fastfield-submit", status: "invalid", reason: "No files provided", folderName, siteUrl: site, libraryPath: library });
      return res.status(400).json({ error: "No files provided" });
    }

        const token = await getAppToken();
        const { driveId, folderId } = await resolveDriveAndFolder(token, site, library, folderName);

        let uploaded = 0;
        for (const f of files) {
            const buffer = await fetchToBuffer(f.url);
            await uploadSmallFile(token, driveId, folderId, f.filename || deriveFilenameFromUrl(f.url), buffer);
            uploaded += 1;
        }

    await logSubmission({
      type: "fastfield-submit",
      status: "ok",
      folderName,
      uploaded,
      files,
      siteUrl: site,
      libraryPath: library,
    });

        res.status(200).json({ ok: true, uploaded, folderName, siteUrl: site, libraryPath: library, driveId, folderId });
    } catch (e) {
    await logSubmission({ type: "fastfield-submit", status: "error", error: e?.message || String(e) });
    res.status(500).json({ error: e.message || String(e) });
    }
}

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    try {
        return JSON.parse(text);
    } catch {
        return {};
    }
}

function normalizeFiles(data) {
    const files = [];
    const candidates = data?.files || data?.attachments || data?.uploadedFiles || [];
    for (const it of candidates) {
        const url = it?.url || it?.href || it?.link || null;
        const filename = it?.filename || it?.name || null;
        if (url) files.push({ url, filename });
    }
    return files;
}

async function fetchToBuffer(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Download failed ${res.status}: ${url}`);
    const arrayBuffer = await res.arrayBuffer();
    return Buffer.from(arrayBuffer);
}

function deriveFilenameFromUrl(url) {
    try {
        const u = new URL(url);
        const pathname = u.pathname;
        const last = pathname.split("/").filter(Boolean).pop();
        return last || "upload.bin";
    } catch {
        return "upload.bin";
    }
}
