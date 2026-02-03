import "dotenv/config";
/*
 Minimal webhook to receive files and upload to SharePoint via Microsoft Graph
 Auth: app-only using client credentials (msal-node)
*/

import express from "express";
import cors from "cors";
import multer from "multer";
import { ConfidentialClientApplication } from "@azure/msal-node";

const PORT = process.env.PORT || 3001;
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*"; // e.g. https://cstonedash.jrbussard.com
const TENANT_ID = process.env.TENANT_ID; // or domain
const MSAL_CLIENT_ID = process.env.MSAL_CLIENT_ID;
const MICROSOFT_CLIENT_SECRET = process.env.MICROSOFT_CLIENT_SECRET;
const MS_GRAPH_SCOPE = process.env.MS_GRAPH_SCOPE || "https://graph.microsoft.com/.default";
const DEFAULT_SITE_URL = process.env.DEFAULT_SITE_URL; // e.g. https://tenant.sharepoint.com/sites/work
const DEFAULT_LIBRARY = process.env.DEFAULT_LIBRARY; // e.g. Documents/Cornerstone Jobs

if (!TENANT_ID || !MSAL_CLIENT_ID || !MICROSOFT_CLIENT_SECRET || !DEFAULT_SITE_URL || !DEFAULT_LIBRARY) {
    // eslint-disable-next-line no-console
    console.warn("Missing env. Required: TENANT_ID, MSAL_CLIENT_ID, MICROSOFT_CLIENT_SECRET, DEFAULT_SITE_URL, DEFAULT_LIBRARY");
}

const msalApp = new ConfidentialClientApplication({
    auth: {
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientId: MSAL_CLIENT_ID,
        clientSecret: MICROSOFT_CLIENT_SECRET,
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
    const host = url.host; // tenant.sharepoint.com
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
    const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
    const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
    const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
    const [driveName, ...subPathParts] = libPathTrimmed.split("/");
    const drive = (drives.value || []).find((d) => d.name === driveName);
    if (!drive) throw new Error(`Library not found: ${driveName}`);

    // Find targetFolderName under drive root or under subPath
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
    // Upload to: /drives/{driveId}/items/{parentId}:/{filename}:/content
    const path = `/drives/${driveId}/items/${parentItemId}:/${encodeURIComponent(filename)}:/content`;
    await graphFetch(path, accessToken, { method: "PUT", headers: { "Content-Type": "application/octet-stream" }, body: buffer });
}

const app = express();
app.use(
    cors({
        origin: (origin, callback) => {
            if (!origin) return callback(null, true);
            if (CORS_ORIGIN === "*") return callback(null, true);
            const allowed = CORS_ORIGIN.split(",").map((s) => s.trim());
            if (allowed.includes(origin)) return callback(null, true);
            return callback(new Error("Not allowed by CORS"));
        },
    })
);
const upload = multer({ storage: multer.memoryStorage() });

app.get("/health", (_req, res) => res.json({ ok: true }));

// multipart/form-data: folderName, file (one or many)
app.post("/upload", upload.array("file"), async (req, res) => {
    try {
        const folderName = req.body.folderName;
        const site = req.body.siteUrl || DEFAULT_SITE_URL;
        const library = req.body.libraryPath || DEFAULT_LIBRARY;
        if (!folderName) return res.status(400).json({ error: "Missing folderName" });
        if (!site || !library) return res.status(400).json({ error: "Missing siteUrl or libraryPath" });
        if (!req.files || req.files.length === 0) return res.status(400).json({ error: "No files uploaded" });

        const token = await getAppToken();
        const { driveId, folderId } = await resolveDriveAndFolder(token, site, library, folderName);

        for (const f of req.files) {
            // For files >4MB you'd use upload sessions; this handles small files for simplicity
            await uploadSmallFile(token, driveId, folderId, f.originalname, f.buffer);
        }

        res.json({ ok: true, uploaded: req.files.length, driveId, folderId });
    } catch (e) {
        // eslint-disable-next-line no-console
        console.error(e);
        res.status(500).json({ error: e.message || String(e) });
    }
});

app.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`Webhook listening on http://localhost:${PORT}`);
});
