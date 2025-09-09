import "isomorphic-fetch";
import Busboy from "busboy";
import crypto from "crypto";
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

function sanitizeFolderName(name) {
    const base = String(name || "").trim();
    const illegal = /["#:*?<>|{}\\/+%&]/g; // remove characters SharePoint dislikes
    let safe = base.replace(illegal, " ").replace(/\s+/g, " ").trim();
    // disallow trailing dot/space
    safe = safe.replace(/[\s.]+$/, "");
    // max length guard
    if (safe.length > 200) safe = safe.slice(0, 200).trim();
    return safe || "Untitled";
}

function deriveFolderNameFromFilename(filename) {
    const nameOnly = String(filename || "").replace(/\.[^.]+$/i, "");
    const firstPart = nameOnly.split(":")[0];
    return sanitizeFolderName(firstPart);
}

async function resolveDrive(accessToken, siteUrl, libraryPath) {
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
    return { drive, subPathParts };
}

async function getParentItemId(accessToken, driveId, subPathParts) {
    if (!subPathParts || subPathParts.length === 0) return "root"; // special alias for root
    const subPath = encodeDrivePath(subPathParts.join("/"));
    const item = await graphFetch(`/drives/${driveId}/root:/${subPath}`, accessToken);
    // item is JSON when ok
    return item.id;
}

async function ensureFolder(accessToken, driveId, parentItemId, targetFolderName) {
    // list existing children
    const children = await graphFetch(
        parentItemId === "root" ? `/drives/${driveId}/root/children` : `/drives/${driveId}/items/${parentItemId}/children`,
        accessToken
    );
    const existing = (children.value || []).find(
        (it) => it.folder && (it.name || "").trim().toLowerCase() === String(targetFolderName).trim().toLowerCase()
    );
    if (existing) return { id: existing.id, created: false };
    // create
    const payload = {
        name: targetFolderName,
        folder: {},
        "@microsoft.graph.conflictBehavior": "fail",
    };
    const created = await graphFetch(
        parentItemId === "root" ? `/drives/${driveId}/root/children` : `/drives/${driveId}/items/${parentItemId}/children`,
        accessToken,
        { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }
    );
    return { id: created.id, created: true };
}

async function uploadSmallFile(accessToken, driveId, parentItemId, filename, buffer) {
    const encoded = encodeURIComponent(filename);
    const path = parentItemId === "root"
        ? `/drives/${driveId}/root:/${encoded}:/content`
        : `/drives/${driveId}/items/${parentItemId}:/${encoded}:/content`;
    await graphFetch(path, accessToken, { method: "PUT", headers: { "Content-Type": "application/pdf" }, body: buffer });
}

async function uploadLargeFile(accessToken, driveId, parentItemId, filename, buffer) {
    const initPath = parentItemId === "root"
        ? `/drives/${driveId}/root:/${encodeURIComponent(filename)}:/createUploadSession`
        : `/drives/${driveId}/items/${parentItemId}:/${encodeURIComponent(filename)}:/createUploadSession`;
    const session = await graphFetch(initPath, accessToken, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ item: { "@microsoft.graph.conflictBehavior": "replace" } }),
    });
    const uploadUrl = session.uploadUrl;
    const chunkSize = 5 * 1024 * 1024; // 5 MB
    let offset = 0;
    while (offset < buffer.length) {
        const end = Math.min(offset + chunkSize, buffer.length);
        const chunk = buffer.subarray(offset, end);
        const contentRange = `bytes ${offset}-${end - 1}/${buffer.length}`;
        const resp = await fetch(uploadUrl, {
            method: "PUT",
            headers: {
                "Content-Length": String(chunk.length),
                "Content-Range": contentRange,
            },
            body: chunk,
        });
        if (!resp.ok && resp.status !== 202 && resp.status !== 201 && resp.status !== 200) {
            const text = await resp.text();
            throw new Error(`Upload chunk failed: ${resp.status} ${text}`);
        }
        offset = end;
    }
}

export const config = { api: { bodyParser: false } };

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    let phase = "start";
    try {
        const traceId = crypto.randomBytes(8).toString("hex");
        const steps = [];
        const push = (msg, meta = {}) => {
            const entry = { ts: new Date().toISOString(), msg, ...meta };
            steps.push(entry);
            // eslint-disable-next-line no-console
            console.log(`[ingest-pdf:${traceId}] ${msg}`, Object.keys(meta).length ? meta : "");
        };
        const debug = String(req.query?.debug || req.headers["x-debug-log"] || "").trim() === "1";
        res.setHeader("X-Request-Id", traceId);
        push("request:start", { ip: req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "", ua: req.headers["user-agent"] || "" });
        let site = DEFAULT_SITE_URL;
        let library = DEFAULT_LIBRARY;
        let fileRec = null;

        phase = "parse_form";
        await new Promise((resolve, reject) => {
            const bb = new Busboy({ headers: req.headers });
            bb.on("file", (_name, file, info) => {
                const chunks = [];
                file.on("data", (d) => chunks.push(d));
                file.on("end", () => {
                    const buf = Buffer.concat(chunks);
                    fileRec = { filename: info.filename, mimeType: info.mimeType || info.mime || "", buffer: buf };
                    push("upload:received", { filename: info.filename, size: buf.length, mime: info.mimeType || info.mime || "" });
                });
            });
            bb.on("field", (name, val) => {
                if (name === "siteUrl") site = val;
                if (name === "libraryPath") library = val;
            });
            bb.on("finish", resolve);
            bb.on("error", reject);
            req.pipe(bb);
        });

        if (!fileRec) {
            push("error:no-file");
            return res.status(400).json({ error: "No file uploaded", traceId });
        }
        const isPdf = /\.pdf$/i.test(fileRec.filename) || /pdf/i.test(fileRec.mimeType || "");
        if (!isPdf) {
            push("error:not-pdf", { filename: fileRec.filename, mime: fileRec.mimeType || "" });
            return res.status(400).json({ error: "File must be a PDF", traceId });
        }

        const folderNameRaw = deriveFolderNameFromFilename(fileRec.filename);
        const folderName = sanitizeFolderName(folderNameRaw);
        push("folder:derived", { from: fileRec.filename, folderName });

        phase = "get_token";
        const token = await getAppToken();
        push("auth:token-acquired");
        phase = "resolve_drive";
        const { drive, subPathParts } = await resolveDrive(token, site, library);
        push("drive:resolved", { driveName: drive.name, driveId: drive.id, site, library });
        phase = "resolve_parent";
        const parentItemId = await getParentItemId(token, drive.id, subPathParts);
        push("parent:resolved", { parentItemId });
        phase = "ensure_folder";
        const { id: folderId, created } = await ensureFolder(token, drive.id, parentItemId, folderName);
        push("folder:ensured", { folderId, created });

        // Upload. Use small upload if <= 4MB, else session
        const fourMB = 4 * 1024 * 1024;
        if (fileRec.buffer.length <= fourMB) {
            phase = "upload_simple";
            push("upload:start", { method: "simple", size: fileRec.buffer.length });
            await uploadSmallFile(token, drive.id, folderId, fileRec.filename, fileRec.buffer);
        } else {
            phase = "upload_chunked";
            push("upload:start", { method: "chunked", size: fileRec.buffer.length });
            await uploadLargeFile(token, drive.id, folderId, fileRec.filename, fileRec.buffer);
        }
        push("upload:done", { folderId, size: fileRec.buffer.length });

        await logSubmission({
            type: "pdf_ingest",
            status: "ok",
            traceId,
            folderName,
            created,
            filename: fileRec.filename,
            size: fileRec.buffer.length,
            steps,
        });

        const payload = { ok: true, traceId, driveId: drive.id, folderId, created, folderName, file: { name: fileRec.filename, size: fileRec.buffer.length } };
        if (debug) payload.debug = { steps };
        return res.status(200).json(payload);
    } catch (e) {
        const traceId = res.getHeader("X-Request-Id") || crypto.randomBytes(8).toString("hex");
        // eslint-disable-next-line no-console
        console.error(`[ingest-pdf:${traceId}] error at ${phase}:`, e?.message || e);
        // best-effort: include steps if present in scope
        try {
            await logSubmission({ type: "pdf_ingest", status: "error", traceId, phase, error: e?.message || String(e) });
        } catch {}
        const debug = String(req.query?.debug || req.headers["x-debug-log"] || "").trim() === "1";
        const payload = { error: e?.message || String(e), traceId, phase };
        return res.status(500).json(debug ? { ok: false, ...payload } : payload);
    }
}
