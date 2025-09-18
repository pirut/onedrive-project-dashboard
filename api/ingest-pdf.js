import "isomorphic-fetch";
import Busboy from "busboy";
import crypto from "crypto";
import fs from "fs";
import os from "os";
import path from "path";
import { Readable } from "stream";
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

export async function removeFileSafe(filePath) {
    if (!filePath) return;
    try {
        await fs.promises.unlink(filePath);
    } catch (err) {
        if (err?.code !== "ENOENT") {
            // eslint-disable-next-line no-console
            console.warn(`[ingest-pdf] failed to clean up temp file ${filePath}:`, err?.message || err);
        }
    }
}

function filenameFromContentDisposition(headerValue) {
    if (!headerValue) return "";
    const match = /filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i.exec(headerValue);
    if (!match) return "";
    return decodeURIComponent(match[1] || match[2] || "").trim();
}

function filenameFromUrl(url) {
    try {
        const parsed = new URL(url);
        const pathname = parsed.pathname || "";
        const parts = pathname.split("/").filter(Boolean);
        return parts.length ? parts[parts.length - 1] : "";
    } catch {
        return "";
    }
}

async function downloadRemotePdf(sourceUrl, fallbackName, push) {
    const tmpFilePath = path.join(os.tmpdir(), `pdf-ingest-remote-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`);
    let parsedUrl;
    try {
        parsedUrl = new URL(sourceUrl);
    } catch (err) {
        const e = new Error("Invalid remote URL");
        e.cause = err;
        throw e;
    }
    if (!(parsedUrl.protocol === "https:" || parsedUrl.protocol === "http:")) {
        throw new Error("Remote URL must use http or https");
    }
    push("remote:download:start", { url: sourceUrl });
    const res = await fetch(sourceUrl, { headers: { "cache-control": "no-cache" } });
    if (!res.ok || !res.body) {
        const bodyText = await res.text().catch(() => "");
        throw Object.assign(new Error(`Remote fetch failed: ${res.status} ${res.statusText}`), {
            status: res.status,
            responseBody: bodyText,
        });
    }
    const contentType = res.headers.get("content-type") || "";
    const disposition = res.headers.get("content-disposition") || "";
    let filename = String(fallbackName || "").trim();
    if (!filename) filename = filenameFromContentDisposition(disposition) || filenameFromUrl(sourceUrl);
    if (!/\.pdf$/i.test(filename || "")) filename = `${filename || "upload"}.pdf`;

    const nodeStream = typeof res.body.getReader === "function" ? Readable.fromWeb(res.body) : res.body;
    if (!nodeStream || typeof nodeStream.pipe !== "function") {
        throw new Error("Remote response stream is not readable");
    }
    let total = 0;
    await new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(tmpFilePath);
        nodeStream.on("data", (chunk) => {
            total += chunk.length;
        });
        nodeStream.on("error", (err) => {
            writeStream.destroy(err);
            removeFileSafe(tmpFilePath).finally(() => reject(err));
        });
        writeStream.on("error", (err) => {
            removeFileSafe(tmpFilePath).finally(() => reject(err));
        });
        writeStream.on("finish", resolve);
        nodeStream.pipe(writeStream);
    });
    if (total === 0) {
        try {
            const stat = await fs.promises.stat(tmpFilePath);
            total = stat.size;
        } catch {}
    }
    push("remote:download:done", { url: sourceUrl, size: total, filename });
    return { filename, mimeType: contentType || "application/pdf", size: total, tmpPath: tmpFilePath };
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
    // Split on underscore with optional surrounding spaces. Also accept ':' for compatibility.
    const parts = nameOnly.split(/\s*[_:]\s*/);
    const firstPart = parts[0] || nameOnly;
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

async function findFolder(accessToken, driveId, parentItemId, targetFolderName) {
    const listPath =
        parentItemId === "root"
            ? `/drives/${driveId}/root/children`
            : `/drives/${driveId}/items/${parentItemId}/children`;
    const children = await graphFetch(listPath, accessToken);
    const match = (children.value || []).find(
        (it) => it.folder && (it.name || "").trim().toLowerCase() === String(targetFolderName).trim().toLowerCase()
    );
    return match ? { id: match.id, name: match.name } : null;
}

async function ensureChildFolder(accessToken, driveId, parentItemId, childName) {
    const found = await findFolder(accessToken, driveId, parentItemId, childName);
    if (found) return { id: found.id, created: false };
    // Create the child folder if missing
    const payload = { name: childName, folder: {}, "@microsoft.graph.conflictBehavior": "fail" };
    const created = await graphFetch(
        `/drives/${driveId}/items/${parentItemId}/children`,
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

async function uploadLargeFileFromStream(accessToken, driveId, parentItemId, filename, filePath, fileSize, logEvent = () => {}) {
    const initPath = parentItemId === "root"
        ? `/drives/${driveId}/root:/${encodeURIComponent(filename)}:/createUploadSession`
        : `/drives/${driveId}/items/${parentItemId}:/${encodeURIComponent(filename)}:/createUploadSession`;
    const session = await graphFetch(initPath, accessToken, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ item: { "@microsoft.graph.conflictBehavior": "replace" } }),
    });
    const uploadUrl = session.uploadUrl;
    const chunkSize = 5 * 1024 * 1024; // 5 MB chunks to stay within Graph limits
    const stream = fs.createReadStream(filePath, { highWaterMark: chunkSize });
    let offset = 0;
    for await (const chunk of stream) {
        const end = offset + chunk.length;
        const contentRange = `bytes ${offset}-${end - 1}/${fileSize}`;
        let resp;
        try {
            resp = await fetch(uploadUrl, {
                method: "PUT",
                headers: {
                    "Content-Length": String(chunk.length),
                    "Content-Range": contentRange,
                },
                body: chunk,
            });
        } catch (networkErr) {
            try {
                logEvent("upload:chunk-error", {
                    contentRange,
                    error: networkErr?.message || String(networkErr),
                    type: "network",
                });
            } catch { /* ignore logging failure */ }
            throw networkErr;
        }
        if (!resp.ok && resp.status !== 202 && resp.status !== 201 && resp.status !== 200) {
            const text = await resp.text();
            try {
                logEvent("upload:chunk-error", {
                    contentRange,
                    status: resp.status,
                    bodyPreview: text.length > 500 ? `${text.slice(0, 500)}...` : text,
                    type: "graph",
                });
            } catch { /* ignore logging failure */ }
            const err = new Error(`Upload chunk failed: ${resp.status} ${text}`);
            err.status = resp.status;
            err.contentRange = contentRange;
            err.responseBody = text;
            throw err;
        }
        offset = end;
    }
}

async function prepareDestination({ token, filename, site, library, push, setPhase }) {
    setPhase("derive_folder");
    const folderNameRaw = deriveFolderNameFromFilename(filename);
    const folderName = sanitizeFolderName(folderNameRaw);
    push("folder:derived", { from: filename, folderName });

    setPhase("resolve_drive");
    const { drive, subPathParts } = await resolveDrive(token, site, library);
    push("drive:resolved", { driveName: drive.name, driveId: drive.id, site, library });

    setPhase("resolve_parent");
    const parentItemId = await getParentItemId(token, drive.id, subPathParts);
    push("parent:resolved", { parentItemId });

    setPhase("find_main_folder");
    const existing = await findFolder(token, drive.id, parentItemId, folderName);
    if (!existing) {
        push("folder:not-found", { folderName });
        const msg = `Target folder not found under library: ${folderName}`;
        const err = new Error(msg);
        err.status = 404;
        err.phase = "find_main_folder";
        throw err;
    }
    push("folder:found", { folderId: existing.id, folderName: existing.name });

    setPhase("ensure_job_walks");
    const jobWalksName = "Job Walks";
    const { id: folderId, created } = await ensureChildFolder(token, drive.id, existing.id, jobWalksName);
    push("folder:job-walks", { folderId, created, name: jobWalksName });

    return {
        driveId: drive.id,
        folderId,
        folderName,
        created,
    };
}

export async function executeIngestWorkflow({
    fileRec,
    site,
    library,
    push,
    setPhase = () => {},
}) {
    if (!fileRec || !fileRec.tmpPath) throw new Error("File record missing tmpPath");
    setPhase("get_token");
    const token = await getAppToken();
    push("auth:token-acquired");

    const destination = await prepareDestination({
        token,
        filename: fileRec.filename,
        site,
        library,
        push,
        setPhase,
    });

    let fileSize = fileRec.size;
    if (!fileSize) {
        try {
            const stat = await fs.promises.stat(fileRec.tmpPath);
            fileSize = stat.size;
        } catch {
            fileSize = 0;
        }
    }
    const fourMB = 4 * 1024 * 1024;
    if (fileSize <= fourMB) {
        setPhase("upload_simple");
        push("upload:start", { method: "simple", size: fileSize });
        const buffer = fileRec.buffer instanceof Buffer ? fileRec.buffer : await fs.promises.readFile(fileRec.tmpPath);
        await uploadSmallFile(token, destination.driveId, destination.folderId, fileRec.filename, buffer);
    } else {
        setPhase("upload_chunked");
        push("upload:start", { method: "chunked", size: fileSize });
        await uploadLargeFileFromStream(token, destination.driveId, destination.folderId, fileRec.filename, fileRec.tmpPath, fileSize, push);
    }
    push("upload:done", { folderId: destination.folderId, size: fileSize });

    return {
        driveId: destination.driveId,
        folderId: destination.folderId,
        created: destination.created,
        folderName: destination.folderName,
        fileSize,
        filename: fileRec.filename,
    };
}

export async function moveFileFromStaging({
    stagingSiteUrl,
    stagingLibraryPath,
    stagingFilename,
    stagingFilenames,
    destinationSiteUrl,
    destinationLibraryPath,
    push,
    setPhase = () => {},
    renameTo,
}) {
    const filenameCandidates = (Array.isArray(stagingFilenames) ? stagingFilenames : [stagingFilename])
        .map((name) => String(name || "").trim())
        .filter(Boolean);
    if (!filenameCandidates.length) throw new Error("Staging filename is required");
    setPhase("get_token");
    const token = await getAppToken();
    push("auth:token-acquired");

    setPhase("resolve_staging_drive");
    const { drive: stagingDrive, subPathParts: stagingSubPathParts } = await resolveDrive(token, stagingSiteUrl, stagingLibraryPath);
    push("staging:drive", { driveId: stagingDrive.id, driveName: stagingDrive.name, path: stagingLibraryPath });

    let stagingItem = null;
    let usedFilename = "";
    const lastErrors = [];
    for (const candidate of filenameCandidates) {
        const fullPathParts = stagingSubPathParts && stagingSubPathParts.length ? [...stagingSubPathParts, candidate] : [candidate];
        const encodedPath = encodeDrivePath(fullPathParts.join("/"));
        setPhase("staging_locate_file");
        try {
            stagingItem = await graphFetch(`/drives/${stagingDrive.id}/root:/${encodedPath}`, token);
            usedFilename = candidate;
            push("staging:file-found", { filename: stagingItem.name || candidate, itemId: stagingItem.id, size: stagingItem.size || null });
            break;
        } catch (err) {
            lastErrors.push({ candidate, status: err?.status || 404, message: err?.message || String(err) });
            push("staging:file-missing", { filename: candidate, status: err?.status || 404 });
        }
    }

    if (!stagingItem) {
        const notFound = new Error(`Staging file not found. Tried: ${filenameCandidates.join(", ")}`);
        notFound.status = 404;
        notFound.phase = "staging_locate_file";
        notFound.details = lastErrors;
        throw notFound;
    }

    const desiredName = renameTo || stagingFilename || usedFilename;
    const destination = await prepareDestination({
        token,
        filename: desiredName,
        site: destinationSiteUrl,
        library: destinationLibraryPath,
        push,
        setPhase,
    });

    if (stagingDrive.id === destination.driveId) {
        setPhase("move_file");
        push("move:start", {
            filename: stagingItem.name || stagingFilename,
            toFolderId: destination.folderId,
            folderName: destination.folderName,
        });

        const moveBody = {
            parentReference: { id: destination.folderId },
            name: desiredName,
        };

        const moved = await graphFetch(
            `/drives/${stagingDrive.id}/items/${stagingItem.id}?@microsoft.graph.conflictBehavior=replace`,
            token,
            { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(moveBody) }
        );

        push("move:done", { itemId: moved.id, name: moved.name, size: moved.size || null });

        return {
            driveId: destination.driveId,
            folderId: destination.folderId,
            folderName: destination.folderName,
            created: destination.created,
            filename: moved.name || desiredName,
            itemId: moved.id,
            size: moved.size || stagingItem.size || null,
        };
    }

    setPhase("copy_file");
    push("copy:start", {
        filename: stagingItem.name || stagingFilename,
        fromDrive: stagingDrive.id,
        toDrive: destination.driveId,
        toFolderId: destination.folderId,
        folderName: destination.folderName,
    });

    const copyResponse = await fetch(
        `https://graph.microsoft.com/v1.0/drives/${stagingDrive.id}/items/${stagingItem.id}/copy`,
        {
            method: "POST",
            headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                parentReference: { driveId: destination.driveId, id: destination.folderId },
                name: desiredName,
            }),
        }
    );

    if (copyResponse.status !== 202 && copyResponse.status !== 200) {
        const text = await copyResponse.text();
        const err = new Error(`Copy failed: ${copyResponse.status} ${text}`);
        err.status = copyResponse.status;
        err.phase = "copy_file";
        push("copy:error", { status: copyResponse.status, body: text.slice(0, 500) });
        throw err;
    }

    let copiedItem = null;
    for (let attempt = 0; attempt < 30; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        try {
            const children = await graphFetch(
                `/drives/${destination.driveId}/items/${destination.folderId}/children`,
                token
            );
            copiedItem = (children.value || []).find(
                (item) => (item.name || "").toLowerCase() === desiredName.toLowerCase()
            );
            if (copiedItem) break;
        } catch (pollErr) {
            push("copy:poll-error", { attempt, message: pollErr?.message || String(pollErr) });
        }
    }

    if (!copiedItem) {
        const err = new Error("Copy operation did not surface the new item");
        err.phase = "copy_file";
        push("copy:error", { reason: "not_found_after_copy" });
        throw err;
    }

    push("copy:done", { itemId: copiedItem.id, name: copiedItem.name, size: copiedItem.size || null });

    try {
        await fetch(`https://graph.microsoft.com/v1.0/drives/${stagingDrive.id}/items/${stagingItem.id}`, {
            method: "DELETE",
            headers: { Authorization: `Bearer ${token}` },
        });
        push("staging:cleanup", { itemId: stagingItem.id });
    } catch (cleanupErr) {
        push("staging:cleanup-error", { message: cleanupErr?.message || String(cleanupErr) });
    }

    return {
        driveId: destination.driveId,
        folderId: destination.folderId,
        folderName: destination.folderName,
        created: destination.created,
        filename: copiedItem.name || desiredName,
        itemId: copiedItem.id,
        size: copiedItem.size || stagingItem.size || null,
    };
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
    let traceId = crypto.randomBytes(8).toString("hex");
    const steps = [];
    const push = (msg, meta = {}) => {
        const entry = { ts: new Date().toISOString(), msg, ...meta };
        steps.push(entry);
        // eslint-disable-next-line no-console
        console.log(`[ingest-pdf:${traceId}] ${msg}`, Object.keys(meta).length ? meta : "");
    };
    let fileRec = null;
    const tempFiles = [];
    let debugMode = false;
    let remoteUrl = "";
    let remoteFilename = "";
    try {
        res.setHeader("X-Request-Id", traceId);
        debugMode = String(req.query?.debug || req.headers["x-debug-log"] || "").trim() === "1";
        push("request:start", { ip: req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "", ua: req.headers["user-agent"] || "" });
        let site = DEFAULT_SITE_URL;
        let library = DEFAULT_LIBRARY;

        const contentType = String(req.headers["content-type"] || "");
        if (/application\/json/i.test(contentType || "")) {
            phase = "parse_json";
            const chunks = [];
            for await (const chunk of req) chunks.push(chunk);
            const raw = Buffer.concat(chunks).toString("utf8");
            let payload = {};
            try {
                payload = raw ? JSON.parse(raw) : {};
            } catch (jsonErr) {
                push("error:json-parse", { message: jsonErr?.message || String(jsonErr) });
                return res.status(400).json({ error: "Invalid JSON payload", traceId });
            }
            if (payload && typeof payload === "object") {
                if (payload.siteUrl) site = String(payload.siteUrl);
                if (payload.libraryPath) library = String(payload.libraryPath);
                remoteUrl = String(payload.uploadthingUrl || payload.fileUrl || payload.remoteUrl || payload.url || "");
                remoteFilename = String(payload.uploadthingFilename || payload.remoteFilename || payload.filename || payload.name || "");
                push("request:json", {
                    hasRemoteUrl: Boolean(remoteUrl),
                    hasSite: Boolean(payload.siteUrl),
                    hasLibrary: Boolean(payload.libraryPath),
                });
            }
        } else {
            phase = "parse_form";
            await new Promise((resolve, reject) => {
                // Busboy supports function-style construction in CJS. Avoid `new` to prevent interop issues.
                const bb = Busboy({ headers: req.headers });
                const pending = [];
                bb.on("file", (_name, file, info) => {
                    const tmpFilePath = path.join(os.tmpdir(), `pdf-ingest-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`);
                    const writeStream = fs.createWriteStream(tmpFilePath);
                    let totalSize = 0;
                    file.on("data", (d) => {
                        totalSize += d.length;
                    });
                    file.on("error", (err) => {
                        writeStream.destroy(err);
                    });
                    const writePromise = new Promise((resolveWrite, rejectWrite) => {
                        writeStream.on("finish", () => {
                            tempFiles.push(tmpFilePath);
                            fileRec = { filename: info.filename, mimeType: info.mimeType || info.mime || "", size: totalSize, tmpPath: tmpFilePath };
                            push("upload:received", { filename: info.filename, size: totalSize, mime: info.mimeType || info.mime || "" });
                            resolveWrite();
                        });
                        writeStream.on("error", (err) => {
                            removeFileSafe(tmpFilePath).finally(() => rejectWrite(err));
                        });
                    });
                    pending.push(writePromise);
                    file.pipe(writeStream);
                });
                bb.on("field", (name, val) => {
                    if (name === "siteUrl") site = val;
                    if (name === "libraryPath") library = val;
                    if (name === "uploadthingUrl" || name === "fileUrl" || name === "remoteUrl") remoteUrl = val;
                    if (name === "uploadthingFilename" || name === "remoteFilename") remoteFilename = val;
                });
                bb.on("finish", () => {
                    Promise.all(pending).then(resolve).catch(reject);
                });
                bb.on("error", reject);
                req.pipe(bb);
            });
        }

        remoteUrl = String(remoteUrl || "").trim();
        remoteFilename = String(remoteFilename || "").trim();

        if (!fileRec) {
            if (remoteUrl) {
                phase = "remote_download";
                try {
                    const remoteFile = await downloadRemotePdf(remoteUrl, remoteFilename, push);
                    tempFiles.push(remoteFile.tmpPath);
                    fileRec = remoteFile;
                } catch (remoteErr) {
                    push("error:remote-download", {
                        url: remoteUrl,
                        message: remoteErr?.message || String(remoteErr),
                        status: remoteErr?.status,
                    });
                    throw remoteErr;
                }
            } else {
                push("error:no-file");
                return res.status(400).json({ error: "No file uploaded", traceId });
            }
        }
        const isPdf = /\.pdf$/i.test(fileRec.filename) || /pdf/i.test(fileRec.mimeType || "");
        if (!isPdf) {
            push("error:not-pdf", { filename: fileRec.filename, mime: fileRec.mimeType || "" });
            return res.status(400).json({ error: "File must be a PDF", traceId });
        }
        const result = await executeIngestWorkflow({
            fileRec,
            site,
            library,
            push,
            setPhase: (p) => {
                phase = p;
            },
        });

        await logSubmission({
            type: "pdf_ingest",
            status: "ok",
            traceId,
            folderName: result.folderName,
            created: result.created,
            filename: result.filename,
            size: result.fileSize,
            steps,
        });

        const payload = {
            ok: true,
            traceId,
            driveId: result.driveId,
            folderId: result.folderId,
            created: result.created,
            folderName: result.folderName,
            file: { name: result.filename, size: result.fileSize },
        };
        if (debugMode) payload.debug = { steps };
        return res.status(200).json(payload);
    } catch (e) {
        traceId = (res.getHeader("X-Request-Id") && String(res.getHeader("X-Request-Id"))) || traceId || crypto.randomBytes(8).toString("hex");
        if (!res.getHeader("X-Request-Id")) res.setHeader("X-Request-Id", traceId);
        push("error:caught", { phase, message: e?.message || String(e) });
        // eslint-disable-next-line no-console
        console.error(`[ingest-pdf:${traceId}] error at ${phase}:`, e?.message || e);
        if (e?.stack) {
            // eslint-disable-next-line no-console
            console.error(`[ingest-pdf:${traceId}] stack:`, e.stack);
        }
        // best-effort: include steps if present in scope
        try {
            await logSubmission({
                type: "pdf_ingest",
                status: "error",
                traceId,
                phase,
                error: e?.message || String(e),
                errorStack: e?.stack || "",
                ...(e && typeof e === "object" && "status" in e ? { errorStatus: e.status } : {}),
                ...(e && typeof e === "object" && "contentRange" in e ? { errorContentRange: e.contentRange } : {}),
                ...(typeof e?.responseBody === "string"
                    ? { errorResponse: e.responseBody.length > 2000 ? `${e.responseBody.slice(0, 2000)}...` : e.responseBody }
                    : {}),
                debug: debugMode,
                steps,
            });
        } catch {}
        const msg = e?.message || String(e);
        const accessDenied = /\b403\b|accessDenied|insufficientPermissions/i.test(msg);
        const payload = {
            ok: false,
            error: msg,
            traceId,
            phase,
            ...(accessDenied
                ? {
                      hint:
                          "App likely lacks write permission. Grant Microsoft Graph application permission (Sites.ReadWrite.All) with admin consent, or if using Sites.Selected, assign write role to this site for your app.",
                  }
                : {}),
            ...(e && typeof e === "object" && "status" in e ? { status: e.status } : {}),
            ...(e && typeof e === "object" && "contentRange" in e ? { contentRange: e.contentRange } : {}),
        };
        if (debugMode) {
            payload.stack = e?.stack || "";
            if (typeof e?.responseBody === "string") {
                payload.responseBody = e.responseBody.length > 2000 ? `${e.responseBody.slice(0, 2000)}...` : e.responseBody;
            }
            payload.steps = steps;
        }
        const code = e?.status ? Number(e.status) : accessDenied ? 403 : 500;
        const safePayload = {
            error: payload.error,
            traceId,
            phase,
            ...(payload.hint ? { hint: payload.hint } : {}),
            ...(payload.status ? { status: payload.status } : {}),
            ...(payload.contentRange ? { contentRange: payload.contentRange } : {}),
        };
        return res.status(code).json(debugMode ? payload : safePayload);
    } finally {
        await Promise.all(tempFiles.map(removeFileSafe));
    }
}
