import "isomorphic-fetch";
import crypto from "crypto";
import fs from "fs";
import os from "os";
import path from "path";
import { Readable } from "stream";
import { File } from "undici";
import { UTApi } from "uploadthing/server";
import { executeIngestWorkflow, removeFileSafe } from "./ingest-pdf.js";
import { logSubmission } from "../lib/kv.js";

export const config = { api: { bodyParser: false } };

const FASTFIELD_API_KEY = process.env.FASTFIELD_API_KEY || "";
const FASTFIELD_WEBHOOK_SECRET = process.env.FASTFIELD_WEBHOOK_SECRET || "";
const DEFAULT_SITE_URL_ENV = process.env.DEFAULT_SITE_URL || "";
const DEFAULT_LIBRARY_ENV = process.env.DEFAULT_LIBRARY || "";

let utApi = null;
function getUploadThingApi() {
    const token = process.env.UPLOADTHING_TOKEN || "";
    if (!token) throw new Error("UPLOADTHING_TOKEN is not configured");
    if (!utApi) utApi = new UTApi({ apiKey: token });
    return utApi;
}

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const raw = Buffer.concat(chunks).toString("utf8");
    if (!raw.trim()) return {};
    try {
        return JSON.parse(raw);
    } catch (err) {
        throw new Error(`Invalid JSON payload: ${err?.message || err}`);
    }
}

const NAME_KEYS = ["filename", "file_name", "name", "title", "originalfilename", "original_filename"];
const URL_KEYS = ["url", "fileurl", "file_url", "downloadurl", "download_url", "link", "downloaduri", "download_uri"];
const SIZE_KEYS = ["size", "filesize", "file_size", "length", "contentlength", "content_length"];
const MIME_KEYS = ["contenttype", "content_type", "mimetype", "mime_type"];

function extractAttachments(payload) {
    const results = [];
    const seen = new Set();
    function visit(val) {
        if (!val || typeof val !== "object") return;
        if (Array.isArray(val)) {
            val.forEach(visit);
            return;
        }
        const entries = Object.entries(val);
        const lowerMap = new Map(entries.map(([k, v]) => [k.toLowerCase(), { key: k, value: v }]));
        const nameEntry = NAME_KEYS.map((k) => lowerMap.get(k)).find(Boolean);
        const urlEntry = URL_KEYS.map((k) => lowerMap.get(k)).find(Boolean);
        if (nameEntry && urlEntry && typeof urlEntry.value === "string") {
            const filename = String(nameEntry.value || "").trim();
            const url = String(urlEntry.value || "").trim();
            if (filename && url) {
                const key = `${filename}::${url}`;
                if (!seen.has(key)) {
                    const sizeEntry = SIZE_KEYS.map((k) => lowerMap.get(k)).find(Boolean);
                    const mimeEntry = MIME_KEYS.map((k) => lowerMap.get(k)).find(Boolean);
                    results.push({
                        filename,
                        url,
                        size: sizeEntry ? Number(sizeEntry.value) : undefined,
                        mimeType: mimeEntry ? String(mimeEntry.value) : undefined,
                    });
                    seen.add(key);
                }
            }
        }
        entries.forEach(([, child]) => visit(child));
    }
    visit(payload);
    return results.filter((att) => /\.pdf$/i.test(att.filename) || /pdf/i.test(att.mimeType || "") || /\.pdf(?:\?.*)?$/i.test(att.url));
}

function buildDownloadHeaders() {
    const headers = { Accept: "application/pdf" };
    if (FASTFIELD_API_KEY) headers["FastField-API-Key"] = FASTFIELD_API_KEY;
    const custom = process.env.FASTFIELD_DOWNLOAD_AUTH_HEADER || "";
    if (custom) {
        const idx = custom.indexOf(":");
        if (idx > 0) {
            const headerName = custom.slice(0, idx).trim();
            const headerValue = custom.slice(idx + 1).trim();
            if (headerName && headerValue) headers[headerName] = headerValue;
        }
    }
    return headers;
}

async function downloadAttachment({ url, filename, mimeType }, push) {
    let parsedUrl;
    try {
        parsedUrl = new URL(url);
    } catch (err) {
        const e = new Error(`Invalid attachment URL: ${url}`);
        e.cause = err;
        throw e;
    }
    if (!(parsedUrl.protocol === "https:" || parsedUrl.protocol === "http:")) {
        throw new Error("Attachment URL must use http or https");
    }
    const safeName = filename || filenameFromUrl(url) || "fastfield.pdf";
    const tmpFilePath = path.join(os.tmpdir(), `fastfield-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`);
    push("fastfield:download:start", { url, filename: safeName });
    const headers = buildDownloadHeaders();
    const res = await fetch(url, { headers });
    if (!res.ok || !res.body) {
        const bodyText = await res.text().catch(() => "");
        const err = new Error(`FastField download failed: ${res.status} ${res.statusText}`);
        err.status = res.status;
        err.responseBody = bodyText;
        throw err;
    }
    const stream = typeof res.body.getReader === "function" ? Readable.fromWeb(res.body) : res.body;
    if (!stream || typeof stream.pipe !== "function") throw new Error("Attachment response stream is not readable");
    let total = 0;
    await new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(tmpFilePath);
        stream.on("data", (chunk) => {
            total += chunk.length;
        });
        stream.on("error", (err) => {
            writeStream.destroy(err);
            removeFileSafe(tmpFilePath).finally(() => reject(err));
        });
        writeStream.on("error", (err) => {
            removeFileSafe(tmpFilePath).finally(() => reject(err));
        });
        writeStream.on("finish", resolve);
        stream.pipe(writeStream);
    });
    if (!total) {
        try {
            const stat = await fs.promises.stat(tmpFilePath);
            total = stat.size;
        } catch {}
    }
    push("fastfield:download:done", { url, filename: safeName, size: total });
    return {
        filename: safeName,
        mimeType: mimeType || "application/pdf",
        size: total,
        tmpPath: tmpFilePath,
    };
}

async function uploadToUploadThing(fileRec, push) {
    const api = getUploadThingApi();
    push("uploadthing:upload:start", { filename: fileRec.filename, size: fileRec.size });
    const buffer = await fs.promises.readFile(fileRec.tmpPath);
    const file = new File([buffer], fileRec.filename, { type: "application/pdf" });
    const result = await api.uploadFiles(file, { metadata: { source: "fastfield" } });
    const uploaded = Array.isArray(result) ? result[0] : result;
    const url = uploaded?.ufsUrl || uploaded?.url || "";
    const key = uploaded?.key || uploaded?.id || "";
    if (!url) {
        const err = new Error("UploadThing did not return a file URL");
        err.responseBody = uploaded || null;
        throw err;
    }
    push("uploadthing:upload:done", { filename: fileRec.filename, key, url });
    return { url, key, raw: uploaded };
}

function filenameFromUrl(url) {
    try {
        const parsed = new URL(url);
        const parts = parsed.pathname.split("/").filter(Boolean);
        return parts.length ? parts[parts.length - 1] : "";
    } catch {
        return "";
    }
}

function resolveSiteAndLibrary(body = {}) {
    const site = body.siteUrl || body.SiteUrl || DEFAULT_SITE_URL_ENV;
    const library = body.libraryPath || body.LibraryPath || DEFAULT_LIBRARY_ENV;
    if (!site || !library) throw new Error("DEFAULT_SITE_URL and DEFAULT_LIBRARY must be configured");
    return { site, library };
}

export default async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Webhook-Secret");
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    if (req.method === "OPTIONS") {
        return res.status(204).end();
    }
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    try {
        if (FASTFIELD_WEBHOOK_SECRET) {
            const provided = String(req.headers["x-webhook-secret"] || req.headers["x-fastfield-secret"] || "");
            if (provided !== FASTFIELD_WEBHOOK_SECRET) {
                return res.status(401).json({ error: "Invalid webhook secret" });
            }
        }

        const body = await readJsonBody(req);
        const { site, library } = resolveSiteAndLibrary(body);
        const attachments = extractAttachments(body);
        if (!attachments.length) {
            return res.status(202).json({ ok: false, reason: "No PDF attachments found" });
        }

        const summary = [];
        const errors = [];

        for (const attachment of attachments) {
            const traceId = crypto.randomBytes(8).toString("hex");
            const steps = [];
            const push = (msg, meta = {}) => {
                const entry = { ts: new Date().toISOString(), msg, ...meta };
                steps.push(entry);
                // eslint-disable-next-line no-console
                console.log(`[fastfield-webhook:${traceId}] ${msg}`, Object.keys(meta).length ? meta : "");
            };
            let phase = "start";
            const tempFiles = [];
            try {
                push("attachment:start", { filename: attachment.filename, url: attachment.url });
                phase = "fastfield_download";
                const fileRec = await downloadAttachment(attachment, push);
                tempFiles.push(fileRec.tmpPath);

                phase = "uploadthing_upload";
                const uploadThingResult = await uploadToUploadThing(fileRec, push);

                phase = "ingest_prepare";
                const ingestResult = await executeIngestWorkflow({
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
                    folderName: ingestResult.folderName,
                    created: ingestResult.created,
                    filename: ingestResult.filename,
                    size: ingestResult.fileSize,
                    uploadthingUrl: uploadThingResult.url,
                    uploadthingKey: uploadThingResult.key,
                    source: "fastfield",
                    steps,
                });

                summary.push({
                    traceId,
                    filename: ingestResult.filename,
                    folderName: ingestResult.folderName,
                    uploadthingUrl: uploadThingResult.url,
                });
            } catch (err) {
                push("error", { filename: attachment.filename, message: err?.message || String(err) });
                await logSubmission({
                    type: "pdf_ingest",
                    status: "error",
                    traceId,
                    phase,
                    error: err?.message || String(err),
                    errorStack: err?.stack || "",
                    source: "fastfield",
                    filename: attachment.filename,
                    steps,
                });
                errors.push({ filename: attachment.filename, message: err?.message || String(err), traceId, phase });
            } finally {
                await Promise.all(tempFiles.map((file) => removeFileSafe(file)));
            }
        }

        const status = errors.length ? 207 : 200;
        return res.status(status).json({ ok: errors.length === 0, processed: summary, errors });
    } catch (err) {
        const msg = err?.message || String(err);
        // eslint-disable-next-line no-console
        console.error("[fastfield-webhook] fatal error:", msg);
        if (err?.stack) {
            // eslint-disable-next-line no-console
            console.error(err.stack);
        }
        return res.status(err?.status || 500).json({ ok: false, error: msg });
    }
}
