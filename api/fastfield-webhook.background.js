import "isomorphic-fetch";
import crypto from "crypto";
import { moveFileFromStaging } from "./ingest-pdf.js";
import { logSubmission } from "../lib/kv.js";

export const config = { api: { bodyParser: false } };

const FASTFIELD_WEBHOOK_SECRET = process.env.FASTFIELD_WEBHOOK_SECRET || "";
const DEFAULT_SITE_URL_ENV = process.env.DEFAULT_SITE_URL || "";
const DEFAULT_LIBRARY_ENV = process.env.DEFAULT_LIBRARY || "";
const FASTFIELD_STAGING_SITE_URL = process.env.FASTFIELD_STAGING_SITE_URL || DEFAULT_SITE_URL_ENV;
const FASTFIELD_STAGING_LIBRARY_PATH = process.env.FASTFIELD_STAGING_LIBRARY_PATH || process.env.FASTFIELD_STAGING_FOLDER_PATH || "";

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
        if (nameEntry || urlEntry) {
            const filename = String(nameEntry?.value || "").trim();
            const url = String(urlEntry?.value || "").trim();
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
        entries.forEach(([, child]) => visit(child));
    }
    visit(payload);
    return results.filter((att) => {
        const name = att.filename || "";
        const url = att.url || "";
        const mime = att.mimeType || "";
        return /\.pdf(?:\?.*)?$/i.test(name) || /\.pdf(?:\?.*)?$/i.test(url) || /pdf/i.test(mime);
    });
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

function buildFilenameCandidates(rawName, url) {
    const candidates = [];
    const add = (name) => {
        const trimmed = String(name || "").trim();
        if (trimmed && !candidates.includes(trimmed)) candidates.push(trimmed);
    };
    const base = rawName || filenameFromUrl(url || "");
    if (base) add(base);
    if (base && !/\.pdf$/i.test(base)) add(`${base}.pdf`);
    if (base) {
        const sanitized = base.replace(/[\\/:*?"<>|]/g, " ").replace(/\s+/g, " ").trim();
        if (sanitized && sanitized !== base) {
            add(sanitized);
            if (!/\.pdf$/i.test(sanitized)) add(`${sanitized}.pdf`);
        }
    }
    const lower = base ? base.toLowerCase() : "";
    if (lower && !/\.pdf$/i.test(lower)) add(`${lower}.pdf`);
    return candidates;
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
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    try {
        if (!FASTFIELD_STAGING_LIBRARY_PATH) {
            throw new Error("FASTFIELD_STAGING_LIBRARY_PATH must be configured");
        }
        if (FASTFIELD_WEBHOOK_SECRET) {
            const provided = String(req.headers["x-webhook-secret"] || req.headers["x-fastfield-secret"] || "");
            if (provided !== FASTFIELD_WEBHOOK_SECRET) {
                return res.status(401).json({ error: "Invalid webhook secret" });
            }
        }

        const body = await readJsonBody(req);
        const payloadPreview = JSON.stringify(body).slice(0, 2000);
        const { site, library } = resolveSiteAndLibrary(body);
        let attachments = extractAttachments(body);
        if (!attachments.length) {
            const fallbackName =
                body.displayReferenceValue ||
                body.displayReference ||
                body.displayName ||
                body.formName ||
                body.title ||
                "";
            if (fallbackName) attachments = [{ filename: String(fallbackName).trim(), url: "" }];
        }
        if (!attachments.length) {
            return res.status(202).json({ ok: false, reason: "No PDF attachments found" });
        }

        const result = await processAttachments({ attachments, site, library, payloadPreview });
        return res.status(result.ok ? 202 : 207).json(result);
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

async function processAttachments({ attachments, site, library, payloadPreview }) {
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
        let filenameCandidates = [];
        try {
            filenameCandidates = buildFilenameCandidates(attachment.filename, attachment.url);
            if (!filenameCandidates.length) {
                push("attachment:missing-filename", { url: attachment.url || "" });
                throw new Error("Attachment did not include a filename");
            }

            push("attachment:start", { filename: filenameCandidates[0], url: attachment.url || "" });

            const moveResult = await moveFileFromStaging({
                stagingSiteUrl: FASTFIELD_STAGING_SITE_URL,
                stagingLibraryPath: FASTFIELD_STAGING_LIBRARY_PATH,
                stagingFilename: filenameCandidates[0],
                stagingFilenames: filenameCandidates,
                destinationSiteUrl: site,
                destinationLibraryPath: library,
                push,
                setPhase: (p) => {
                    phase = p;
                },
            });

            await logSubmission({
                type: "pdf_ingest",
                status: "ok",
                traceId,
                folderName: moveResult.folderName,
                created: moveResult.created,
                filename: moveResult.filename,
                size: moveResult.size,
                source: "fastfield_move",
                stagingFilename: filenameCandidates[0],
                payloadPreview,
                steps,
            });

            summary.push({ traceId, filename: moveResult.filename, folderName: moveResult.folderName });
        } catch (err) {
            push("error", { message: err?.message || String(err), phase });
            await logSubmission({
                type: "pdf_ingest",
                status: "error",
                traceId,
                phase,
                error: err?.message || String(err),
                errorStack: err?.stack || "",
                source: "fastfield_move",
                filename: filenameCandidates[0] || attachment.filename || filenameFromUrl(attachment.url || ""),
                payloadPreview,
                steps,
            });
            errors.push({ traceId, filename: filenameCandidates[0] || attachment.filename || "", message: err?.message || String(err) });
        }
    }

    return { ok: errors.length === 0, processed: summary, errors };
}
