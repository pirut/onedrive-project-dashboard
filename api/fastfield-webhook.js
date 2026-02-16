import { processStagingJobWalks } from "../lib/process-staging.js";
import { logSubmission } from "../lib/kv.js";

export const config = { api: { bodyParser: false } };

const FASTFIELD_WEBHOOK_SECRET = process.env.FASTFIELD_WEBHOOK_SECRET || "";

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

export default async function handler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Webhook-Secret");
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    try {
        if (!process.env.FASTFIELD_STAGING_LIBRARY_PATH) {
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

        const result = await processStagingJobWalks({
            onResult: async (entry) => {
                const base = {
                    type: "pdf_ingest",
                    traceId: entry.traceId,
                    steps: entry.steps || [],
                    payloadPreview,
                };
                if (entry.status === "ok") {
                    await logSubmission({
                        ...base,
                        status: "ok",
                        source: "fastfield_move",
                        filename: entry.filename,
                        folderName: entry.folderName,
                    });
                } else if (entry.status === "skipped") {
                    await logSubmission({
                        ...base,
                        status: "skipped",
                        source: "fastfield_move",
                        filename: entry.filename,
                        reason: entry.reason || "",
                        phase: entry.phase || "",
                    });
                } else {
                    await logSubmission({
                        ...base,
                        status: "error",
                        source: "fastfield_move",
                        filename: entry.filename,
                        error: entry.error || "",
                        phase: entry.phase || "",
                    });
                }
            },
        });

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
