import "../../lib/planner-sync/bootstrap.js";
import { previewBcChanges, syncBcToPremium } from "../../lib/premium-sync/index.js";
import { logger } from "../../lib/planner-sync/logger.js";

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

function parseBool(value) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

function isAuthorized(req) {
    const expected = (process.env.CRON_SECRET || "").trim();
    if (!expected) return true;

    const fromQuery = req.query?.cronSecret;
    const fromHeader =
        req.headers["x-cron-secret"] ||
        req.headers["x-vercel-cron-secret"] ||
        (req.headers["authorization"] || "").replace(/^Bearer\s+/i, "");

    const provided = String(fromQuery || fromHeader || "").trim();
    return provided === expected;
}

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (!isAuthorized(req)) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const body = req.method === "POST" ? await readJsonBody(req) : null;
    const requestId = body?.requestId ? String(body.requestId) : url.searchParams.get("requestId") || undefined;
    const dryRun = parseBool(body?.dryRun ?? url.searchParams.get("dryRun")) === true;

    try {
        if (dryRun) {
            const preview = await previewBcChanges({ requestId });
            res.status(200).json({ ok: true, dryRun: true, preview });
            return;
        }

        const result = await syncBcToPremium(undefined, {
            requestId,
            preferPlanner: false,
        });

        res.status(200).json({ ok: true, dryRun: false, result });
    } catch (error) {
        const errorMessage = error?.message || String(error);
        logger.error("BC queue cron sync failed", { error: errorMessage, requestId });
        res.status(500).json({ ok: false, error: errorMessage, requestId });
    }
}
