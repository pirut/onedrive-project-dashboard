import { syncPlannerPlanTitlesAndDedupe } from "../../lib/planner-sync/index.js";
import { getCronSecret, isCronAuthorized } from "../../lib/planner-sync/cron-auth.js";
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
    if (value == null) return false;
    const normalized = String(value).trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(normalized);
}

export default async function handler(req, res) {
    const requestId = Math.random().toString(36).slice(2, 12);

    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const cronSecret = getCronSecret();
    const provided = req.headers["x-cron-secret"] || req.query?.cronSecret || req.query?.cron_secret;
    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("Plan maintenance cron unauthorized", { requestId });
        res.status(401).json({ ok: false, error: "Unauthorized", requestId });
        return;
    }

    try {
        const body = req.method === "POST" ? await readJsonBody(req) : null;
        const projectNo = body?.projectNo || req.query?.projectNo || req.query?.project_no || "";
        const dryRun = parseBool(body?.dryRun) || parseBool(req.query?.dryRun) || parseBool(req.query?.dry_run);

        const result = await syncPlannerPlanTitlesAndDedupe({
            projectNo: projectNo ? String(projectNo).trim() : undefined,
            dryRun,
        });
        res.status(200).json({ ok: true, result, requestId });
    } catch (error) {
        const errorMessage = error?.message || String(error);
        logger.error("Plan maintenance cron failed", { requestId, error: errorMessage });
        res.status(500).json({ ok: false, error: errorMessage, requestId });
    }
}
