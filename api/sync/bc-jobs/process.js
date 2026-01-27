import { acquireBcJobLock, releaseBcJobLock } from "../../../lib/planner-sync/bc-webhook-store.js";
import { processBcJobQueue } from "../../../lib/planner-sync/bc-job-processor.js";
import { getCronSecret, isCronAuthorized } from "../../../lib/planner-sync/cron-auth.js";
import { logger } from "../../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    const startTime = Date.now();
    const requestId = Math.random().toString(36).slice(2, 12);

    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const cronSecret = getCronSecret();
    const provided = req.headers["x-cron-secret"] || req.query?.cronSecret || req.query?.cron_secret;
    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("BC jobs process unauthorized", { requestId });
        res.status(401).json({ ok: false, error: "Unauthorized", requestId });
        return;
    }

    let maxJobs = Number(req.query?.maxJobs || 25);
    if (Number.isNaN(maxJobs) || maxJobs <= 0) maxJobs = 25;

    if (req.body?.maxJobs && typeof req.body.maxJobs === "number") {
        maxJobs = Math.floor(req.body.maxJobs);
    }

    const lock = await acquireBcJobLock();
    if (!lock) {
        res.status(409).json({ ok: false, error: "Queue locked", requestId });
        return;
    }

    try {
        const result = await processBcJobQueue({ maxJobs, requestId });
        const duration = Date.now() - startTime;
        res.status(200).json({ ok: true, result, requestId, duration });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error?.message || String(error);
        logger.error("POST /api/sync/bc-jobs/process - Failed", { requestId, duration, error: errorMessage });
        res.status(500).json({ ok: false, error: errorMessage, requestId, duration });
    } finally {
        await releaseBcJobLock(lock);
    }
}
