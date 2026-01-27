import { runPollingSync, runSmartPollingSync, syncBcToPlanner } from "../../lib/planner-sync/index.js";
import { getSyncConfig } from "../../lib/planner-sync/config.js";
import { getCronSecret, isCronAuthorized } from "../../lib/planner-sync/cron-auth.js";
import { logger } from "../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    const requestId = Math.random().toString(36).slice(2, 12);
    const cronSecret = getCronSecret();
    const provided = req.headers["x-cron-secret"] || req.query?.cronSecret || req.query?.cron_secret;
    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("Sync cron unauthorized", { requestId });
        res.status(401).json({ ok: false, error: "Unauthorized", requestId });
        return;
    }

    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const { useSmartPolling, enablePollingFallback } = getSyncConfig();
        if (!enablePollingFallback) {
            res.status(200).json({ ok: true, skipped: true, reason: "Polling fallback disabled", requestId });
            return;
        }
        if (useSmartPolling) {
            const smartResult = await runSmartPollingSync();
            res.status(200).json({ ok: true, result: { smartPolling: smartResult } });
            return;
        }
        const bcResult = await syncBcToPlanner();
        const pollResult = await runPollingSync();
        res.status(200).json({ ok: true, result: { bcToPlanner: bcResult, plannerToBc: pollResult } });
    } catch (error) {
        logger.error("Sync cron failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
