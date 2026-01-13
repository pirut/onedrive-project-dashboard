import { runPollingSync, runSmartPollingSync, syncBcToPlanner } from "../../lib/planner-sync/index.js";
import { getSyncConfig } from "../../lib/planner-sync/config.js";
import { logger } from "../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const { useSmartPolling } = getSyncConfig();
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
