import { runPollingSync, runSmartPollingSync } from "../../lib/planner-sync/index.js";
import { getSyncConfig } from "../../lib/planner-sync/config.js";
import { logger } from "../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    try {
        const { useSmartPolling } = getSyncConfig();
        const result = useSmartPolling ? await runSmartPollingSync() : await runPollingSync();
        res.status(200).json({ ok: true, result });
    } catch (error) {
        logger.error("Polling sync failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
