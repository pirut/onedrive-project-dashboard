import { runPollingSync } from "../../../lib/planner-sync/index.js";
import { logger } from "../../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const result = await runPollingSync();
        res.status(200).json({ ok: true, result });
    } catch (error) {
        logger.error("Polling cron failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
