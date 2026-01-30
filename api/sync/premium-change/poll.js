import "../../../lib/planner-sync/bootstrap.js";
import { runPremiumChangePoll } from "../../../lib/premium-sync/index.js";
import { logger } from "../../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    try {
        const result = await runPremiumChangePoll();
        res.status(200).json({ ok: true, result });
    } catch (error) {
        logger.error("Premium change poll failed", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
