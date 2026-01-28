import "../../../lib/planner-sync/bootstrap.js";
import { runPremiumChangePoll } from "../../../lib/premium-sync/index.js";
import { getCronSecret, isCronAuthorized } from "../../../lib/planner-sync/cron-auth.js";
import { logger } from "../../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const provided = req.headers["x-cron-secret"] || req.query?.cronSecret || req.query?.cronsecret || "";
    const expected = getCronSecret();
    if (expected && !isCronAuthorized(String(provided))) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
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
