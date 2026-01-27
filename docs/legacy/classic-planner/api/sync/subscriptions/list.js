import { GraphClient } from "../../../lib/planner-sync/graph-client.js";
import { logger } from "../../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const graphClient = new GraphClient();
        const all = await graphClient.listSubscriptions();
        const planner = all.filter((sub) => (sub.resource || "").includes("/planner/"));
        res.status(200).json({
            ok: true,
            total: all.length,
            plannerCount: planner.length,
            items: planner,
        });
    } catch (error) {
        logger.error("GET /api/sync/subscriptions/list - Failed", {
            error: error?.message || String(error),
        });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
