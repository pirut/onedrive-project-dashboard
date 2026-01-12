import { runPollingSync } from "../../lib/planner-sync/index.js";
import { logger } from "../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    try {
        const base = `http://${req.headers.host || "localhost"}`;
        const url = new URL(req.url || "", base);
        const force = url.searchParams.get("force") === "1" || url.searchParams.get("force") === "true";
        const result = await runPollingSync({ force });
        res.status(200).json({ ok: true, result });
    } catch (error) {
        logger.error("Polling sync failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
