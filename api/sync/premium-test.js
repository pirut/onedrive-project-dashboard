import { DataverseClient } from "../../lib/dataverse-client.js";
import { getDataverseMappingConfig } from "../../lib/premium-sync/config.js";
import { logger } from "../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const dataverse = new DataverseClient();
        const mapping = getDataverseMappingConfig();
        const who = await dataverse.whoAmI();
        res.status(200).json({ ok: true, whoAmI: who, mapping });
    } catch (error) {
        logger.error("Premium test failed", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
