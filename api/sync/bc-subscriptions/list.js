import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { getBcSubscription } from "../../../lib/planner-sync/bc-webhook-store.js";
import { logger } from "../../../lib/planner-sync/logger.js";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

function parseEntitySets(raw) {
    if (!raw) return [];
    return String(raw)
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
}

export default async function handler(req, res) {
    const startTime = Date.now();
    const requestId = Math.random().toString(36).slice(2, 12);

    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const entitySets = parseEntitySets(req.query?.entitySets);
        const targets = entitySets.length ? entitySets : DEFAULT_ENTITY_SETS;
        const stored = [];
        for (const entitySet of targets) {
            stored.push({ entitySet, subscription: await getBcSubscription(entitySet) });
        }

        const bcClient = new BusinessCentralClient();
        let live = [];
        try {
            live = await bcClient.listWebhookSubscriptions();
        } catch (error) {
            logger.warn("BC subscription list failed", {
                requestId,
                error: error?.message || String(error),
            });
            live = { error: error?.message || String(error) };
        }

        const duration = Date.now() - startTime;
        res.status(200).json({ ok: true, stored, live, requestId, duration });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error?.message || String(error);
        res.status(500).json({ ok: false, error: errorMessage, requestId, duration });
    }
}
