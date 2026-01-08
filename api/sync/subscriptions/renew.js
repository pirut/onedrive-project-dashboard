import { GraphClient } from "../../../lib/planner-sync/graph-client.js";
import { listStoredSubscriptions, saveStoredSubscriptions } from "../../../lib/planner-sync/subscriptions-store.js";
import { logger } from "../../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const stored = await listStoredSubscriptions();
    if (!stored.length) {
        res.status(200).json({ ok: true, renewed: 0 });
        return;
    }

    const graphClient = new GraphClient();
    const expirationDateTime = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();
    const renewed = [];
    const remaining = [];

    for (const sub of stored) {
        try {
            const updated = await graphClient.renewSubscription(sub.id, expirationDateTime);
            if (updated?.expirationDateTime) {
                renewed.push({ id: sub.id, expirationDateTime: updated.expirationDateTime });
                remaining.push({
                    ...sub,
                    expirationDateTime: updated.expirationDateTime,
                });
            } else {
                remaining.push({
                    ...sub,
                    expirationDateTime,
                });
            }
        } catch (error) {
            logger.warn("Subscription renewal failed", { subscriptionId: sub.id, error: error?.message || String(error) });
        }
    }

    await saveStoredSubscriptions(remaining);
    res.status(200).json({ ok: true, renewed, total: stored.length });
}
