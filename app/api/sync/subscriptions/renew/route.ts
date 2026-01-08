import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { listStoredSubscriptions, saveStoredSubscriptions } from "../../../../../lib/planner-sync/subscriptions-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST() {
    const stored = await listStoredSubscriptions();
    if (!stored.length) {
        return new Response(JSON.stringify({ ok: true, renewed: 0 }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    }

    const graphClient = new GraphClient();
    const expirationDateTime = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();
    const renewed: { id: string; expirationDateTime?: string }[] = [];
    const remaining = [] as typeof stored;

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
            logger.warn("Subscription renewal failed", { subscriptionId: sub.id, error: (error as Error)?.message });
        }
    }

    await saveStoredSubscriptions(remaining);

    return new Response(JSON.stringify({ ok: true, renewed, total: stored.length }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}
