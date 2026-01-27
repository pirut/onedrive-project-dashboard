import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

function parseEntitySets(raw: string | null) {
    if (!raw) return [];
    return raw
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
}

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    try {
        const entitySets = parseEntitySets(url.searchParams.get("entitySets"));
        const targets = entitySets.length ? entitySets : DEFAULT_ENTITY_SETS;
        const stored = [] as Array<{ entitySet: string; subscription: unknown | null }>;
        for (const entitySet of targets) {
            stored.push({ entitySet, subscription: await getBcSubscription(entitySet) });
        }

        const bcClient = new BusinessCentralClient();
        let live: unknown = [];
        try {
            live = await bcClient.listWebhookSubscriptions();
        } catch (error) {
            logger.warn("BC subscription list failed", {
                requestId,
                error: error instanceof Error ? error.message : String(error),
            });
            live = { error: error instanceof Error ? error.message : String(error) };
        }

        const duration = Date.now() - startTime;
        return new Response(JSON.stringify({ ok: true, stored, live, requestId, duration }), {
            status: 200,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }
}
