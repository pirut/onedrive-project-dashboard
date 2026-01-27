import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { deleteBcSubscription, getBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("POST /api/sync/bc-subscriptions/delete - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        let body: { entitySet?: string; entitySets?: string[]; subscriptionId?: string } | null = null;
        try {
            const bodyText = await request.text();
            if (bodyText) body = JSON.parse(bodyText);
        } catch (error) {
            logger.warn("Failed to parse request body", { requestId, error: error instanceof Error ? error.message : String(error) });
        }

        const entitySets = Array.isArray(body?.entitySets) && body?.entitySets.length
            ? body.entitySets
            : body?.entitySet
            ? [body.entitySet]
            : DEFAULT_ENTITY_SETS;

        const bcClient = new BusinessCentralClient();
        const deleted: string[] = [];
        const skipped: string[] = [];

        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const stored = await getBcSubscription(normalized);
            const subscriptionId = body?.subscriptionId || stored?.id;
            if (!subscriptionId) {
                skipped.push(normalized);
                continue;
            }

            try {
                await bcClient.deleteWebhookSubscription(subscriptionId);
                deleted.push(normalized);
            } catch (error) {
                logger.warn("Failed to delete BC subscription", {
                    requestId,
                    entitySet: normalized,
                    subscriptionId,
                    error: error instanceof Error ? error.message : String(error),
                });
            }

            await deleteBcSubscription(normalized);
        }

        const duration = Date.now() - startTime;
        return new Response(JSON.stringify({ ok: true, deleted, skipped, requestId, duration }), {
            status: 200,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/bc-subscriptions/delete - Failed", { requestId, duration, error: errorMessage });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }
}
