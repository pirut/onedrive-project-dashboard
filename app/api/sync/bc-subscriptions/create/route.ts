import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription, saveBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

function resolveBaseUrl(request: Request) {
    const url = new URL(request.url);
    const proto = request.headers.get("x-forwarded-proto") || url.protocol.replace(":", "");
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || url.host;
    return `${proto}://${host}`;
}

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("POST /api/sync/bc-subscriptions/create - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        let body: { notificationUrl?: string; entitySets?: string[] } | null = null;
        try {
            const bodyText = await request.text();
            if (bodyText) body = JSON.parse(bodyText);
        } catch (error) {
            logger.warn("Failed to parse request body", {
                requestId,
                error: error instanceof Error ? error.message : String(error),
            });
        }

        const entitySets = Array.isArray(body?.entitySets) && body?.entitySets.length ? body.entitySets : DEFAULT_ENTITY_SETS;
        const envNotificationUrl = process.env.BC_WEBHOOK_NOTIFICATION_URL;
        const notificationUrl = body?.notificationUrl || envNotificationUrl || `${resolveBaseUrl(request)}/api/webhooks/bc`;

        if (!notificationUrl.startsWith("https://")) {
            logger.warn("BC webhook notificationUrl is not HTTPS", { requestId, notificationUrl });
        }

        const clientState = (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined;
        const bcClient = new BusinessCentralClient();

        const created: Array<{ entitySet: string; id?: string; resource?: string; expirationDateTime?: string }> = [];
        const skipped: string[] = [];

        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const stored = await getBcSubscription(normalized);
            if (stored?.expirationDateTime) {
                const expMs = Date.parse(stored.expirationDateTime);
                if (Number.isFinite(expMs) && expMs > Date.now() + 60 * 1000) {
                    skipped.push(normalized);
                    continue;
                }
            }

            const subscription = await bcClient.createWebhookSubscription({
                entitySet: normalized,
                notificationUrl,
                clientState,
            });

            created.push({
                entitySet: normalized,
                id: subscription?.id,
                resource: subscription?.resource,
                expirationDateTime: subscription?.expirationDateTime,
            });

            await saveBcSubscription(normalized, {
                id: subscription?.id || "",
                entitySet: normalized,
                resource: subscription?.resource,
                expirationDateTime: subscription?.expirationDateTime,
                createdAt: new Date().toISOString(),
                notificationUrl,
                clientState,
            });
        }

        const duration = Date.now() - startTime;
        logger.info("POST /api/sync/bc-subscriptions/create - Success", {
            requestId,
            duration,
            createdCount: created.length,
            skippedCount: skipped.length,
        });

        return new Response(
            JSON.stringify({ ok: true, created, skipped, notificationUrl, requestId, duration }),
            {
                status: 200,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            }
        );
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/bc-subscriptions/create - Failed", { requestId, duration, error: errorMessage });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}
