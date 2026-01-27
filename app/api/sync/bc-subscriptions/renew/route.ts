import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription, saveBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { getCronSecret, isCronAuthorized } from "../../../../../lib/planner-sync/cron-auth";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["projectTasks"];
const SUBSCRIPTION_TTL_HOURS = 48;
const RENEWAL_BUFFER_HOURS = 6;

function resolveBaseUrl(request: Request) {
    const url = new URL(request.url);
    const proto = request.headers.get("x-forwarded-proto") || url.protocol.replace(":", "");
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || url.host;
    return `${proto}://${host}`;
}

function buildExpirationDate() {
    return new Date(Date.now() + SUBSCRIPTION_TTL_HOURS * 60 * 60 * 1000).toISOString();
}

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    const cronSecret = getCronSecret();
    const provided = request.headers.get("x-cron-secret") || url.searchParams.get("cronSecret") || url.searchParams.get("cron_secret");

    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("BC subscription renew unauthorized", { requestId });
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized", requestId }), {
            status: 401,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }

    try {
        let body: { entitySets?: string[] } | null = null;
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
        const expirationDateTime = buildExpirationDate();
        const bcClient = new BusinessCentralClient();
        const renewed: string[] = [];
        const created: string[] = [];
        const skipped: string[] = [];
        const failed: Array<{ entitySet: string; error: string }> = [];

        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const stored = await getBcSubscription(normalized);
            if (stored?.expirationDateTime) {
                const expMs = Date.parse(stored.expirationDateTime);
                const bufferMs = RENEWAL_BUFFER_HOURS * 60 * 60 * 1000;
                if (Number.isFinite(expMs) && expMs > Date.now() + bufferMs) {
                    skipped.push(normalized);
                    continue;
                }
            }

            try {
                if (stored?.id) {
                    const updated = await bcClient.renewWebhookSubscription(stored.id, expirationDateTime);
                    await saveBcSubscription(normalized, {
                        ...stored,
                        id: updated?.id || stored.id,
                        expirationDateTime: updated?.expirationDateTime || expirationDateTime,
                    });
                    renewed.push(normalized);
                    continue;
                }
            } catch (error) {
                logger.warn("BC subscription renewal failed", {
                    requestId,
                    entitySet: normalized,
                    error: error instanceof Error ? error.message : String(error),
                });
            }

            try {
                const subscription = await bcClient.createWebhookSubscription({
                    entitySet: normalized,
                    notificationUrl: stored?.notificationUrl || process.env.BC_WEBHOOK_NOTIFICATION_URL || `${resolveBaseUrl(request)}/api/webhooks/bc`,
                    clientState: stored?.clientState || (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined,
                    expirationDateTime,
                });
                await saveBcSubscription(normalized, {
                    id: subscription?.id || "",
                    entitySet: normalized,
                    resource: subscription?.resource,
                    expirationDateTime: subscription?.expirationDateTime || expirationDateTime,
                    createdAt: new Date().toISOString(),
                    notificationUrl: stored?.notificationUrl || process.env.BC_WEBHOOK_NOTIFICATION_URL || `${resolveBaseUrl(request)}/api/webhooks/bc`,
                    clientState: stored?.clientState || (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined,
                });
                created.push(normalized);
            } catch (error) {
                failed.push({ entitySet: normalized, error: error instanceof Error ? error.message : String(error) });
            }
        }

        const duration = Date.now() - startTime;
        return new Response(
            JSON.stringify({ ok: true, renewed, created, skipped, failed, requestId, duration }),
            {
                status: 200,
                headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
            }
        );
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/bc-subscriptions/renew - Failed", { requestId, duration, error: errorMessage });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }
}
