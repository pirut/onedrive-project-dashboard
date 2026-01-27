import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription, saveBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { getBcConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

function normalizeValue(value: string | undefined | null) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function buildExpectedResource(entitySet: string) {
    const { publisher, group, version, companyId } = getBcConfig();
    return `api/${publisher}/${group}/${version}/companies(${companyId})/${entitySet}`.replace(/^\/+/, "");
}

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

        const created: Array<{ entitySet: string; id?: string; resource?: string; expirationDateTime?: string; existing?: boolean }> = [];
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

            try {
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
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                if (!/subscription already exist/i.test(errorMessage)) {
                    throw error;
                }

                const expectedResource = normalizeValue(buildExpectedResource(normalized));
                const expectedNotification = normalizeValue(notificationUrl);
                const existing = (await bcClient.listWebhookSubscriptions()).find((item) => {
                    const resource = normalizeValue(item?.resource as string | undefined);
                    const notify = normalizeValue(item?.notificationUrl as string | undefined);
                    return resource === expectedResource && notify === expectedNotification;
                });

                if (!existing?.id) {
                    throw error;
                }

                await saveBcSubscription(normalized, {
                    id: existing.id,
                    entitySet: normalized,
                    resource: existing.resource,
                    expirationDateTime: existing.expirationDateTime,
                    createdAt: new Date().toISOString(),
                    notificationUrl,
                    clientState,
                });

                created.push({
                    entitySet: normalized,
                    id: existing.id,
                    resource: existing.resource,
                    expirationDateTime: existing.expirationDateTime,
                    existing: true,
                });
            }
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
