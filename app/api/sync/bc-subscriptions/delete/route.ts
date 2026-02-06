import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { deleteBcSubscription, getBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { getBcConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["premiumSyncQueue"];

function normalizeValue(value: string | undefined | null) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function matchesResource(resource: string | undefined | null, entitySet: string) {
    const normalized = normalizeValue(resource);
    if (!normalized) return false;
    const { publisher, group, version, companyId } = getBcConfig();
    const expected = normalizeValue(`api/${publisher}/${group}/${version}/companies(${companyId})/${entitySet}`);
    if (normalized === expected) return true;
    const entityLower = normalizeValue(entitySet);
    const companyToken = normalizeValue(`companies(${companyId})/${entityLower}`);
    if (companyToken && normalized.includes(companyToken)) return true;
    if (entityLower && normalized.endsWith(`/${entityLower}`)) return true;
    return false;
}

function resolveNotificationUrl(request: Request, body?: { notificationUrl?: string } | null) {
    const configured = (process.env.BC_WEBHOOK_NOTIFICATION_URL || "").trim();
    if (body?.notificationUrl) return String(body.notificationUrl).trim();
    if (configured) return configured;
    const proto = request.headers.get("x-forwarded-proto") || "https";
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || "localhost";
    return `${proto}://${host}/api/webhooks/bc`;
}

function extractODataId(item: { [key: string]: unknown }) {
    const raw = item?.["@odata.id"] || item?.["@odata.editLink"] || item?.odataId;
    if (!raw) return null;
    const match = String(raw).match(/subscriptions\(([^)]+)\)/i);
    return match ? match[1] : null;
}

function pickSubscriptionId(item: {
    id?: string;
    Id?: string;
    ID?: string;
    subscriptionId?: string;
    subscriptionid?: string;
    subscriptionID?: string;
    systemId?: string;
}) {
    return (
        item?.id ||
        item?.Id ||
        item?.ID ||
        item?.subscriptionId ||
        item?.subscriptionid ||
        item?.subscriptionID ||
        item?.systemId ||
        extractODataId(item as { [key: string]: unknown }) ||
        null
    );
}

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
        const notificationUrl = resolveNotificationUrl(request, body);
        const normalizedNotificationUrl = normalizeValue(notificationUrl);

        const bcClient = new BusinessCentralClient();
        const deleted: string[] = [];
        const skipped: string[] = [];
        let listCount: number | null = null;

        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const stored = await getBcSubscription(normalized);
            let subscriptionId = body?.subscriptionId || stored?.id;
            if (!subscriptionId) {
                try {
                    const list = await bcClient.listWebhookSubscriptions();
                    listCount = list.length;
                    const match = list.find((item) => {
                        if (!matchesResource(item?.resource, normalized)) return false;
                        if (!normalizedNotificationUrl) return true;
                        return normalizeValue(item?.notificationUrl) === normalizedNotificationUrl;
                    });
                    const matchedId = pickSubscriptionId(match as { id?: string });
                    if (matchedId) subscriptionId = matchedId;
                } catch (error) {
                    logger.warn("Failed to list BC subscriptions for delete", {
                        requestId,
                        entitySet: normalized,
                        error: error instanceof Error ? error.message : String(error),
                    });
                }
            }
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
        return new Response(
            JSON.stringify({
                ok: true,
                deleted,
                skipped,
                notificationUrl,
                listCount,
                requestId,
                duration,
            }),
            {
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
