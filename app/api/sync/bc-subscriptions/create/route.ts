import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription, saveBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { getBcConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["premiumSyncQueue"];
const EXPIRY_BUFFER_MS = 60 * 1000;

function normalizeValue(value: string | undefined | null) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function buildExpectedResource(entitySet: string) {
    const { publisher, group, version, companyId } = getBcConfig();
    return `api/${publisher}/${group}/${version}/companies(${companyId})/${entitySet}`.replace(/^\/+/, "");
}

function matchesResource(resource: string | undefined | null, entitySet: string) {
    const normalized = normalizeValue(resource);
    if (!normalized) return false;
    const expected = normalizeValue(buildExpectedResource(entitySet));
    if (normalized === expected) return true;
    const { companyId } = getBcConfig();
    const entityLower = normalizeValue(entitySet);
    const companyToken = normalizeValue(`companies(${companyId})/${entityLower}`);
    if (companyToken && normalized.includes(companyToken)) return true;
    if (entityLower && normalized.endsWith(`/${entityLower}`)) return true;
    return false;
}

function resolveBaseUrl(request: Request) {
    const url = new URL(request.url);
    const proto = request.headers.get("x-forwarded-proto") || url.protocol.replace(":", "");
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || url.host;
    return `${proto}://${host}`;
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

function isExpiringSoon(expirationDateTime: string | undefined | null, bufferMs = EXPIRY_BUFFER_MS) {
    if (!expirationDateTime) return true;
    const expMs = Date.parse(String(expirationDateTime));
    if (!Number.isFinite(expMs)) return true;
    return expMs <= Date.now() + bufferMs;
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

                let resolved = subscription;
                let resolvedId = pickSubscriptionId(resolved as { id?: string });
                if (!resolvedId) {
                    const expectedNotification = normalizeValue(notificationUrl);
                    try {
                        const list = await bcClient.listWebhookSubscriptions();
                        const match = list.find((item) => {
                            const resource = normalizeValue(item?.resource as string | undefined);
                            const notify = normalizeValue(item?.notificationUrl as string | undefined);
                            if (!matchesResource(resource, normalized)) return false;
                            if (!notify) return true;
                            return notify === expectedNotification;
                        });
                        if (match) {
                            resolved = match as typeof subscription;
                            resolvedId = pickSubscriptionId(match as { id?: string });
                        }
                    } catch (error) {
                        logger.warn("BC subscription create fallback lookup failed", {
                            requestId,
                            entitySet: normalized,
                            error: error instanceof Error ? error.message : String(error),
                        });
                    }
                }

                created.push({
                    entitySet: normalized,
                    id: resolvedId || undefined,
                    resource: resolved?.resource,
                    expirationDateTime: resolved?.expirationDateTime,
                });

                await saveBcSubscription(normalized, {
                    id: resolvedId || "",
                    entitySet: normalized,
                    resource: resolved?.resource,
                    expirationDateTime: resolved?.expirationDateTime,
                    createdAt: new Date().toISOString(),
                    notificationUrl,
                    clientState,
                });
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                if (!/subscription already exist/i.test(errorMessage)) {
                    throw error;
                }

                const expectedNotification = normalizeValue(notificationUrl);
                const list = await bcClient.listWebhookSubscriptions();
                const existing = list.find((item) => {
                    const resource = normalizeValue(item?.resource as string | undefined);
                    const notify = normalizeValue(item?.notificationUrl as string | undefined);
                    if (!matchesResource(resource, normalized)) return false;
                    if (!notify) return true;
                    return notify === expectedNotification;
                }) || (stored?.id ? stored : null);

                const existingId = pickSubscriptionId(existing as { id?: string });
                if (!existingId) {
                    throw error;
                }

                if (isExpiringSoon((existing as { expirationDateTime?: string })?.expirationDateTime)) {
                    try {
                        await bcClient.deleteWebhookSubscription(existingId);
                    } catch (deleteError) {
                        logger.warn("Failed to delete expiring BC subscription before recreate", {
                            requestId,
                            entitySet: normalized,
                            subscriptionId: existingId,
                            error: deleteError instanceof Error ? deleteError.message : String(deleteError),
                        });
                    }
                    const recreated = await bcClient.createWebhookSubscription({
                        entitySet: normalized,
                        notificationUrl,
                        clientState,
                    });
                    const recreatedId = pickSubscriptionId(recreated as { id?: string }) || existingId;
                    await saveBcSubscription(normalized, {
                        id: recreatedId,
                        entitySet: normalized,
                        resource: (recreated as { resource?: string })?.resource || (existing as { resource?: string })?.resource,
                        expirationDateTime:
                            (recreated as { expirationDateTime?: string })?.expirationDateTime ||
                            (existing as { expirationDateTime?: string })?.expirationDateTime,
                        createdAt: new Date().toISOString(),
                        notificationUrl,
                        clientState,
                    });

                    created.push({
                        entitySet: normalized,
                        id: recreatedId,
                        resource: (recreated as { resource?: string })?.resource || (existing as { resource?: string })?.resource,
                        expirationDateTime:
                            (recreated as { expirationDateTime?: string })?.expirationDateTime ||
                            (existing as { expirationDateTime?: string })?.expirationDateTime,
                        existing: false,
                        recreated: true,
                    });
                    continue;
                }

                await saveBcSubscription(normalized, {
                    id: existingId,
                    entitySet: normalized,
                    resource: existing.resource,
                    expirationDateTime: existing.expirationDateTime,
                    createdAt: new Date().toISOString(),
                    notificationUrl,
                    clientState,
                });

                created.push({
                    entitySet: normalized,
                    id: existingId,
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
