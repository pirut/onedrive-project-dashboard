import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { getBcSubscription, saveBcSubscription } from "../../../lib/planner-sync/bc-webhook-store.js";
import { getBcConfig } from "../../../lib/planner-sync/config.js";
import { logger } from "../../../lib/planner-sync/logger.js";

const DEFAULT_ENTITY_SETS = [
    (process.env.BC_SYNC_QUEUE_ENTITY_SET || "").trim() || "premiumSyncQueue",
].filter(Boolean);

function normalizeValue(value) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function buildExpectedResource(entitySet) {
    const { publisher, group, version, companyId } = getBcConfig();
    return `api/${publisher}/${group}/${version}/companies(${companyId})/${entitySet}`.replace(/^\/+/, "");
}

function matchesResource(resource, entitySet) {
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

function resolveBaseUrl(req) {
    const proto = req.headers["x-forwarded-proto"] || "http";
    const host = req.headers["x-forwarded-host"] || req.headers.host;
    return `${proto}://${host}`;
}

function extractODataId(item) {
    const raw = item?.["@odata.id"] || item?.["@odata.editLink"] || item?.odataId;
    if (!raw) return null;
    const match = String(raw).match(/subscriptions\(([^)]+)\)/i);
    return match ? match[1] : null;
}

function pickSubscriptionId(item) {
    return (
        item?.id ||
        item?.Id ||
        item?.ID ||
        item?.subscriptionId ||
        item?.subscriptionid ||
        item?.subscriptionID ||
        item?.systemId ||
        extractODataId(item) ||
        null
    );
}

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

export default async function handler(req, res) {
    const startTime = Date.now();
    const requestId = Math.random().toString(36).slice(2, 12);

    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    logger.info("POST /api/sync/bc-subscriptions/create - Request received", { requestId });

    try {
        const body = await readJsonBody(req);
        const entitySets = Array.isArray(body?.entitySets) && body?.entitySets.length ? body.entitySets : DEFAULT_ENTITY_SETS;
        const envNotificationUrl = process.env.BC_WEBHOOK_NOTIFICATION_URL;
        const notificationUrl = body?.notificationUrl || envNotificationUrl || `${resolveBaseUrl(req)}/api/webhooks/bc`;
        if (!notificationUrl.startsWith("https://")) {
            logger.warn("BC webhook notificationUrl is not HTTPS", { requestId, notificationUrl });
        }

        const clientState = (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined;
        const bcClient = new BusinessCentralClient();

        const created = [];
        const skipped = [];

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
                let resolvedId = pickSubscriptionId(resolved);
                if (!resolvedId) {
                    const expectedNotification = normalizeValue(notificationUrl);
                    try {
                        const list = await bcClient.listWebhookSubscriptions();
                        const match = list.find((item) => {
                            const resource = normalizeValue(item?.resource);
                            const notify = normalizeValue(item?.notificationUrl);
                            if (!matchesResource(resource, normalized)) return false;
                            if (!notify) return true;
                            return notify === expectedNotification;
                        });
                        if (match) {
                            resolved = match;
                            resolvedId = pickSubscriptionId(match);
                        }
                    } catch (error) {
                        logger.warn("BC subscription create fallback lookup failed", {
                            requestId,
                            entitySet: normalized,
                            error: error?.message || String(error),
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
                const errorMessage = error?.message || String(error);
                if (!/subscription already exist/i.test(errorMessage)) {
                    throw error;
                }

                const expectedNotification = normalizeValue(notificationUrl);
                const list = await bcClient.listWebhookSubscriptions();
                const existing = list.find((item) => {
                    const resource = normalizeValue(item?.resource);
                    const notify = normalizeValue(item?.notificationUrl);
                    if (!matchesResource(resource, normalized)) return false;
                    if (!notify) return true;
                    return notify === expectedNotification;
                }) || (stored?.id ? stored : null);

                const existingId = pickSubscriptionId(existing);
                if (!existingId) {
                    throw error;
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
        res.status(200).json({ ok: true, created, skipped, notificationUrl, requestId, duration });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error?.message || String(error);
        logger.error("POST /api/sync/bc-subscriptions/create - Failed", { requestId, duration, error: errorMessage });
        res.status(500).json({ ok: false, error: errorMessage, requestId, duration });
    }
}
