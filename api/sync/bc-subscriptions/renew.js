import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { getBcSubscription, saveBcSubscription } from "../../../lib/planner-sync/bc-webhook-store.js";
import { getCronSecret, isCronAuthorized } from "../../../lib/planner-sync/cron-auth.js";
import { logger } from "../../../lib/planner-sync/logger.js";

const DEFAULT_ENTITY_SETS = ["projectTasks"];
const SUBSCRIPTION_TTL_HOURS = 48;
const RENEWAL_BUFFER_HOURS = 6;

function resolveBaseUrl(req) {
    const proto = req.headers["x-forwarded-proto"] || "http";
    const host = req.headers["x-forwarded-host"] || req.headers.host;
    return `${proto}://${host}`;
}

function buildExpirationDate() {
    return new Date(Date.now() + SUBSCRIPTION_TTL_HOURS * 60 * 60 * 1000).toISOString();
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

    if (req.method !== "POST" && req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const cronSecret = getCronSecret();
    const provided = req.headers["x-cron-secret"] || req.query?.cronSecret || req.query?.cron_secret;
    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("BC subscription renew unauthorized", { requestId });
        res.status(401).json({ ok: false, error: "Unauthorized", requestId });
        return;
    }

    try {
        const body = await readJsonBody(req);
        const entitySets = Array.isArray(body?.entitySets) && body?.entitySets.length ? body.entitySets : DEFAULT_ENTITY_SETS;
        const expirationDateTime = buildExpirationDate();
        const bcClient = new BusinessCentralClient();
        const renewed = [];
        const created = [];
        const skipped = [];
        const failed = [];

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
                    error: error?.message || String(error),
                });
            }

            try {
                const subscription = await bcClient.createWebhookSubscription({
                    entitySet: normalized,
                    notificationUrl: stored?.notificationUrl || process.env.BC_WEBHOOK_NOTIFICATION_URL || `${resolveBaseUrl(req)}/api/webhooks/bc`,
                    clientState: stored?.clientState || (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined,
                    expirationDateTime,
                });
                await saveBcSubscription(normalized, {
                    id: subscription?.id || "",
                    entitySet: normalized,
                    resource: subscription?.resource,
                    expirationDateTime: subscription?.expirationDateTime || expirationDateTime,
                    createdAt: new Date().toISOString(),
                    notificationUrl: stored?.notificationUrl || process.env.BC_WEBHOOK_NOTIFICATION_URL || `${resolveBaseUrl(req)}/api/webhooks/bc`,
                    clientState: stored?.clientState || (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined,
                });
                if (stored?.id && stored.id !== subscription?.id) {
                    try {
                        await bcClient.deleteWebhookSubscription(stored.id);
                    } catch (error) {
                        logger.warn("Failed to delete previous BC subscription after recreate", {
                            requestId,
                            entitySet: normalized,
                            subscriptionId: stored.id,
                            error: error?.message || String(error),
                        });
                    }
                }
                created.push(normalized);
            } catch (error) {
                failed.push({ entitySet: normalized, error: error?.message || String(error) });
            }
        }

        const duration = Date.now() - startTime;
        res.status(200).json({ ok: true, renewed, created, skipped, failed, requestId, duration });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error?.message || String(error);
        logger.error("POST /api/sync/bc-subscriptions/renew - Failed", { requestId, duration, error: errorMessage });
        res.status(500).json({ ok: false, error: errorMessage, requestId, duration });
    }
}
