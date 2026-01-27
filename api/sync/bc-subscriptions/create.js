import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { getBcSubscription, saveBcSubscription } from "../../../lib/planner-sync/bc-webhook-store.js";
import { logger } from "../../../lib/planner-sync/logger.js";

const DEFAULT_ENTITY_SETS = ["projectTasks"];
const SUBSCRIPTION_TTL_HOURS = 48;

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
        const expirationDateTime = buildExpirationDate();
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

            const subscription = await bcClient.createWebhookSubscription({
                entitySet: normalized,
                notificationUrl,
                clientState,
                expirationDateTime,
            });

            created.push({
                entitySet: normalized,
                id: subscription?.id,
                resource: subscription?.resource,
                expirationDateTime: subscription?.expirationDateTime || expirationDateTime,
            });

            await saveBcSubscription(normalized, {
                id: subscription?.id || "",
                entitySet: normalized,
                resource: subscription?.resource,
                expirationDateTime: subscription?.expirationDateTime || expirationDateTime,
                createdAt: new Date().toISOString(),
                notificationUrl,
                clientState,
            });
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
