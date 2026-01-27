import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { deleteBcSubscription, getBcSubscription } from "../../../lib/planner-sync/bc-webhook-store.js";
import { getBcConfig } from "../../../lib/planner-sync/config.js";
import { logger } from "../../../lib/planner-sync/logger.js";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

function normalizeValue(value) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function matchesResource(resource, entitySet) {
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

    logger.info("POST /api/sync/bc-subscriptions/delete - Request received", { requestId });

    try {
        const body = await readJsonBody(req);
        const entitySets = Array.isArray(body?.entitySets) && body?.entitySets.length
            ? body.entitySets
            : body?.entitySet
            ? [body.entitySet]
            : DEFAULT_ENTITY_SETS;

        const bcClient = new BusinessCentralClient();
        const deleted = [];
        const skipped = [];

        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const stored = await getBcSubscription(normalized);
            let subscriptionId = body?.subscriptionId || stored?.id;
            if (!subscriptionId) {
                try {
                    const list = await bcClient.listWebhookSubscriptions();
                    const match = list.find((item) => matchesResource(item?.resource, normalized));
                    if (match?.id) subscriptionId = match.id;
                } catch (error) {
                    logger.warn("Failed to list BC subscriptions for delete", {
                        requestId,
                        entitySet: normalized,
                        error: error?.message || String(error),
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
                    error: error?.message || String(error),
                });
            }

            await deleteBcSubscription(normalized);
        }

        const duration = Date.now() - startTime;
        res.status(200).json({ ok: true, deleted, skipped, requestId, duration });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error?.message || String(error);
        logger.error("POST /api/sync/bc-subscriptions/delete - Failed", { requestId, duration, error: errorMessage });
        res.status(500).json({ ok: false, error: errorMessage, requestId, duration });
    }
}
