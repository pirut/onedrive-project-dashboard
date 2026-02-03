import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { getBcSubscription } from "../../../lib/planner-sync/bc-webhook-store.js";
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

function resolveNotificationUrl(req, url) {
    const configured = (process.env.BC_WEBHOOK_NOTIFICATION_URL || "").trim();
    const fromQuery = (url?.searchParams?.get("notificationUrl") || "").trim();
    if (fromQuery) return fromQuery;
    if (configured) return configured;
    const proto = req.headers["x-forwarded-proto"] || "https";
    const host = req.headers["x-forwarded-host"] || req.headers.host;
    return `${proto}://${host}/api/webhooks/bc`;
}

function resolveEntitySets(url) {
    const multi = url.searchParams.getAll("entitySet");
    if (multi.length) return multi.filter(Boolean);
    const csv = (url.searchParams.get("entitySets") || "").trim();
    if (csv) return csv.split(",").map((item) => item.trim()).filter(Boolean);
    return DEFAULT_ENTITY_SETS;
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

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const entitySets = resolveEntitySets(url);
    const notificationUrl = resolveNotificationUrl(req, url);
    const normalizedNotificationUrl = normalizeValue(notificationUrl);

    try {
        const bcClient = new BusinessCentralClient();
        const list = await bcClient.listWebhookSubscriptions();
        const stored = [];
        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const entry = await getBcSubscription(normalized);
            stored.push({
                entitySet: normalized,
                storedId: entry?.id || null,
                storedResource: entry?.resource || null,
                storedExpiration: entry?.expirationDateTime || null,
            });
        }

        const items = list.map((item) => {
            const matches = entitySets.filter((entitySet) => matchesResource(item?.resource, entitySet));
            const notificationMatch = normalizedNotificationUrl
                ? normalizeValue(item?.notificationUrl) === normalizedNotificationUrl
                : null;
            return {
                id: pickSubscriptionId(item),
                resource: item?.resource || null,
                notificationUrl: item?.notificationUrl || null,
                clientState: item?.clientState || null,
                expirationDateTime: item?.expirationDateTime || null,
                odataId: item?.["@odata.id"] || item?.["@odata.editLink"] || null,
                matches,
                notificationMatch,
            };
        });

        res.status(200).json({
            ok: true,
            entitySets,
            notificationUrl,
            count: items.length,
            stored,
            items,
        });
    } catch (error) {
        const errorMessage = error?.message || String(error);
        logger.error("GET /api/sync/bc-subscriptions/list - Failed", { error: errorMessage });
        res.status(500).json({ ok: false, error: errorMessage });
    }
}
