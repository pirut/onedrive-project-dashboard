import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

function escapeODataString(value) {
    return String(value || "").replace(/'/g, "''");
}

function resolveSecret(req) {
    const setupSecret = (process.env.WEBHOOK_SETUP_SECRET || process.env.CRON_SECRET || "").trim();
    if (!setupSecret) return true;
    const headerSecret =
        req.headers["x-setup-secret"] ||
        req.headers["x-cron-secret"] ||
        (req.headers["authorization"] || "").replace(/^Bearer\s+/i, "");
    return String(headerSecret || "").trim() === setupSecret;
}

function resolveWebhookUrl(req) {
    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    return (process.env.DATAVERSE_NOTIFICATION_URL || "").trim() || `${origin}/api/webhooks/dataverse`;
}

async function listEndpoints(dataverse, { webhookUrl, endpointId }) {
    if (endpointId) {
        try {
            const endpoint = await dataverse.getById("serviceendpoints", endpointId, [
                "serviceendpointid",
                "name",
                "url",
                "authtype",
                "contract",
            ]);
            return endpoint ? [endpoint] : [];
        } catch (error) {
            const message =
                error && typeof error === "object" && "message" in error ? error.message : String(error);
            logger.warn("Dataverse service endpoint lookup failed", { endpointId, error: message });
            return [];
        }
    }

    if (webhookUrl) {
        const filter = `contract eq 8 and url eq '${escapeODataString(webhookUrl)}'`;
        const res = await dataverse.list("serviceendpoints", {
            select: ["serviceendpointid", "name", "url", "authtype", "contract"],
            filter,
            top: 50,
        });
        return res.value || [];
    }

    const res = await dataverse.list("serviceendpoints", {
        select: ["serviceendpointid", "name", "url", "authtype", "contract"],
        filter: "contract eq 8",
        top: 200,
    });
    return res.value || [];
}

async function listSteps(dataverse, endpointId) {
    const filter = `_eventhandler_value eq ${endpointId}`;
    const res = await dataverse.list("sdkmessageprocessingsteps", {
        select: [
            "sdkmessageprocessingstepid",
            "name",
            "mode",
            "stage",
            "rank",
            "statecode",
            "statuscode",
            "asyncautodelete",
            "_sdkmessageid_value",
            "_sdkmessagefilterid_value",
        ],
        filter,
        top: 200,
    });
    return res.value || [];
}

async function resolveMessageName(dataverse, messageId, cache) {
    if (!messageId) return null;
    if (cache.has(messageId)) return cache.get(messageId) || null;
    try {
        const row = await dataverse.getById("sdkmessages", messageId, ["sdkmessageid", "name"]);
        const name = row?.name ? String(row.name) : null;
        cache.set(messageId, name);
        return name;
    } catch (error) {
        cache.set(messageId, null);
        return null;
    }
}

async function resolveFilterEntity(dataverse, filterId, cache) {
    if (!filterId) return null;
    if (cache.has(filterId)) return cache.get(filterId) || null;
    try {
        const row = await dataverse.getById("sdkmessagefilters", filterId, ["sdkmessagefilterid", "primaryobjecttypecode"]);
        const name = row?.primaryobjecttypecode ? String(row.primaryobjecttypecode) : null;
        cache.set(filterId, name);
        return name;
    } catch (error) {
        cache.set(filterId, null);
        return null;
    }
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (!resolveSecret(req)) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const endpointId = String(url.searchParams.get("endpointId") || "").trim();
    const webhookUrl = String(url.searchParams.get("webhookUrl") || "").trim() || resolveWebhookUrl(req);

    try {
        const dataverse = new DataverseClient();
        const endpoints = await listEndpoints(dataverse, { webhookUrl, endpointId: endpointId || null });

        const messageCache = new Map();
        const filterCache = new Map();
        const detailed = [];

        for (const endpoint of endpoints) {
            const id = endpoint.serviceendpointid || endpoint.serviceEndpointId || endpoint.serviceendpointId;
            if (!id) continue;
            const steps = await listSteps(dataverse, id);
            const enriched = [];
            for (const step of steps) {
                const messageId = step._sdkmessageid_value || step.sdkmessageid;
                const filterId = step._sdkmessagefilterid_value || step.sdkmessagefilterid;
                const messageName = await resolveMessageName(dataverse, messageId, messageCache);
                const primaryEntity = await resolveFilterEntity(dataverse, filterId, filterCache);
                enriched.push({
                    id: step.sdkmessageprocessingstepid || null,
                    name: step.name || null,
                    mode: step.mode,
                    stage: step.stage,
                    rank: step.rank,
                    statecode: step.statecode,
                    statuscode: step.statuscode,
                    asyncautodelete: step.asyncautodelete,
                    messageId,
                    messageName,
                    filterId,
                    primaryEntity,
                });
            }
            detailed.push({
                endpoint: {
                    id,
                    name: endpoint.name || null,
                    url: endpoint.url || null,
                    authtype: endpoint.authtype,
                    contract: endpoint.contract,
                },
                steps: enriched,
            });
        }

        res.status(200).json({
            ok: true,
            webhookUrl,
            endpointId: endpointId || null,
            endpoints: detailed,
        });
    } catch (error) {
        logger.error("Debug Dataverse webhook failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
