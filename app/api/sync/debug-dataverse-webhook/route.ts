import { DataverseClient } from "../../../../../lib/dataverse-client";
import { logger } from "../../../../../lib/planner-sync/logger";

function escapeODataString(value: unknown) {
    return String(value || "").replace(/'/g, "''");
}

function resolveSecret(request: Request) {
    const setupSecret = (process.env.WEBHOOK_SETUP_SECRET || process.env.CRON_SECRET || "").trim();
    if (!setupSecret) return true;
    const headerSecret =
        request.headers.get("x-setup-secret") ||
        request.headers.get("x-cron-secret") ||
        (request.headers.get("authorization") || "").replace(/^Bearer\s+/i, "");
    return String(headerSecret || "").trim() === setupSecret;
}

function resolveWebhookUrl(request: Request) {
    const origin = `${request.headers.get("x-forwarded-proto") || "https"}://${request.headers.get("host") || "localhost"}`;
    return (process.env.DATAVERSE_NOTIFICATION_URL || "").trim() || `${origin}/api/webhooks/dataverse`;
}

async function listEndpoints(dataverse: DataverseClient, { webhookUrl, endpointId }: { webhookUrl?: string; endpointId?: string | null }) {
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
            logger.warn("Dataverse service endpoint lookup failed", { endpointId, error: (error as Error)?.message });
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

async function listSteps(dataverse: DataverseClient, endpointId: string) {
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

async function resolveMessageName(dataverse: DataverseClient, messageId: string, cache: Map<string, string | null>) {
    if (!messageId) return null;
    if (cache.has(messageId)) return cache.get(messageId) || null;
    try {
        const row = await dataverse.getById("sdkmessages", messageId, ["sdkmessageid", "name"]);
        const name = row?.name ? String(row.name) : null;
        cache.set(messageId, name);
        return name;
    } catch {
        cache.set(messageId, null);
        return null;
    }
}

async function resolveFilterEntity(dataverse: DataverseClient, filterId: string, cache: Map<string, string | null>) {
    if (!filterId) return null;
    if (cache.has(filterId)) return cache.get(filterId) || null;
    try {
        const row = await dataverse.getById("sdkmessagefilters", filterId, ["sdkmessagefilterid", "primaryobjecttypecode"]);
        const name = row?.primaryobjecttypecode ? String(row.primaryobjecttypecode) : null;
        cache.set(filterId, name);
        return name;
    } catch {
        cache.set(filterId, null);
        return null;
    }
}

export async function GET(request: Request) {
    if (!resolveSecret(request)) {
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }, null, 2), {
            status: 401,
            headers: { "Content-Type": "application/json" },
        });
    }

    const url = new URL(request.url);
    const endpointId = String(url.searchParams.get("endpointId") || "").trim();
    const webhookUrl = String(url.searchParams.get("webhookUrl") || "").trim() || resolveWebhookUrl(request);

    try {
        const dataverse = new DataverseClient();
        const endpoints = await listEndpoints(dataverse, { webhookUrl, endpointId: endpointId || null });

        const messageCache = new Map<string, string | null>();
        const filterCache = new Map<string, string | null>();
        const detailed: Array<Record<string, unknown>> = [];

        for (const endpoint of endpoints) {
            const id = (endpoint as { serviceendpointid?: string }).serviceendpointid;
            if (!id) continue;
            const steps = await listSteps(dataverse, id);
            const enriched = [] as Array<Record<string, unknown>>;
            for (const step of steps as Array<Record<string, unknown>>) {
                const messageId = step._sdkmessageid_value as string | undefined;
                const filterId = step._sdkmessagefilterid_value as string | undefined;
                const messageName = await resolveMessageName(dataverse, messageId || "", messageCache);
                const primaryEntity = await resolveFilterEntity(dataverse, filterId || "", filterCache);
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

        return new Response(JSON.stringify({
            ok: true,
            webhookUrl,
            endpointId: endpointId || null,
            endpoints: detailed,
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Debug Dataverse webhook failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}
