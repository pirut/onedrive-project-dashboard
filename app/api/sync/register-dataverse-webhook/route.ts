import { DataverseClient } from "../../../../../lib/dataverse-client";
import { logger } from "../../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

function escapeODataString(value: unknown) {
    return String(value || "").replace(/'/g, "''");
}

function normalizeMessages(value: unknown) {
    if (!value) return ["Create", "Update", "Delete"];
    if (Array.isArray(value)) {
        return value.map((entry) => String(entry).trim()).filter(Boolean);
    }
    return String(value)
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean);
}

function resolveWebhookUrl(request: Request, body: Record<string, unknown> | null) {
    const bodyUrl = body?.webhookUrl ? String(body.webhookUrl).trim() : "";
    const envUrl = (process.env.DATAVERSE_NOTIFICATION_URL || "").trim();
    if (bodyUrl) return bodyUrl;
    if (envUrl) return envUrl;
    const origin = `${request.headers.get("x-forwarded-proto") || "https"}://${request.headers.get("host") || "localhost"}`;
    return `${origin}/api/webhooks/dataverse`;
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

async function getSdkMessageId(dataverse: DataverseClient, messageName: string) {
    const filter = `name eq '${escapeODataString(messageName)}'`;
    const res = await dataverse.list("sdkmessages", { select: ["sdkmessageid", "name"], filter, top: 1 });
    return (res.value?.[0] as { sdkmessageid?: string })?.sdkmessageid || "";
}

async function getSdkMessageFilterId(dataverse: DataverseClient, entityLogicalName: string, messageId: string) {
    const filter = `primaryobjecttypecode eq '${escapeODataString(entityLogicalName)}' and _sdkmessageid_value eq ${messageId}`;
    const res = await dataverse.list("sdkmessagefilters", { select: ["sdkmessagefilterid"], filter, top: 1 });
    return (res.value?.[0] as { sdkmessagefilterid?: string })?.sdkmessagefilterid || "";
}

async function findExistingEndpoint(dataverse: DataverseClient, url: string) {
    const filter = `contract eq 8 and url eq '${escapeODataString(url)}'`;
    const res = await dataverse.list("serviceendpoints", { select: ["serviceendpointid", "name", "url"], filter, top: 1 });
    return (res.value?.[0] as { serviceendpointid?: string })?.serviceendpointid || "";
}

async function findExistingStep(dataverse: DataverseClient, endpointId: string, messageId: string, filterId: string) {
    const filter = `_eventhandler_value eq ${endpointId} and _sdkmessageid_value eq ${messageId} and _sdkmessagefilterid_value eq ${filterId}`;
    const res = await dataverse.list("sdkmessageprocessingsteps", {
        select: ["sdkmessageprocessingstepid"],
        filter,
        top: 1,
    });
    return (res.value?.[0] as { sdkmessageprocessingstepid?: string })?.sdkmessageprocessingstepid || "";
}

export async function POST(request: Request) {
    if (!resolveSecret(request)) {
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }, null, 2), {
            status: 401,
            headers: { "Content-Type": "application/json" },
        });
    }

    const body = await readJsonBody(request);
    const webhookUrl = resolveWebhookUrl(request, body);
    const entityLogicalName = body?.entityLogicalName ? String(body.entityLogicalName).trim() : "msdyn_projecttask";
    const messages = normalizeMessages(body?.messages);
    const asyncMode = body?.asyncMode !== false;
    const stage = Number.isFinite(Number(body?.stage)) ? Number(body.stage) : 40;

    try {
        const dataverse = new DataverseClient();
        let endpointId = await findExistingEndpoint(dataverse, webhookUrl);
        if (!endpointId) {
            const payload = {
                name: body?.endpointName ? String(body.endpointName) : "cstonedash-dataverse-webhook",
                url: webhookUrl,
                contract: 8,
                connectionmode: 1,
                messageformat: 2,
                path: body?.endpointPath ? String(body.endpointPath) : "cstonedash-webhook",
                solutionnamespace: body?.solutionNamespace ? String(body.solutionNamespace) : "cstonedash",
                authtype: 0,
            };
            const created = await dataverse.create("serviceendpoints", payload);
            endpointId = created.entityId || "";
        }

        if (!endpointId) {
            return new Response(JSON.stringify({ ok: false, error: "Failed to create service endpoint" }, null, 2), {
                status: 400,
                headers: { "Content-Type": "application/json" },
            });
        }

        const results: Array<Record<string, unknown>> = [];
        for (const messageName of messages) {
            const messageId = await getSdkMessageId(dataverse, messageName);
            if (!messageId) {
                results.push({ message: messageName, ok: false, error: "sdkmessage not found" });
                continue;
            }
            const filterId = await getSdkMessageFilterId(dataverse, entityLogicalName, messageId);
            if (!filterId) {
                results.push({ message: messageName, ok: false, error: "sdkmessagefilter not found" });
                continue;
            }
            const existingStep = await findExistingStep(dataverse, endpointId, messageId, filterId);
            if (existingStep) {
                results.push({ message: messageName, ok: true, stepId: existingStep, skipped: true });
                continue;
            }
            const stepPayload = {
                name: `Webhook ${entityLogicalName} ${messageName}`,
                asyncautodelete: true,
                mode: asyncMode ? 1 : 0,
                stage,
                "eventhandler_serviceendpoint@odata.bind": `/serviceendpoints(${endpointId})`,
                "sdkmessageid@odata.bind": `/sdkmessages(${messageId})`,
                "sdkmessagefilterid@odata.bind": `/sdkmessagefilters(${filterId})`,
            };
            const createdStep = await dataverse.create("sdkmessageprocessingsteps", stepPayload);
            results.push({ message: messageName, ok: true, stepId: createdStep.entityId || null });
        }

        return new Response(JSON.stringify({
            ok: true,
            webhookUrl,
            entityLogicalName,
            endpointId,
            results,
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Register Dataverse webhook failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}
