import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

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

function escapeODataString(value) {
    return String(value || "").replace(/'/g, "''");
}

function normalizeMessages(value) {
    if (!value) return ["Create", "Update", "Delete"];
    if (Array.isArray(value)) {
        return value.map((entry) => String(entry).trim()).filter(Boolean);
    }
    return String(value)
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean);
}

function resolveWebhookUrl(req, body) {
    const bodyUrl = body?.webhookUrl ? String(body.webhookUrl).trim() : "";
    const envUrl = (process.env.DATAVERSE_NOTIFICATION_URL || "").trim();
    if (bodyUrl) return bodyUrl;
    if (envUrl) return envUrl;
    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    return `${origin}/api/webhooks/dataverse`;
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

async function getSdkMessageId(dataverse, messageName) {
    const filter = `name eq '${escapeODataString(messageName)}'`;
    const res = await dataverse.list("sdkmessages", { select: ["sdkmessageid", "name"], filter, top: 1 });
    return res.value?.[0]?.sdkmessageid || "";
}

async function getSdkMessageFilterId(dataverse, entityLogicalName, messageId) {
    const filter = `primaryobjecttypecode eq '${escapeODataString(entityLogicalName)}' and _sdkmessageid_value eq ${messageId}`;
    const res = await dataverse.list("sdkmessagefilters", { select: ["sdkmessagefilterid"], filter, top: 1 });
    return res.value?.[0]?.sdkmessagefilterid || "";
}

async function findExistingEndpoint(dataverse, url) {
    const filter = `contract eq 8 and url eq '${escapeODataString(url)}'`;
    const res = await dataverse.list("serviceendpoints", { select: ["serviceendpointid", "name", "url"], filter, top: 1 });
    const row = res.value?.[0];
    return row?.serviceendpointid || "";
}

async function findExistingStep(dataverse, endpointId, messageId, filterId) {
    const filter = `_eventhandler_value eq ${endpointId} and _sdkmessageid_value eq ${messageId} and _sdkmessagefilterid_value eq ${filterId}`;
    const res = await dataverse.list("sdkmessageprocessingsteps", {
        select: ["sdkmessageprocessingstepid"],
        filter,
        top: 1,
    });
    return res.value?.[0]?.sdkmessageprocessingstepid || "";
}

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (!resolveSecret(req)) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
        return;
    }

    const body = await readJsonBody(req);
    const webhookUrl = resolveWebhookUrl(req, body);
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
            res.status(400).json({ ok: false, error: "Failed to create service endpoint" });
            return;
        }

        const results = [];
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

        res.status(200).json({
            ok: true,
            webhookUrl,
            entityLogicalName,
            endpointId,
            results,
        });
    } catch (error) {
        logger.error("Register Dataverse webhook failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
