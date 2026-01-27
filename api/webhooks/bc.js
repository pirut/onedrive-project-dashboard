import { enqueueBcJobs } from "../../lib/planner-sync/bc-webhook-store.js";
import { appendBcWebhookLog } from "../../lib/planner-sync/bc-webhook-log.js";
import { logger } from "../../lib/planner-sync/logger.js";

function normalizeSystemId(value) {
    let output = (value || "").trim();
    if (!output) return "";
    if ((output.startsWith("'") && output.endsWith("'")) || (output.startsWith('"') && output.endsWith('"'))) {
        output = output.slice(1, -1);
    }
    if (output.startsWith("{") && output.endsWith("}")) {
        output = output.slice(1, -1);
    }
    return output.trim();
}

function parseResource(resource) {
    const raw = (resource || "").trim();
    if (!raw) return { entitySet: "", systemId: "" };

    let cleaned = raw;
    try {
        if (raw.startsWith("http")) {
            const url = new URL(raw);
            cleaned = `${url.pathname}${url.search}`.replace(/^\//, "");
        }
    } catch {
        cleaned = raw.replace(/^\//, "");
    }

    const companyMatch = cleaned.match(/companies\([^\)]+\)\/([^\(\/]+)\(([^\)]+)\)/i);
    if (companyMatch) {
        return {
            entitySet: companyMatch[1],
            systemId: normalizeSystemId(companyMatch[2]),
        };
    }

    const entityMatch = cleaned.match(/([^\/]+)\(([^\)]+)\)/i);
    if (entityMatch) {
        return {
            entitySet: entityMatch[1],
            systemId: normalizeSystemId(entityMatch[2]),
        };
    }

    return { entitySet: "", systemId: "" };
}

function readValidationToken(payload) {
    if (!payload || typeof payload !== "object") return null;
    const token = payload.validationToken ?? payload.validationtoken;
    return typeof token === "string" ? token : null;
}

function resolveClientState(notification) {
    return typeof notification?.clientState === "string" ? notification.clientState : "";
}

const LOG_SAMPLE_LIMIT = 10;

function buildLogItems(notifications) {
    if (!notifications?.length) return [];
    return notifications.slice(0, LOG_SAMPLE_LIMIT).map((notification) => {
        const resourceInfo = parseResource(notification.resource);
        const systemId = resourceInfo.systemId || normalizeSystemId(notification.resourceData?.id || notification.id);
        return {
            entitySet: resourceInfo.entitySet,
            systemId,
            changeType: notification.changeType,
            resource: notification.resource,
            subscriptionId: notification.subscriptionId,
        };
    });
}

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return { payload: null, raw: "" };
    try {
        return { payload: JSON.parse(text), raw: text };
    } catch {
        return { payload: null, raw: text };
    }
}

export default async function handler(req, res) {
    const requestId = Math.random().toString(36).slice(2, 12);
    const validationToken = req.query?.validationToken;

    if (req.method === "GET") {
        if (validationToken) {
            logger.info("GET /api/webhooks/bc - Validation token received", { requestId });
            await appendBcWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "validation",
                location: "query",
            });
            res.setHeader("Content-Type", "text/plain");
            res.setHeader("X-Request-ID", requestId);
            res.status(200).send(validationToken);
            return;
        }
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (validationToken) {
        logger.info("POST /api/webhooks/bc - Validation token received", { requestId });
        await appendBcWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "validation",
            location: "query",
        });
        res.setHeader("Content-Type", "text/plain");
        res.setHeader("X-Request-ID", requestId);
        res.status(200).send(validationToken);
        return;
    }

    const { payload } = await readJsonBody(req);
    if (!payload) {
        await appendBcWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "invalid_json",
            error: "Invalid JSON",
        });
        res.status(400).json({ ok: false, error: "Invalid JSON" });
        return;
    }

    const bodyToken = readValidationToken(payload);
    if (bodyToken) {
        logger.info("POST /api/webhooks/bc - Validation token received in body", { requestId });
        await appendBcWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "validation",
            location: "body",
        });
        res.setHeader("Content-Type", "text/plain");
        res.setHeader("X-Request-ID", requestId);
        res.status(200).send(bodyToken);
        return;
    }

    const notifications = payload?.value || [];
    if (!notifications.length) {
        await appendBcWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "notification",
            count: 0,
        });
        res.status(202).json({ ok: true, received: 0 });
        return;
    }

    const sharedSecret = (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim();
    if (!sharedSecret) {
        logger.debug("BC webhook shared secret not configured", { requestId });
    }
    const jobs = [];
    let secretMismatch = 0;
    let missingResource = 0;

    for (const notification of notifications) {
        if (sharedSecret) {
            const clientState = resolveClientState(notification);
            if (!clientState || clientState !== sharedSecret) {
                secretMismatch += 1;
                continue;
            }
        }

        const resourceInfo = parseResource(notification.resource);
        const systemId = resourceInfo.systemId || normalizeSystemId(notification.resourceData?.id || notification.id);
        const entitySet = resourceInfo.entitySet;
        if (!entitySet || !systemId) {
            missingResource += 1;
            continue;
        }

        jobs.push({
            entitySet,
            systemId,
            changeType: notification.changeType,
            receivedAt: new Date().toISOString(),
            subscriptionId: notification.subscriptionId,
            resource: notification.resource,
        });
    }

    const enqueueResult = await enqueueBcJobs(jobs);
    await appendBcWebhookLog({
        ts: new Date().toISOString(),
        requestId,
        type: "notification",
        count: notifications.length,
        enqueued: enqueueResult.enqueued,
        deduped: enqueueResult.deduped,
        skipped: enqueueResult.skipped,
        secretMismatch,
        missingResource,
        items: buildLogItems(notifications),
    });

    res.status(202).json({
        ok: true,
        received: notifications.length,
        enqueued: enqueueResult.enqueued,
        deduped: enqueueResult.deduped,
        skipped: enqueueResult.skipped,
        secretMismatch,
        missingResource,
    });
}
