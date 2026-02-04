import {
    acquireBcJobLock,
    enqueueBcJobs,
    getBcSubscription,
    releaseBcJobLock,
} from "../../lib/planner-sync/bc-webhook-store.js";
import { processBcJobQueue } from "../../lib/planner-sync/bc-job-processor.js";
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
const DEBUG_LOG_LIMIT = 10;

function shouldDebugLog() {
    const flag = String(process.env.BC_WEBHOOK_DEBUG_LOG || "").trim().toLowerCase();
    return ["1", "true", "yes", "on"].includes(flag);
}

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

function buildDebugSamples(items) {
    if (!items?.length) return [];
    return items.slice(0, DEBUG_LOG_LIMIT).map((item) => ({
        entitySet: item.entitySet,
        systemId: item.systemId,
        changeType: item.changeType,
        subscriptionId: item.subscriptionId,
    }));
}

async function isSubscriptionAllowed(entitySet, subscriptionId, cache) {
    if (!subscriptionId) return true;
    if (cache.has(entitySet)) {
        const storedId = cache.get(entitySet);
        return !storedId || storedId === subscriptionId;
    }
    const stored = await getBcSubscription(entitySet);
    const storedId = stored?.id || stored?.subscriptionId || null;
    cache.set(entitySet, storedId);
    return !storedId || storedId === subscriptionId;
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

function resolveForwardTargets() {
    const raw = String(process.env.BC_WEBHOOK_FORWARD_URL || "").trim();
    if (!raw) return [];
    return raw
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
}

async function forwardWebhookPayload({ payload, raw, requestId }) {
    const targets = resolveForwardTargets();
    if (!targets.length) return;
    const body = raw || (payload ? JSON.stringify(payload) : "");
    if (!body) return;
    await Promise.all(
        targets.map(async (target) => {
            try {
                await fetch(target, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "X-Forwarded-By": "bc-webhook-forwarder",
                        "X-Request-ID": requestId,
                    },
                    body,
                });
            } catch (error) {
                logger.warn("BC webhook forward failed", {
                    requestId,
                    target,
                    error: error?.message || String(error),
                });
            }
        })
    );
}

function resolveInlineProcessing(req) {
    if (typeof req.query?.process === "string") {
        const flag = req.query.process.trim().toLowerCase();
        if (["1", "true", "yes", "on"].includes(flag)) return true;
        if (["0", "false", "no", "off"].includes(flag)) return false;
    }
    if (process.env.BC_WEBHOOK_PROCESS_INLINE) {
        const flag = String(process.env.BC_WEBHOOK_PROCESS_INLINE).trim().toLowerCase();
        if (["1", "true", "yes", "on"].includes(flag)) return true;
        if (["0", "false", "no", "off"].includes(flag)) return false;
    }
    return false;
}

function resolveInlineMaxJobs(req) {
    const raw = req.query?.maxJobs;
    const fromQuery = typeof raw === "string" ? Number(raw) : null;
    if (Number.isFinite(fromQuery) && fromQuery > 0) return Math.floor(fromQuery);
    const env = Number(process.env.BC_WEBHOOK_INLINE_MAX_JOBS);
    if (Number.isFinite(env) && env > 0) return Math.floor(env);
    return 25;
}

function resolveRetryDelayMs(req) {
    const raw = req.query?.retryDelayMs;
    const fromQuery = typeof raw === "string" ? Number(raw) : null;
    if (Number.isFinite(fromQuery) && fromQuery >= 0) return Math.floor(fromQuery);
    const env = Number(process.env.BC_WEBHOOK_RETRY_DELAY_MS);
    if (Number.isFinite(env) && env >= 0) return Math.floor(env);
    return 10000;
}

function resolveRetryCount(req) {
    const raw = req.query?.retryCount;
    const fromQuery = typeof raw === "string" ? Number(raw) : null;
    if (Number.isFinite(fromQuery) && fromQuery >= 0) return Math.floor(fromQuery);
    const env = Number(process.env.BC_WEBHOOK_RETRY_COUNT);
    if (Number.isFinite(env) && env >= 0) return Math.floor(env);
    return 2;
}

async function waitMs(ms) {
    if (!ms || ms <= 0) return;
    await new Promise((resolve) => setTimeout(resolve, ms));
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

    const { payload, raw } = await readJsonBody(req);
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
    if (notifications.length) {
        forwardWebhookPayload({ payload, raw, requestId }).catch((error) => {
            logger.warn("BC webhook forward errored", {
                requestId,
                error: error?.message || String(error),
            });
        });
    }
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
    let subscriptionMismatch = 0;
    const subscriptionCache = new Map();

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

        if (!(await isSubscriptionAllowed(entitySet, notification.subscriptionId, subscriptionCache))) {
            subscriptionMismatch += 1;
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
    const processInline = resolveInlineProcessing(req);
    let processed = null;
    let processSkipped = null;
    if (shouldDebugLog()) {
        logger.info("BC webhook parsed notifications", {
            requestId,
            received: notifications.length,
            accepted: jobs.length,
            secretMismatch,
            missingResource,
            samples: buildDebugSamples(jobs),
        });
    }
    if (processInline) {
        const maxJobs = resolveInlineMaxJobs(req);
        const retryDelayMs = resolveRetryDelayMs(req);
        const retryCount = resolveRetryCount(req);
        let attempts = 0;
        while (attempts <= retryCount) {
            const lock = await acquireBcJobLock();
            if (!lock) {
                processSkipped = "locked";
                attempts += 1;
                if (attempts > retryCount) break;
                await waitMs(retryDelayMs);
                continue;
            }
            try {
                processed = await processBcJobQueue({ maxJobs, requestId });
                processSkipped = null;
                break;
            } catch (error) {
                logger.error("BC webhook inline processing failed", { requestId, error: error?.message || String(error) });
                processSkipped = "error";
                break;
            } finally {
                await releaseBcJobLock(lock);
            }
        }
    }
    if (shouldDebugLog()) {
        logger.info("BC webhook enqueue/process summary", {
            requestId,
            processInline,
            enqueued: enqueueResult.enqueued,
            deduped: enqueueResult.deduped,
            skipped: enqueueResult.skipped,
            processed: processed ? processed.processed : 0,
            processSkipped,
        });
    }
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
        subscriptionMismatch,
        items: buildLogItems(notifications),
        processed: processed ? processed.processed : 0,
        processSkipped,
        skipReasons: processed?.skipReasons,
    });

    res.status(202).json({
        ok: true,
        received: notifications.length,
        enqueued: enqueueResult.enqueued,
        deduped: enqueueResult.deduped,
        skipped: enqueueResult.skipped,
        secretMismatch,
        missingResource,
        subscriptionMismatch,
        processed: processed ? processed.processed : 0,
        processSkipped,
        skipReasons: processed?.skipReasons,
    });
}
