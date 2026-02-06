import { acquireBcJobLock, enqueueBcJobs, BcWebhookJob, releaseBcJobLock } from "../../../../../lib/planner-sync/bc-webhook-store";
import { appendBcWebhookLog } from "../../../../../lib/planner-sync/bc-webhook-log";
import { processBcJobQueue } from "../../../../../lib/planner-sync/bc-job-processor";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

type BcNotification = {
    subscriptionId?: string;
    clientState?: string;
    resource?: string;
    changeType?: string;
    resourceData?: { id?: string };
    id?: string;
};

const LOG_SAMPLE_LIMIT = 10;
const DEBUG_LOG_LIMIT = 10;
const QUEUE_ENTITY_SET = (process.env.BC_SYNC_QUEUE_ENTITY_SET || "premiumSyncQueue").trim().toLowerCase();

function shouldDebugLog() {
    const flag = (process.env.BC_WEBHOOK_DEBUG_LOG || "").trim().toLowerCase();
    return ["1", "true", "yes", "on"].includes(flag);
}

function normalizeSystemId(value: string | null | undefined) {
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

function parseResource(resource: string | undefined) {
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

function readValidationToken(payload: unknown) {
    if (!payload || typeof payload !== "object") return null;
    const record = payload as Record<string, unknown>;
    const token = record.validationToken ?? record.validationtoken;
    return typeof token === "string" ? token : null;
}

function resolveClientState(notification: BcNotification) {
    return typeof notification?.clientState === "string" ? notification.clientState : "";
}

function buildLogItems(notifications: BcNotification[]) {
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

function buildDebugSamples(items: BcWebhookJob[]) {
    if (!items?.length) return [];
    return items.slice(0, DEBUG_LOG_LIMIT).map((item) => ({
        entitySet: item.entitySet,
        systemId: item.systemId,
        changeType: item.changeType,
        subscriptionId: item.subscriptionId,
    }));
}

function resolveInlineProcessing(url: URL) {
    const flag = url.searchParams.get("process")?.trim().toLowerCase();
    if (flag) {
        if (["1", "true", "yes", "on"].includes(flag)) return true;
        if (["0", "false", "no", "off"].includes(flag)) return false;
    }
    const env = (process.env.BC_WEBHOOK_PROCESS_INLINE || "").trim().toLowerCase();
    if (env) {
        if (["1", "true", "yes", "on"].includes(env)) return true;
        if (["0", "false", "no", "off"].includes(env)) return false;
    }
    return false;
}

function resolveInlineMaxJobs(url: URL) {
    const raw = url.searchParams.get("maxJobs");
    const fromQuery = raw ? Number(raw) : Number.NaN;
    if (Number.isFinite(fromQuery) && fromQuery > 0) return Math.floor(fromQuery);
    const env = Number(process.env.BC_WEBHOOK_INLINE_MAX_JOBS);
    if (Number.isFinite(env) && env > 0) return Math.floor(env);
    return 25;
}

function shouldSkipNotification(entitySet: string, changeType: string | undefined) {
    if (!QUEUE_ENTITY_SET) return false;
    const normalizedEntity = (entitySet || "").trim().toLowerCase();
    const normalizedChangeType = (changeType || "").trim().toLowerCase();
    return normalizedEntity === QUEUE_ENTITY_SET && normalizedChangeType === "deleted";
}

function resolveRetryDelayMs(url: URL) {
    const raw = url.searchParams.get("retryDelayMs");
    const fromQuery = raw ? Number(raw) : Number.NaN;
    if (Number.isFinite(fromQuery) && fromQuery >= 0) return Math.floor(fromQuery);
    const env = Number(process.env.BC_WEBHOOK_RETRY_DELAY_MS);
    if (Number.isFinite(env) && env >= 0) return Math.floor(env);
    return 10000;
}

function resolveRetryCount(url: URL) {
    const raw = url.searchParams.get("retryCount");
    const fromQuery = raw ? Number(raw) : Number.NaN;
    if (Number.isFinite(fromQuery) && fromQuery >= 0) return Math.floor(fromQuery);
    const env = Number(process.env.BC_WEBHOOK_RETRY_COUNT);
    if (Number.isFinite(env) && env >= 0) return Math.floor(env);
    return 2;
}

async function waitMs(ms: number) {
    if (!ms || ms <= 0) return;
    await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("POST /api/webhooks/bc - Request received", {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
    });

    try {
        const queryToken = url.searchParams.get("validationToken");
        if (queryToken) {
            logger.info("Validation token received - responding to BC subscription validation", { requestId });
            await appendBcWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "validation",
                location: "query",
            });
            return new Response(queryToken, {
                status: 200,
                headers: {
                    "Content-Type": "text/plain",
                    "X-Request-ID": requestId,
                },
            });
        }

        let payload: { value?: BcNotification[] } | null = null;
        let rawBody = "";
        try {
            rawBody = await request.text();
            if (rawBody) {
                payload = JSON.parse(rawBody);
            }
        } catch (error) {
            logger.error("Failed to parse BC webhook payload", {
                requestId,
                error: error instanceof Error ? error.message : String(error),
            });
            await appendBcWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "invalid_json",
                error: error instanceof Error ? error.message : String(error),
            });
            return new Response(JSON.stringify({ ok: false, error: "Invalid JSON", requestId }), {
                status: 400,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }

        const bodyToken = readValidationToken(payload);
        if (bodyToken) {
            logger.info("Validation token received in body - responding", { requestId });
            await appendBcWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "validation",
                location: "body",
            });
            return new Response(bodyToken, {
                status: 200,
                headers: {
                    "Content-Type": "text/plain",
                    "X-Request-ID": requestId,
                },
            });
        }

        const notifications = payload?.value || [];
        if (!notifications.length) {
            await appendBcWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "notification",
                count: 0,
            });
            return new Response(JSON.stringify({ ok: true, received: 0, requestId }), {
                status: 202,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }

        const sharedSecret = (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim();
        if (!sharedSecret) {
            logger.debug("BC webhook shared secret not configured", { requestId });
        }
        const jobs: BcWebhookJob[] = [];
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
            if (shouldSkipNotification(entitySet, notification.changeType)) {
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
        const processInline = resolveInlineProcessing(url);
        let processed: { processed?: number; skipReasons?: Record<string, number> } | null = null;
        let processSkipped: string | null = null;
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
            const requestedMaxJobs = resolveInlineMaxJobs(url);
            const maxJobs = Math.max(requestedMaxJobs, Math.min(100, Math.max(0, jobs.length)));
            const retryDelayMs = resolveRetryDelayMs(url);
            const retryCount = resolveRetryCount(url);
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
                    logger.error("BC webhook inline processing failed", { requestId, error: error instanceof Error ? error.message : String(error) });
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
                processed: processed?.processed || 0,
                processSkipped: processSkipped || undefined,
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
            items: buildLogItems(notifications),
            processed: processed?.processed || 0,
            processSkipped: processSkipped || undefined,
            skipReasons: processed?.skipReasons,
        });
        const duration = Date.now() - startTime;

        logger.info("POST /api/webhooks/bc - Success", {
            requestId,
            duration,
            received: notifications.length,
            enqueued: enqueueResult.enqueued,
            deduped: enqueueResult.deduped,
            skipped: enqueueResult.skipped,
            secretMismatch,
            missingResource,
        });

        return new Response(
            JSON.stringify({
                ok: true,
                received: notifications.length,
                enqueued: enqueueResult.enqueued,
                deduped: enqueueResult.deduped,
                skipped: enqueueResult.skipped,
                secretMismatch,
                missingResource,
                processed: processed?.processed || 0,
                processSkipped: processSkipped || undefined,
                skipReasons: processed?.skipReasons,
                requestId,
                duration,
            }),
            {
                status: 202,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            }
        );
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/webhooks/bc - Unexpected error", { requestId, duration, error: errorMessage });
        await appendBcWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "error",
            error: errorMessage,
        });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}

export async function GET(request: Request) {
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    const validationToken = url.searchParams.get("validationToken");
    if (validationToken) {
        logger.info("GET /api/webhooks/bc - Validation token received", { requestId });
        await appendBcWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "validation",
            location: "query",
        });
        return new Response(validationToken, {
            status: 200,
            headers: {
                "Content-Type": "text/plain",
                "X-Request-ID": requestId,
            },
        });
    }
    return new Response(
        JSON.stringify({
            ok: false,
            error: "Method not allowed. Use POST.",
            supportedMethods: ["POST"],
        }),
        {
            status: 405,
            headers: { "Content-Type": "application/json" },
        }
    );
}
