import { enqueueBcJobs, BcWebhookJob } from "../../../../../lib/planner-sync/bc-webhook-store";
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
