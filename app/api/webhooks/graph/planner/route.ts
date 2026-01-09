import { enqueueAndProcessNotifications } from "../../../../../lib/planner-sync";
import { getGraphConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";
import { appendWebhookLog } from "../../../../../lib/planner-sync/webhook-log";

export const dynamic = "force-dynamic";

type GraphNotification = {
    subscriptionId?: string;
    clientState?: string;
    resource?: string;
    resourceData?: {
        id?: string;
    };
};

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    logger.info("POST /api/webhooks/graph/planner - Request received", {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
    });

    try {
        const validationToken = url.searchParams.get("validationToken");
        if (validationToken) {
            logger.info("Validation token received - responding to Graph subscription validation", { requestId });
            await appendWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "validation",
            });
            return new Response(validationToken, {
                status: 200,
                headers: { 
                    "Content-Type": "text/plain",
                    "X-Request-ID": requestId,
                },
            });
        }

        let payload: { value?: GraphNotification[] } | null = null;
        try {
            const bodyText = await request.text();
            logger.debug("Request body received", { requestId, bodyLength: bodyText.length });
            if (bodyText) {
                payload = JSON.parse(bodyText);
                logger.debug("Request body parsed", { 
                    requestId, 
                    notificationCount: payload?.value?.length || 0,
                });
            }
        } catch (parseError) {
            const errorMessage = parseError instanceof Error ? parseError.message : String(parseError);
            logger.error("Failed to parse request body", { 
                requestId, 
                error: errorMessage,
                stack: parseError instanceof Error ? parseError.stack : undefined,
            });
            await appendWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "invalid_json",
                error: errorMessage,
            });
            return new Response(JSON.stringify({ ok: false, error: "Invalid JSON", requestId }), {
                status: 400,
                headers: { 
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }

        const notifications = payload?.value || [];
        logger.info("Processing notifications", { requestId, count: notifications.length });
        
        if (!notifications.length) {
            logger.info("No notifications in payload", { requestId });
            await appendWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "notification",
                notificationCount: 0,
                validCount: 0,
                invalidCount: 0,
            });
            return new Response(JSON.stringify({ ok: true, received: 0, requestId }), {
                status: 202,
                headers: { 
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }

        const { clientState } = getGraphConfig();
        logger.debug("Validating notifications", { requestId, expectedClientState: clientState });
        let clientStateMismatchCount = 0;
        let missingTaskIdCount = 0;
        const items = notifications
            .map((notification, index) => {
                logger.debug("Processing notification", { 
                    requestId, 
                    index, 
                    subscriptionId: notification.subscriptionId,
                    resource: notification.resource,
                    clientState: notification.clientState,
                });
                
                if (notification.clientState !== clientState) {
                    logger.warn("Graph notification clientState mismatch", {
                        requestId,
                        subscriptionId: notification.subscriptionId,
                        expected: clientState,
                        received: notification.clientState,
                    });
                    clientStateMismatchCount += 1;
                    return null;
                }
                const taskId = notification.resourceData?.id || notification.resource?.split("/").pop();
                if (!taskId) {
                    logger.warn("No task ID found in notification", { 
                        requestId, 
                        notification,
                    });
                    missingTaskIdCount += 1;
                    return null;
                }
                logger.debug("Extracted task ID from notification", { requestId, taskId });
                return {
                    taskId,
                    subscriptionId: notification.subscriptionId,
                    receivedAt: new Date().toISOString(),
                };
            })
            .filter(Boolean) as { taskId: string; subscriptionId?: string; receivedAt: string }[];

        logger.info("Valid notifications extracted", { requestId, validCount: items.length, totalCount: notifications.length });

        if (items.length) {
            logger.info("Enqueueing notifications for processing", { requestId, count: items.length });
            enqueueAndProcessNotifications(items).catch((error) => {
                const errorMessage = error instanceof Error ? error.message : String(error);
                logger.error("Failed to enqueue planner notifications", { 
                    requestId,
                    error: errorMessage,
                    stack: error instanceof Error ? error.stack : undefined,
                });
            });
        }

        await appendWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "notification",
            notificationCount: notifications.length,
            validCount: items.length,
            invalidCount: notifications.length - items.length,
            clientStateMismatchCount,
            missingTaskIdCount,
            taskIds: items.map((item) => item.taskId).slice(0, 20),
        });

        const duration = Date.now() - startTime;
        logger.info("POST /api/webhooks/graph/planner - Success", {
            requestId,
            duration,
            received: items.length,
        });

        return new Response(JSON.stringify({ ok: true, received: items.length, requestId, duration }), {
            status: 202,
            headers: { 
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;
        
        logger.error("POST /api/webhooks/graph/planner - Unexpected error", {
            requestId,
            duration,
            error: errorMessage,
            stack: errorStack,
        });
        await appendWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "error",
            error: errorMessage,
        });
        
        return new Response(JSON.stringify({ 
            ok: false, 
            error: errorMessage,
            requestId,
            duration,
            ...(process.env.NODE_ENV === "development" ? { stack: errorStack } : {}),
        }), {
            status: 500,
            headers: { 
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}

// Handle unsupported methods
export async function GET(request: Request) {
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    const validationToken = url.searchParams.get("validationToken");
    if (validationToken) {
        logger.info("GET /api/webhooks/graph/planner - Validation token received", { requestId });
        await appendWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "validation",
        });
        return new Response(validationToken, {
            status: 200,
            headers: {
                "Content-Type": "text/plain",
                "X-Request-ID": requestId,
            },
        });
    }
    return new Response(JSON.stringify({ 
        ok: false, 
        error: "Method not allowed. Use POST.",
        supportedMethods: ["POST"],
    }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
    });
}
