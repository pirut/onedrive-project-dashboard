import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { listStoredSubscriptions, saveStoredSubscriptions } from "../../../../../lib/planner-sync/subscriptions-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    logger.info("POST /api/sync/subscriptions/renew - Request received", {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
    });

    try {
        logger.info("Loading stored subscriptions", { requestId });
        const stored = await listStoredSubscriptions();
        logger.info("Loaded stored subscriptions", { requestId, count: stored.length });
        
        if (!stored.length) {
            logger.info("No subscriptions to renew", { requestId });
            return new Response(JSON.stringify({ ok: true, renewed: 0, requestId }), {
                status: 200,
                headers: { 
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }

        const graphClient = new GraphClient();
        const expirationDateTime = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();
        logger.info("Renewing subscriptions", { requestId, expirationDateTime, count: stored.length });
        
        const renewed: { id: string; expirationDateTime?: string }[] = [];
        const remaining = [] as typeof stored;

        for (const sub of stored) {
            try {
                logger.debug("Renewing subscription", { requestId, subscriptionId: sub.id, planId: sub.planId });
                const updated = await graphClient.renewSubscription(sub.id, expirationDateTime);
                if (updated?.expirationDateTime) {
                    logger.info("Subscription renewed successfully", { requestId, subscriptionId: sub.id });
                    renewed.push({ id: sub.id, expirationDateTime: updated.expirationDateTime });
                    remaining.push({
                        ...sub,
                        expirationDateTime: updated.expirationDateTime,
                    });
                } else {
                    logger.warn("Subscription renewal returned no expiration", { requestId, subscriptionId: sub.id });
                    remaining.push({
                        ...sub,
                        expirationDateTime,
                    });
                }
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                logger.warn("Subscription renewal failed", { 
                    requestId,
                    subscriptionId: sub.id, 
                    error: errorMessage,
                    stack: error instanceof Error ? error.stack : undefined,
                });
                // Continue with other subscriptions even if one fails
            }
        }

        logger.info("Saving updated subscriptions", { requestId, count: remaining.length });
        await saveStoredSubscriptions(remaining);

        const duration = Date.now() - startTime;
        logger.info("POST /api/sync/subscriptions/renew - Success", {
            requestId,
            duration,
            renewed: renewed.length,
            total: stored.length,
        });

        return new Response(JSON.stringify({ ok: true, renewed, total: stored.length, requestId, duration }), {
            status: 200,
            headers: { 
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;
        
        logger.error("POST /api/sync/subscriptions/renew - Unexpected error", {
            requestId,
            duration,
            error: errorMessage,
            stack: errorStack,
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
export async function GET() {
    return new Response(JSON.stringify({ 
        ok: false, 
        error: "Method not allowed. Use POST.",
        supportedMethods: ["POST"],
    }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
    });
}
