import { runPollingSync } from "../../../../lib/planner-sync";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    logger.info("POST /api/sync/run-poll - Request received", {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
    });

    try {
        logger.info("Starting polling sync", { requestId });
        const force = url.searchParams.get("force") === "1" || url.searchParams.get("force") === "true";
        const result = await runPollingSync({ force });
        const duration = Date.now() - startTime;
        
        logger.info("POST /api/sync/run-poll - Success", {
            requestId,
            duration,
            resultType: typeof result,
            resultKeys: typeof result === "object" && result !== null ? Object.keys(result) : undefined,
        });
        
        return new Response(JSON.stringify({ 
            ok: true, 
            result,
            requestId,
            duration,
        }), {
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
        
        logger.error("POST /api/sync/run-poll - Polling sync failed", { 
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
