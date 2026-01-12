import { runPollingSync, syncBcToPlanner } from "../../../../lib/planner-sync";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    logger.info("POST /api/sync/run-bc-to-planner - Request received", {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
    });

    try {
        let body: { projectNo?: string } | null = null;
        try {
            const bodyText = await request.text();
            logger.debug("Request body received", { requestId, bodyLength: bodyText.length });
            if (bodyText) {
                body = JSON.parse(bodyText);
                logger.debug("Request body parsed", { requestId, body });
            }
        } catch (parseError) {
            logger.warn("Failed to parse request body", { 
                requestId, 
                error: parseError instanceof Error ? parseError.message : String(parseError),
                stack: parseError instanceof Error ? parseError.stack : undefined,
            });
            body = null;
        }
        
        const projectNo = body?.projectNo?.trim();
        logger.info("Processing sync request", { requestId, projectNo: projectNo || "all projects" });

        try {
            const bcResult = await syncBcToPlanner(projectNo || undefined);
            const pollResult = await runPollingSync();
            const result = { bcToPlanner: bcResult, plannerToBc: pollResult };
            const duration = Date.now() - startTime;
            logger.info("POST /api/sync/run-bc-to-planner - Success", {
                requestId,
                duration,
                projectNo: projectNo || "all",
                resultCount: Array.isArray(result) ? result.length : typeof result === "object" ? Object.keys(result).length : 1,
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
            
            logger.error("BC to Planner sync failed", { 
                requestId,
                duration,
                error: errorMessage,
                stack: errorStack,
                projectNo: projectNo || "all",
            });
            
            return new Response(JSON.stringify({ 
                ok: false, 
                error: errorMessage,
                requestId,
                duration,
                ...(process.env.NODE_ENV === "development" ? { stack: errorStack } : {}),
            }), {
                status: 400,
                headers: { 
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;
        
        logger.error("POST /api/sync/run-bc-to-planner - Unexpected error", {
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
