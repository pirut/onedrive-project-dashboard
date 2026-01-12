import { logger } from "../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    const info = {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
        timestamp: new Date().toISOString(),
        routes: {
            "GET /api/debug": "Debug endpoint - shows request info and available routes",
            "POST /api/debug": "Debug endpoint - shows request info with body",
            "POST /api/sync/run-bc-to-planner": "Sync Business Central to Planner",
            "POST /api/sync/run-poll": "Run polling sync",
            "GET /api/sync/planner-test": "Verify Planner API connectivity",
            "POST /api/sync/subscriptions/create": "Create Graph subscriptions",
            "POST /api/sync/subscriptions/delete": "Delete Graph subscriptions",
            "POST /api/sync/subscriptions/renew": "Renew Graph subscriptions",
            "POST /api/webhooks/graph/planner": "Graph webhook endpoint",
        },
    };

    logger.info("GET /api/debug - Debug info requested", info);

    return new Response(JSON.stringify({ 
        ok: true, 
        ...info,
        duration: Date.now() - startTime,
    }, null, 2), {
        status: 200,
        headers: { 
            "Content-Type": "application/json",
            "X-Request-ID": requestId,
        },
    });
}

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    let body: unknown = null;
    try {
        const bodyText = await request.text();
        if (bodyText) {
            body = JSON.parse(bodyText);
        }
    } catch {
        body = null;
    }

    const info = {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
        body,
        timestamp: new Date().toISOString(),
        routes: {
            "GET /api/debug": "Debug endpoint - shows request info and available routes",
            "POST /api/debug": "Debug endpoint - shows request info with body",
            "POST /api/sync/run-bc-to-planner": "Sync Business Central to Planner",
            "POST /api/sync/run-poll": "Run polling sync",
            "GET /api/sync/planner-test": "Verify Planner API connectivity",
            "POST /api/sync/subscriptions/create": "Create Graph subscriptions",
            "POST /api/sync/subscriptions/delete": "Delete Graph subscriptions",
            "POST /api/sync/subscriptions/renew": "Renew Graph subscriptions",
            "POST /api/webhooks/graph/planner": "Graph webhook endpoint",
        },
    };

    logger.info("POST /api/debug - Debug info with body requested", info);

    return new Response(JSON.stringify({ 
        ok: true, 
        ...info,
        duration: Date.now() - startTime,
    }, null, 2), {
        status: 200,
        headers: { 
            "Content-Type": "application/json",
            "X-Request-ID": requestId,
        },
    });
}
