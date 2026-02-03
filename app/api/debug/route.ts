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
            "POST /api/sync/bc-to-premium": "BC → Planner Premium sync",
            "POST /api/sync/premium-to-bc": "Premium → BC sync (Dataverse delta)",
            "POST /api/sync/auto": "Auto sync (choose most recent changes)",
            "GET /api/sync/premium-project-link": "Resolve Premium plan link for a BC project",
            "GET /api/sync/debug-bookable-resource": "Debug Dataverse bookable resource lookup",
            "GET /api/sync/list-bookable-resources": "List Dataverse bookable resources",
            "GET /api/sync/debug-project-team": "List Dataverse project team members",
            "GET /api/sync/debug-operation-sets": "List Dataverse operation sets (schedule API)",
            "POST /api/sync/clear-operation-sets": "Delete Dataverse operation sets (schedule API)",
            "GET /api/sync/debug-dataverse-webhook": "List Dataverse webhook endpoints + steps",
            "GET /api/sync/debug-dataverse-webhook-jobs": "List Dataverse webhook async jobs",
            "POST /api/sync/premium-change/poll": "Poll Premium changes via Dataverse delta (legacy)",
            "GET /api/sync/projects": "List Premium project sync state",
            "POST /api/sync/projects": "Toggle Premium project sync / clear links",
            "GET /api/sync/debug-bc-timestamps": "Inspect BC modified timestamps",
            "GET /api/sync/premium-test": "Verify Dataverse connectivity",
            "POST /api/webhooks/dataverse": "Dataverse webhook endpoint",
            "POST /api/webhooks/bc": "Business Central webhook endpoint",
            "POST /api/sync/bc-subscriptions/create": "Create BC webhook subscriptions",
            "POST /api/sync/bc-subscriptions/renew": "Renew BC webhook subscriptions",
            "POST /api/sync/bc-subscriptions/delete": "Delete BC webhook subscriptions",
            "POST /api/sync/clear-bc-sync-lock": "Clear BC syncLock for a task",
            "POST /api/sync/register-dataverse-webhook": "Register Dataverse webhook for Premium changes",
            "POST /api/sync/bc-jobs/process": "Process queued BC webhook jobs",
            "GET /api/sync/premium-log": "List Premium sync logs",
            "GET /api/sync/webhook-log": "List recent Dataverse webhook deliveries",
            "GET /api/sync/webhook-log-stream?include=1": "Stream Dataverse webhook deliveries (SSE)",
            "GET /api/sync/bc-webhook-log": "List recent BC webhook deliveries",
            "GET /api/sync/bc-webhook-log-stream?include=1": "Stream BC webhook deliveries (SSE)",
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
            "POST /api/sync/bc-to-premium": "BC → Planner Premium sync",
            "POST /api/sync/premium-to-bc": "Premium → BC sync (Dataverse delta)",
            "POST /api/sync/auto": "Auto sync (choose most recent changes)",
            "GET /api/sync/premium-project-link": "Resolve Premium plan link for a BC project",
            "GET /api/sync/debug-bookable-resource": "Debug Dataverse bookable resource lookup",
            "GET /api/sync/list-bookable-resources": "List Dataverse bookable resources",
            "GET /api/sync/debug-project-team": "List Dataverse project team members",
            "GET /api/sync/debug-operation-sets": "List Dataverse operation sets (schedule API)",
            "POST /api/sync/clear-operation-sets": "Delete Dataverse operation sets (schedule API)",
            "GET /api/sync/debug-dataverse-webhook": "List Dataverse webhook endpoints + steps",
            "GET /api/sync/debug-dataverse-webhook-jobs": "List Dataverse webhook async jobs",
            "POST /api/sync/premium-change/poll": "Poll Premium changes via Dataverse delta (legacy)",
            "GET /api/sync/projects": "List Premium project sync state",
            "POST /api/sync/projects": "Toggle Premium project sync / clear links",
            "GET /api/sync/debug-bc-timestamps": "Inspect BC modified timestamps",
            "GET /api/sync/premium-test": "Verify Dataverse connectivity",
            "POST /api/webhooks/dataverse": "Dataverse webhook endpoint",
            "POST /api/webhooks/bc": "Business Central webhook endpoint",
            "POST /api/sync/bc-subscriptions/create": "Create BC webhook subscriptions",
            "POST /api/sync/bc-subscriptions/renew": "Renew BC webhook subscriptions",
            "POST /api/sync/bc-subscriptions/delete": "Delete BC webhook subscriptions",
            "POST /api/sync/clear-bc-sync-lock": "Clear BC syncLock for a task",
            "POST /api/sync/register-dataverse-webhook": "Register Dataverse webhook for Premium changes",
            "POST /api/sync/bc-jobs/process": "Process queued BC webhook jobs",
            "GET /api/sync/premium-log": "List Premium sync logs",
            "GET /api/sync/webhook-log": "List recent Dataverse webhook deliveries",
            "GET /api/sync/webhook-log-stream?include=1": "Stream Dataverse webhook deliveries (SSE)",
            "GET /api/sync/bc-webhook-log": "List recent BC webhook deliveries",
            "GET /api/sync/bc-webhook-log-stream?include=1": "Stream BC webhook deliveries (SSE)",
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
