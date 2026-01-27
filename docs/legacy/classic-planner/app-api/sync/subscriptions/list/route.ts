import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("GET /api/sync/subscriptions/list - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const graphClient = new GraphClient();
        const all = await graphClient.listSubscriptions();
        const planner = all.filter((sub) => (sub.resource || "").includes("/planner/"));

        const duration = Date.now() - startTime;
        logger.info("GET /api/sync/subscriptions/list - Success", {
            requestId,
            duration,
            total: all.length,
            plannerCount: planner.length,
        });

        return new Response(JSON.stringify({
            ok: true,
            total: all.length,
            plannerCount: planner.length,
            items: planner,
            requestId,
            duration,
        }, null, 2), {
            status: 200,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("GET /api/sync/subscriptions/list - Failed", {
            requestId,
            duration,
            error: errorMessage,
        });
        return new Response(JSON.stringify({
            ok: false,
            error: errorMessage,
            requestId,
            duration,
        }, null, 2), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}
