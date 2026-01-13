import { runPollingSync, runSmartPollingSync, syncBcToPlanner } from "../../../../../lib/planner-sync";
import { getSyncConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("GET /api/sync/poll-cron - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const { useSmartPolling } = getSyncConfig();
        if (useSmartPolling) {
            const smartResult = await runSmartPollingSync();
            const duration = Date.now() - startTime;

            logger.info("GET /api/sync/poll-cron - Smart polling success", {
                requestId,
                duration,
            });

            return new Response(
                JSON.stringify({ ok: true, result: { smartPolling: smartResult }, requestId, duration }, null, 2),
                {
                    status: 200,
                    headers: {
                        "Content-Type": "application/json",
                        "X-Request-ID": requestId,
                    },
                }
            );
        }

        const bcResult = await syncBcToPlanner();
        const pollResult = await runPollingSync();
        const duration = Date.now() - startTime;

        logger.info("GET /api/sync/poll-cron - Success", {
            requestId,
            duration,
        });

        return new Response(
            JSON.stringify({ ok: true, result: { bcToPlanner: bcResult, plannerToBc: pollResult }, requestId, duration }, null, 2),
            {
                status: 200,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            }
        );
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("GET /api/sync/poll-cron - Failed", { requestId, duration, error: errorMessage });

        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }, null, 2), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}
