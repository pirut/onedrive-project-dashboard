import { runPollingSync, runSmartPollingSync, syncBcToPlanner } from "../../../../../lib/planner-sync";
import { getSyncConfig } from "../../../../../lib/planner-sync/config";
import { getCronSecret, isCronAuthorized } from "../../../../../lib/planner-sync/cron-auth";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    const cronSecret = getCronSecret();
    const provided = request.headers.get("x-cron-secret") || url.searchParams.get("cronSecret") || url.searchParams.get("cron_secret");

    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("GET /api/sync/poll-cron - Unauthorized", { requestId });
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized", requestId }, null, 2), {
            status: 401,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }

    logger.info("GET /api/sync/poll-cron - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const { useSmartPolling, enablePollingFallback } = getSyncConfig();
        if (!enablePollingFallback) {
            const duration = Date.now() - startTime;
            logger.info("GET /api/sync/poll-cron - Skipped (polling disabled)", { requestId, duration });
            return new Response(
                JSON.stringify({ ok: true, skipped: true, reason: "Polling fallback disabled", requestId, duration }, null, 2),
                {
                    status: 200,
                    headers: {
                        "Content-Type": "application/json",
                        "X-Request-ID": requestId,
                    },
                }
            );
        }
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
