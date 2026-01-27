import { syncPlannerPlanTitlesAndDedupe } from "../../../../lib/planner-sync";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    let body: { projectNo?: string; dryRun?: boolean } | null = null;
    try {
        const text = await request.text();
        if (text) {
            body = JSON.parse(text);
        }
    } catch (error) {
        logger.warn("POST /api/sync/plan-maintenance - Failed to parse body", {
            requestId,
            error: error instanceof Error ? error.message : String(error),
        });
        body = null;
    }

    const projectNo = body?.projectNo?.trim();
    const dryRun = Boolean(body?.dryRun);

    logger.info("POST /api/sync/plan-maintenance - Request received", {
        requestId,
        url: url.toString(),
        projectNo: projectNo || "all",
        dryRun,
    });

    try {
        const result = await syncPlannerPlanTitlesAndDedupe({
            projectNo: projectNo || undefined,
            dryRun,
        });
        const duration = Date.now() - startTime;
        return new Response(
            JSON.stringify({ ok: true, result, requestId, duration }),
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
        const message = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/plan-maintenance - Failed", {
            requestId,
            error: message,
            duration,
        });
        return new Response(
            JSON.stringify({ ok: false, error: message, requestId, duration }),
            {
                status: 400,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            }
        );
    }
}
