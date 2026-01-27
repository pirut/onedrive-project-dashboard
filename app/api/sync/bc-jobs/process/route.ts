import { acquireBcJobLock, releaseBcJobLock } from "../../../../../lib/planner-sync/bc-webhook-store";
import { processBcJobQueue } from "../../../../../lib/planner-sync/bc-job-processor";
import { getCronSecret, isCronAuthorized } from "../../../../../lib/planner-sync/cron-auth";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

async function handle(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    const cronSecret = getCronSecret();
    const provided = request.headers.get("x-cron-secret") || url.searchParams.get("cronSecret") || url.searchParams.get("cron_secret");

    if (!cronSecret || !isCronAuthorized(provided || "")) {
        logger.warn("BC jobs process unauthorized", { requestId });
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized", requestId }), {
            status: 401,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }

    let maxJobs = Number(url.searchParams.get("maxJobs") || "25");
    if (Number.isNaN(maxJobs) || maxJobs <= 0) maxJobs = 25;

    try {
        const bodyText = await request.text();
        if (bodyText) {
            try {
                const body = JSON.parse(bodyText) as { maxJobs?: number };
                if (typeof body?.maxJobs === "number" && body.maxJobs > 0) maxJobs = Math.floor(body.maxJobs);
            } catch {
                // ignore
            }
        }
    } catch {
        // ignore
    }

    const lock = await acquireBcJobLock();
    if (!lock) {
        return new Response(JSON.stringify({ ok: false, error: "Queue locked", requestId }), {
            status: 409,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }

    try {
        const result = await processBcJobQueue({ maxJobs, requestId });
        const duration = Date.now() - startTime;
        return new Response(JSON.stringify({ ok: true, result, requestId, duration }), {
            status: 200,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/bc-jobs/process - Failed", { requestId, duration, error: errorMessage });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    } finally {
        await releaseBcJobLock(lock);
    }
}

export async function POST(request: Request) {
    return handle(request);
}

export async function GET(request: Request) {
    return handle(request);
}
