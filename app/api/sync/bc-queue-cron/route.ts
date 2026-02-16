import { previewBcChanges, syncBcToPremium } from "../../../../../lib/premium-sync";
import { logger } from "../../../../../lib/planner-sync/logger";
import { acquireBcJobLock, releaseBcJobLock } from "../../../../../lib/planner-sync/bc-webhook-store";

export const dynamic = "force-dynamic";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

function parseBool(value: unknown) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

function parseNumber(value: unknown, fallback: number) {
    if (value == null || value === "") return fallback;
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
}

function isAuthorized(request: Request, url: URL) {
    const expected = (process.env.CRON_SECRET || "").trim();
    if (!expected) return true;

    const candidates = [
        url.searchParams.get("cronSecret"),
        request.headers.get("x-cron-secret"),
        request.headers.get("x-vercel-cron-secret"),
        (request.headers.get("authorization") || "").replace(/^Bearer\s+/i, ""),
    ]
        .map((value) => String(value || "").trim())
        .filter(Boolean);

    return candidates.includes(expected);
}

async function handle(request: Request) {
    const url = new URL(request.url);
    if (!isAuthorized(request, url)) {
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }, null, 2), {
            status: 401,
            headers: { "Content-Type": "application/json" },
        });
    }

    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const requestId = body?.requestId ? String(body.requestId) : url.searchParams.get("requestId") || undefined;
    const dryRun = parseBool(body?.dryRun ?? url.searchParams.get("dryRun")) === true;
    const lockTtlSeconds = Math.max(
        60,
        Math.floor(parseNumber(process.env.BC_QUEUE_CRON_LOCK_TTL_SECONDS, 600))
    );
    const lock = await acquireBcJobLock(lockTtlSeconds);
    if (!lock) {
        return new Response(JSON.stringify({ ok: true, skipped: true, reason: "locked", requestId }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        if (dryRun) {
            const preview = await previewBcChanges({ requestId });
            return new Response(JSON.stringify({ ok: true, dryRun: true, preview }, null, 2), {
                status: 200,
                headers: { "Content-Type": "application/json" },
            });
        }

        const result = await syncBcToPremium(undefined, {
            requestId,
            preferPlanner: false,
        });

        return new Response(JSON.stringify({ ok: true, dryRun: false, result }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        const errorMessage = (error as Error)?.message || String(error);
        logger.error("BC queue cron sync failed", { error: errorMessage, requestId });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    } finally {
        await releaseBcJobLock(lock);
    }
}

export async function GET(request: Request) {
    return handle(request);
}

export async function POST(request: Request) {
    return handle(request);
}
