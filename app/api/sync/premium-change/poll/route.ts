import { runPremiumChangePoll } from "../../../../../lib/premium-sync";
import { getCronSecret, isCronAuthorized } from "../../../../../lib/planner-sync/cron-auth";
import { logger } from "../../../../../lib/planner-sync/logger";

function getProvidedSecret(request: Request) {
    const header = request.headers.get("x-cron-secret") || "";
    if (header) return header;
    const url = new URL(request.url);
    return url.searchParams.get("cronSecret") || url.searchParams.get("cronsecret") || "";
}

export async function GET(request: Request) {
    return handle(request);
}

export async function POST(request: Request) {
    return handle(request);
}

async function handle(request: Request) {
    const expected = getCronSecret();
    const provided = getProvidedSecret(request);
    if (expected && !isCronAuthorized(provided)) {
        return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }, null, 2), {
            status: 401,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        const result = await runPremiumChangePoll();
        return new Response(JSON.stringify({ ok: true, result }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Premium change poll failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
