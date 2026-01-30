import { runPremiumChangePoll } from "../../../../../lib/premium-sync";
import { logger } from "../../../../../lib/planner-sync/logger";

export async function GET(request: Request) {
    return handle(request);
}

export async function POST(request: Request) {
    return handle(request);
}

async function handle(request: Request) {
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
