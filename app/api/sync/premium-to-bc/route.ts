import { syncPremiumChanges } from "../../../../../lib/premium-sync";
import { logger } from "../../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

async function handle(request: Request) {
    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const requestId = body?.requestId ? String(body.requestId) : undefined;

    try {
        const result = await syncPremiumChanges({ requestId });
        return new Response(JSON.stringify({ ok: true, result }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Premium to BC sync failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function GET(request: Request) {
    return handle(request);
}

export async function POST(request: Request) {
    return handle(request);
}
