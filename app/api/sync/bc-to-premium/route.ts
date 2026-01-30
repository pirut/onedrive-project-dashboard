import { syncBcToPremium, syncPremiumChanges } from "../../../../lib/premium-sync";
import { logger } from "../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

async function handle(request: Request) {
    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const projectNos = Array.isArray(body?.projectNos) ? body.projectNos.map((value: unknown) => String(value).trim()).filter(Boolean) : [];
    const includePremiumChanges = body?.includePremiumChanges !== false;

    try {
        const bcResult = await syncBcToPremium(projectNo || undefined, {
            requestId: body?.requestId ? String(body.requestId) : undefined,
            projectNos: projectNos.length ? projectNos : undefined,
        });
        let premiumResult = null;
        if (includePremiumChanges) {
            premiumResult = await syncPremiumChanges({ requestId: body?.requestId ? String(body.requestId) : undefined });
        }
        return new Response(JSON.stringify({ ok: true, result: { bcToPremium: bcResult, premiumToBc: premiumResult } }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("BC to Premium sync failed", { error: (error as Error)?.message || String(error) });
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
