import { syncBcToPremium, syncPremiumChanges } from "../../../../lib/premium-sync";
import { logger } from "../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

export async function POST(request: Request) {
    const body = await readJsonBody(request);
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const projectNos = Array.isArray(body?.projectNos) ? body.projectNos.map((value: unknown) => String(value).trim()).filter(Boolean) : [];
    const includePremiumChanges = body?.includePremiumChanges === true;

    try {
        const bcResult = await syncBcToPremium(projectNo || undefined, {
            requestId: body?.requestId ? String(body.requestId) : undefined,
            projectNos: projectNos.length ? projectNos : undefined,
            preferPlanner: false,
            forceTaskRecreate: true,
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
        logger.error("BC to Premium reseed failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
