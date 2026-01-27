import { listPlannerLog } from "../../../../lib/planner-sync/planner-log";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const url = new URL(request.url);
    const rawLimit = Number(url.searchParams.get("limit") || 200);
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? rawLimit : 200;
    const items = await listPlannerLog(limit);
    return new Response(JSON.stringify({ ok: true, items }, null, 2), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}
