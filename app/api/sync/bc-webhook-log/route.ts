import { listBcWebhookLog } from "../../../../lib/planner-sync/bc-webhook-log";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const url = new URL(request.url);
    const rawLimit = Number(url.searchParams.get("limit") || 50);
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? rawLimit : 50;
    const items = await listBcWebhookLog(limit);
    return new Response(JSON.stringify({ ok: true, items }, null, 2), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}
