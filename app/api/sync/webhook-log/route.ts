import { listPremiumWebhookLog } from "../../../../lib/premium-sync";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const url = new URL(request.url);
    const rawLimit = Number(url.searchParams.get("limit") || 50);
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? rawLimit : 50;
    const items = await listPremiumWebhookLog(limit);
    return new Response(JSON.stringify({ ok: true, items }, null, 2), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}
