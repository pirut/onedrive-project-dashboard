import crypto from "crypto";
import { appendPremiumWebhookLog, syncPremiumTaskIds } from "../../../../lib/premium-sync";
import { logger } from "../../../../lib/planner-sync/logger";

function safeEqual(a: string, b: string) {
    const ab = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    if (ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

function extractTaskIds(payload: Record<string, unknown>) {
    const ids = new Set<string>();
    const maybeAdd = (value: unknown) => {
        if (typeof value === "string" && value.trim()) ids.add(value.trim());
    };
    maybeAdd((payload as { Id?: string }).Id || (payload as { id?: string }).id);
    maybeAdd((payload as { primaryEntityId?: string }).primaryEntityId || (payload as { PrimaryEntityId?: string }).PrimaryEntityId);
    const inputParams = (payload as { InputParameters?: unknown }).InputParameters || (payload as { inputParameters?: unknown }).inputParameters;
    if (Array.isArray(inputParams)) {
        for (const param of inputParams) {
            const value = (param as { Value?: { Id?: string; id?: string } })?.Value || (param as { value?: { Id?: string; id?: string } })?.value;
            maybeAdd(value?.Id || value?.id);
        }
    }
    const target = (payload as { Target?: { Id?: string; id?: string } }).Target || (payload as { target?: { Id?: string; id?: string } }).target;
    if (target) {
        maybeAdd(target.Id || target.id);
    }
    return Array.from(ids);
}

export async function GET() {
    const requestId = crypto.randomUUID();
    await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "ping" });
    return new Response(JSON.stringify({ ok: true, requestId }, null, 2), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}

export async function POST(request: Request) {
    const requestId = crypto.randomUUID();
    const expectedSecret = (process.env.DATAVERSE_WEBHOOK_SECRET || "").trim();
    if (expectedSecret) {
        const provided =
            request.headers.get("x-dataverse-secret") ||
            request.headers.get("x-webhook-secret") ||
            request.headers.get("x-ms-dynamics-webhook-key") ||
            "";
        if (!safeEqual(provided, expectedSecret)) {
            await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "unauthorized" });
            return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }, null, 2), {
                status: 401,
                headers: { "Content-Type": "application/json" },
            });
        }
    }

    let payload: Record<string, unknown> | null = null;
    try {
        payload = (await request.json()) as Record<string, unknown>;
    } catch {
        payload = null;
    }

    if (!payload) {
        await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "invalid_json" });
        return new Response(JSON.stringify({ ok: false, error: "Invalid JSON" }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    const taskIds = extractTaskIds(payload);
    await appendPremiumWebhookLog({
        ts: new Date().toISOString(),
        requestId,
        type: "notification",
        notificationCount: 1,
        taskIds,
    });

    if (!taskIds.length) {
        await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "skipped", reason: "no_task_ids" });
        return new Response(JSON.stringify({ ok: true, skipped: true, reason: "no_task_ids" }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        const result = await syncPremiumTaskIds(taskIds, { requestId });
        return new Response(JSON.stringify({ ok: true, taskIds, result }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Dataverse webhook processing failed", { requestId, error: (error as Error)?.message || String(error) });
        await appendPremiumWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "error",
            error: (error as Error)?.message || String(error),
        });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}
