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
    const guidPattern = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
    const normalizeGuid = (value: unknown) => {
        const trimmed = String(value || "").trim().replace(/^\{/, "").replace(/\}$/, "");
        return guidPattern.test(trimmed) ? trimmed : "";
    };
    const maybeAdd = (value: unknown) => {
        const guid = normalizeGuid(value);
        if (guid) ids.add(guid);
    };
    const maybeExtractFromEntityRef = (value: unknown) => {
        if (!value || typeof value !== "object") return;
        maybeAdd((value as { Id?: string }).Id || (value as { id?: string }).id);
    };
    const extractFromObject = (value: unknown) => {
        if (!value || typeof value !== "object") return;
        const objectValue = value as Record<string, unknown>;
        maybeAdd(objectValue.Id || objectValue.id || objectValue.primaryEntityId || objectValue.PrimaryEntityId);
        maybeExtractFromEntityRef(objectValue.Target || objectValue.target);
        maybeExtractFromEntityRef(objectValue.EntityReference || objectValue.entityReference);

        const inputParams = objectValue.InputParameters || objectValue.inputParameters;
        if (Array.isArray(inputParams)) {
            for (const param of inputParams) {
                const parameter = param as Record<string, unknown>;
                maybeExtractFromEntityRef(parameter.Value || parameter.value);
                maybeExtractFromEntityRef(parameter.Parameter || parameter.parameter);
            }
        } else if (inputParams && typeof inputParams === "object") {
            for (const paramValue of Object.values(inputParams as Record<string, unknown>)) {
                if (paramValue && typeof paramValue === "object") {
                    const valueRecord = paramValue as Record<string, unknown>;
                    maybeExtractFromEntityRef(valueRecord.Value || valueRecord.value || valueRecord);
                }
            }
        }

        const imageCollections = [
            objectValue.PreEntityImages,
            objectValue.PostEntityImages,
            objectValue.preEntityImages,
            objectValue.postEntityImages,
        ];
        for (const collection of imageCollections) {
            if (!collection || typeof collection !== "object") continue;
            for (const image of Object.values(collection as Record<string, unknown>)) {
                maybeExtractFromEntityRef(image);
            }
        }
    };

    const items = Array.isArray((payload as { value?: unknown[] }).value)
        ? ((payload as { value?: unknown[] }).value as unknown[])
        : [payload];
    for (const item of items) {
        extractFromObject(item);
        if (item && typeof item === "object" && Array.isArray((item as { value?: unknown[] }).value)) {
            for (const nested of (item as { value?: unknown[] }).value || []) {
                extractFromObject(nested);
            }
        }
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
        logger.warn("Dataverse webhook missing task ids", { requestId });
        return new Response(JSON.stringify({ ok: true, skipped: true, reason: "no_task_ids" }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        const result = await syncPremiumTaskIds(taskIds, { requestId, respectPreferBc: false });
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
