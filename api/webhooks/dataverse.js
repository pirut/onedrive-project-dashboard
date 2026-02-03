import crypto from "crypto";
import { appendPremiumWebhookLog, runPremiumSyncDecision } from "../../lib/premium-sync/index.js";
import { logger } from "../../lib/planner-sync/logger.js";

function readEnv(name) {
    return (process.env[name] || "").trim();
}

function safeEqual(a, b) {
    const ab = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    if (ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

function extractTaskIds(payload) {
    if (!payload || typeof payload !== "object") return [];
    const ids = new Set();
    const maybeAdd = (value) => {
        if (typeof value === "string" && value.trim()) ids.add(value.trim());
    };
    maybeAdd(payload.Id || payload.id || payload.primaryEntityId || payload.PrimaryEntityId);
    const inputParams = payload.InputParameters || payload.inputParameters;
    if (Array.isArray(inputParams)) {
        for (const param of inputParams) {
            maybeAdd(param?.Value?.Id || param?.value?.Id || param?.Value?.id || param?.value?.id);
        }
    }
    const target = payload.Target || payload.target;
    if (target) {
        maybeAdd(target.Id || target.id);
    }
    return Array.from(ids);
}

export default async function handler(req, res) {
    const requestId = crypto.randomUUID();
    if (req.method === "GET") {
        logger.info("Dataverse webhook ping", { requestId });
        await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "ping" });
        res.status(200).json({ ok: true, requestId });
        return;
    }

    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const expectedSecret = readEnv("DATAVERSE_WEBHOOK_SECRET");
    if (expectedSecret) {
        const provided = String(
            req.headers["x-dataverse-secret"] ||
                req.headers["x-webhook-secret"] ||
                req.headers["x-ms-dynamics-webhook-key"] ||
                ""
        );
        if (!safeEqual(provided, expectedSecret)) {
            logger.warn("Dataverse webhook unauthorized", {
                requestId,
                headerKeys: Object.keys(req.headers || {}),
            });
            await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "unauthorized" });
            res.status(401).json({ ok: false, error: "Unauthorized" });
            return;
        }
    }
    const payload = await readJsonBody(req);
    if (!payload) {
        logger.warn("Dataverse webhook invalid JSON", { requestId });
        await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "invalid_json" });
        res.status(400).json({ ok: false, error: "Invalid JSON" });
        return;
    }

    const taskIds = extractTaskIds(payload);
    await appendPremiumWebhookLog({
        ts: new Date().toISOString(),
        requestId,
        type: "notification",
        notificationCount: 1,
        taskIds,
    });
    logger.info("Dataverse webhook notification", { requestId, taskIds, notificationCount: 1 });

    try {
        const { decision, result } = await runPremiumSyncDecision({ requestId });
        res.status(200).json({ ok: true, decision, result });
    } catch (error) {
        logger.error("Dataverse webhook processing failed", { requestId, error: error?.message || String(error) });
        await appendPremiumWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "error",
            error: error?.message || String(error),
        });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
