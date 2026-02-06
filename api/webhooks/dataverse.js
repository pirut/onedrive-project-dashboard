import crypto from "crypto";
import { appendPremiumWebhookLog, syncPremiumTaskIds } from "../../lib/premium-sync/index.js";
import { wasPremiumTaskIdUpdatedByBc } from "../../lib/premium-sync/bc-write-store.js";
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
    const guidPattern = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
    const normalizeGuid = (value) => {
        const trimmed = String(value || "").trim().replace(/^\{/, "").replace(/\}$/, "");
        return guidPattern.test(trimmed) ? trimmed : "";
    };
    const maybeAdd = (value) => {
        const guid = normalizeGuid(value);
        if (guid) ids.add(guid);
    };
    const maybeExtractFromEntityRef = (value) => {
        if (!value || typeof value !== "object") return;
        maybeAdd(value.Id || value.id);
    };
    const extractFromObject = (value) => {
        if (!value || typeof value !== "object") return;
        maybeAdd(value.Id || value.id || value.primaryEntityId || value.PrimaryEntityId);
        maybeExtractFromEntityRef(value.Target || value.target);
        maybeExtractFromEntityRef(value.EntityReference || value.entityReference);

        const inputParams = value.InputParameters || value.inputParameters;
        if (Array.isArray(inputParams)) {
            for (const param of inputParams) {
                maybeExtractFromEntityRef(param?.Value || param?.value);
                maybeExtractFromEntityRef(param?.Parameter || param?.parameter);
            }
        } else if (inputParams && typeof inputParams === "object") {
            for (const paramValue of Object.values(inputParams)) {
                maybeExtractFromEntityRef(paramValue?.Value || paramValue?.value || paramValue);
            }
        }

        const imageCollections = [
            value.PreEntityImages,
            value.PostEntityImages,
            value.preEntityImages,
            value.postEntityImages,
        ];
        for (const collection of imageCollections) {
            if (!collection || typeof collection !== "object") continue;
            for (const image of Object.values(collection)) {
                maybeExtractFromEntityRef(image);
            }
        }
    };

    const items = Array.isArray(payload.value) ? payload.value : [payload];
    for (const item of items) {
        extractFromObject(item);
        if (Array.isArray(item?.value)) {
            for (const nested of item.value) {
                extractFromObject(nested);
            }
        }
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
    const filtered = [];
    const ignored = [];
    if (taskIds.length) {
        await Promise.all(
            taskIds.map(async (id) => {
                if (await wasPremiumTaskIdUpdatedByBc(id)) {
                    ignored.push(id);
                } else {
                    filtered.push(id);
                }
            })
        );
    }
    await appendPremiumWebhookLog({
        ts: new Date().toISOString(),
        requestId,
        type: "notification",
        notificationCount: 1,
        taskIds: filtered,
        ignoredTaskIds: ignored,
    });
    logger.info("Dataverse webhook notification", {
        requestId,
        taskIds: filtered,
        ignored: ignored.length,
        notificationCount: 1,
    });

    if (!filtered.length) {
        const reason = taskIds.length ? "bc_origin" : "no_task_ids";
        if (reason === "bc_origin") {
            logger.info("Dataverse webhook ignored BC-origin task ids", { requestId, ignored: ignored.length });
        } else {
            logger.warn("Dataverse webhook missing task ids", { requestId });
        }
        await appendPremiumWebhookLog({ ts: new Date().toISOString(), requestId, type: "skipped", reason });
        res.status(200).json({ ok: true, skipped: true, reason, ignored });
        return;
    }

    try {
        const result = await syncPremiumTaskIds(filtered, { requestId, respectPreferBc: false });
        res.status(200).json({ ok: true, taskIds: filtered, ignored, result });
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
