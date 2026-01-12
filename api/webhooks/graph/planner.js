import { enqueueAndProcessNotifications } from "../../../lib/planner-sync/index.js";
import { getGraphConfig } from "../../../lib/planner-sync/config.js";
import { logger } from "../../../lib/planner-sync/logger.js";
import { appendWebhookLog } from "../../../lib/planner-sync/webhook-log.js";

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

function respondValidation(res, requestId, token) {
    res.setHeader("Content-Type", "text/plain");
    res.setHeader("X-Request-ID", requestId);
    res.status(200).send(token);
}

export default async function handler(req, res) {
    const requestId = Math.random().toString(36).slice(2, 12);
    const validationToken = req.query?.validationToken;

    if (req.method === "GET") {
        if (validationToken) {
            logger.info("GET /api/webhooks/graph/planner - Validation token received", { requestId });
            await appendWebhookLog({
                ts: new Date().toISOString(),
                requestId,
                type: "validation",
            });
            respondValidation(res, requestId, validationToken);
            return;
        }
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    if (validationToken) {
        logger.info("POST /api/webhooks/graph/planner - Validation token received", { requestId });
        await appendWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "validation",
        });
        respondValidation(res, requestId, validationToken);
        return;
    }

    const payload = await readJsonBody(req);
    if (!payload) {
        await appendWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "invalid_json",
            error: "Invalid JSON",
        });
        res.status(400).json({ ok: false, error: "Invalid JSON" });
        return;
    }

    const notifications = payload?.value || [];
    if (!notifications.length) {
        await appendWebhookLog({
            ts: new Date().toISOString(),
            requestId,
            type: "notification",
            notificationCount: 0,
            validCount: 0,
            invalidCount: 0,
        });
        res.status(202).json({ ok: true, received: 0 });
        return;
    }

    const { clientState } = getGraphConfig();
    let clientStateMismatchCount = 0;
    let clientStateMissingCount = 0;
    let missingTaskIdCount = 0;
    const items = notifications
        .map((notification) => {
            if (clientState && !notification.clientState) {
                clientStateMissingCount += 1;
            } else if (clientState && notification.clientState !== clientState) {
                clientStateMismatchCount += 1;
                return null;
            }
            const taskId = notification.resourceData?.id || notification.resource?.split("/").pop();
            if (!taskId) {
                missingTaskIdCount += 1;
                return null;
            }
            return {
                taskId,
                subscriptionId: notification.subscriptionId,
                receivedAt: new Date().toISOString(),
            };
        })
        .filter(Boolean);

    if (items.length) {
        enqueueAndProcessNotifications(items).catch((error) => {
            logger.error("Failed to enqueue planner notifications", { requestId, error: error?.message || String(error) });
        });
    }

    await appendWebhookLog({
        ts: new Date().toISOString(),
        requestId,
        type: "notification",
        notificationCount: notifications.length,
        validCount: items.length,
        invalidCount: notifications.length - items.length,
        clientStateMismatchCount,
        clientStateMissingCount,
        missingTaskIdCount,
        taskIds: items.map((item) => item.taskId).slice(0, 20),
    });

    res.status(202).json({ ok: true, received: items.length });
}
