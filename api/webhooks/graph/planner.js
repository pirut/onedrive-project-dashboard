import { enqueueAndProcessNotifications } from "../../../lib/planner-sync/index.js";
import { getGraphConfig } from "../../../lib/planner-sync/config.js";
import { logger } from "../../../lib/planner-sync/logger.js";

function readOrigin(req) {
    const proto = req.headers["x-forwarded-proto"] || "https";
    const host = req.headers["x-forwarded-host"] || req.headers.host;
    return `${proto}://${host}`;
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

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key");
    if (req.method === "OPTIONS") {
        res.status(204).end();
        return;
    }
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const url = new URL(req.url, readOrigin(req));
    const validationToken = url.searchParams.get("validationToken");
    if (validationToken) {
        res.status(200).setHeader("Content-Type", "text/plain");
        res.end(validationToken);
        return;
    }

    const payload = await readJsonBody(req);
    if (!payload) {
        res.status(400).json({ ok: false, error: "Invalid JSON" });
        return;
    }

    const notifications = Array.isArray(payload.value) ? payload.value : [];
    if (!notifications.length) {
        res.status(202).json({ ok: true, received: 0 });
        return;
    }

    const { clientState } = getGraphConfig();
    const items = notifications
        .map((notification) => {
            if (notification.clientState !== clientState) {
                logger.warn("Graph notification clientState mismatch", {
                    subscriptionId: notification.subscriptionId,
                });
                return null;
            }
            const taskId = notification.resourceData?.id || notification.resource?.split("/").pop();
            if (!taskId) return null;
            return {
                taskId,
                subscriptionId: notification.subscriptionId,
                receivedAt: new Date().toISOString(),
            };
        })
        .filter(Boolean);

    if (items.length) {
        enqueueAndProcessNotifications(items).catch((error) => {
            logger.error("Failed to enqueue planner notifications", { error: error?.message || String(error) });
        });
    }

    res.status(202).json({ ok: true, received: items.length });
}
