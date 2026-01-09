import { BusinessCentralClient } from "../../../lib/planner-sync/bc-client.js";
import { GraphClient } from "../../../lib/planner-sync/graph-client.js";
import { getGraphConfig, getPlannerConfig, getSyncConfig } from "../../../lib/planner-sync/config.js";
import { listStoredSubscriptions, saveStoredSubscriptions } from "../../../lib/planner-sync/subscriptions-store.js";
import { logger } from "../../../lib/planner-sync/logger.js";

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

function resolveBaseUrl(req) {
    const proto = req.headers["x-forwarded-proto"] || "https";
    const host = req.headers["x-forwarded-host"] || req.headers.host;
    return `${proto}://${host}`;
}

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const body = await readJsonBody(req);
    const graphConfig = getGraphConfig();
    const plannerConfig = getPlannerConfig();
    const syncConfig = getSyncConfig();
    const graphClient = new GraphClient();

    const planIds = new Set();
    if (Array.isArray(body?.planIds)) {
        body.planIds.forEach((id) => id && planIds.add(id));
    }

    if (syncConfig.syncMode === "singlePlan") {
        if (!plannerConfig.defaultPlanId) {
            res.status(400).json({ ok: false, error: "PLANNER_DEFAULT_PLAN_ID required" });
            return;
        }
        planIds.add(plannerConfig.defaultPlanId);
    } else {
        try {
            const bcClient = new BusinessCentralClient();
            const tasks = await bcClient.listProjectTasks("plannerPlanId ne ''");
            for (const task of tasks) {
                if (task.plannerPlanId) planIds.add(task.plannerPlanId);
            }
        } catch (error) {
            logger.warn("Failed to read BC plan IDs for subscriptions", { error: error?.message || String(error) });
        }
        if (!planIds.size && plannerConfig.defaultPlanId) {
            planIds.add(plannerConfig.defaultPlanId);
        }
    }

    if (!planIds.size) {
        res.status(400).json({ ok: false, error: "No plan IDs available for subscription" });
        return;
    }

    const envNotificationUrl = process.env.GRAPH_NOTIFICATION_URL || process.env.PLANNER_NOTIFICATION_URL;
    const baseUrl = body?.notificationUrl || envNotificationUrl || `${resolveBaseUrl(req)}/api/webhooks/graph/planner`;
    if (!baseUrl.startsWith("https://")) {
        logger.warn("Graph subscription notificationUrl is not HTTPS", { notificationUrl: baseUrl });
    }
    const expirationDateTime = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();

    const stored = await listStoredSubscriptions();
    const nowMs = Date.now();
    const activePlanIds = new Set();
    for (const sub of stored) {
        if (!sub.planId || !sub.expirationDateTime) continue;
        const exp = Date.parse(sub.expirationDateTime);
        if (Number.isNaN(exp)) continue;
        if (exp > nowMs + 60 * 1000) {
            activePlanIds.add(sub.planId);
        }
    }

    const created = [];
    for (const planId of planIds) {
        if (activePlanIds.has(planId)) {
            logger.info("Existing subscription is still active; skipping create", { planId });
            continue;
        }
        const resource = `/planner/plans/${planId}/tasks`;
        const subscription = await graphClient.createSubscription({
            changeType: "created,updated,deleted",
            notificationUrl: baseUrl,
            resource,
            expirationDateTime,
            clientState: graphConfig.clientState,
            latestSupportedTlsVersion: "v1_2",
        });
        created.push({
            id: subscription.id,
            planId,
            resource: subscription.resource,
            expirationDateTime: subscription.expirationDateTime,
        });
    }

    const now = new Date().toISOString();
    const updated = stored.filter((item) => !created.find((createdItem) => createdItem.id === item.id));
    for (const subscription of created) {
        updated.push({
            id: subscription.id,
            planId: subscription.planId,
            resource: subscription.resource,
            expirationDateTime: subscription.expirationDateTime,
            createdAt: now,
        });
    }
    await saveStoredSubscriptions(updated);

    res.status(200).json({ ok: true, created, notificationUrl: baseUrl });
}
