import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { getGraphConfig, getPlannerConfig, getSyncConfig } from "../../../../../lib/planner-sync/config";
import { listStoredSubscriptions, saveStoredSubscriptions } from "../../../../../lib/planner-sync/subscriptions-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

function resolveBaseUrl(request: Request) {
    const url = new URL(request.url);
    const proto = request.headers.get("x-forwarded-proto") || url.protocol.replace(":", "");
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || url.host;
    return `${proto}://${host}`;
}

export async function POST(request: Request) {
    let body: { notificationUrl?: string; planIds?: string[] } | null = null;
    try {
        body = await request.json();
    } catch {
        body = null;
    }

    const graphConfig = getGraphConfig();
    const plannerConfig = getPlannerConfig();
    const syncConfig = getSyncConfig();
    const graphClient = new GraphClient();

    const planIds = new Set<string>();
    if (Array.isArray(body?.planIds)) {
        body?.planIds?.forEach((id) => id && planIds.add(id));
    }

    if (syncConfig.syncMode === "singlePlan") {
        if (!plannerConfig.defaultPlanId) {
            return new Response(JSON.stringify({ ok: false, error: "PLANNER_DEFAULT_PLAN_ID required" }), {
                status: 400,
                headers: { "Content-Type": "application/json" },
            });
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
            logger.warn("Failed to read BC plan IDs for subscriptions", { error: (error as Error)?.message });
        }
        if (!planIds.size && plannerConfig.defaultPlanId) {
            planIds.add(plannerConfig.defaultPlanId);
        }
    }

    if (!planIds.size) {
        return new Response(JSON.stringify({ ok: false, error: "No plan IDs available for subscription" }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    const baseUrl = body?.notificationUrl || `${resolveBaseUrl(request)}/api/webhooks/graph/planner`;
    if (!baseUrl.startsWith("https://")) {
        logger.warn("Graph subscription notificationUrl is not HTTPS", { notificationUrl: baseUrl });
    }
    const expirationDateTime = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();

    const stored = await listStoredSubscriptions();
    const nowMs = Date.now();
    const activePlanIds = new Set<string>();
    for (const sub of stored) {
        if (!sub.planId || !sub.expirationDateTime) continue;
        const exp = Date.parse(sub.expirationDateTime);
        if (Number.isNaN(exp)) continue;
        if (exp > nowMs + 60 * 1000) {
            activePlanIds.add(sub.planId);
        }
    }

    const created = [] as { id: string; planId: string; resource?: string; expirationDateTime?: string }[];
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
    const updated = stored.filter(
        (item) => !created.find((createdItem) => createdItem.id === item.id)
    );
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

    return new Response(JSON.stringify({ ok: true, created }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}
