import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { getGraphConfig, getPlannerConfig, getSyncConfig } from "../../../../../lib/planner-sync/config";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "../../../../../lib/planner-sync/project-sync-store";
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
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    
    logger.info("POST /api/sync/subscriptions/create - Request received", {
        requestId,
        method: request.method,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
        headers: Object.fromEntries(request.headers.entries()),
    });

    try {
        let body: { notificationUrl?: string; planIds?: string[] } | null = null;
        try {
            const bodyText = await request.text();
            logger.debug("Request body received", { requestId, bodyLength: bodyText.length });
            if (bodyText) {
                body = JSON.parse(bodyText);
                logger.debug("Request body parsed", { requestId, hasNotificationUrl: !!body?.notificationUrl, planIdsCount: body?.planIds?.length || 0 });
            }
        } catch (parseError) {
            logger.warn("Failed to parse request body", { 
                requestId, 
                error: parseError instanceof Error ? parseError.message : String(parseError),
                stack: parseError instanceof Error ? parseError.stack : undefined,
            });
            body = null;
        }

    const graphConfig = getGraphConfig();
    const plannerConfig = getPlannerConfig();
    const syncConfig = getSyncConfig();
    const graphClient = new GraphClient();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());

    const planIds = new Set<string>();
    if (Array.isArray(body?.planIds)) {
        body?.planIds?.forEach((id) => id && planIds.add(id));
    }

    logger.info("Resolving plan IDs for subscription", { 
        requestId, 
        syncMode: syncConfig.syncMode,
        hasDefaultPlanId: !!plannerConfig.defaultPlanId,
    });

    if (syncConfig.syncMode === "singlePlan") {
        if (!plannerConfig.defaultPlanId) {
            logger.error("PLANNER_DEFAULT_PLAN_ID required but not configured", { requestId });
            return new Response(JSON.stringify({ ok: false, error: "PLANNER_DEFAULT_PLAN_ID required", requestId }), {
                status: 400,
                headers: { 
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }
        planIds.add(plannerConfig.defaultPlanId);
        logger.info("Using single plan mode", { requestId, planId: plannerConfig.defaultPlanId });
    } else {
        try {
            logger.info("Fetching plan IDs from Business Central", { requestId });
            const bcClient = new BusinessCentralClient();
            const tasks = await bcClient.listProjectTasks("plannerPlanId ne ''");
            logger.info("Retrieved tasks from BC", { requestId, taskCount: tasks.length });
            for (const task of tasks) {
                if (disabledProjects.has(normalizeProjectNo(task.projectNo))) continue;
                if (task.plannerPlanId) planIds.add(task.plannerPlanId);
            }
            logger.info("Extracted plan IDs from tasks", { requestId, planIdsCount: planIds.size });
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            logger.warn("Failed to read BC plan IDs for subscriptions", { 
                requestId,
                error: errorMessage,
                stack: error instanceof Error ? error.stack : undefined,
            });
        }
        if (!planIds.size && plannerConfig.defaultPlanId) {
            logger.info("No plan IDs from BC, using default", { requestId, planId: plannerConfig.defaultPlanId });
            planIds.add(plannerConfig.defaultPlanId);
        }
    }

    if (!planIds.size) {
        logger.error("No plan IDs available for subscription", { requestId });
        return new Response(JSON.stringify({ ok: false, error: "No plan IDs available for subscription", requestId }), {
            status: 400,
            headers: { 
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }

    const envNotificationUrl = process.env.GRAPH_NOTIFICATION_URL || process.env.PLANNER_NOTIFICATION_URL;
    const baseUrl = body?.notificationUrl || envNotificationUrl || `${resolveBaseUrl(request)}/api/webhooks/graph/planner`;
    logger.info("Using notification URL", { requestId, baseUrl });
    
    if (!baseUrl.startsWith("https://")) {
        logger.warn("Graph subscription notificationUrl is not HTTPS", { requestId, notificationUrl: baseUrl });
    }
    const expirationDateTime = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();
    logger.info("Subscription expiration", { requestId, expirationDateTime });

    logger.info("Loading stored subscriptions", { requestId });
    const stored = await listStoredSubscriptions();
    logger.info("Loaded stored subscriptions", { requestId, count: stored.length });
    
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
    logger.info("Active subscriptions found", { requestId, activeCount: activePlanIds.size, activePlanIds: Array.from(activePlanIds) });

    const created = [] as { id: string; planId: string; resource?: string; expirationDateTime?: string }[];
    for (const planId of planIds) {
        if (activePlanIds.has(planId)) {
            logger.info("Existing subscription is still active; skipping create", { requestId, planId });
            continue;
        }
        logger.info("Creating subscription for plan", { requestId, planId });
        try {
            const resource = `/planner/plans/${planId}/tasks`;
            const subscription = await graphClient.createSubscription({
                changeType: "created,updated,deleted",
                notificationUrl: baseUrl,
                resource,
                expirationDateTime,
                clientState: graphConfig.clientState,
                latestSupportedTlsVersion: "v1_2",
            });
            logger.info("Subscription created successfully", { requestId, planId, subscriptionId: subscription.id });
            created.push({
                id: subscription.id,
                planId,
                resource: subscription.resource,
                expirationDateTime: subscription.expirationDateTime,
            });
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            logger.error("Failed to create subscription for plan", { 
                requestId, 
                planId, 
                error: errorMessage,
                stack: error instanceof Error ? error.stack : undefined,
            });
            throw error;
        }
    }
    
    logger.info("Saving subscriptions", { requestId, createdCount: created.length });
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
    
    const duration = Date.now() - startTime;
    logger.info("POST /api/sync/subscriptions/create - Success", {
        requestId,
        duration,
        createdCount: created.length,
        planIds: Array.from(planIds),
    });

    return new Response(JSON.stringify({ ok: true, created, notificationUrl: baseUrl, requestId, duration }), {
        status: 200,
        headers: { 
            "Content-Type": "application/json",
            "X-Request-ID": requestId,
        },
    });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        const errorStack = error instanceof Error ? error.stack : undefined;
        
        logger.error("POST /api/sync/subscriptions/create - Unexpected error", {
            requestId,
            duration,
            error: errorMessage,
            stack: errorStack,
        });
        
        return new Response(JSON.stringify({ 
            ok: false, 
            error: errorMessage,
            requestId,
            duration,
            ...(process.env.NODE_ENV === "development" ? { stack: errorStack } : {}),
        }), {
            status: 500,
            headers: { 
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}

// Handle unsupported methods
export async function GET() {
    return new Response(JSON.stringify({ 
        ok: false, 
        error: "Method not allowed. Use POST.",
        supportedMethods: ["POST"],
    }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
    });
}
