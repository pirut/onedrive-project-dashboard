import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { getGraphConfig, getPlannerConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

type PlannerTestChecks = {
    groupPlans?: { ok: boolean; count?: number; sample?: { id: string; title?: string }[]; error?: string };
    plan?: { ok: boolean; id?: string; title?: string; owner?: string; error?: string };
    buckets?: { ok: boolean; count?: number; sample?: { id: string; name?: string }[]; error?: string };
    tasks?: {
        ok: boolean;
        count?: number;
        sample?: { id: string; title?: string; bucketId?: string; percentComplete?: number }[];
        error?: string;
    };
    plannerBaseUrl?: { ok: boolean; value?: string };
};

async function resolvePlannerBaseUrl(graphClient: GraphClient) {
    const envBase = (process.env.PLANNER_WEB_BASE || "").trim();
    if (envBase) return envBase.replace(/\/+$/, "");
    const envDomain = (process.env.PLANNER_TENANT_DOMAIN || "").trim();
    if (envDomain) return `https://tasks.office.com/${envDomain}`;
    try {
        const domain = await graphClient.getDefaultDomain();
        if (domain) return `https://tasks.office.com/${domain}`;
    } catch (error) {
        logger.warn("Planner test failed to resolve default domain", {
            error: error instanceof Error ? error.message : String(error),
        });
    }
    return "https://planner.cloud.microsoft";
}

function buildPlannerPlanUrl(planId: string | undefined, baseUrl: string, tenantId?: string) {
    if (!planId) return undefined;
    const base = (baseUrl || "https://planner.cloud.microsoft").replace(/\/+$/, "");
    if (base.includes("planner.cloud.microsoft")) {
        const tid = tenantId ? `?tid=${encodeURIComponent(tenantId)}` : "";
        return `${base}/webui/plan/${planId}/view/board${tid}`;
    }
    if (base.includes("planner.office.com")) {
        const tid = tenantId ? `?tid=${encodeURIComponent(tenantId)}` : "";
        return `${base}/plan/${planId}${tid}`;
    }
    return `${base}/Home/PlanViews/${planId}`;
}

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("GET /api/sync/planner-test - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const graphClient = new GraphClient();
        const plannerConfig = getPlannerConfig();
        const graphConfig = getGraphConfig();
        const planIdParam = (url.searchParams.get("planId") || "").trim();

        const response: {
            ok: boolean;
            connected: boolean;
            requestId: string;
            duration?: number;
            planId?: string | null;
            planSource?: string;
            planUrl?: string;
            groupId?: string;
            checks: PlannerTestChecks;
        } = {
            ok: true,
            connected: false,
            requestId,
            planId: null,
            planSource: planIdParam ? "query" : plannerConfig.defaultPlanId ? "defaultPlanId" : "group",
            groupId: plannerConfig.groupId,
            checks: {},
        };

        let planId = planIdParam || plannerConfig.defaultPlanId || undefined;
        if (!planId) {
            try {
                const plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
                response.checks.groupPlans = {
                    ok: true,
                    count: plans.length,
                    sample: plans.slice(0, 5).map((plan) => ({ id: plan.id, title: plan.title })),
                };
                planId = plans[0]?.id;
                response.planSource = planId ? "group" : response.planSource;
            } catch (error) {
                response.checks.groupPlans = {
                    ok: false,
                    error: error instanceof Error ? error.message : String(error),
                };
            }
        }

        response.planId = planId || null;

        if (!planId) {
            response.checks.plan = { ok: false, error: "No planId resolved from query, default, or group." };
        } else {
            try {
                const plan = await graphClient.getPlan(planId);
                response.checks.plan = {
                    ok: true,
                    id: plan?.id,
                    title: plan?.title,
                    owner: plan?.owner,
                };
            } catch (error) {
                response.checks.plan = {
                    ok: false,
                    error: error instanceof Error ? error.message : String(error),
                };
            }

            try {
                const buckets = await graphClient.listBuckets(planId);
                response.checks.buckets = {
                    ok: true,
                    count: buckets.length,
                    sample: buckets.slice(0, 5).map((bucket) => ({ id: bucket.id, name: bucket.name })),
                };
            } catch (error) {
                response.checks.buckets = {
                    ok: false,
                    error: error instanceof Error ? error.message : String(error),
                };
            }

            try {
                const tasks = await graphClient.listTasks(planId);
                response.checks.tasks = {
                    ok: true,
                    count: tasks.length,
                    sample: tasks.slice(0, 5).map((task) => ({
                        id: task.id,
                        title: task.title,
                        bucketId: task.bucketId,
                        percentComplete: task.percentComplete,
                    })),
                };
            } catch (error) {
                response.checks.tasks = {
                    ok: false,
                    error: error instanceof Error ? error.message : String(error),
                };
            }
        }

        const plannerBaseUrl = await resolvePlannerBaseUrl(graphClient);
        response.planUrl = buildPlannerPlanUrl(planId, plannerBaseUrl, graphConfig.tenantId);
        response.checks.plannerBaseUrl = { ok: true, value: plannerBaseUrl };

        response.connected = Boolean(response.checks.plan?.ok && response.checks.buckets?.ok && response.checks.tasks?.ok);
        response.duration = Date.now() - startTime;

        logger.info("GET /api/sync/planner-test - Completed", {
            requestId,
            duration: response.duration,
            connected: response.connected,
            planId: response.planId,
        });

        return new Response(JSON.stringify(response, null, 2), {
            status: 200,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("GET /api/sync/planner-test - Failed", {
            requestId,
            duration,
            error: errorMessage,
        });
        return new Response(JSON.stringify({
            ok: false,
            error: errorMessage,
            requestId,
            duration,
        }, null, 2), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}
