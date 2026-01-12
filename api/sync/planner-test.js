import { GraphClient } from "../../lib/planner-sync/graph-client.js";
import { getGraphConfig, getPlannerConfig } from "../../lib/planner-sync/config.js";
import { logger } from "../../lib/planner-sync/logger.js";

async function resolvePlannerBaseUrl(graphClient) {
    const envBase = (process.env.PLANNER_WEB_BASE || "").trim();
    if (envBase) return envBase.replace(/\/+$/, "");
    const envDomain = (process.env.PLANNER_TENANT_DOMAIN || "").trim();
    if (envDomain) return `https://tasks.office.com/${envDomain}`;
    try {
        const domain = await graphClient.getDefaultDomain();
        if (domain) return `https://tasks.office.com/${domain}`;
    } catch (error) {
        logger.warn("Planner test failed to resolve default domain", {
            error: error?.message || String(error),
        });
    }
    return "https://planner.cloud.microsoft";
}

function buildPlannerPlanUrl(planId, baseUrl, tenantId) {
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

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const graphClient = new GraphClient();
        const plannerConfig = getPlannerConfig();
        const graphConfig = getGraphConfig();
        const planIdParam = (req.query.planId || "").toString().trim();

        const response = {
            ok: true,
            connected: false,
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
                    error: error?.message || String(error),
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
                    error: error?.message || String(error),
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
                    error: error?.message || String(error),
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
                    error: error?.message || String(error),
                };
            }
        }

        const plannerBaseUrl = await resolvePlannerBaseUrl(graphClient);
        response.planUrl = buildPlannerPlanUrl(planId, plannerBaseUrl, graphConfig.tenantId);
        response.checks.plannerBaseUrl = { ok: true, value: plannerBaseUrl };

        response.connected = Boolean(
            response.checks.plan?.ok &&
                response.checks.buckets?.ok &&
                response.checks.tasks?.ok
        );

        res.status(200).json(response);
    } catch (error) {
        logger.error("GET /api/sync/planner-test - Failed", {
            error: error?.message || String(error),
        });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
