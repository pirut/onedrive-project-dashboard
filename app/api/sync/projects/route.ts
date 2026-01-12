import { BusinessCentralClient, BcProjectTask } from "../../../../lib/planner-sync/bc-client";
import { GraphClient } from "../../../../lib/planner-sync/graph-client";
import { getGraphConfig, getPlannerConfig } from "../../../../lib/planner-sync/config";
import {
    buildDisabledProjectSet,
    listProjectSyncSettings,
    normalizeProjectNo,
    saveProjectSyncSettings,
} from "../../../../lib/planner-sync/project-sync-store";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

function buildPlanTitle(projectNo: string, projectDescription?: string | null) {
    const cleaned = (projectDescription || "").trim();
    return cleaned ? `${projectNo} - ${cleaned}` : projectNo;
}

async function resolvePlannerBaseUrl(graphClient: GraphClient) {
    const envBase = (process.env.PLANNER_WEB_BASE || "").trim();
    if (envBase) return envBase.replace(/\/+$/, "");
    const envDomain = (process.env.PLANNER_TENANT_DOMAIN || "").trim();
    if (envDomain) return `https://tasks.office.com/${envDomain}`;
    try {
        const domain = await graphClient.getDefaultDomain();
        if (domain) return `https://tasks.office.com/${domain}`;
    } catch (error) {
        logger.warn("Failed to resolve Planner tenant domain", { error: (error as Error)?.message });
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

function normalizeTitle(title?: string | null) {
    return (title || "").trim().toLowerCase();
}

type ClearPlannerLinksResult = {
    total: number;
    cleared: number;
    skipped: number;
    failed: number;
    resolvedPlanId?: string;
    error?: string;
};

async function setProjectSyncDisabled(projectNo: string, disabled: boolean, note?: string) {
    const settings = await listProjectSyncSettings();
    const normalized = normalizeProjectNo(projectNo);
    const idx = settings.findIndex((item) => normalizeProjectNo(item.projectNo) === normalized);

    if (disabled) {
        const entry = {
            projectNo,
            disabled: true,
            updatedAt: new Date().toISOString(),
            ...(note ? { note } : {}),
        };
        if (idx >= 0) {
            settings[idx] = { ...settings[idx], ...entry };
        } else {
            settings.push(entry);
        }
    } else if (idx >= 0) {
        settings.splice(idx, 1);
    }

    await saveProjectSyncSettings(settings);
}

async function clearPlannerLinksForProject(bcClient: BusinessCentralClient, projectNo: string): Promise<ClearPlannerLinksResult> {
    const escaped = projectNo.replace(/'/g, "''");
    const result: ClearPlannerLinksResult = { total: 0, cleared: 0, skipped: 0, failed: 0 };
    let tasks: BcProjectTask[] = [];
    try {
        tasks = await bcClient.listProjectTasks(`projectNo eq '${escaped}'`);
    } catch (error) {
        result.error = (error as Error)?.message || String(error);
        return result;
    }
    result.total = tasks.length;
    for (const task of tasks) {
        if (!result.resolvedPlanId && task.plannerPlanId) {
            result.resolvedPlanId = task.plannerPlanId;
        }
        if (!task.plannerTaskId && !task.plannerPlanId && !task.plannerBucket) {
            result.skipped += 1;
            continue;
        }
        if (!task.systemId) {
            result.failed += 1;
            continue;
        }
        try {
            await bcClient.patchProjectTask(task.systemId, {
                plannerTaskId: "",
                plannerPlanId: "",
                plannerBucket: "",
                lastPlannerEtag: "",
                syncLock: false,
            });
            result.cleared += 1;
        } catch {
            result.failed += 1;
        }
    }
    return result;
}

async function loadProjects() {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const plannerConfig = getPlannerConfig();
    const { tenantId } = getGraphConfig();
    const settings = await listProjectSyncSettings();
    const disabledProjects = buildDisabledProjectSet(settings);

    const projects = await bcClient.listProjects();

    let tasks = [];
    try {
        tasks = await bcClient.listProjectTasks("plannerPlanId ne ''");
    } catch (error) {
        logger.warn("Failed to load BC tasks for planner plan map", { error: (error as Error)?.message });
    }

    let plans = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list Planner plans", { error: (error as Error)?.message });
    }

    const baseUrl = await resolvePlannerBaseUrl(graphClient);
    const planMap = new Map(plans.map((plan) => [plan.id, plan]));
    const planByTitle = new Map(plans.map((plan) => [normalizeTitle(plan.title), plan]));
    const projectPlanMap = new Map<string, string>();
    for (const task of tasks) {
        const projectNo = (task.projectNo || "").trim();
        if (!projectNo || !task.plannerPlanId) continue;
        if (!projectPlanMap.has(projectNo)) {
            projectPlanMap.set(projectNo, task.plannerPlanId);
        }
    }

    const rows = projects
        .map((project) => {
            const projectNo = (project.projectNo || "").trim();
            if (!projectNo) return null;
            const planTitle = buildPlanTitle(projectNo, project.description);
            let planId = projectPlanMap.get(projectNo);
            let planLinked = Boolean(planId);
            if (!planId) {
                const planMatch = planByTitle.get(normalizeTitle(planTitle)) || planByTitle.get(normalizeTitle(projectNo));
                if (planMatch?.id) {
                    planId = planMatch.id;
                    planLinked = false;
                }
            }
            const plan = planId ? planMap.get(planId) : null;
            const planUrl = buildPlannerPlanUrl(planId, baseUrl, tenantId);
            return {
                projectNo,
                description: project.description || "",
                status: project.status || "",
                planId: planId || "",
                planTitle: plan?.title || "",
                planExists: Boolean(plan?.id),
                planLinked,
                planUrl,
                syncDisabled: disabledProjects.has(normalizeProjectNo(projectNo)),
            };
        })
        .filter(Boolean)
        .sort((a, b) => a.projectNo.localeCompare(b.projectNo, undefined, { numeric: true, sensitivity: "base" }));

    const linkedPlanIds = new Set(rows.map((row) => row.planId).filter(Boolean));
    const orphanPlans = plans
        .filter((plan) => !linkedPlanIds.has(plan.id))
        .map((plan) => ({
            planId: plan.id,
            title: plan.title || "",
            planUrl: buildPlannerPlanUrl(plan.id, baseUrl, tenantId),
        }))
        .sort((a, b) => a.title.localeCompare(b.title, undefined, { sensitivity: "base" }));

    return { projects: rows, orphanPlans, baseUrl };
}

export async function GET() {
    try {
        const payload = await loadProjects();
        return new Response(JSON.stringify({ ok: true, ...payload }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Failed to load Planner projects", { error: (error as Error)?.message });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function POST(request: Request) {
    let body: {
        projectNo?: string;
        disabled?: boolean | string | number;
        note?: string;
        action?: string;
        planId?: string;
    } | null = null;
    try {
        const text = await request.text();
        body = text ? JSON.parse(text) : null;
    } catch {
        body = null;
    }

    const projectNo = String(body?.projectNo || "").trim();
    if (!projectNo) {
        return new Response(JSON.stringify({ ok: false, error: "projectNo required" }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    const action = typeof body?.action === "string" ? body.action.trim().toLowerCase() : "";
    const disabled =
        body?.disabled === true ||
        body?.disabled === "true" ||
        body?.disabled === 1 ||
        body?.disabled === "1";
    const note = typeof body?.note === "string" ? body.note.trim() : "";

    if (action === "deleteplan" || action === "delete-plan") {
        await setProjectSyncDisabled(projectNo, true, note);
        const bcClient = new BusinessCentralClient();
        const graphClient = new GraphClient();
        const clearedTasks = await clearPlannerLinksForProject(bcClient, projectNo);
        if (clearedTasks.error) {
            logger.warn("Failed to clear BC Planner links", { projectNo, error: clearedTasks.error });
        }
        let resolvedPlanId = typeof body?.planId === "string" ? body.planId.trim() : "";
        if (!resolvedPlanId && clearedTasks.resolvedPlanId) {
            resolvedPlanId = clearedTasks.resolvedPlanId;
        }
        let planDelete: { attempted: boolean; ok?: boolean; planId?: string; error?: string } = { attempted: false };
        if (resolvedPlanId) {
            try {
                const ok = await graphClient.deletePlan(resolvedPlanId);
                planDelete = { attempted: true, ok, planId: resolvedPlanId };
            } catch (error) {
                planDelete = {
                    attempted: true,
                    ok: false,
                    planId: resolvedPlanId,
                    error: (error as Error)?.message || String(error),
                };
                logger.warn("Planner plan deletion failed", { projectNo, planId: resolvedPlanId, error: planDelete.error });
            }
        }
        return new Response(JSON.stringify({ ok: true, projectNo, disabled: true, clearedTasks, planDelete }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    }

    await setProjectSyncDisabled(projectNo, disabled, note);

    return new Response(JSON.stringify({ ok: true, projectNo, disabled }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}

export async function PUT() {
    return new Response(JSON.stringify({ ok: false, error: "Method not allowed" }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
    });
}
