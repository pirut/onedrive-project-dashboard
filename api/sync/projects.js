import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import { GraphClient } from "../../lib/planner-sync/graph-client.js";
import { getGraphConfig, getPlannerConfig } from "../../lib/planner-sync/config.js";
import {
    buildDisabledProjectSet,
    listProjectSyncSettings,
    normalizeProjectNo,
    saveProjectSyncSettings,
} from "../../lib/planner-sync/project-sync-store.js";
import { logger } from "../../lib/planner-sync/logger.js";

const BC_SYNC_FIELDS = ["lastSyncAt"];

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

function buildPlanTitle(projectNo, projectDescription) {
    const cleaned = (projectDescription || "").trim();
    return cleaned ? `${projectNo} - ${cleaned}` : projectNo;
}

async function resolvePlannerBaseUrl(graphClient) {
    const envBase = (process.env.PLANNER_WEB_BASE || "").trim();
    if (envBase) return envBase.replace(/\/+$/, "");
    const envDomain = (process.env.PLANNER_TENANT_DOMAIN || "").trim();
    if (envDomain) return `https://tasks.office.com/${envDomain}`;
    try {
        const domain = await graphClient.getDefaultDomain();
        if (domain) return `https://tasks.office.com/${domain}`;
    } catch (error) {
        logger.warn("Failed to resolve Planner tenant domain", { error: error?.message || String(error) });
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

function normalizeTitle(title) {
    return (title || "").trim().toLowerCase();
}

function parseDateMs(value) {
    if (!value) return null;
    const ms = Date.parse(String(value));
    return Number.isNaN(ms) ? null : ms;
}

function resolveTaskSyncMs(task) {
    let latest = null;
    for (const field of BC_SYNC_FIELDS) {
        const raw = task?.[field];
        if (typeof raw !== "string") continue;
        const ms = parseDateMs(raw);
        if (ms != null && (latest == null || ms > latest)) {
            latest = ms;
        }
    }
    return latest;
}

async function setProjectSyncDisabled(projectNo, disabled, note) {
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

async function clearPlannerLinksForProject(bcClient, projectNo) {
    const escaped = projectNo.replace(/'/g, "''");
    const result = { total: 0, cleared: 0, skipped: 0, failed: 0 };
    let tasks = [];
    try {
        tasks = await bcClient.listProjectTasks(`projectNo eq '${escaped}'`);
    } catch (error) {
        result.error = error?.message || String(error);
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
        logger.warn("Failed to load BC tasks for planner plan map", { error: error?.message || String(error) });
    }

    let plans = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list Planner plans", { error: error?.message || String(error) });
    }

    const baseUrl = await resolvePlannerBaseUrl(graphClient);
    const planMap = new Map(plans.map((plan) => [plan.id, plan]));
    const planByTitle = new Map(plans.map((plan) => [normalizeTitle(plan.title), plan]));
    const projectPlanMap = new Map();
    const projectSyncMap = new Map();
    for (const task of tasks) {
        const projectNo = (task.projectNo || "").trim();
        if (!projectNo || !task.plannerPlanId) continue;
        if (!projectPlanMap.has(projectNo)) {
            projectPlanMap.set(projectNo, task.plannerPlanId);
        }
        const syncMs = resolveTaskSyncMs(task);
        if (syncMs != null) {
            const current = projectSyncMap.get(projectNo);
            if (current == null || syncMs > current) {
                projectSyncMap.set(projectNo, syncMs);
            }
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
            const lastSyncMs = projectSyncMap.get(projectNo) || null;
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
                lastSyncAt: lastSyncMs ? new Date(lastSyncMs).toISOString() : "",
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

export default async function handler(req, res) {
    res.setHeader("Cache-Control", "no-store");
    if (req.method === "GET") {
        try {
            const payload = await loadProjects();
            res.status(200).json({ ok: true, ...payload });
            return;
        } catch (error) {
            logger.error("Failed to load Planner projects", { error: error?.message || String(error) });
            res.status(500).json({ ok: false, error: error?.message || String(error) });
            return;
        }
    }

    if (req.method === "POST") {
        const body = await readJsonBody(req);
        const projectNo = String(body?.projectNo || "").trim();
        if (!projectNo) {
            res.status(400).json({ ok: false, error: "projectNo required" });
            return;
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
            let planDelete = { attempted: false };
            if (resolvedPlanId) {
                try {
                    const ok = await graphClient.deletePlan(resolvedPlanId);
                    planDelete = { attempted: true, ok, planId: resolvedPlanId };
                } catch (error) {
                    planDelete = {
                        attempted: true,
                        ok: false,
                        planId: resolvedPlanId,
                        error: error?.message || String(error),
                    };
                    logger.warn("Planner plan deletion failed", { projectNo, planId: resolvedPlanId, error: planDelete.error });
                }
            }
            res.status(200).json({ ok: true, projectNo, disabled: true, clearedTasks, planDelete });
            return;
        }

        await setProjectSyncDisabled(projectNo, disabled, note);
        res.status(200).json({ ok: true, projectNo, disabled });
        return;
    }

    res.status(405).json({ ok: false, error: "Method not allowed" });
}
