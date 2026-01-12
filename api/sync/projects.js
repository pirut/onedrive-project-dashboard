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
        const disabled =
            body?.disabled === true ||
            body?.disabled === "true" ||
            body?.disabled === 1 ||
            body?.disabled === "1";
        const note = typeof body?.note === "string" ? body.note.trim() : "";

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
        res.status(200).json({ ok: true, projectNo, disabled });
        return;
    }

    res.status(405).json({ ok: false, error: "Method not allowed" });
}
