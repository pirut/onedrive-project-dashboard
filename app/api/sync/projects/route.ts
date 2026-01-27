import { BusinessCentralClient } from "../../../../lib/planner-sync/bc-client";
import {
    buildDisabledProjectSet,
    listProjectSyncSettings,
    normalizeProjectNo,
    saveProjectSyncSettings,
} from "../../../../lib/planner-sync/project-sync-store";
import { logger } from "../../../../lib/planner-sync/logger";

const BC_SYNC_FIELDS = ["lastSyncAt"];

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

function parseDateMs(value: string | null | undefined) {
    if (!value) return null;
    const ms = Date.parse(String(value));
    return Number.isNaN(ms) ? null : ms;
}

function resolveTaskSyncMs(task: Record<string, unknown>) {
    let latest: number | null = null;
    for (const field of BC_SYNC_FIELDS) {
        const raw = task?.[field] as string | undefined;
        if (typeof raw !== "string") continue;
        const ms = parseDateMs(raw);
        if (ms != null && (latest == null || ms > latest)) {
            latest = ms;
        }
    }
    return latest;
}

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

async function clearPremiumLinksForProject(bcClient: BusinessCentralClient, projectNo: string) {
    const escaped = projectNo.replace(/'/g, "''");
    const result: Record<string, unknown> = { total: 0, cleared: 0, skipped: 0, failed: 0 };
    let tasks: Record<string, unknown>[] = [];
    try {
        tasks = await bcClient.listProjectTasks(`projectNo eq '${escaped}'`);
    } catch (error) {
        result.error = (error as Error)?.message || String(error);
        return result;
    }
    result.total = tasks.length;
    for (const task of tasks) {
        const hasLink = Boolean(task.plannerTaskId || task.plannerPlanId || task.plannerBucket);
        if (!hasLink) {
            result.skipped = Number(result.skipped || 0) + 1;
            continue;
        }
        if (!task.systemId) {
            result.failed = Number(result.failed || 0) + 1;
            continue;
        }
        try {
            await bcClient.patchProjectTask(String(task.systemId), {
                plannerTaskId: "",
                plannerPlanId: "",
                plannerBucket: "",
                lastPlannerEtag: "",
                syncLock: false,
            });
            result.cleared = Number(result.cleared || 0) + 1;
        } catch {
            result.failed = Number(result.failed || 0) + 1;
        }
    }
    return result;
}

async function loadProjects() {
    const bcClient = new BusinessCentralClient();
    const settings = await listProjectSyncSettings();
    const disabledProjects = buildDisabledProjectSet(settings);

    const projects = await bcClient.listProjects();

    let tasks: Record<string, unknown>[] = [];
    try {
        tasks = await bcClient.listProjectTasks();
    } catch (error) {
        logger.warn("Failed to load BC tasks", { error: (error as Error)?.message || String(error) });
    }

    const projectSyncMap = new Map<string, number>();
    const projectPremiumMap = new Map<string, string>();
    for (const task of tasks) {
        const projectNo = String(task.projectNo || "").trim();
        if (!projectNo) continue;
        if (task.plannerPlanId && !projectPremiumMap.has(projectNo)) {
            projectPremiumMap.set(projectNo, String(task.plannerPlanId));
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
            const projectNo = String(project.projectNo || "").trim();
            if (!projectNo) return null;
            const lastSyncMs = projectSyncMap.get(projectNo) || null;
            return {
                projectNo,
                description: project.description || "",
                status: project.status || "",
                premiumProjectId: projectPremiumMap.get(projectNo) || "",
                syncDisabled: disabledProjects.has(normalizeProjectNo(projectNo)),
                lastSyncAt: lastSyncMs ? new Date(lastSyncMs).toISOString() : "",
            };
        })
        .filter(Boolean)
        .sort((a, b) => a.projectNo.localeCompare(b.projectNo, undefined, { numeric: true, sensitivity: "base" }));

    return { projects: rows };
}

export async function GET() {
    try {
        const payload = await loadProjects();
        return new Response(JSON.stringify({ ok: true, ...payload }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Failed to load Premium projects", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function POST(request: Request) {
    const body = await readJsonBody(request);
    const projectNo = String(body?.projectNo || "").trim();
    if (!projectNo) {
        return new Response(JSON.stringify({ ok: false, error: "projectNo required" }, null, 2), {
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

    if (action === "clear-links" || action === "clearlinks") {
        await setProjectSyncDisabled(projectNo, true, note);
        const bcClient = new BusinessCentralClient();
        const clearedTasks = await clearPremiumLinksForProject(bcClient, projectNo);
        if (clearedTasks.error) {
            logger.warn("Failed to clear BC Premium links", { projectNo, error: clearedTasks.error });
        }
        return new Response(JSON.stringify({ ok: true, projectNo, disabled: true, clearedTasks }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    }

    await setProjectSyncDisabled(projectNo, disabled, note);
    return new Response(JSON.stringify({ ok: true, projectNo, disabled }, null, 2), {
        status: 200,
        headers: { "Content-Type": "application/json" },
    });
}
