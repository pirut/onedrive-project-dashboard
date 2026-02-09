import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import {
    buildDisabledProjectSet,
    listProjectSyncSettings,
    normalizeProjectNo,
    saveProjectSyncSettings,
} from "../../lib/planner-sync/project-sync-store.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { ensurePremiumProjectTeamAccess } from "../../lib/premium-sync/index.js";
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

function readString(value) {
    return typeof value === "string" ? value.trim() : "";
}

function readBool(value, fallback = false) {
    if (value == null) return fallback;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return fallback;
}

function readStringList(value) {
    if (Array.isArray(value)) {
        return Array.from(new Set(value.map((item) => String(item || "").trim()).filter(Boolean)));
    }
    if (typeof value === "string") {
        return Array.from(
            new Set(
                value
                    .split(",")
                    .map((item) => item.trim())
                    .filter(Boolean)
            )
        );
    }
    return undefined;
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

async function clearPremiumLinksForProject(bcClient, projectNo) {
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
    const settings = await listProjectSyncSettings();
    const disabledProjects = buildDisabledProjectSet(settings);

    const projects = await bcClient.listProjects();

    let tasks = [];
    try {
        tasks = await bcClient.listProjectTasks();
    } catch (error) {
        logger.warn("Failed to load BC tasks", { error: error?.message || String(error) });
    }

    const projectSyncMap = new Map();
    const projectPremiumMap = new Map();
    for (const task of tasks) {
        const projectNo = (task.projectNo || "").trim();
        if (!projectNo) continue;
        if (task.plannerPlanId && !projectPremiumMap.has(projectNo)) {
            projectPremiumMap.set(projectNo, task.plannerPlanId);
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

export default async function handler(req, res) {
    res.setHeader("Cache-Control", "no-store");
    if (req.method === "GET") {
        try {
            const payload = await loadProjects();
            res.status(200).json({ ok: true, ...payload });
            return;
        } catch (error) {
            logger.error("Failed to load Premium projects", { error: error?.message || String(error) });
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
        const premiumProjectId = String(body?.premiumProjectId || body?.projectId || "").trim();

        if (action === "clear-links" || action === "clearlinks") {
            await setProjectSyncDisabled(projectNo, true, note);
            const bcClient = new BusinessCentralClient();
            const clearedTasks = await clearPremiumLinksForProject(bcClient, projectNo);
            if (clearedTasks.error) {
                logger.warn("Failed to clear BC Premium links", { projectNo, error: clearedTasks.error });
            }
            res.status(200).json({ ok: true, projectNo, disabled: true, clearedTasks });
            return;
        }

        if (action === "share-access" || action === "shareaccess" || action === "share-team" || action === "shareteam") {
            if (!premiumProjectId) {
                res.status(400).json({ ok: false, error: "premiumProjectId (or projectId) required" });
                return;
            }
            try {
                const dataverse = new DataverseClient();
                const access = await ensurePremiumProjectTeamAccess(dataverse, premiumProjectId, {
                    projectNo,
                    plannerOwnerTeamId: readString(body?.plannerOwnerTeamId || body?.ownerTeamId),
                    plannerOwnerTeamAadGroupId: readString(
                        body?.plannerOwnerTeamAadGroupId || body?.ownerTeamAadGroupId || body?.ownerAadGroupId
                    ),
                    plannerGroupId: readString(body?.plannerGroupId),
                    plannerGroupResourceIds: readStringList(body?.plannerGroupResourceIds),
                    plannerPrimaryResourceId: readString(body?.plannerPrimaryResourceId),
                    plannerPrimaryResourceName: readString(body?.plannerPrimaryResourceName),
                    plannerShareReminderTaskEnabled: readBool(body?.plannerShareReminderTaskEnabled, false),
                    plannerShareReminderTaskTitle: readString(body?.plannerShareReminderTaskTitle),
                });
                res.status(200).json({ ok: true, projectNo, premiumProjectId, access });
                return;
            } catch (error) {
                logger.error("Failed to share Premium project access", {
                    projectNo,
                    premiumProjectId,
                    error: error?.message || String(error),
                });
                res.status(500).json({ ok: false, error: error?.message || String(error) });
                return;
            }
        }

        await setProjectSyncDisabled(projectNo, disabled, note);
        res.status(200).json({ ok: true, projectNo, disabled });
        return;
    }

    res.status(405).json({ ok: false, error: "Method not allowed" });
}
