import { BusinessCentralClient } from "./bc-client.js";
import { GraphClient } from "./graph-client.js";
import { getGraphConfig, getPlannerConfig, getSyncConfig } from "./config.js";
import { logger } from "./logger.js";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "./project-sync-store.js";
import { enqueueNotifications, processQueue } from "./queue.js";

const DEFAULT_BUCKET_NAME = "General";
const HEADING_BUCKETS = {
    "JOB NAME": "Pre-Construction",
    INSTALLATION: "Installation",
    "CHANGE ORDER": "Change Orders",
    "CHANGE ORDERS": "Change Orders",
    REVENUE: null,
};

function hasField(task, field) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function normalizeBucketName(name) {
    const trimmed = (name || "").trim();
    return trimmed || DEFAULT_BUCKET_NAME;
}

async function resolvePlannerBaseUrl(graphClient) {
    const envBase = (process.env.PLANNER_WEB_BASE || "").trim();
    if (envBase)
        return envBase.replace(/\/+$/, "");
    const envDomain = (process.env.PLANNER_TENANT_DOMAIN || "").trim();
    if (envDomain)
        return `https://tasks.office.com/${envDomain}`;
    try {
        const domain = await graphClient.getDefaultDomain();
        if (domain)
            return `https://tasks.office.com/${domain}`;
    }
    catch (error) {
        logger.warn("Failed to resolve Planner tenant domain", { error: error?.message || String(error) });
    }
    return "https://planner.cloud.microsoft";
}

function buildPlannerPlanUrl(planId, baseUrl, tenantId) {
    if (!planId)
        return undefined;
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

function resolveBucketFromHeading(description) {
    const heading = (description || "").trim();
    if (!heading) return { bucket: DEFAULT_BUCKET_NAME, skip: false };
    const normalized = heading.toUpperCase();
    if (Object.prototype.hasOwnProperty.call(HEADING_BUCKETS, normalized)) {
        const mapped = HEADING_BUCKETS[normalized];
        return { bucket: mapped, skip: mapped == null };
    }
    return { bucket: normalizeBucketName(heading), skip: false };
}
function normalizeDateOnly(value) {
    if (!value) return null;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return null;
    return date.toISOString().slice(0, 10);
}

function toPlannerDate(value) {
    const dateOnly = normalizeDateOnly(value || null);
    if (!dateOnly) return null;
    // Planner only accepts dates within 1984-01-01 and 2149-12-31.
    if (dateOnly < "1984-01-01" || dateOnly > "2149-12-31") return null;
    const date = new Date(`${dateOnly}T00:00:00Z`);
    if (Number.isNaN(date.getTime())) return null;
    return date.toISOString();
}

function toBcDate(value) {
    return normalizeDateOnly(value || null);
}

function toPlannerPercent(value) {
    if (value == null) return 0;
    if (value >= 100) return 100;
    if (value > 0) return 50;
    return 0;
}

function toBcPercent(value) {
    if (value == null) return 0;
    if (value >= 100) return 100;
    if (value >= 50) return 50;
    return 0;
}

const BC_MODIFIED_FIELDS = [
    "systemModifiedAt",
    "lastModifiedDateTime",
    "lastModifiedAt",
    "modifiedAt",
    "modifiedOn",
    "lastModifiedOn",
    "systemModifiedOn",
];
function parseDateMs(value) {
    if (!value) return null;
    const ms = Date.parse(value);
    if (Number.isNaN(ms)) return null;
    return ms;
}
function resolveBcModifiedAt(task) {
    for (const field of BC_MODIFIED_FIELDS) {
        const raw = task?.[field];
        if (typeof raw !== "string") continue;
        const ms = parseDateMs(raw);
        if (ms != null) {
            return { ms, field, raw };
        }
    }
    return { ms: null, field: null, raw: null };
}
function resolvePlannerModifiedAt(task) {
    const raw = task?.lastModifiedDateTime || null;
    return { ms: parseDateMs(raw), raw };
}
function resolveSyncDecision(bcTask, plannerTask) {
    const lastSyncAt = parseDateMs(bcTask.lastSyncAt || null);
    const bcModified = resolveBcModifiedAt(bcTask);
    const plannerModified = resolvePlannerModifiedAt(plannerTask);
    const bcChangedSinceSync = lastSyncAt != null && bcModified.ms != null ? bcModified.ms > lastSyncAt : null;
    const plannerChangedSinceSync = lastSyncAt != null && plannerModified.ms != null ? plannerModified.ms > lastSyncAt : null;
    const { preferBc } = getSyncConfig();
    if (lastSyncAt != null) {
        if (bcChangedSinceSync && plannerChangedSinceSync) {
            if (!preferBc && bcModified.ms != null && plannerModified.ms != null) {
                return {
                    decision: bcModified.ms >= plannerModified.ms ? "bc" : "planner",
                    lastSyncAt,
                    bcModified,
                    plannerModified,
                    bcChangedSinceSync,
                    plannerChangedSinceSync,
                };
            }
            return { decision: "bc", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
        }
        if (bcChangedSinceSync && !plannerChangedSinceSync) {
            return { decision: "bc", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
        }
        if (!bcChangedSinceSync && plannerChangedSinceSync) {
            return { decision: "planner", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
        }
        if (bcChangedSinceSync === false && plannerChangedSinceSync === false) {
            return { decision: "none", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
        }
        if (plannerChangedSinceSync) {
            return { decision: "planner", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
        }
        return { decision: "bc", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
    }
    if (bcModified.ms != null && plannerModified.ms != null) {
        if (bcModified.ms === plannerModified.ms) {
            return { decision: "none", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
        }
        return {
            decision: bcModified.ms >= plannerModified.ms ? "bc" : "planner",
            lastSyncAt,
            bcModified,
            plannerModified,
            bcChangedSinceSync,
            plannerChangedSinceSync,
        };
    }
    if (bcModified.ms != null) {
        return { decision: "bc", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
    }
    if (plannerModified.ms != null) {
        return { decision: "planner", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
    }
    return { decision: "bc", lastSyncAt, bcModified, plannerModified, bcChangedSinceSync, plannerChangedSinceSync };
}

function formatPlannerDescription(task) {
    const formatValue = (val) => (val == null ? "" : String(val));
    const lines = [
        `ProjectNo: ${formatValue(task.projectNo)}`,
        `TaskNo: ${formatValue(task.taskNo)}`,
        `TaskType: ${formatValue(task.taskType)}`,
        `AssignedPersonName: ${formatValue(task.assignedPersonName)}`,
        `BudgetTotalCost: ${formatValue(task.budgetTotalCost)}`,
        `ActualTotalCost: ${formatValue(task.actualTotalCost)}`,
        `StartDate: ${formatValue(task.startDate)}`,
        `EndDate: ${formatValue(task.endDate)}`,
        `ManualStartDate: ${formatValue(task.manualStartDate)}`,
        `ManualEndDate: ${formatValue(task.manualEndDate)}`,
    ];
    return lines.join("\n");
}

function buildPlanTitle(projectNo, projectDescription) {
    const cleaned = (projectDescription || "").trim();
    return cleaned ? `${projectNo} - ${cleaned}` : projectNo;
}

function buildPlannerTitle(task, prefix) {
    const description = (task.description || "").trim();
    const taskNo = (task.taskNo || "").trim();
    const base = description || taskNo || "Untitled Task";
    return `${prefix || ""}${base}`;
}

function isStaleSyncLock(task, timeoutMinutes) {
    if (!task.syncLock) return false;
    if (timeoutMinutes <= 0) return false;
    const lastSync = task.lastSyncAt ? Date.parse(task.lastSyncAt) : NaN;
    if (Number.isNaN(lastSync)) return true;
    return Date.now() - lastSync > timeoutMinutes * 60 * 1000;
}

function filterTasksForProject(tasks, projectNo) {
    const normalized = (projectNo || "").trim().toLowerCase();
    if (!normalized) return tasks;
    return tasks.filter((task) => (task.projectNo || "").trim().toLowerCase() === normalized);
}

function isProjectDisabled(disabledProjects, projectNo) {
    if (!projectNo) return false;
    const normalized = normalizeProjectNo(projectNo);
    return normalized ? disabledProjects.has(normalized) : false;
}

function buildBcPatch(task, updates) {
    const patch = {};
    for (const [key, value] of Object.entries(updates)) {
        if (hasField(task, key)) {
            patch[key] = value;
        }
    }
    return patch;
}

async function updateBcTaskWithSyncLock(bcClient, task, updates) {
    if (!task.systemId) {
        throw new Error("BC task missing systemId");
    }
    const patch = buildBcPatch(task, updates);
    if (!Object.keys(patch).length) return;

    if (hasField(task, "syncLock")) {
        await bcClient.patchProjectTask(task.systemId, { syncLock: true });
    }
    await bcClient.patchProjectTask(task.systemId, {
        ...patch,
        ...(hasField(task, "syncLock") ? { syncLock: false } : {}),
    });
}

async function resolvePlanForProject(graphClient, projectNo, tasks, projectTitle) {
    const { syncMode, allowDefaultPlanFallback } = getSyncConfig();
    const plannerConfig = getPlannerConfig();

    if (syncMode === "singlePlan") {
        if (!plannerConfig.defaultPlanId) {
            throw new Error("PLANNER_DEFAULT_PLAN_ID is required for SYNC_MODE=singlePlan");
        }
        return { planId: plannerConfig.defaultPlanId, titlePrefix: "" };
    }

    const existingPlanId = tasks.find((task) => task.plannerPlanId)?.plannerPlanId;
    if (existingPlanId) {
        try {
            const plan = await graphClient.getPlan(existingPlanId);
            const planTitle = (plan?.title || "").trim();
            if (planTitle && (planTitle === projectTitle || planTitle === projectNo)) {
                return { planId: existingPlanId, titlePrefix: "" };
            }
            if (planTitle) {
                logger.warn("Ignoring existing planner plan; title mismatch", {
                    projectNo,
                    planId: existingPlanId,
                    planTitle,
                });
            }
        }
        catch (error) {
            logger.warn("Failed to verify existing planner plan", {
                projectNo,
                planId: existingPlanId,
                error: error?.message || String(error),
            });
        }
    }

    let plans = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list plans for group", { error: error?.message || String(error) });
    }
    const matchingPlan = plans.find((plan) => {
        const title = (plan.title || "").trim();
        return title === projectTitle || title === projectNo;
    });
    if (matchingPlan?.id) {
        return { planId: matchingPlan.id, titlePrefix: "" };
    }

    let planCreateError;
    try {
        const createdPlan = await graphClient.createPlan(plannerConfig.groupId, projectTitle);
        return { planId: createdPlan.id, titlePrefix: "" };
    } catch (error) {
        planCreateError = error?.message || String(error);
        logger.warn("Plan creation failed", {
            projectNo,
            error: planCreateError,
        });
    }

    if (!allowDefaultPlanFallback) {
        throw new Error(`Plan creation failed: ${planCreateError || "unknown error"}`);
    }
    if (!plannerConfig.defaultPlanId) {
        throw new Error(`Plan creation failed and PLANNER_DEFAULT_PLAN_ID is not set: ${planCreateError || "unknown error"}`);
    }
    return { planId: plannerConfig.defaultPlanId, titlePrefix: `${projectNo} - ` };
}

async function ensureBucket(graphClient, planId, bucketName, cache) {
    const normalizedName = normalizeBucketName(bucketName);
    const key = normalizedName.toLowerCase();
    if (!cache.has(planId)) {
        const buckets = await graphClient.listBuckets(planId);
        const bucketMap = new Map();
        for (const bucket of buckets) {
            if (bucket.name) {
                bucketMap.set(bucket.name.trim().toLowerCase(), bucket.id);
            }
        }
        cache.set(planId, bucketMap);
    }
    const bucketMap = cache.get(planId);
    if (bucketMap.has(key)) {
        return bucketMap.get(key);
    }
    const created = await graphClient.createBucket(planId, normalizedName);
    bucketMap.set(key, created.id);
    return created.id;
}

async function upsertPlannerTask(bcClient, graphClient, task, planId, bucketId, bucketName, titlePrefix) {
    if (task.syncLock) {
        const { syncLockTimeoutMinutes } = getSyncConfig();
        if (isStaleSyncLock(task, syncLockTimeoutMinutes)) {
            if (!task.systemId) {
                logger.warn("Sync lock stale but systemId missing; skipping", {
                    taskNo: task.taskNo,
                    projectNo: task.projectNo,
                });
                return;
            }
            try {
                await bcClient.patchProjectTask(task.systemId, { syncLock: false });
                task = { ...task, syncLock: false };
                logger.warn("Cleared stale syncLock", {
                    taskNo: task.taskNo,
                    projectNo: task.projectNo,
                    lastSyncAt: task.lastSyncAt,
                });
            } catch (error) {
                logger.warn("Failed to clear stale syncLock; skipping", {
                    taskNo: task.taskNo,
                    projectNo: task.projectNo,
                    error: error?.message || String(error),
                });
                return;
            }
        } else {
            logger.info("Skipping BC task with syncLock", { taskNo: task.taskNo, projectNo: task.projectNo });
            return;
        }
    }

    const desiredTitle = buildPlannerTitle(task, titlePrefix);
    const desiredStart = toPlannerDate(task.manualStartDate || task.startDate || null);
    const desiredDue = toPlannerDate(task.manualEndDate || task.endDate || null);
    const desiredPercent = toPlannerPercent(task.percentComplete || 0);
    const desiredDescription = formatPlannerDescription(task);

    if (!task.plannerTaskId) {
        const payload = {
            planId,
            bucketId,
            title: desiredTitle,
            percentComplete: desiredPercent,
        };
        if (desiredStart) payload.startDateTime = desiredStart;
        if (desiredDue) payload.dueDateTime = desiredDue;
        const created = await graphClient.createTask(payload);
        const details = await graphClient.getTaskDetails(created.id);
        if (details?.["@odata.etag"]) {
            await graphClient.updateTaskDetails(created.id, { description: desiredDescription }, details["@odata.etag"]);
        }
        const latest = await graphClient.getTask(created.id);
        await updateBcTaskWithSyncLock(bcClient, task, {
            plannerTaskId: created.id,
            plannerPlanId: planId,
            plannerBucket: bucketName,
            lastPlannerEtag: latest?.["@odata.etag"],
            lastSyncAt: new Date().toISOString(),
        });
        logger.info("Planner task created", { taskId: created.id, projectNo: task.projectNo, taskNo: task.taskNo });
        return;
    }

    let plannerTask = null;
    let details = null;
    try {
        plannerTask = await graphClient.getTask(task.plannerTaskId);
        details = await graphClient.getTaskDetails(task.plannerTaskId);
    } catch (error) {
        logger.error("Failed to fetch Planner task", {
            taskId: task.plannerTaskId,
            error: error?.message || String(error),
        });
        return;
    }

    if (!plannerTask) return;

    const syncDecision = resolveSyncDecision(task, plannerTask);
    if (syncDecision.decision === "planner") {
        logger.info("Skipping BC → Planner update; Planner is newer", {
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            lastSyncAt: task.lastSyncAt,
            plannerModifiedAt: syncDecision.plannerModified.raw,
            bcModifiedAt: syncDecision.bcModified.raw,
        });
        return;
    }
    if (syncDecision.decision === "none") {
        logger.info("Skipping BC → Planner update; no changes since last sync", {
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            lastSyncAt: task.lastSyncAt,
        });
        return;
    }

    const desiredStartDate = normalizeDateOnly(desiredStart || null);
    const desiredDueDate = normalizeDateOnly(desiredDue || null);
    const buildChanges = (currentTask) => {
        const changes = {};
        if ((currentTask.title || "") !== desiredTitle) changes.title = desiredTitle;
        if (currentTask.bucketId !== bucketId) changes.bucketId = bucketId;
        const plannerStart = normalizeDateOnly(currentTask.startDateTime || null);
        const plannerDue = normalizeDateOnly(currentTask.dueDateTime || null);
        if (plannerStart !== desiredStartDate) changes.startDateTime = desiredStart;
        if (plannerDue !== desiredDueDate) changes.dueDateTime = desiredDue;
        if ((currentTask.percentComplete || 0) !== desiredPercent) changes.percentComplete = desiredPercent;
        return changes;
    };
    const isConflict = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 409") || msg.toLowerCase().includes("conflict");
    };

    const changes = buildChanges(plannerTask);

    let updatedPlanner = false;
    if (Object.keys(changes).length) {
        const etag = plannerTask["@odata.etag"] || task.lastPlannerEtag;
        if (!etag) {
            logger.warn("Missing Planner ETag; skipping update", { taskId: task.plannerTaskId });
        } else {
            try {
                await graphClient.updateTask(task.plannerTaskId, changes, etag);
                updatedPlanner = true;
            } catch (error) {
                if (!isConflict(error)) {
                    throw error;
                }
                logger.warn("Planner update conflict; retrying with latest task", {
                    taskId: task.plannerTaskId,
                    error: error?.message || String(error),
                });
                let latestTask = null;
                try {
                    latestTask = await graphClient.getTask(task.plannerTaskId);
                } catch (reloadError) {
                    logger.warn("Planner task reload failed after conflict", {
                        taskId: task.plannerTaskId,
                        error: reloadError?.message || String(reloadError),
                    });
                    return;
                }
                if (!latestTask) return;
                const retryChanges = buildChanges(latestTask);
                if (!Object.keys(retryChanges).length) return;
                const retryEtag = latestTask["@odata.etag"];
                if (!retryEtag) {
                    logger.warn("Missing Planner ETag after conflict; skipping update", { taskId: task.plannerTaskId });
                    return;
                }
                try {
                    await graphClient.updateTask(task.plannerTaskId, retryChanges, retryEtag);
                    updatedPlanner = true;
                } catch (retryError) {
                    logger.warn("Planner retry update failed", {
                        taskId: task.plannerTaskId,
                        error: retryError?.message || String(retryError),
                    });
                    return;
                }
            }
        }
    }

    if (details?.description !== desiredDescription && details?.["@odata.etag"]) {
        await graphClient.updateTaskDetails(task.plannerTaskId, { description: desiredDescription }, details["@odata.etag"]);
        updatedPlanner = true;
    }

    if (!updatedPlanner) {
        logger.info("No BC → Planner changes detected; skipping metadata update", {
            taskId: task.plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
        });
        return;
    }

    const latest = await graphClient.getTask(task.plannerTaskId);
    await updateBcTaskWithSyncLock(bcClient, task, {
        plannerPlanId: planId,
        plannerBucket: bucketName,
        lastPlannerEtag: latest?.["@odata.etag"],
        lastSyncAt: new Date().toISOString(),
    });
}

async function applyPlannerUpdateToBc(bcClient, graphClient, bcTask, plannerTask) {
    if (bcTask.syncLock) {
        logger.info("Skipping inbound update for sync-locked task", {
            taskId: plannerTask.id,
            projectNo: bcTask.projectNo,
        });
        return;
    }

    const syncDecision = resolveSyncDecision(bcTask, plannerTask);
    if (syncDecision.decision === "bc") {
        logger.info("Skipping inbound Planner update; BC is newer", {
            projectNo: bcTask.projectNo,
            taskNo: bcTask.taskNo,
            lastSyncAt: bcTask.lastSyncAt,
            plannerModifiedAt: syncDecision.plannerModified.raw,
            bcModifiedAt: syncDecision.bcModified.raw,
        });
        return;
    }
    if (syncDecision.decision === "none") {
        logger.info("Skipping inbound Planner update; no changes since last sync", {
            projectNo: bcTask.projectNo,
            taskNo: bcTask.taskNo,
            lastSyncAt: bcTask.lastSyncAt,
        });
        return;
    }

    let bucketName;
    if (plannerTask.bucketId) {
        try {
            bucketName = (await graphClient.getBucket(plannerTask.bucketId))?.name;
        } catch (error) {
            logger.warn("Planner bucket lookup failed", {
                bucketId: plannerTask.bucketId,
                error: error?.message || String(error),
            });
        }
    }
    const bcPercent = toBcPercent(plannerTask.percentComplete ?? 0);
    const startDate = toBcDate(plannerTask.startDateTime || null);
    const dueDate = toBcDate(plannerTask.dueDateTime || null);

    const updates = {
        percentComplete: bcPercent,
        plannerBucket: bucketName || bcTask.plannerBucket,
        plannerPlanId: plannerTask.planId || bcTask.plannerPlanId,
        plannerTaskId: plannerTask.id,
        lastPlannerEtag: plannerTask["@odata.etag"],
        lastSyncAt: new Date().toISOString(),
    };

    if (hasField(bcTask, "manualStartDate")) {
        updates.manualStartDate = startDate;
    } else {
        updates.startDate = startDate;
    }

    if (hasField(bcTask, "manualEndDate")) {
        updates.manualEndDate = dueDate;
    } else {
        updates.endDate = dueDate;
    }

    await updateBcTaskWithSyncLock(bcClient, bcTask, updates);
}

export async function syncPlannerNotification(notification) {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());

    const bcTask = await bcClient.findProjectTaskByPlannerTaskId(notification.taskId);
    if (!bcTask) {
        logger.info("No BC task found for Planner notification", { taskId: notification.taskId });
        return;
    }
    if (isProjectDisabled(disabledProjects, bcTask.projectNo)) {
        logger.info("Skipping Planner notification for disabled project", {
            taskId: notification.taskId,
            projectNo: bcTask.projectNo,
        });
        return;
    }

    let plannerTask = null;
    try {
        plannerTask = await graphClient.getTask(notification.taskId);
    } catch (error) {
        logger.warn("Planner task lookup failed", { taskId: notification.taskId, error: error?.message || String(error) });
        return;
    }
    if (!plannerTask) {
        logger.warn("Planner task not found", { taskId: notification.taskId });
        return;
    }

    if (bcTask.lastPlannerEtag && bcTask.lastPlannerEtag === plannerTask["@odata.etag"]) {
        logger.info("Planner task unchanged; skipping inbound sync", { taskId: notification.taskId });
        return;
    }

    await applyPlannerUpdateToBc(bcClient, graphClient, bcTask, plannerTask);
}

export async function triggerNotificationProcessing() {
    await processQueue(syncPlannerNotification);
}

export async function syncBcToPlanner(projectNo) {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const bucketCache = new Map();
    const plannerBaseUrl = await resolvePlannerBaseUrl(graphClient);
    const { tenantId } = getGraphConfig();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());

    if (projectNo) {
        if (isProjectDisabled(disabledProjects, projectNo)) {
            logger.info("Project sync disabled; skipping BC → Planner", { projectNo });
            return { projectNo, tasks: 0, skipped: true, reason: "sync disabled" };
        }
        const rawTasks = await bcClient.listProjectTasks(`projectNo eq '${projectNo.replace(/'/g, "''")}'`);
        const tasks = filterTasksForProject(rawTasks, projectNo);
        if (rawTasks.length && tasks.length !== rawTasks.length) {
            logger.warn("Filtered BC tasks by projectNo", {
                projectNo,
                before: rawTasks.length,
                after: tasks.length,
            });
        }
        if (!tasks.length) return { projectNo, tasks: 0 };
        let projectDescription;
        try {
            const projects = await bcClient.listProjects(`projectNo eq '${projectNo.replace(/'/g, "''")}'`);
            projectDescription = projects[0]?.description;
        } catch (error) {
            logger.warn("Failed to load project description", { projectNo, error: error?.message || String(error) });
        }
        const planTitle = buildPlanTitle(projectNo, projectDescription);
        const planId = await syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projectNo, planTitle);
        return { projectNo, tasks: tasks.length, planId, planUrl: buildPlannerPlanUrl(planId, plannerBaseUrl, tenantId) };
    }

    let projects = null;
    try {
        projects = await bcClient.listProjects();
    } catch (error) {
        logger.warn("BC projects endpoint unavailable; require projectNo", { error: error?.message || String(error) });
        throw new Error("Projects endpoint unavailable; pass projectNo to sync");
    }

    if (!projects || !projects.length) return { projects: 0, tasks: 0 };

    let totalTasks = 0;
    const plans = [];
    const skippedProjects = [];
    for (const project of projects) {
        const projNo = (project.projectNo || "").trim();
        if (!projNo) continue;
        if (isProjectDisabled(disabledProjects, projNo)) {
            skippedProjects.push(projNo);
            continue;
        }
        const planTitle = buildPlanTitle(projNo, project.description);
        const rawTasks = await bcClient.listProjectTasks(`projectNo eq '${projNo.replace(/'/g, "''")}'`);
        const tasks = filterTasksForProject(rawTasks, projNo);
        if (rawTasks.length && tasks.length !== rawTasks.length) {
            logger.warn("Filtered BC tasks by projectNo", {
                projectNo: projNo,
                before: rawTasks.length,
                after: tasks.length,
            });
        }
        totalTasks += tasks.length;
        const planId = await syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projNo, planTitle);
        plans.push({ projectNo: projNo, planId, planUrl: buildPlannerPlanUrl(planId, plannerBaseUrl, tenantId) });
    }

    return { projects: projects.length, tasks: totalTasks, plans, skippedProjects };
}

async function syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projectNo, planTitle) {
    const { syncMode, allowDefaultPlanFallback } = getSyncConfig();
    const { planId, titlePrefix } = await resolvePlanForProject(graphClient, projectNo, tasks, planTitle);
    const orderedTasks = [...tasks].sort((a, b) => {
        const aKey = (a.taskNo || "").toString();
        const bKey = (b.taskNo || "").toString();
        return aKey.localeCompare(bKey, undefined, { numeric: true, sensitivity: "base" });
    });
    let currentBucket = DEFAULT_BUCKET_NAME;
    let skipSection = false;

    for (const task of orderedTasks) {
        const taskType = (task.taskType || "").toLowerCase();
        if (taskType === "heading") {
            const resolved = resolveBucketFromHeading(task.description);
            if (resolved.skip) {
                currentBucket = null;
                skipSection = true;
                continue;
            }
            currentBucket = resolved.bucket;
            skipSection = false;
            await ensureBucket(graphClient, planId, currentBucket, bucketCache);
            continue;
        }
        if (taskType !== "posting") continue;
        if (skipSection || !currentBucket) continue;
        const bucketId = await ensureBucket(graphClient, planId, currentBucket, bucketCache);
        const planMismatch = syncMode === "perProjectPlan" &&
            !allowDefaultPlanFallback &&
            task.plannerPlanId &&
            task.plannerPlanId !== planId;
        if (planMismatch) {
            logger.warn("Planner plan mismatch; creating new task in per-project plan", {
                projectNo,
                taskNo: task.taskNo,
                fromPlanId: task.plannerPlanId,
                toPlanId: planId,
            });
        }
        const syncTask = planMismatch ? { ...task, plannerTaskId: undefined, plannerPlanId: undefined } : task;
        await upsertPlannerTask(bcClient, graphClient, syncTask, planId, bucketId, currentBucket, titlePrefix);
    }

    return planId;
}

export async function runPollingSync() {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const { pollMinutes } = getSyncConfig();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());
    const isNotFound = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 404");
    };

    const tasks = await bcClient.listProjectTasks("plannerTaskId ne ''");
    const cutoff = Date.now() - pollMinutes * 60 * 1000;

    let processed = 0;
    let skippedDisabled = 0;
    for (const task of tasks) {
        if (isProjectDisabled(disabledProjects, task.projectNo)) {
            skippedDisabled += 1;
            continue;
        }
        const lastSync = task.lastSyncAt ? Date.parse(task.lastSyncAt) : 0;
        if (lastSync && lastSync > cutoff) continue;
        if (!task.plannerTaskId) continue;
        let plannerTask = null;
        try {
            plannerTask = await graphClient.getTask(task.plannerTaskId);
        } catch (error) {
            logger.warn("Planner task lookup failed during polling", {
                taskId: task.plannerTaskId,
                error: error?.message || String(error),
            });
            if (task.systemId && isNotFound(error)) {
                try {
                    await bcClient.patchProjectTask(task.systemId, {
                        plannerTaskId: "",
                        plannerPlanId: "",
                        plannerBucket: "",
                        lastPlannerEtag: "",
                        syncLock: false,
                    });
                    logger.warn("Cleared stale Planner linkage after 404", {
                        taskId: task.plannerTaskId,
                        projectNo: task.projectNo,
                        taskNo: task.taskNo,
                    });
                } catch (patchError) {
                    logger.warn("Failed to clear stale Planner linkage", {
                        taskId: task.plannerTaskId,
                        error: patchError?.message || String(patchError),
                    });
                }
            }
            continue;
        }
        if (!plannerTask) continue;
        if (task.lastPlannerEtag && task.lastPlannerEtag === plannerTask["@odata.etag"]) continue;
        await applyPlannerUpdateToBc(bcClient, graphClient, task, plannerTask);
        processed += 1;
    }

    return { processed, total: tasks.length, skippedDisabled };
}

export async function enqueueAndProcessNotifications(items) {
    await enqueueNotifications(items);
    processQueue(syncPlannerNotification).catch((error) => {
        logger.error("Notification processing error", { error: error?.message || String(error) });
    });
}
