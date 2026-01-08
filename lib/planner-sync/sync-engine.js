import { BusinessCentralClient } from "./bc-client.js";
import { GraphClient } from "./graph-client.js";
import { getPlannerConfig, getSyncConfig } from "./config.js";
import { logger } from "./logger.js";
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

function buildPlannerTitle(task, prefix) {
    const taskNo = (task.taskNo || "").trim();
    const description = (task.description || "").trim();
    const base = [taskNo, description].filter(Boolean).join(" - ") || "Untitled Task";
    return `${prefix || ""}${base}`;
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

async function resolvePlanForProject(graphClient, projectNo, tasks) {
    const { syncMode } = getSyncConfig();
    const plannerConfig = getPlannerConfig();

    if (syncMode === "singlePlan") {
        if (!plannerConfig.defaultPlanId) {
            throw new Error("PLANNER_DEFAULT_PLAN_ID is required for SYNC_MODE=singlePlan");
        }
        return { planId: plannerConfig.defaultPlanId, titlePrefix: "" };
    }

    const existingPlanId = tasks.find((task) => task.plannerPlanId)?.plannerPlanId;
    if (existingPlanId) {
        return { planId: existingPlanId, titlePrefix: "" };
    }

    let plans = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list plans for group", { error: error?.message || String(error) });
    }
    const matchingPlan = plans.find((plan) => (plan.title || "").trim() === projectNo);
    if (matchingPlan?.id) {
        return { planId: matchingPlan.id, titlePrefix: "" };
    }

    try {
        const createdPlan = await graphClient.createPlan(plannerConfig.groupId, projectNo);
        return { planId: createdPlan.id, titlePrefix: "" };
    } catch (error) {
        logger.warn("Plan creation failed; falling back to default plan", {
            projectNo,
            error: error?.message || String(error),
        });
    }

    if (!plannerConfig.defaultPlanId) {
        throw new Error("Plan creation failed and PLANNER_DEFAULT_PLAN_ID is not set");
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
        logger.info("Skipping BC task with syncLock", { taskNo: task.taskNo, projectNo: task.projectNo });
        return;
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

    const changes = {};
    if ((plannerTask.title || "") !== desiredTitle) changes.title = desiredTitle;
    if (plannerTask.bucketId !== bucketId) changes.bucketId = bucketId;

    const plannerStart = normalizeDateOnly(plannerTask.startDateTime || null);
    const plannerDue = normalizeDateOnly(plannerTask.dueDateTime || null);
    const desiredStartDate = normalizeDateOnly(desiredStart || null);
    const desiredDueDate = normalizeDateOnly(desiredDue || null);

    if (plannerStart !== desiredStartDate) changes.startDateTime = desiredStart;
    if (plannerDue !== desiredDueDate) changes.dueDateTime = desiredDue;
    if ((plannerTask.percentComplete || 0) !== desiredPercent) changes.percentComplete = desiredPercent;

    if (Object.keys(changes).length) {
        const etag = task.lastPlannerEtag || plannerTask["@odata.etag"];
        if (!etag) {
            logger.warn("Missing Planner ETag; skipping update", { taskId: task.plannerTaskId });
        } else {
            await graphClient.updateTask(task.plannerTaskId, changes, etag);
        }
    }

    if (details?.description !== desiredDescription && details?.["@odata.etag"]) {
        await graphClient.updateTaskDetails(task.plannerTaskId, { description: desiredDescription }, details["@odata.etag"]);
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

    const bcTask = await bcClient.findProjectTaskByPlannerTaskId(notification.taskId);
    if (!bcTask) {
        logger.info("No BC task found for Planner notification", { taskId: notification.taskId });
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

    if (projectNo) {
        const tasks = await bcClient.listProjectTasks(`projectNo eq '${projectNo.replace(/'/g, "''")}'`);
        if (!tasks.length) return { projectNo, tasks: 0 };
        await syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projectNo);
        return { projectNo, tasks: tasks.length };
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
    for (const project of projects) {
        const projNo = (project.projectNo || "").trim();
        if (!projNo) continue;
        const tasks = await bcClient.listProjectTasks(`projectNo eq '${projNo.replace(/'/g, "''")}'`);
        totalTasks += tasks.length;
        await syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projNo);
    }

    return { projects: projects.length, tasks: totalTasks };
}

async function syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projectNo) {
    const { planId, titlePrefix } = await resolvePlanForProject(graphClient, projectNo, tasks);
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
        await upsertPlannerTask(bcClient, graphClient, task, planId, bucketId, currentBucket, titlePrefix);
    }
}

export async function runPollingSync() {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const { pollMinutes } = getSyncConfig();

    const tasks = await bcClient.listProjectTasks("plannerTaskId ne ''");
    const cutoff = Date.now() - pollMinutes * 60 * 1000;

    let processed = 0;
    for (const task of tasks) {
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
            continue;
        }
        if (!plannerTask) continue;
        if (task.lastPlannerEtag && task.lastPlannerEtag === plannerTask["@odata.etag"]) continue;
        await applyPlannerUpdateToBc(bcClient, graphClient, task, plannerTask);
        processed += 1;
    }

    return { processed, total: tasks.length };
}

export async function enqueueAndProcessNotifications(items) {
    await enqueueNotifications(items);
    processQueue(syncPlannerNotification).catch((error) => {
        logger.error("Notification processing error", { error: error?.message || String(error) });
    });
}
