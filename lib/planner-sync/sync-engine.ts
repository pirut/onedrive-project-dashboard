import { BusinessCentralClient, BcProjectTask } from "./bc-client";
import { GraphClient, PlannerTask, PlannerTaskDetails } from "./graph-client";
import { getPlannerConfig, getSyncConfig } from "./config";
import { logger } from "./logger";
import { PlannerNotification, enqueueNotifications, processQueue } from "./queue";

const DEFAULT_BUCKET_NAME = "General";

function hasField(task: BcProjectTask, field: string) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function normalizeBucketName(name?: string | null) {
    const trimmed = (name || "").trim();
    return trimmed || DEFAULT_BUCKET_NAME;
}

function normalizeDateOnly(value?: string | null) {
    if (!value) return null;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return null;
    return date.toISOString().slice(0, 10);
}

function toPlannerDate(value?: string | null) {
    const dateOnly = normalizeDateOnly(value || null);
    if (!dateOnly) return null;
    // Planner only accepts dates within 1984-01-01 and 2149-12-31.
    if (dateOnly < "1984-01-01" || dateOnly > "2149-12-31") return null;
    const date = new Date(`${dateOnly}T00:00:00Z`);
    if (Number.isNaN(date.getTime())) return null;
    return date.toISOString();
}

function toBcDate(value?: string | null) {
    return normalizeDateOnly(value || null);
}

function toPlannerPercent(value?: number | null) {
    if (value == null) return 0;
    if (value >= 100) return 100;
    if (value > 0) return 50;
    return 0;
}

function toBcPercent(value?: number | null) {
    if (value == null) return 0;
    if (value >= 100) return 100;
    if (value >= 50) return 50;
    return 0;
}

function formatPlannerDescription(task: BcProjectTask) {
    const formatValue = (val: unknown) => (val == null ? "" : String(val));
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

function buildPlannerTitle(task: BcProjectTask, prefix: string | null) {
    const taskNo = (task.taskNo || "").trim();
    const description = (task.description || "").trim();
    const base = [taskNo, description].filter(Boolean).join(" - ") || "Untitled Task";
    return `${prefix || ""}${base}`;
}

function buildBcPatch(task: BcProjectTask, updates: Record<string, unknown>) {
    const patch: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(updates)) {
        if (hasField(task, key)) {
            patch[key] = value;
        }
    }
    return patch;
}

async function updateBcTaskWithSyncLock(
    bcClient: BusinessCentralClient,
    task: BcProjectTask,
    updates: Record<string, unknown>
) {
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

async function resolvePlanForProject(
    graphClient: GraphClient,
    projectNo: string,
    tasks: BcProjectTask[]
) {
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

    let plans: { id: string; title?: string }[] = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list plans for group", { error: (error as Error)?.message });
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
            error: (error as Error)?.message,
        });
    }

    if (!plannerConfig.defaultPlanId) {
        throw new Error("Plan creation failed and PLANNER_DEFAULT_PLAN_ID is not set");
    }
    return { planId: plannerConfig.defaultPlanId, titlePrefix: `${projectNo} - ` };
}

async function ensureBucket(
    graphClient: GraphClient,
    planId: string,
    bucketName: string,
    cache: Map<string, Map<string, string>>
) {
    const normalizedName = normalizeBucketName(bucketName);
    const key = normalizedName.toLowerCase();
    if (!cache.has(planId)) {
        const buckets = await graphClient.listBuckets(planId);
        const bucketMap = new Map<string, string>();
        for (const bucket of buckets) {
            if (bucket.name) {
                bucketMap.set(bucket.name.trim().toLowerCase(), bucket.id);
            }
        }
        cache.set(planId, bucketMap);
    }
    const bucketMap = cache.get(planId) as Map<string, string>;
    if (bucketMap.has(key)) {
        return bucketMap.get(key) as string;
    }
    const created = await graphClient.createBucket(planId, normalizedName);
    bucketMap.set(key, created.id);
    return created.id;
}

async function upsertPlannerTask(
    bcClient: BusinessCentralClient,
    graphClient: GraphClient,
    task: BcProjectTask,
    planId: string,
    bucketId: string,
    bucketName: string,
    titlePrefix: string
) {
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
        const payload: Record<string, unknown> = {
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
            await graphClient.updateTaskDetails(created.id, { description: desiredDescription }, details["@odata.etag"] as string);
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

    let plannerTask: PlannerTask | null = null;
    let details: PlannerTaskDetails | null = null;
    try {
        plannerTask = await graphClient.getTask(task.plannerTaskId);
        details = await graphClient.getTaskDetails(task.plannerTaskId);
    } catch (error) {
        logger.error("Failed to fetch Planner task", {
            taskId: task.plannerTaskId,
            error: (error as Error)?.message,
        });
        return;
    }

    if (!plannerTask) return;

    const changes: Record<string, unknown> = {};
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
        await graphClient.updateTaskDetails(task.plannerTaskId, { description: desiredDescription }, details["@odata.etag"] as string);
    }

    const latest = await graphClient.getTask(task.plannerTaskId);
    await updateBcTaskWithSyncLock(bcClient, task, {
        plannerPlanId: planId,
        plannerBucket: bucketName,
        lastPlannerEtag: latest?.["@odata.etag"],
        lastSyncAt: new Date().toISOString(),
    });
}

async function applyPlannerUpdateToBc(
    bcClient: BusinessCentralClient,
    graphClient: GraphClient,
    bcTask: BcProjectTask,
    plannerTask: PlannerTask
) {
    if (bcTask.syncLock) {
        logger.info("Skipping inbound update for sync-locked task", {
            taskId: plannerTask.id,
            projectNo: bcTask.projectNo,
        });
        return;
    }

    let bucketName: string | undefined;
    if (plannerTask.bucketId) {
        try {
            bucketName = (await graphClient.getBucket(plannerTask.bucketId))?.name;
        } catch (error) {
            logger.warn("Planner bucket lookup failed", {
                bucketId: plannerTask.bucketId,
                error: (error as Error)?.message,
            });
        }
    }
    const bcPercent = toBcPercent(plannerTask.percentComplete ?? 0);
    const startDate = toBcDate(plannerTask.startDateTime || null);
    const dueDate = toBcDate(plannerTask.dueDateTime || null);

    const updates: Record<string, unknown> = {
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

export async function syncPlannerNotification(notification: PlannerNotification) {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();

    const bcTask = await bcClient.findProjectTaskByPlannerTaskId(notification.taskId);
    if (!bcTask) {
        logger.info("No BC task found for Planner notification", { taskId: notification.taskId });
        return;
    }

    let plannerTask: PlannerTask | null = null;
    try {
        plannerTask = await graphClient.getTask(notification.taskId);
    } catch (error) {
        logger.warn("Planner task lookup failed", { taskId: notification.taskId, error: (error as Error)?.message });
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

export async function syncBcToPlanner(projectNo?: string) {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const bucketCache = new Map<string, Map<string, string>>();

    if (projectNo) {
        const tasks = await bcClient.listProjectTasks(`projectNo eq '${projectNo.replace(/'/g, "''")}'`);
        if (!tasks.length) return { projectNo, tasks: 0 };
        await syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projectNo);
        return { projectNo, tasks: tasks.length };
    }

    let projects: { projectNo?: string }[] | null = null;
    try {
        projects = await bcClient.listProjects();
    } catch (error) {
        logger.warn("BC projects endpoint unavailable; require projectNo", { error: (error as Error)?.message });
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

async function syncProjectTasks(
    bcClient: BusinessCentralClient,
    graphClient: GraphClient,
    tasks: BcProjectTask[],
    bucketCache: Map<string, Map<string, string>>,
    projectNo: string
) {
    const { planId, titlePrefix } = await resolvePlanForProject(graphClient, projectNo, tasks);
    const orderedTasks = [...tasks].sort((a, b) => {
        const aKey = (a.taskNo || "").toString();
        const bKey = (b.taskNo || "").toString();
        return aKey.localeCompare(bKey, undefined, { numeric: true, sensitivity: "base" });
    });
    let currentBucket = DEFAULT_BUCKET_NAME;

    for (const task of orderedTasks) {
        const taskType = (task.taskType || "").toLowerCase();
        if (taskType === "heading") {
            currentBucket = normalizeBucketName(task.description || DEFAULT_BUCKET_NAME);
            await ensureBucket(graphClient, planId, currentBucket, bucketCache);
            continue;
        }
        if (taskType !== "posting") continue;
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
        let plannerTask: PlannerTask | null = null;
        try {
            plannerTask = await graphClient.getTask(task.plannerTaskId);
        } catch (error) {
            logger.warn("Planner task lookup failed during polling", {
                taskId: task.plannerTaskId,
                error: (error as Error)?.message,
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

export async function enqueueAndProcessNotifications(items: PlannerNotification[]) {
    await enqueueNotifications(items);
    processQueue(syncPlannerNotification).catch((error) => {
        logger.error("Notification processing error", { error: (error as Error)?.message });
    });
}
