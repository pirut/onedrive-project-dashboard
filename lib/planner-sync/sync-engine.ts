import { BusinessCentralClient, BcProjectTask } from "./bc-client";
import { GraphClient, PlannerTask, PlannerTaskDetails } from "./graph-client";
import { getGraphConfig, getPlannerConfig, getSyncConfig } from "./config";
import { logger } from "./logger";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "./project-sync-store";
import { PlannerNotification, enqueueNotifications, processQueue } from "./queue";

const DEFAULT_BUCKET_NAME = "General";
const HEADING_BUCKETS = [
    { match: "JOB NAME", bucket: "Pre-Construction" },
    { match: "INSTALL", bucket: "Installation" },
    { match: "CHANGE ORDER", bucket: "Change Orders" },
    { match: "REVENUE", bucket: null },
] as const;

function hasField(task: BcProjectTask, field: string) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function normalizeBucketName(name?: string | null) {
    const trimmed = (name || "").trim();
    return trimmed || DEFAULT_BUCKET_NAME;
}

function hasTimeZoneSuffix(value: string) {
    return /([zZ]|[+-]\d{2}:\d{2})$/.test(value);
}

function getTimeZoneOffsetMs(date: Date, timeZone: string) {
    try {
        const formatter = new Intl.DateTimeFormat("en-US", {
            timeZone,
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hour12: false,
        });
        const parts = formatter.formatToParts(date);
        const values: Record<string, string> = {};
        for (const part of parts) {
            if (part.type !== "literal") values[part.type] = part.value;
        }
        const asUtc = Date.UTC(
            Number(values.year),
            Number(values.month) - 1,
            Number(values.day),
            Number(values.hour),
            Number(values.minute),
            Number(values.second)
        );
        return asUtc - date.getTime();
    } catch {
        return 0;
    }
}

function parseNaiveDateInTimeZone(value: string, timeZone: string) {
    const match = value.trim().match(
        /^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?$/
    );
    if (!match) return null;
    const [, y, mo, d, h = "0", mi = "0", s = "0", msRaw = "0"] = match;
    const ms = Number(msRaw.padEnd(3, "0"));
    const guessUtc = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s), ms);
    const offset = getTimeZoneOffsetMs(new Date(guessUtc), timeZone);
    return guessUtc - offset;
}

function parseDateMs(value?: string | null) {
    if (!value) return null;
    const str = String(value).trim();
    if (!str) return null;
    if (hasTimeZoneSuffix(str)) {
        const ms = Date.parse(str);
        return Number.isNaN(ms) ? null : ms;
    }
    const { timeZone } = getSyncConfig();
    const tzMs = parseNaiveDateInTimeZone(str, timeZone);
    if (tzMs != null) return tzMs;
    const fallback = Date.parse(str);
    return Number.isNaN(fallback) ? null : fallback;
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

function resolveBucketFromHeading(description?: string | null) {
    const heading = (description || "").trim();
    if (!heading) return { bucket: DEFAULT_BUCKET_NAME, skip: false };
    const normalized = heading.toUpperCase();
    for (const entry of HEADING_BUCKETS) {
        if (normalized.includes(entry.match)) {
            return { bucket: entry.bucket ?? null, skip: entry.bucket == null };
        }
    }
    return { bucket: normalizeBucketName(heading), skip: false };
}
function normalizeDateOnly(value?: string | null) {
    const ms = parseDateMs(value || null);
    if (ms == null) return null;
    const date = new Date(ms);
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

const BC_MODIFIED_FIELDS = [
    "systemModifiedAt",
    "lastModifiedDateTime",
    "lastModifiedAt",
    "modifiedAt",
    "modifiedOn",
    "lastModifiedOn",
    "systemModifiedOn",
] as const;

function resolveBcModifiedAt(task: BcProjectTask) {
    for (const field of BC_MODIFIED_FIELDS) {
        const raw = (task as Record<string, unknown>)[field];
        if (typeof raw !== "string") continue;
        const ms = parseDateMs(raw);
        if (ms != null) {
            return { ms, field, raw };
        }
    }
    return { ms: null as number | null, field: null as string | null, raw: null as string | null };
}

function resolvePlannerModifiedAt(task: PlannerTask | null) {
    const raw = task?.lastModifiedDateTime || null;
    return { ms: parseDateMs(raw), raw };
}

type SyncDecision = "bc" | "planner" | "none";

function resolveSyncDecision(bcTask: BcProjectTask, plannerTask: PlannerTask | null) {
    const lastSyncAt = parseDateMs(bcTask.lastSyncAt || null);
    const bcModified = resolveBcModifiedAt(bcTask);
    const plannerModified = resolvePlannerModifiedAt(plannerTask);
    const plannerEtag = typeof plannerTask?.["@odata.etag"] === "string" ? plannerTask["@odata.etag"] : null;
    const lastPlannerEtag = typeof bcTask.lastPlannerEtag === "string" ? bcTask.lastPlannerEtag : null;
    const plannerEtagChanged = plannerEtag && lastPlannerEtag ? plannerEtag !== lastPlannerEtag : null;
    const { bcModifiedGraceMs } = getSyncConfig();
    const bcGrace = Number.isFinite(bcModifiedGraceMs) ? bcModifiedGraceMs : 0;
    const bcChangedSinceSync = lastSyncAt != null
        ? bcModified.ms != null
            ? bcModified.ms > lastSyncAt + bcGrace
            : false
        : null;
    let plannerChangedSinceSync: boolean | null = null;
    if (lastSyncAt != null) {
        if (plannerModified.ms != null) {
            plannerChangedSinceSync = plannerModified.ms > lastSyncAt;
        } else if (plannerEtagChanged != null) {
            plannerChangedSinceSync = plannerEtagChanged;
        }
    } else if (plannerEtagChanged != null) {
        plannerChangedSinceSync = plannerEtagChanged;
    }

    if (lastSyncAt != null) {
        const bcChanged = bcChangedSinceSync === true;
        const plannerChanged = plannerChangedSinceSync === true;
        if (bcChanged) {
            return {
                decision: "bc",
                lastSyncAt,
                bcModified,
                plannerModified,
                bcChangedSinceSync,
                plannerChangedSinceSync,
                plannerEtagChanged,
            };
        }
        if (plannerChanged) {
            return {
                decision: "planner",
                lastSyncAt,
                bcModified,
                plannerModified,
                bcChangedSinceSync,
                plannerChangedSinceSync,
                plannerEtagChanged,
            };
        }
        return {
            decision: "none",
            lastSyncAt,
            bcModified,
            plannerModified,
            bcChangedSinceSync,
            plannerChangedSinceSync,
            plannerEtagChanged,
        };
    }

    if (plannerModified.ms != null || plannerEtagChanged === true) {
        return {
            decision: "planner",
            lastSyncAt,
            bcModified,
            plannerModified,
            bcChangedSinceSync,
            plannerChangedSinceSync,
            plannerEtagChanged,
        };
    }
    if (bcModified.ms != null) {
        return {
            decision: "bc",
            lastSyncAt,
            bcModified,
            plannerModified,
            bcChangedSinceSync,
            plannerChangedSinceSync,
            plannerEtagChanged,
        };
    }
    return {
        decision: "planner",
        lastSyncAt,
        bcModified,
        plannerModified,
        bcChangedSinceSync,
        plannerChangedSinceSync,
        plannerEtagChanged,
    };
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

function buildPlanTitle(projectNo: string, projectDescription?: string | null) {
    const cleaned = (projectDescription || "").trim();
    return cleaned ? `${projectNo} - ${cleaned}` : projectNo;
}

function buildPlannerTitle(task: BcProjectTask, prefix: string | null) {
    const description = (task.description || "").trim();
    const taskNo = (task.taskNo || "").trim();
    const base = description || taskNo || "Untitled Task";
    return `${prefix || ""}${base}`;
}

function isStaleSyncLock(task: BcProjectTask, timeoutMinutes: number) {
    if (!task.syncLock) return false;
    if (timeoutMinutes <= 0) return false;
    const lastSync = task.lastSyncAt ? Date.parse(task.lastSyncAt) : NaN;
    if (Number.isNaN(lastSync)) return true;
    return Date.now() - lastSync > timeoutMinutes * 60 * 1000;
}

function filterTasksForProject(tasks: BcProjectTask[], projectNo: string) {
    const normalized = (projectNo || "").trim().toLowerCase();
    if (!normalized) return tasks;
    return tasks.filter((task) => (task.projectNo || "").trim().toLowerCase() === normalized);
}

function isProjectDisabled(disabledProjects: Set<string>, projectNo?: string | null) {
    if (!projectNo) return false;
    const normalized = normalizeProjectNo(projectNo);
    return normalized ? disabledProjects.has(normalized) : false;
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
    tasks: BcProjectTask[],
    projectTitle: string
) {
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
        } catch (error) {
            logger.warn("Failed to verify existing planner plan", {
                projectNo,
                planId: existingPlanId,
                error: (error as Error)?.message,
            });
        }
    }

    let plans: { id: string; title?: string }[] = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list plans for group", { error: (error as Error)?.message });
    }
    const matchingPlan = plans.find((plan) => {
        const title = (plan.title || "").trim();
        return title === projectTitle || title === projectNo;
    });
    if (matchingPlan?.id) {
        return { planId: matchingPlan.id, titlePrefix: "" };
    }

    let planCreateError: string | undefined;
    try {
        const createdPlan = await graphClient.createPlan(plannerConfig.groupId, projectTitle);
        return { planId: createdPlan.id, titlePrefix: "" };
    } catch (error) {
        planCreateError = (error as Error)?.message || String(error);
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
                    error: (error as Error)?.message,
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
    const buildChanges = (currentTask: PlannerTask) => {
        const changes: Record<string, unknown> = {};
        if ((currentTask.title || "") !== desiredTitle) changes.title = desiredTitle;
        if (currentTask.bucketId !== bucketId) changes.bucketId = bucketId;

        const plannerStart = normalizeDateOnly(currentTask.startDateTime || null);
        const plannerDue = normalizeDateOnly(currentTask.dueDateTime || null);

        if (plannerStart !== desiredStartDate) changes.startDateTime = desiredStart;
        if (plannerDue !== desiredDueDate) changes.dueDateTime = desiredDue;
        if ((currentTask.percentComplete || 0) !== desiredPercent) changes.percentComplete = desiredPercent;
        return changes;
    };
    const isConflict = (error: unknown) => {
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
                    error: (error as Error)?.message,
                });
                let latestTask: PlannerTask | null = null;
                try {
                    latestTask = await graphClient.getTask(task.plannerTaskId);
                } catch (reloadError) {
                    logger.warn("Planner task reload failed after conflict", {
                        taskId: task.plannerTaskId,
                        error: (reloadError as Error)?.message,
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
                        error: (retryError as Error)?.message,
                    });
                    return;
                }
            }
        }
    }

    if (details?.description !== desiredDescription && details?.["@odata.etag"]) {
        await graphClient.updateTaskDetails(task.plannerTaskId, { description: desiredDescription }, details["@odata.etag"] as string);
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
    const plannerPercent = plannerTask.percentComplete ?? 0;
    const plannerInProgress = plannerPercent > 0 && plannerPercent < 100;
    const currentBcPercent = typeof bcTask.percentComplete === "number" ? bcTask.percentComplete : null;
    const bcPercent = plannerInProgress
        ? currentBcPercent != null && currentBcPercent > 0 && currentBcPercent < 100
            ? currentBcPercent
            : 50
        : toBcPercent(plannerPercent);
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

    const hasChanges = Object.entries(updates).some(([key, value]) => {
        const current = (bcTask as Record<string, unknown>)[key];
        if (current == null && value == null) return false;
        return current !== value;
    });
    if (!hasChanges) {
        logger.info("No Planner → BC changes detected; skipping update", {
            projectNo: bcTask.projectNo,
            taskNo: bcTask.taskNo,
            taskId: plannerTask.id,
        });
        return;
    }

    await updateBcTaskWithSyncLock(bcClient, bcTask, updates);
}

export async function syncPlannerNotification(notification: PlannerNotification) {
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
        let projectDescription: string | undefined;
        try {
            const projects = await bcClient.listProjects(`projectNo eq '${projectNo.replace(/'/g, "''")}'`);
            projectDescription = projects[0]?.description;
        } catch (error) {
            logger.warn("Failed to load project description", { projectNo, error: (error as Error)?.message });
        }
        const planTitle = buildPlanTitle(projectNo, projectDescription);
        const planId = await syncProjectTasks(bcClient, graphClient, tasks, bucketCache, projectNo, planTitle);
        return { projectNo, tasks: tasks.length, planId, planUrl: buildPlannerPlanUrl(planId, plannerBaseUrl, tenantId) };
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
    const plans: { projectNo: string; planId?: string; planUrl?: string }[] = [];
    const skippedProjects: string[] = [];
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

async function syncProjectTasks(
    bcClient: BusinessCentralClient,
    graphClient: GraphClient,
    tasks: BcProjectTask[],
    bucketCache: Map<string, Map<string, string>>,
    projectNo: string,
    planTitle: string
) {
    const { syncMode, allowDefaultPlanFallback } = getSyncConfig();
    const { planId, titlePrefix } = await resolvePlanForProject(graphClient, projectNo, tasks, planTitle);
    const orderedTasks = [...tasks].sort((a, b) => {
        const aKey = (a.taskNo || "").toString();
        const bKey = (b.taskNo || "").toString();
        return aKey.localeCompare(bKey, undefined, { numeric: true, sensitivity: "base" });
    });
    let currentBucket: string | null = DEFAULT_BUCKET_NAME;
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
        const planMismatch =
            syncMode === "perProjectPlan" &&
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
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());

    const isNotFound = (error: unknown) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 404");
    };

    const tasks = await bcClient.listProjectTasks("plannerTaskId ne ''");

    let processed = 0;
    let skippedDisabled = 0;
    for (const task of tasks) {
        if (isProjectDisabled(disabledProjects, task.projectNo)) {
            skippedDisabled += 1;
            continue;
        }
        if (!task.plannerTaskId) continue;
        let plannerTask: PlannerTask | null = null;
        try {
            plannerTask = await graphClient.getTask(task.plannerTaskId);
        } catch (error) {
            logger.warn("Planner task lookup failed during polling", {
                taskId: task.plannerTaskId,
                error: (error as Error)?.message,
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
                        error: (patchError as Error)?.message,
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

export async function enqueueAndProcessNotifications(items: PlannerNotification[]) {
    await enqueueNotifications(items);
    processQueue(syncPlannerNotification).catch((error) => {
        logger.error("Notification processing error", { error: (error as Error)?.message });
    });
}
