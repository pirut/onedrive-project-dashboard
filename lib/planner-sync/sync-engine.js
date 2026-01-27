import { BusinessCentralClient } from "./bc-client.js";
import { GraphClient } from "./graph-client.js";
import { getBcConfig, getGraphConfig, getPlannerConfig, getSyncConfig } from "./config.js";
import { getBcProjectChangeCursor, saveBcProjectChangeCursor } from "./bc-change-store.js";
import { clearPlannerDeltaState, getPlannerDeltaState, savePlannerDeltaState } from "./delta-store.js";
import { logger } from "./logger.js";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "./project-sync-store.js";
import { clearSmartPollingQueue, getSmartPollingQueue, saveSmartPollingQueue } from "./smart-polling-store.js";
import { enqueueNotifications, processQueue } from "./queue.js";

const DEFAULT_BUCKET_NAME = "General";
const HEADING_TASK_BUCKETS = new Map([
    [1000, "Pre-Construction"],
    [2000, "Installation"],
    [3000, null],
    [4000, "Change Orders"],
]);
const assignmentCache = new Map();
let groupMembersCache = null;
const missingAssigneeCache = new Set();

function hasField(task, field) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function normalizeBucketName(name) {
    const trimmed = (name || "").trim();
    return trimmed || DEFAULT_BUCKET_NAME;
}

function resolveAssigneeIdentity(task) {
    const code = (task.assignedPersonCode || "").trim();
    const name = (task.assignedPersonName || "").trim();
    if (code && code.includes("@")) return code;
    return name || code || null;
}

function hasPlannerAssignments(task) {
    const assignments = task?.assignments;
    if (assignments == null) return null;
    if (typeof assignments !== "object") return false;
    return Object.keys(assignments).length > 0;
}

function buildPlannerAssignments(userId) {
    return {
        [userId]: {
            "@odata.type": "microsoft.graph.plannerAssignment",
            orderHint: " !",
        },
    };
}

function logMissingAssignee(identity, meta) {
    const key = identity.trim().toLowerCase();
    if (!key)
        return;
    if (!missingAssigneeCache.has(key)) {
        missingAssigneeCache.add(key);
        logger.warn("Planner assignee not found for BC task", { ...meta, assignee: identity });
        return;
    }
    logger.debug("Planner assignee not found for BC task", { ...meta, assignee: identity });
}

async function resolveAssigneeUserId(graphClient, identity) {
    const trimmed = identity.trim();
    if (!trimmed) return null;
    const key = trimmed.toLowerCase();
    if (assignmentCache.has(key)) return assignmentCache.get(key) || null;
    if (groupMembersCache == null) {
        try {
            const { groupId } = getPlannerConfig();
            groupMembersCache = await graphClient.listGroupMembers(groupId);
        } catch (error) {
            logger.warn("Planner group member lookup failed", { error: error?.message || String(error) });
            groupMembersCache = [];
        }
    }
    const isEmail = key.includes("@");
    const match = groupMembersCache.find((member) => {
        if (!member?.id) return false;
        const mail = (member.mail || "").toLowerCase();
        const upn = (member.userPrincipalName || "").toLowerCase();
        const displayName = (member.displayName || "").toLowerCase();
        return isEmail ? mail === key || upn === key : displayName === key;
    });
    let userId = match?.id || null;
    if (!userId) {
        try {
            userId = await graphClient.findUserIdByIdentity(trimmed);
        } catch (error) {
            logger.warn("Planner user lookup failed", { identity: trimmed, error: error?.message || String(error) });
        }
    }
    assignmentCache.set(key, userId || null);
    return userId;
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

function resolveBucketFromHeading(taskNo, description) {
    const rawTaskNo = String(taskNo || "").trim();
    const match = rawTaskNo.match(/\d+/);
    const taskNumber = match ? Number(match[0]) : Number.NaN;
    if (!Number.isNaN(taskNumber) && HEADING_TASK_BUCKETS.has(taskNumber)) {
        const mapped = HEADING_TASK_BUCKETS.get(taskNumber) ?? null;
        return { bucket: mapped, skip: mapped == null };
    }
    const heading = (description || "").trim();
    if (!heading) return { bucket: DEFAULT_BUCKET_NAME, skip: false };
    return { bucket: normalizeBucketName(heading), skip: false };
}
function hasTimeZoneSuffix(value) {
    return /([zZ]|[+-]\d{2}:\d{2})$/.test(value);
}
function getTimeZoneOffsetMs(date, timeZone) {
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
        const values = {};
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
function parseNaiveDateInTimeZone(value, timeZone) {
    const match = value.trim().match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?$/);
    if (!match) return null;
    const [, y, mo, d, h = "0", mi = "0", s = "0", msRaw = "0"] = match;
    const ms = Number(msRaw.padEnd(3, "0"));
    const guessUtc = Date.UTC(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(s), ms);
    const offset = getTimeZoneOffsetMs(new Date(guessUtc), timeZone);
    return guessUtc - offset;
}
function parseDateMs(value) {
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
function normalizeDateOnly(value) {
    const ms = parseDateMs(value || null);
    if (ms == null) return null;
    const date = new Date(ms);
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

function hasBcChangedSinceSync(task, bcGraceMs) {
    const lastSyncAt = parseDateMs(task.lastSyncAt || null);
    if (lastSyncAt == null) return null;
    const bcModified = resolveBcModifiedAt(task);
    if (bcModified.ms == null) return null;
    return bcModified.ms > lastSyncAt + bcGraceMs;
}
function resolveSyncDecision(bcTask, plannerTask) {
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
    let plannerChangedSinceSync = null;
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
        if (bcChanged && plannerChanged) {
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

function normalizeProjectPlanKey(projectNo) {
    return (projectNo || "").trim().toLowerCase();
}

function matchesProjectPlanTitle(title, projectNo) {
    const normalizedTitle = (title || "").trim().toLowerCase();
    const key = normalizeProjectPlanKey(projectNo);
    if (!normalizedTitle || !key) return false;
    if (normalizedTitle === key || normalizedTitle.startsWith(`${key} -`)) return true;
    if (!key.includes("-")) return false;
    const spacedKey = key.replace(/-/g, " - ");
    return normalizedTitle === spacedKey || normalizedTitle.startsWith(`${spacedKey} -`);
}

function extractProjectNoFromPlanTitle(title) {
    const trimmed = (title || "").trim();
    if (!trimmed) return null;
    const match = trimmed.match(/^\s*(PR\d+(?:-\d+)?)\b/i);
    if (!match) return null;
    return normalizeProjectPlanKey(match[1]);
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
    let existingPlan = null;
    if (existingPlanId) {
        try {
            const plan = await graphClient.getPlan(existingPlanId);
            const planTitle = (plan?.title || "").trim();
            if (planTitle && matchesProjectPlanTitle(planTitle, projectNo)) {
                existingPlan = plan;
            }
            else if (planTitle) {
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
    const matchingPlans = plans.filter((plan) => matchesProjectPlanTitle(plan.title || "", projectNo));
    if (existingPlan && !matchingPlans.some((plan) => plan.id === existingPlan?.id)) {
        matchingPlans.push(existingPlan);
    }
    if (matchingPlans.length) {
        const sorted = [...matchingPlans].sort((a, b) => {
            const aCreated = Date.parse(String(a.createdDateTime || ""));
            const bCreated = Date.parse(String(b.createdDateTime || ""));
            if (Number.isFinite(aCreated) && Number.isFinite(bCreated) && aCreated !== bCreated) {
                return aCreated - bCreated;
            }
            const aTitle = (a.title || "").trim().toLowerCase();
            const bTitle = (b.title || "").trim().toLowerCase();
            return aTitle.localeCompare(bTitle);
        });
        const keep = sorted[0];
        for (const plan of sorted.slice(1)) {
            try {
                const deleted = await graphClient.deletePlan(plan.id);
                logger.warn("Removed duplicate Planner plan for project", {
                    projectNo,
                    planId: plan.id,
                    planTitle: plan.title,
                    deleted,
                });
            }
            catch (error) {
                logger.warn("Failed to remove duplicate Planner plan", {
                    projectNo,
                    planId: plan.id,
                    planTitle: plan.title,
                    error: error?.message || String(error),
                });
            }
        }
        return { planId: keep.id, titlePrefix: "" };
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
    const assigneeIdentity = resolveAssigneeIdentity(task);

    if (!task.plannerTaskId) {
        let desiredAssignments = null;
        if (assigneeIdentity) {
            const assigneeId = await resolveAssigneeUserId(graphClient, assigneeIdentity);
            if (assigneeId) {
                desiredAssignments = buildPlannerAssignments(assigneeId);
            } else {
                logMissingAssignee(assigneeIdentity, { projectNo: task.projectNo, taskNo: task.taskNo });
            }
        }
        const payload = {
            planId,
            bucketId,
            title: desiredTitle,
            percentComplete: desiredPercent,
        };
        if (desiredStart) payload.startDateTime = desiredStart;
        if (desiredDue) payload.dueDateTime = desiredDue;
        if (desiredAssignments) payload.assignments = desiredAssignments;
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

    const plannerHasAssignments = hasPlannerAssignments(plannerTask);
    let desiredAssignments = null;
    if (plannerHasAssignments === false && assigneeIdentity) {
        const assigneeId = await resolveAssigneeUserId(graphClient, assigneeIdentity);
        if (assigneeId) {
            desiredAssignments = buildPlannerAssignments(assigneeId);
        } else {
            logMissingAssignee(assigneeIdentity, { projectNo: task.projectNo, taskNo: task.taskNo });
        }
    }

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
        if (!plannerHasAssignments && desiredAssignments) changes.assignments = desiredAssignments;
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
    const plannerHasAssignments = hasPlannerAssignments(plannerTask);
    const bcAssignee = resolveAssigneeIdentity(bcTask);
    const shouldClearAssignee = plannerHasAssignments === false && !!bcAssignee;

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
    if (shouldClearAssignee && hasField(bcTask, "assignedPersonCode")) {
        updates.assignedPersonCode = "";
    }

    const hasChanges = Object.entries(updates).some(([key, value]) => {
        const current = bcTask?.[key];
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
    const plannerConfig = getPlannerConfig();
    const { planId, titlePrefix } = await resolvePlanForProject(graphClient, projectNo, tasks, planTitle);
    if (syncMode === "perProjectPlan" && planId && planTitle) {
        if (!plannerConfig.defaultPlanId || planId !== plannerConfig.defaultPlanId) {
            try {
                const plan = await graphClient.getPlan(planId);
                const currentTitle = (plan?.title || "").trim();
                const desiredTitle = planTitle.trim();
                if (currentTitle && desiredTitle && currentTitle !== desiredTitle) {
                    await graphClient.updatePlan(planId, { title: desiredTitle }, plan?.["@odata.etag"]);
                    logger.info("Updated Planner plan title", {
                        projectNo,
                        planId,
                        fromTitle: currentTitle,
                        toTitle: desiredTitle,
                    });
                }
            }
            catch (error) {
                logger.warn("Failed to update Planner plan title", {
                    projectNo,
                    planId,
                    error: error?.message || String(error),
                });
            }
        }
    }
    const orderedTasks = [...tasks].sort((a, b) => {
        const aKey = (a.taskNo || "").toString();
        const bKey = (b.taskNo || "").toString();
        return aKey.localeCompare(bKey, undefined, { numeric: true, sensitivity: "base" });
    });
    let currentBucket = DEFAULT_BUCKET_NAME;
    let skipSection = false;
    let headingCount = 0;
    let postingCount = 0;
    let skippedCount = 0;

    const { bcModifiedGraceMs } = getSyncConfig();
    const bcGraceMs = Number.isFinite(bcModifiedGraceMs) ? bcModifiedGraceMs : 0;

    for (const task of orderedTasks) {
        const rawTaskType = (task.taskType || "").trim();
        let taskType = rawTaskType.toLowerCase();
        if (!taskType || (taskType !== "heading" && taskType !== "posting")) {
            taskType = "posting";
        }
        if (taskType === "heading") {
            headingCount += 1;
            const resolved = resolveBucketFromHeading(task.taskNo, task.description);
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
        if (taskType !== "posting") {
            skippedCount += 1;
            continue;
        }
        if (skipSection || !currentBucket) {
            skippedCount += 1;
            continue;
        }
        postingCount += 1;
        const bucketId = await ensureBucket(graphClient, planId, currentBucket, bucketCache);
        const planMismatch = syncMode === "perProjectPlan" &&
            !allowDefaultPlanFallback &&
            task.plannerPlanId &&
            task.plannerPlanId !== planId;
        const bcChanged = hasBcChangedSinceSync(task, bcGraceMs);
        const desiredBucketName = normalizeBucketName(currentBucket);
        const currentBcBucket = (task.plannerBucket || "").trim().toLowerCase();
        const desiredBucket = desiredBucketName.toLowerCase();
        const bucketMatches = currentBcBucket ? currentBcBucket === desiredBucket : false;
        if (!planMismatch && task.plannerTaskId && bcChanged === false && bucketMatches) {
            logger.info("Skipping BC → Planner update; no changes since last sync", {
                projectNo,
                taskNo: task.taskNo,
                lastSyncAt: task.lastSyncAt,
            });
            continue;
        }
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

    if (postingCount === 0) {
        logger.info("No posting tasks synced for project", {
            projectNo,
            totalTasks: orderedTasks.length,
            headingCount,
            skippedCount,
        });
    }

    return planId;
}

function buildPlannerDeltaScopeKey(planId) {
    const { tenantId } = getGraphConfig();
    const { groupId } = getPlannerConfig();
    return `planner:${tenantId}:${groupId}:${planId}`;
}

function buildBcProjectChangeScopeKey() {
    const { tenantId, environment, companyId, apiBase, publisher, group, version } = getBcConfig();
    const trimmedBase = (apiBase || "").replace(/\/+$/, "");
    return `bc:${tenantId}:${environment}:${companyId}:${trimmedBase}:${publisher}:${group}:${version}`;
}

function isDeltaTokenInvalid(error) {
    const msg = error instanceof Error ? error.message : String(error);
    const lowered = msg.toLowerCase();
    return (
        msg.includes("-> 410") ||
        lowered.includes("syncstatenotfound") ||
        (lowered.includes("syncstate") && lowered.includes("not found")) ||
        lowered.includes("invalidsynctoken") ||
        lowered.includes("invaliddeltatoken") ||
        lowered.includes("resyncrequired")
    );
}

function isDeltaUnsupported(error) {
    const msg = error instanceof Error ? error.message : String(error);
    const lowered = msg.toLowerCase();
    return (
        msg.includes("-> 404") ||
        msg.includes("-> 501") ||
        (msg.includes("-> 405") && (lowered.includes("publication") || lowered.includes("certain fields")))
    );
}

async function collectPlannerDeltaChanges(graphClient, planId, deltaLink) {
    const items = [];
    let pageCount = 0;
    let nextLink = deltaLink || null;
    let newDeltaLink = null;

    while (true) {
        const page = await graphClient.listPlannerPlanTasksDelta(planId, nextLink || undefined);
        pageCount += 1;
        if (page?.value?.length) items.push(...page.value);
        if (page?.nextLink) {
            nextLink = page.nextLink;
            continue;
        }
        newDeltaLink = page?.deltaLink || null;
        break;
    }

    if (!newDeltaLink) {
        throw new Error("Planner delta response missing @odata.deltaLink");
    }
    return { items, deltaLink: newDeltaLink, pageCount };
}

function dedupePlannerDeltaItems(items) {
    const map = new Map();
    for (const item of items || []) {
        if (!item?.id) continue;
        map.set(item.id, item);
    }
    return Array.from(map.values());
}

function buildPlannerTaskScore(task) {
    const hasPlanId = !!(task.plannerPlanId || "").trim();
    const lastSyncMs = parseDateMs(task.lastSyncAt);
    const modifiedMs =
        parseDateMs(task.systemModifiedAt) ??
        parseDateMs(task.lastModifiedDateTime) ??
        parseDateMs(task.modifiedAt);
    return {
        hasPlanId: hasPlanId ? 1 : 0,
        lastSyncMs: lastSyncMs ?? -1,
        modifiedMs: modifiedMs ?? -1,
    };
}

function isBetterPlannerTask(candidate, current) {
    const candidateScore = buildPlannerTaskScore(candidate);
    const currentScore = buildPlannerTaskScore(current);
    if (candidateScore.hasPlanId !== currentScore.hasPlanId) {
        return candidateScore.hasPlanId > currentScore.hasPlanId;
    }
    if (candidateScore.lastSyncMs !== currentScore.lastSyncMs) {
        return candidateScore.lastSyncMs > currentScore.lastSyncMs;
    }
    if (candidateScore.modifiedMs !== currentScore.modifiedMs) {
        return candidateScore.modifiedMs > currentScore.modifiedMs;
    }
    return false;
}

function selectPrimaryPlannerTask(tasks) {
    let best = tasks[0];
    for (const task of tasks.slice(1)) {
        if (isBetterPlannerTask(task, best)) {
            best = task;
        }
    }
    return best;
}

async function clearDuplicatePlannerLink(bcClient, task, plannerTaskId, primaryTask) {
    if (!task.systemId) {
        logger.warn("Duplicate Planner linkage missing systemId; skipping", {
            plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
        });
        return false;
    }
    try {
        await bcClient.patchProjectTask(task.systemId, {
            plannerTaskId: "",
            plannerPlanId: "",
            plannerBucket: "",
            lastPlannerEtag: "",
            syncLock: false,
        });
        logger.warn("Cleared duplicate Planner linkage", {
            plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            keptProjectNo: primaryTask?.projectNo,
            keptTaskNo: primaryTask?.taskNo,
        });
        return true;
    } catch (error) {
        logger.warn("Failed to clear duplicate Planner linkage", {
            plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            error: error?.message || String(error),
        });
        return false;
    }
}

async function dedupePlannerTaskLinks(bcClient, tasks) {
    const grouped = new Map();
    for (const task of tasks || []) {
        const plannerTaskId = (task.plannerTaskId || "").trim();
        if (!plannerTaskId) continue;
        if (!grouped.has(plannerTaskId)) grouped.set(plannerTaskId, []);
        grouped.get(plannerTaskId).push(task);
    }

    let cleared = 0;
    for (const [plannerTaskId, groupedTasks] of grouped.entries()) {
        if (groupedTasks.length <= 1) continue;
        const primary = selectPrimaryPlannerTask(groupedTasks);
        logger.warn("Duplicate Planner linkage detected; clearing extras", {
            plannerTaskId,
            count: groupedTasks.length,
            keepProjectNo: primary?.projectNo,
            keepTaskNo: primary?.taskNo,
        });
        for (const task of groupedTasks) {
            if (task === primary) continue;
            const success = await clearDuplicatePlannerLink(bcClient, task, plannerTaskId, primary);
            if (success) cleared += 1;
            task.plannerTaskId = "";
            task.plannerPlanId = "";
            task.plannerBucket = "";
            task.lastPlannerEtag = "";
            task.syncLock = false;
        }
    }

    if (cleared) {
        logger.info("Cleared duplicate Planner task links in BC", { cleared });
    }

    return tasks.filter((task) => (task.plannerTaskId || "").trim());
}

function buildPlannerTaskIndex(tasks) {
    const map = new Map();
    for (const task of tasks || []) {
        const plannerTaskId = (task.plannerTaskId || "").trim();
        if (!plannerTaskId) continue;
        if (!map.has(plannerTaskId)) {
            map.set(plannerTaskId, task);
        }
    }
    return map;
}

async function clearPlannerLink(bcClient, task) {
    if (!task.systemId) {
        logger.warn("Planner task removed but systemId missing; skipping", {
            taskId: task.plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
        });
        return false;
    }
    try {
        await bcClient.patchProjectTask(task.systemId, {
            plannerTaskId: "",
            plannerPlanId: "",
            plannerBucket: "",
            lastPlannerEtag: "",
            syncLock: false,
        });
        logger.warn("Cleared stale Planner linkage after delete", {
            taskId: task.plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
        });
        return true;
    } catch (error) {
        logger.warn("Failed to clear stale Planner linkage", {
            taskId: task.plannerTaskId,
            error: error?.message || String(error),
        });
        return false;
    }
}

async function runPlannerDeltaSync(options = {}) {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());
    const affectedProjectNos = new Set();
    const isNotFound = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 404");
    };
    const isRateLimited = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 429");
    };
    const isTransientGraphError = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 500") || msg.includes("-> 502") || msg.includes("-> 503") || msg.includes("-> 504");
    };
    const { persist = true } = options;

    const rawTasks = await bcClient.listProjectTasks("plannerTaskId ne ''");
    const tasks = await dedupePlannerTaskLinks(bcClient, rawTasks);
    const plannerTaskIndex = buildPlannerTaskIndex(tasks);
    const skippedDisabled = tasks.reduce(
        (count, task) => (isProjectDisabled(disabledProjects, task.projectNo) ? count + 1 : count),
        0
    );

    const stats = {
        created: 0,
        updated: 0,
        removed: 0,
        processed: 0,
        skippedDisabled: 0,
        skippedUnlinked: 0,
        cleared: 0,
    };

    const plannerConfig = getPlannerConfig();
    const { syncMode } = getSyncConfig();
    const planIds = new Set();
    if (syncMode === "singlePlan" && plannerConfig.defaultPlanId) {
        planIds.add(plannerConfig.defaultPlanId);
    } else {
        try {
            const plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
            for (const plan of plans || []) {
                if (plan?.id) planIds.add(plan.id);
            }
        } catch (error) {
            logger.warn("Planner delta plan list failed; falling back to BC task planIds", {
                error: error?.message || String(error),
            });
        }
        for (const task of tasks) {
            const planId = (task.plannerPlanId || "").trim();
            if (planId) planIds.add(planId);
        }
        if (plannerConfig.defaultPlanId) {
            planIds.add(plannerConfig.defaultPlanId);
        }
    }

    if (!planIds.size) {
        logger.warn("Planner delta sync skipped; no planIds resolved", {
            syncMode,
            groupId: plannerConfig.groupId,
        });
        return {
            processed: 0,
        total: tasks.length,
        skippedDisabled,
        affectedProjects: [],
        mode: "initial",
        persisted: persist,
        plans: 0,
        };
    }

    let totalPages = 0;
    let mode = "initial";

    for (const planId of planIds) {
        const scopeKey = buildPlannerDeltaScopeKey(planId);
        const storedDelta = await getPlannerDeltaState(scopeKey);
        const planMode = storedDelta?.deltaLink ? "incremental" : "initial";
        mode = mode === "initial" && planMode === "incremental" ? "incremental" : mode;

        logger.debug("Planner delta sync starting", {
            scope: scopeKey,
            planId,
            mode: planMode,
            hasDeltaToken: planMode === "incremental",
        });

        let deltaResult = null;
        try {
            deltaResult = await collectPlannerDeltaChanges(graphClient, planId, storedDelta?.deltaLink);
        } catch (error) {
            if (storedDelta?.deltaLink && isDeltaTokenInvalid(error)) {
                logger.info("Planner delta token invalid; resetting", {
                    scope: scopeKey,
                    planId,
                    error: error?.message || String(error),
                });
                if (persist) {
                    await clearPlannerDeltaState(scopeKey);
                }
                else {
                    logger.info("Planner delta token invalid; skipping store reset (dry run)", { scope: scopeKey });
                }
                try {
                    deltaResult = await collectPlannerDeltaChanges(graphClient, planId, null);
                }
                catch (resetError) {
                    if (isRateLimited(resetError) || isTransientGraphError(resetError)) {
                        logger.warn("Planner delta rate limited after reset; skipping plan", {
                            scope: scopeKey,
                            planId,
                            error: resetError?.message || String(resetError),
                        });
                        continue;
                    }
                    throw resetError;
                }
            }
            else if (isRateLimited(error) || isTransientGraphError(error)) {
                logger.warn("Planner delta request failed; skipping plan", {
                    scope: scopeKey,
                    planId,
                    error: error?.message || String(error),
                });
                continue;
            }
            else {
                throw error;
            }
        }

        if (!deltaResult) {
            continue;
        }

        totalPages += deltaResult.pageCount;
        const items = dedupePlannerDeltaItems(deltaResult.items);
        const before = { ...stats };

        // Fetch all delta pages first so updates only apply after a complete delta pass.
        for (const item of items) {
            if (!item?.id) continue;
            const isRemoved = !!item["@removed"];
            if (isRemoved) stats.removed += 1;

            const bcTask = plannerTaskIndex.get(item.id);
            if (!bcTask) {
                if (!isRemoved) stats.created += 1;
                stats.skippedUnlinked += 1;
                continue;
            }
            const affectedProjectNo = (bcTask.projectNo || "").trim();
            if (affectedProjectNo) affectedProjectNos.add(affectedProjectNo);
            if (isProjectDisabled(disabledProjects, bcTask.projectNo)) {
                stats.skippedDisabled += 1;
                continue;
            }
            if (isRemoved) {
                const cleared = await clearPlannerLink(bcClient, bcTask);
                if (cleared) stats.cleared += 1;
                continue;
            }
            stats.updated += 1;
            const plannerTask = item;
            if (bcTask.lastPlannerEtag && plannerTask?.["@odata.etag"] && bcTask.lastPlannerEtag === plannerTask["@odata.etag"]) {
                continue;
            }
            await applyPlannerUpdateToBc(bcClient, graphClient, bcTask, plannerTask);
            stats.processed += 1;
        }

        if (persist) {
            await savePlannerDeltaState(scopeKey, deltaResult.deltaLink);
        }
        else {
            logger.debug("Planner delta sync skipped persisting delta token", { scope: scopeKey });
        }

        const planStats = {
            created: stats.created - before.created,
            updated: stats.updated - before.updated,
            removed: stats.removed - before.removed,
            processed: stats.processed - before.processed,
            skippedDisabled: stats.skippedDisabled - before.skippedDisabled,
            skippedUnlinked: stats.skippedUnlinked - before.skippedUnlinked,
            cleared: stats.cleared - before.cleared,
        };

        logger.debug("Planner delta sync complete", {
            scope: scopeKey,
            planId,
            mode: planMode,
            pages: deltaResult.pageCount,
            changes: items.length,
            created: planStats.created,
            updated: planStats.updated,
            removed: planStats.removed,
            processed: planStats.processed,
            skippedDisabled: planStats.skippedDisabled,
            skippedUnlinked: planStats.skippedUnlinked,
            affectedProjects: affectedProjectNos.size,
            cleared: planStats.cleared,
            persisted: persist,
        });
    }

    logger.info("Planner delta sync summary", {
        mode,
        plans: planIds.size,
        pages: totalPages,
        changes: stats.created + stats.updated + stats.removed,
        created: stats.created,
        updated: stats.updated,
        removed: stats.removed,
        processed: stats.processed,
        skippedDisabled: stats.skippedDisabled,
        skippedUnlinked: stats.skippedUnlinked,
        affectedProjects: affectedProjectNos.size,
        cleared: stats.cleared,
        persisted: persist,
    });

    return {
        processed: stats.processed,
        total: tasks.length,
        skippedDisabled,
        affectedProjects: Array.from(affectedProjectNos),
        mode,
        persisted: persist,
        plans: planIds.size,
        pages: totalPages,
    };
}

async function runPlannerFullSync() {
    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());
    const isNotFound = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 404");
    };

    const rawTasks = await bcClient.listProjectTasks("plannerTaskId ne ''");
    const tasks = await dedupePlannerTaskLinks(bcClient, rawTasks);

    let processed = 0;
    let skippedDisabled = 0;
    for (const task of tasks) {
        if (isProjectDisabled(disabledProjects, task.projectNo)) {
            skippedDisabled += 1;
            continue;
        }
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

export async function runSmartPollingSync(options = {}) {
    const { dryRun = false } = options;
    const bcClient = new BusinessCentralClient();
    const disabledProjects = buildDisabledProjectSet(await listProjectSyncSettings());
    const syncConfig = getSyncConfig();
    const { usePlannerDelta, syncMode, allowDefaultPlanFallback, maxProjectsPerRun } = syncConfig;
    const bcScopeKey = buildBcProjectChangeScopeKey();
    const lastSeq = await getBcProjectChangeCursor(bcScopeKey);
    const bcChangedProjectNos = new Set();
    let bcLastSeq = null;

    const isNotFound = (error) => {
        const msg = error instanceof Error ? error.message : String(error);
        return msg.includes("-> 404");
    };

    try {
        const bcChanges = await bcClient.listProjectChangesSince(lastSeq);
        bcLastSeq = bcChanges.lastSeq ?? null;
        for (const change of bcChanges.items || []) {
            const projectNo = (change.projectNo || "").trim();
            if (projectNo) bcChangedProjectNos.add(projectNo);
        }
        logger.info("BC project change feed read", {
            scope: bcScopeKey,
            lastSeq,
            pages: bcChanges.pageCount,
            changes: bcChanges.items.length,
            projects: bcChangedProjectNos.size,
        });
    } catch (error) {
        if (isNotFound(error)) {
            logger.warn("BC project change feed unavailable; skipping BC change lookup", {
                scope: bcScopeKey,
                error: error?.message || String(error),
            });
        } else {
            throw error;
        }
    }

    const missingProjects = [];
    if (syncMode === "perProjectPlan") {
        const graphClient = new GraphClient();
        const plannerConfig = getPlannerConfig();
        let projects = null;
        try {
            projects = await bcClient.listProjects();
        } catch (error) {
            logger.warn("BC projects lookup failed; skipping new project check", {
                error: error?.message || String(error),
            });
        }
        if (projects?.length) {
            let plans = null;
            try {
                plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
            } catch (error) {
                logger.warn("Planner plan list failed; skipping new project check", {
                    error: error?.message || String(error),
                });
            }
            if (plans) {
                const planTitles = [];
                for (const plan of plans || []) {
                    const title = (plan?.title || "").trim();
                    if (title) {
                        planTitles.push(title);
                    }
                }
                for (const project of projects) {
                    const projectNo = (project.projectNo || "").trim();
                    if (!projectNo) continue;
                    if (isProjectDisabled(disabledProjects, projectNo)) continue;
                    const planTitle = buildPlanTitle(projectNo, project.description);
                    const hasPlan = planTitles.some((title) => matchesProjectPlanTitle(title, projectNo));
                    if (hasPlan) continue;
                    missingProjects.push({ projectNo, planTitle });
                }
                if (missingProjects.length) {
                    logger.info("BC projects missing Planner plans detected", {
                        count: missingProjects.length,
                        dryRun,
                    });
                    if (!dryRun) {
                        for (const project of missingProjects) {
                            try {
                                const createdPlan = await graphClient.createPlan(plannerConfig.groupId, project.planTitle);
                                planTitleIndex.set(project.planTitle.trim().toLowerCase(), createdPlan.id);
                                planTitleIndex.set(project.projectNo.trim().toLowerCase(), createdPlan.id);
                                logger.info("Planner plan created for BC project", {
                                    projectNo: project.projectNo,
                                    planId: createdPlan.id,
                                });
                            } catch (error) {
                                const planCreateError = error?.message || String(error);
                                logger.warn("Plan creation failed for BC project", {
                                    projectNo: project.projectNo,
                                    error: planCreateError,
                                });
                                if (!allowDefaultPlanFallback) {
                                    throw new Error(`Plan creation failed: ${planCreateError || "unknown error"}`);
                                }
                                if (!plannerConfig.defaultPlanId) {
                                    throw new Error(`Plan creation failed and PLANNER_DEFAULT_PLAN_ID is not set: ${planCreateError || "unknown error"}`);
                                }
                                logger.warn("Plan creation failed; falling back to default plan", {
                                    projectNo: project.projectNo,
                                    defaultPlanId: plannerConfig.defaultPlanId,
                                });
                            }
                        }
                    } else {
                        logger.info("Smart polling dry run; skipping Planner plan creation", {
                            projects: missingProjects.map((project) => project.projectNo),
                        });
                    }
                }
            }
        }
    }
    if (missingProjects.length) {
        for (const project of missingProjects) {
            bcChangedProjectNos.add(project.projectNo);
        }
    }

    const plannerAffectedProjectNos = new Set();
    let plannerProcessed = 0;
    let plannerMode = "initial";

    if (usePlannerDelta) {
        try {
            const deltaResult = await runPlannerDeltaSync({ persist: !dryRun });
            plannerProcessed = deltaResult.processed;
            plannerMode = deltaResult.mode === "incremental" ? "incremental" : "initial";
            for (const projectNo of deltaResult.affectedProjects || []) {
                const trimmed = (projectNo || "").trim();
                if (trimmed) plannerAffectedProjectNos.add(trimmed);
            }
        } catch (error) {
            if (isDeltaUnsupported(error)) {
                logger.warn("Planner delta unavailable; falling back to full polling", {
                    error: error?.message || String(error),
                });
                const fullResult = await runPlannerFullSync();
                plannerProcessed = fullResult.processed;
                plannerMode = "initial";
            } else {
                throw error;
            }
        }
    } else {
        logger.info("Planner delta disabled; running full polling sync");
        const fullResult = await runPlannerFullSync();
        plannerProcessed = fullResult.processed;
        plannerMode = "initial";
    }

    const projectNoByNormalized = new Map();
    const addProjectNo = (projectNo) => {
        const trimmed = (projectNo || "").trim();
        if (!trimmed) return;
        const normalized = normalizeProjectNo(trimmed);
        if (!normalized) return;
        if (!projectNoByNormalized.has(normalized)) {
            projectNoByNormalized.set(normalized, trimmed);
        }
    };
    for (const projectNo of bcChangedProjectNos) addProjectNo(projectNo);

    let projectsToSync = [];
    for (const [normalized, projectNo] of projectNoByNormalized.entries()) {
        if (disabledProjects.has(normalized)) continue;
        projectsToSync.push(projectNo);
    }
    projectsToSync.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));

    let shouldSaveCursor = true;
    if (!dryRun && maxProjectsPerRun > 0 && projectsToSync.length > maxProjectsPerRun) {
        const existingQueue = await getSmartPollingQueue();
        const currentSeq = bcLastSeq ?? (existingQueue?.lastSeq ?? null);
        let queuedProjects = [];
        if (existingQueue && existingQueue.projects?.length && existingQueue.lastSeq === currentSeq) {
            const combined = new Set(existingQueue.projects);
            for (const projectNo of projectsToSync) combined.add(projectNo);
            queuedProjects = Array.from(combined);
        } else {
            queuedProjects = projectsToSync;
        }
        queuedProjects.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
        projectsToSync = queuedProjects.slice(0, maxProjectsPerRun);
        const remaining = queuedProjects.slice(projectsToSync.length);
        if (remaining.length) {
            await saveSmartPollingQueue({ lastSeq: currentSeq, projects: remaining });
            shouldSaveCursor = false;
        } else {
            await clearSmartPollingQueue();
        }
        logger.info("Smart polling queue chunk", {
            totalQueued: queuedProjects.length,
            batchSize: projectsToSync.length,
            remaining: remaining.length,
        });
    } else if (!dryRun && maxProjectsPerRun > 0 && projectsToSync.length === 0) {
        await clearSmartPollingQueue();
    }

    logger.info("Smart polling project selection", {
        bcChangedProjects: bcChangedProjectNos.size,
        plannerAffectedProjects: plannerAffectedProjectNos.size,
        projectsToSync: projectsToSync.length,
        dryRun,
    });

    if (!dryRun) {
        for (const projectNo of projectsToSync) {
            await syncBcToPlanner(projectNo);
        }
    } else if (projectsToSync.length) {
        logger.info("Smart polling dry run; skipping BC → Planner sync", { projects: projectsToSync });
    }

    let lastSeqSaved = null;
    if (!dryRun && bcLastSeq != null && shouldSaveCursor) {
        await saveBcProjectChangeCursor(bcScopeKey, bcLastSeq);
        lastSeqSaved = bcLastSeq;
        logger.info("Saved BC project change cursor", { scope: bcScopeKey, lastSeq: bcLastSeq });
    } else if (!dryRun && bcLastSeq != null && !shouldSaveCursor) {
        logger.info("Smart polling deferred BC cursor save; pending project queue", { scope: bcScopeKey, lastSeq: bcLastSeq });
    } else if (dryRun && bcLastSeq != null) {
        logger.info("Smart polling dry run; skipping BC cursor save", { scope: bcScopeKey, lastSeq: bcLastSeq });
    }

    return {
        bc: { changedProjects: bcChangedProjectNos.size, lastSeqSaved },
        planner: { processed: plannerProcessed, affectedProjects: plannerAffectedProjectNos.size, mode: plannerMode },
        syncedProjects: projectsToSync,
    };
}

export async function syncPlannerPlanTitlesAndDedupe(options = {}) {
    const { projectNo, dryRun = false } = options;
    const { syncMode } = getSyncConfig();
    if (syncMode !== "perProjectPlan") {
        logger.info("Planner plan maintenance skipped; sync mode not perProjectPlan", { syncMode });
        return { ok: false, skipped: true, reason: "syncMode not perProjectPlan", syncMode };
    }

    const plannerConfig = getPlannerConfig();
    if (!plannerConfig.groupId) {
        throw new Error("PLANNER_GROUP_ID is required to maintain planner plans");
    }

    const bcClient = new BusinessCentralClient();
    const graphClient = new GraphClient();
    const normalizedProjectNo = projectNo ? normalizeProjectNo(projectNo) : "";
    const escapedProjectNo = projectNo ? projectNo.replace(/'/g, "''") : "";

    let projects = [];
    try {
        projects = await bcClient.listProjects(escapedProjectNo ? `projectNo eq '${escapedProjectNo}'` : undefined);
    } catch (error) {
        logger.warn("Failed to load BC projects for plan maintenance", { error: error?.message });
        throw error;
    }

    const projectMap = new Map();
    for (const project of projects) {
        const projNo = (project.projectNo || "").trim();
        if (!projNo) continue;
        const key = normalizeProjectNo(projNo);
        if (normalizedProjectNo && key !== normalizedProjectNo) continue;
        projectMap.set(key, { projectNo: projNo, description: project.description });
    }

    let plans = [];
    try {
        plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
    } catch (error) {
        logger.warn("Failed to list Planner plans for maintenance", { error: error?.message });
        throw error;
    }

    const planGroups = new Map();
    for (const plan of plans) {
        const key = extractProjectNoFromPlanTitle(plan.title || "");
        if (!key) continue;
        if (normalizedProjectNo && key !== normalizedProjectNo) continue;
        const group = planGroups.get(key) || [];
        group.push(plan);
        planGroups.set(key, group);
    }

    const summary = {
        ok: true,
        dryRun,
        projectNo: projectNo || null,
        plansScanned: plans.length,
        groups: planGroups.size,
        keptPlans: 0,
        deletedPlans: 0,
        updatedTitles: 0,
        skippedNoProject: 0,
        skippedNoPlan: 0,
        skippedDefaultPlan: 0,
        failedDeletes: 0,
        failedUpdates: 0,
    };

    for (const key of projectMap.keys()) {
        if (!planGroups.has(key)) summary.skippedNoPlan += 1;
    }

    for (const [projectKey, groupPlans] of planGroups) {
        const project = projectMap.get(projectKey);
        if (!project) {
            summary.skippedNoProject += 1;
            continue;
        }

        const desiredTitle = buildPlanTitle(project.projectNo, project.description);
        const sorted = [...groupPlans].sort((a, b) => {
            const aCreated = Date.parse(String(a.createdDateTime || ""));
            const bCreated = Date.parse(String(b.createdDateTime || ""));
            if (Number.isFinite(aCreated) && Number.isFinite(bCreated) && aCreated !== bCreated) {
                return aCreated - bCreated;
            }
            const aTitle = (a.title || "").trim().toLowerCase();
            const bTitle = (b.title || "").trim().toLowerCase();
            return aTitle.localeCompare(bTitle);
        });

        let keep = sorted[0];
        if (plannerConfig.defaultPlanId) {
            const defaultPlan = sorted.find((plan) => plan.id === plannerConfig.defaultPlanId);
            if (defaultPlan) keep = defaultPlan;
        }

        summary.keptPlans += 1;

        for (const plan of sorted) {
            if (plan.id === keep.id) continue;
            if (plannerConfig.defaultPlanId && plan.id === plannerConfig.defaultPlanId) {
                summary.skippedDefaultPlan += 1;
                continue;
            }
            try {
                if (!dryRun) {
                    await graphClient.deletePlan(plan.id);
                }
                summary.deletedPlans += 1;
                logger.info("Removed duplicate Planner plan", {
                    projectNo: project.projectNo,
                    planId: plan.id,
                    planTitle: plan.title,
                    dryRun,
                });
            } catch (error) {
                summary.failedDeletes += 1;
                logger.warn("Failed to remove duplicate Planner plan", {
                    projectNo: project.projectNo,
                    planId: plan.id,
                    planTitle: plan.title,
                    error: error?.message,
                });
            }
        }

        const normalizedKeepTitle = (keep.title || "").trim();
        if (desiredTitle && normalizedKeepTitle !== desiredTitle) {
            if (plannerConfig.defaultPlanId && keep.id === plannerConfig.defaultPlanId) {
                summary.skippedDefaultPlan += 1;
            } else {
                try {
                    if (!dryRun) {
                        await graphClient.updatePlan(keep.id, { title: desiredTitle });
                    }
                    summary.updatedTitles += 1;
                    logger.info("Updated Planner plan title", {
                        projectNo: project.projectNo,
                        planId: keep.id,
                        fromTitle: keep.title,
                        toTitle: desiredTitle,
                        dryRun,
                    });
                } catch (error) {
                    summary.failedUpdates += 1;
                    logger.warn("Failed to update Planner plan title", {
                        projectNo: project.projectNo,
                        planId: keep.id,
                        error: error?.message,
                    });
                }
            }
        }
    }

    return summary;
}

export async function runPollingSync() {
    const { usePlannerDelta } = getSyncConfig();
    if (usePlannerDelta) {
        try {
            return await runPlannerDeltaSync();
        } catch (error) {
            if (isDeltaUnsupported(error)) {
                logger.warn("Planner delta unavailable; falling back to full polling", {
                    error: error?.message || String(error),
                });
            } else {
                throw error;
            }
        }
    }
    return runPlannerFullSync();
}

export async function enqueueAndProcessNotifications(items) {
    await enqueueNotifications(items);
    processQueue(syncPlannerNotification).catch((error) => {
        logger.error("Notification processing error", { error: error?.message || String(error) });
    });
}
