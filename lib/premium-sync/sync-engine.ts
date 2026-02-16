import { BusinessCentralClient, BcProject, BcProjectTask } from "../planner-sync/bc-client.js";
import { logger } from "../planner-sync/logger.js";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "../planner-sync/project-sync-store.js";
import { DataverseClient, DataverseEntity } from "../dataverse-client.js";
import { getDataverseDeltaLink, saveDataverseDeltaLink } from "./delta-store.js";
import { getDataverseMappingConfig, getPremiumSyncConfig } from "./config.js";
import { markPremiumTaskIdsFromBc } from "./bc-write-store.js";
import { markBcTaskSystemIdsFromPremium, wasBcTaskSystemIdUpdatedByPremium } from "./premium-write-store.js";
import crypto from "crypto";

const HEADING_TASK_SECTIONS = new Map<number, string | null>([
    [1000, "Pre-Construction"],
    [2000, "Installation"],
    [3000, "Revenue"],
    [4000, "Change Orders"],
]);

const warnedNonGuidPlanProjects = new Set<string>();
const warnedNonGuidTaskProjects = new Set<string>();
const warnedDefaultBucketProjects = new Set<string>();
const warnedMissingTaskProjectLookup = new Set<string>();
const BC_QUEUE_ENTITY_SET = (process.env.BC_SYNC_QUEUE_ENTITY_SET || "premiumSyncQueue").trim();
const BC_QUEUE_PROJECT_NO_FIELD = (process.env.BC_SYNC_QUEUE_PROJECTNO_FIELD || "projectNo").trim();
const BC_QUEUE_TASK_SYSTEM_ID_FIELD = (process.env.BC_SYNC_QUEUE_TASKSYSTEMID_FIELD || "projectTaskSystemId").trim();
const BC_QUEUE_TIME_FIELDS = [
    "changedAt",
    "systemModifiedAt",
    "lastModifiedDateTime",
    "modifiedAt",
    "modifiedOn",
    "lastModifiedOn",
    "systemModifiedOn",
    "createdAt",
    "createdOn",
    "systemCreatedAt",
];
const BC_QUEUE_PAGE_SIZE = Math.max(1, Math.floor(Number(process.env.BC_SYNC_QUEUE_PAGE_SIZE || 5000)));
const BC_QUEUE_MAX_PAGES = Math.max(1, Math.floor(Number(process.env.BC_SYNC_QUEUE_MAX_PAGES || 20)));
const BC_PROJECT_BOOTSTRAP_RETRY_ATTEMPTS = Math.max(
    0,
    Math.floor(Number(process.env.BC_PROJECT_BOOTSTRAP_RETRY_ATTEMPTS || 4))
);
const BC_PROJECT_BOOTSTRAP_RETRY_DELAY_MS = Math.max(
    100,
    Math.floor(Number(process.env.BC_PROJECT_BOOTSTRAP_RETRY_DELAY_MS || 800))
);
const defaultBucketFieldCache = { value: undefined as string | null | undefined };
const warnedDefaultBucketFields = new Set<string>();

type TaskIndex = {
    byId: Set<string>;
    byTaskNo: Map<string, string>;
    byTitle: Map<string, string>;
};

type ScheduleFallbackState = {
    unavailable: boolean;
    reason?: string;
    warned?: boolean;
};

function hasField(task: BcProjectTask, field: string) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function escapeODataString(value: string) {
    return value.replace(/'/g, "''");
}

const lookupFieldCache = new Map<string, string | null>();

async function resolveLookupField(
    dataverse: DataverseClient,
    entityLogicalName: string,
    targetLogicalName: string
) {
    const key = `${entityLogicalName}:${targetLogicalName}`;
    if (lookupFieldCache.has(key)) return lookupFieldCache.get(key) || null;
    try {
        const relRes = await dataverse.requestRaw(
            `/EntityDefinitions(LogicalName='${entityLogicalName}')/ManyToOneRelationships?$select=ReferencingAttribute,ReferencedEntity`
        );
        const relData = (await relRes.json()) as {
            value?: Array<{ ReferencingAttribute?: string; ReferencedEntity?: string }>;
        };
        const rels = Array.isArray(relData?.value) ? relData.value : [];
        const relMatch = rels.find(
            (rel) => String(rel.ReferencedEntity || "").toLowerCase() === targetLogicalName.toLowerCase()
        );
        if (relMatch?.ReferencingAttribute) {
            const resolved = String(relMatch.ReferencingAttribute);
            lookupFieldCache.set(key, resolved);
            return resolved;
        }
    } catch (error) {
        logger.warn("Dataverse lookup field resolve failed", {
            entityLogicalName,
            targetLogicalName,
            error: (error as Error)?.message,
        });
    }
    try {
        const res = await dataverse.requestRaw(
            `/EntityDefinitions(LogicalName='${entityLogicalName}')/Attributes/Microsoft.Dynamics.CRM.LookupAttributeMetadata?$select=LogicalName,Targets`
        );
        const data = (await res.json()) as { value?: Array<{ LogicalName?: string; Targets?: string[] }> };
        const attrs = Array.isArray(data?.value) ? data.value : [];
        const match = attrs.find((attr) => {
            if (!Array.isArray(attr.Targets)) return false;
            return attr.Targets.some((target) => String(target).toLowerCase() === targetLogicalName.toLowerCase());
        });
        const logical = match?.LogicalName ? String(match.LogicalName) : "";
        const resolved = logical && logical.trim() ? logical.trim() : null;
        lookupFieldCache.set(key, resolved);
        if (resolved) return resolved;
    } catch (error) {
        logger.warn("Dataverse lookup field resolve failed", {
            entityLogicalName,
            targetLogicalName,
            error: (error as Error)?.message,
        });
    }
    try {
        const res = await dataverse.requestRaw(
            `/EntityDefinitions(LogicalName='${entityLogicalName}')/Attributes?$select=LogicalName,AttributeType`
        );
        const data = (await res.json()) as { value?: Array<{ LogicalName?: string; AttributeType?: string }> };
        const attrs = Array.isArray(data?.value) ? data.value : [];
        const lookups = attrs.filter((attr) => String(attr.AttributeType || "").toLowerCase() === "lookup");
        for (const attr of lookups) {
            if (!attr.LogicalName) continue;
            try {
                const detail = await dataverse.requestRaw(
                    `/EntityDefinitions(LogicalName='${entityLogicalName}')/Attributes(LogicalName='${attr.LogicalName}')/Microsoft.Dynamics.CRM.LookupAttributeMetadata?$select=Targets`
                );
                const detailData = (await detail.json()) as { Targets?: string[] };
                if (Array.isArray(detailData?.Targets)) {
                    const hit = detailData.Targets.some(
                        (target) => String(target).toLowerCase() === targetLogicalName.toLowerCase()
                    );
                    if (hit) {
                        const resolved = String(attr.LogicalName);
                        lookupFieldCache.set(key, resolved);
                        return resolved;
                    }
                }
            } catch {
                continue;
            }
        }
    } catch (error) {
        logger.warn("Dataverse lookup field resolve failed", {
            entityLogicalName,
            targetLogicalName,
            error: (error as Error)?.message,
        });
    }
    lookupFieldCache.set(key, null);
    return null;
}

async function getProjectTeamLookupFields(dataverse: DataverseClient) {
    const project =
        (await resolveLookupField(dataverse, "msdyn_projectteam", "msdyn_project")) || "msdyn_projectid";
    const resource =
        (await resolveLookupField(dataverse, "msdyn_projectteam", "bookableresource")) || "msdyn_bookableresourceid";
    return { project, resource };
}

async function getAssignmentLookupFields(dataverse: DataverseClient) {
    const project =
        (await resolveLookupField(dataverse, "msdyn_resourceassignment", "msdyn_project")) || "msdyn_projectid";
    const task =
        (await resolveLookupField(dataverse, "msdyn_resourceassignment", "msdyn_projecttask")) || "msdyn_taskid";
    const team =
        (await resolveLookupField(dataverse, "msdyn_resourceassignment", "msdyn_projectteam")) ||
        "msdyn_projectteamid";
    return { project, task, team };
}

function formatODataGuid(value: string) {
    const trimmed = value.trim().replace(/^\{/, "").replace(/\}$/, "");
    return trimmed;
}

function isGuid(value: string) {
    const trimmed = value.trim().replace(/^\{/, "").replace(/\}$/, "");
    return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(trimmed);
}

function parseTaskNumber(taskNo?: string | number | null) {
    const raw = String(taskNo || "");
    const match = raw.match(/\d+/);
    if (!match) return Number.NaN;
    return Number(match[0]);
}

function buildAllowedTaskNumberSet(syncConfig: ReturnType<typeof getPremiumSyncConfig>) {
    const values = Array.isArray(syncConfig.allowedTaskNumbers) ? syncConfig.allowedTaskNumbers : [];
    const cleaned = values.filter((value) => Number.isFinite(value));
    return new Set(cleaned);
}

function isAllowedSyncTaskNo(taskNo: string | number | null | undefined, allowlist: Set<number>) {
    if (!allowlist.size) return true;
    const taskNumber = parseTaskNumber(taskNo);
    if (!Number.isFinite(taskNumber)) return false;
    return allowlist.has(taskNumber);
}

function normalizeEntitySetName(value: string | undefined | null) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function parseBoolEnv(value: string | undefined | null, defaultValue: boolean) {
    if (value == null || value === "") return defaultValue;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return defaultValue;
}

function isScheduleManagedTaskEntity(mapping: ReturnType<typeof getDataverseMappingConfig>) {
    return normalizeEntitySetName(mapping.taskEntitySet) === "msdyn_projecttasks";
}

function warnNonGuidPlannerPlanId(projectNo: string | undefined, projectId: string) {
    const key = (projectNo || "").trim() || projectId;
    if (!key) return;
    if (warnedNonGuidPlanProjects.has(key)) return;
    warnedNonGuidPlanProjects.add(key);
    logger.warn("Ignoring non-GUID plannerPlanId; will resolve project by BC data", {
        projectNo,
        projectId,
    });
}

function warnNonGuidPlannerTaskId(projectNo: string | undefined, taskNo: string | undefined, taskId: string) {
    const key = (projectNo || "").trim() || taskId || (taskNo || "").trim();
    if (!key) return;
    if (warnedNonGuidTaskProjects.has(key)) return;
    warnedNonGuidTaskProjects.add(key);
    logger.warn("Ignoring non-GUID plannerTaskId; will resolve by BC keys", {
        projectNo,
        taskNo,
        taskId,
    });
}

function isInvalidDefaultBucketError(error: unknown) {
    const message = (error as Error)?.message || String(error || "");
    const normalized = message.toLowerCase();
    return normalized.includes("e_invaliddefaultbucket") || normalized.includes("invalid default bucket");
}

function isOperationSetLimitError(error: unknown) {
    const message = (error as Error)?.message || String(error || "");
    const normalized = message.toLowerCase();
    if (normalized.includes("scheduleapi-ov-0004")) return true;
    if (normalized.includes("maximum number of operation set")) return true;
    if (normalized.includes("maximum number of operationset")) return true;
    return normalized.includes("operation set allowed per user");
}

function isOperationSetMissingError(error: unknown) {
    const message = (error as Error)?.message || String(error || "");
    const normalized = message.toLowerCase();
    if (normalized.includes("operationsetdoesnotexist")) return true;
    if (normalized.includes("operation set does not exist")) return true;
    if (normalized.includes("one or more operation sets do not exist")) return true;
    return normalized.includes("-> 404");
}

function extractOperationSetId(row: DataverseEntity) {
    if (!row || typeof row !== "object") return null;
    const direct = row.msdyn_operationsetid || row.OperationSetId || row.operationSetId;
    if (typeof direct === "string" && direct.trim()) return direct.trim();
    for (const key of Object.keys(row)) {
        if (key.toLowerCase().endsWith("operationsetid")) {
            const value = row[key];
            if (typeof value === "string" && value.trim()) return value.trim();
        }
    }
    return null;
}

function isTerminalOperationSet(row: DataverseEntity) {
    if (row.msdyn_completedon || row.msdyn_executedon) return true;
    const stateCode = Number(row.statecode);
    if (Number.isFinite(stateCode) && stateCode !== 0) return true;
    const statusCode = Number(row.statuscode ?? row.msdyn_status);
    if (Number.isFinite(statusCode) && statusCode === 192350003) return true;
    return false;
}

async function resolveOperationSetEntitySet(dataverse: DataverseClient) {
    try {
        const res = await dataverse.requestRaw("/EntityDefinitions(LogicalName='msdyn_operationset')?$select=EntitySetName");
        const data = (await res.json()) as { EntitySetName?: string; entitySetName?: string };
        const entitySet = data?.EntitySetName || data?.entitySetName;
        if (entitySet) return String(entitySet);
    } catch (error) {
        logger.warn("Operation set entity set lookup failed", { error: (error as Error)?.message });
    }
    return "msdyn_operationsets";
}

async function clearCompletedOperationSetsForCapacity(dataverse: DataverseClient, meta: Record<string, unknown> = {}) {
    const entitySet = await resolveOperationSetEntitySet(dataverse);
    const cleanupAgeRaw = Number(process.env.DATAVERSE_OPERATION_SET_CAPACITY_CLEANUP_MIN_AGE_MINUTES || 0);
    const olderThanMinutes = Number.isFinite(cleanupAgeRaw) ? Math.max(0, Math.floor(cleanupAgeRaw)) : 0;
    const pageSize = 50;
    const maxPages = 10;
    const maxDelete = 250;
    const cutoffMs = Date.now() - olderThanMinutes * 60 * 1000;

    let pages = 0;
    let scanned = 0;
    let nextLink: string | null = null;
    const idsToDelete: string[] = [];

    while (pages < maxPages && idsToDelete.length < maxDelete) {
        const path =
            nextLink ||
            `/${entitySet}?$select=msdyn_operationsetid,msdyn_completedon,msdyn_executedon,createdon,modifiedon,statecode,statuscode,msdyn_status&$top=${pageSize}`;
        const res = await dataverse.requestRaw(path);
        const data = (await res.json()) as { value?: DataverseEntity[]; "@odata.nextLink"?: string };
        const rows = Array.isArray(data?.value) ? data.value : [];
        scanned += rows.length;

        for (const row of rows) {
            if (!isTerminalOperationSet(row)) continue;
            const ts =
                parseDateMs(String(row.msdyn_completedon || "")) ??
                parseDateMs(String(row.msdyn_executedon || "")) ??
                parseDateMs(String(row.modifiedon || "")) ??
                parseDateMs(String(row.createdon || ""));
            if (ts == null || ts > cutoffMs) continue;
            const id = extractOperationSetId(row);
            if (!id) continue;
            idsToDelete.push(id);
            if (idsToDelete.length >= maxDelete) break;
        }

        nextLink = data?.["@odata.nextLink"] || null;
        pages += 1;
        if (!nextLink) break;
    }

    let deleted = 0;
    let failed = 0;
    for (const id of idsToDelete) {
        try {
            await dataverse.delete(entitySet, id);
            deleted += 1;
        } catch (error) {
            failed += 1;
            logger.warn("Operation set delete failed during capacity cleanup", {
                ...meta,
                operationSetId: id,
                error: (error as Error)?.message,
            });
        }
    }

    return { entitySet, scanned, considered: idsToDelete.length, deleted, failed, olderThanMinutes };
}

async function createOperationSetWithRecovery(options: {
    dataverse: DataverseClient;
    projectId: string;
    description: string;
    requestId?: string;
    projectNo?: string;
}) {
    try {
        return await options.dataverse.createOperationSet(options.projectId, options.description);
    } catch (error) {
        if (!isOperationSetLimitError(error)) throw error;
        const cleanup = await clearCompletedOperationSetsForCapacity(options.dataverse, {
            requestId: options.requestId,
            projectNo: options.projectNo,
            projectId: options.projectId,
        });
        logger.warn("Dataverse operation set capacity reached; cleaned completed sets and retrying", {
            requestId: options.requestId,
            projectNo: options.projectNo,
            projectId: options.projectId,
            ...cleanup,
            error: (error as Error)?.message,
        });
        return options.dataverse.createOperationSet(options.projectId, options.description);
    }
}

async function runWithConcurrency<T, R>(
    items: T[],
    limit: number,
    handler: (item: T, index: number) => Promise<R>
): Promise<R[]> {
    if (!items.length) return [];
    const safeLimit = Math.max(1, Math.floor(limit));
    const results: R[] = new Array(items.length);
    let nextIndex = 0;
    const workers = new Array(Math.min(safeLimit, items.length)).fill(0).map(async () => {
        while (true) {
            const index = nextIndex;
            if (index >= items.length) return;
            nextIndex += 1;
            results[index] = await handler(items[index], index);
        }
    });
    await Promise.all(workers);
    return results;
}

function sleep(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function reloadProjectTasksAfterPropagationDelay(
    bcClient: BusinessCentralClient,
    projectNo: string,
    options: {
        requestId?: string;
        attempts: number;
        baseDelayMs: number;
        minTaskCount: number;
        reason: string;
    }
) {
    const attempts = Math.max(0, Math.floor(options.attempts));
    const minTaskCount = Math.max(1, Math.floor(options.minTaskCount));
    if (!attempts) {
        return { tasks: [] as BcProjectTask[], attempt: 0 };
    }
    const filter = `projectNo eq '${escapeODataString(projectNo)}'`;
    let latest: BcProjectTask[] = [];
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
        await sleep(attempt * Math.max(100, Math.floor(options.baseDelayMs)));
        try {
            const reloadedTasks = await bcClient.listProjectTasks(filter);
            latest = reloadedTasks;
            if (reloadedTasks.length >= minTaskCount) {
                return { tasks: reloadedTasks, attempt };
            }
        } catch (error) {
            logger.warn("BC task reload after propagation delay failed", {
                requestId: options.requestId,
                projectNo,
                reason: options.reason,
                attempt,
                error: (error as Error)?.message,
            });
        }
    }
    return { tasks: latest, attempt: attempts };
}

function createEmptyTaskIndex(): TaskIndex {
    return { byId: new Set(), byTaskNo: new Map(), byTitle: new Map() };
}

function addToTaskIndex(index: TaskIndex | null | undefined, taskId: string, task: BcProjectTask, title?: string) {
    if (!index || !taskId) return;
    index.byId.add(taskId);
    const taskNo = task.taskNo ? String(task.taskNo).trim() : "";
    if (taskNo) {
        index.byTaskNo.set(taskNo, taskId);
    }
    const safeTitle = title ? String(title).trim() : "";
    if (safeTitle) {
        index.byTitle.set(safeTitle.toLowerCase(), taskId);
    }
}

async function loadProjectTaskIndex(
    dataverse: DataverseClient,
    projectId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
): Promise<TaskIndex> {
    const index = createEmptyTaskIndex();
    if (!projectId) return index;
    const projectFilterField = mapping.taskProjectIdField || `${mapping.taskProjectLookupField}/${mapping.projectIdField}`;
    if (!projectFilterField) return index;
    const select = [mapping.taskIdField, mapping.taskBcNoField, mapping.taskTitleField].filter(Boolean) as string[];
    const params = new URLSearchParams();
    if (select.length) params.set("$select", select.join(","));
    params.set("$filter", `${projectFilterField} eq ${formatODataGuid(projectId)}`);
    try {
        let next: string | null = `/${mapping.taskEntitySet}${params.toString() ? `?${params.toString()}` : ""}`;
        while (next) {
            const res = await dataverse.requestRaw(next, {
                headers: {
                    Prefer: "odata.maxpagesize=200",
                },
            });
            const data = (await res.json()) as { value?: DataverseEntity[]; "@odata.nextLink"?: string };
            const rows = Array.isArray(data?.value) ? data.value : [];
            for (const row of rows) {
                const id = row?.[mapping.taskIdField];
                if (typeof id !== "string" || !id.trim()) continue;
                const taskId = id.trim();
                index.byId.add(taskId);
                if (mapping.taskBcNoField) {
                    const bcNo = row?.[mapping.taskBcNoField];
                    if (typeof bcNo === "string" && bcNo.trim()) {
                        index.byTaskNo.set(bcNo.trim(), taskId);
                    }
                }
                const title = row?.[mapping.taskTitleField];
                if (typeof title === "string" && title.trim()) {
                    index.byTitle.set(title.trim().toLowerCase(), taskId);
                }
            }
            next = data?.["@odata.nextLink"] || null;
        }
    } catch (error) {
        logger.debug("Dataverse task index load failed; continuing without cache", {
            projectId,
            error: (error as Error)?.message,
        });
    }
    return index;
}

async function ensureProjectDefaultBucket(dataverse: DataverseClient, projectId: string, bucketId: string) {
    if (!projectId || !bucketId) return false;
    const binding = dataverse.buildLookupBinding("msdyn_projectbuckets", bucketId);
    if (!binding) return false;
    const override = (process.env.DATAVERSE_DEFAULT_BUCKET_FIELD || "").trim();
    if (defaultBucketFieldCache.value === undefined && !override) {
        defaultBucketFieldCache.value = (await resolveLookupField(dataverse, "msdyn_project", "msdyn_projectbucket")) || null;
    }
    const candidates = [];
    if (override) {
        candidates.push(override);
    } else if (defaultBucketFieldCache.value) {
        candidates.push(defaultBucketFieldCache.value);
    } else {
        return false;
    }
    let lastError: string | null = null;
    for (const field of candidates) {
        try {
            await dataverse.update("msdyn_projects", projectId, { [`${field}@odata.bind`]: binding });
            defaultBucketFieldCache.value = field;
            return true;
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            lastError = message;
            if (!warnedDefaultBucketFields.has(field)) {
                warnedDefaultBucketFields.add(field);
                logger.warn("Dataverse default bucket update failed", { projectId, field, error: message });
            }
            return false;
        }
    }
    if (override && !warnedDefaultBucketProjects.has(projectId)) {
        warnedDefaultBucketProjects.add(projectId);
        logger.warn("Dataverse default bucket update failed (override)", {
            projectId,
            field: override,
            error: lastError || "Unknown error",
        });
    }
    return false;
}

function isStaleSyncLock(task: BcProjectTask, timeoutMinutes: number) {
    if (!task.syncLock) return false;
    if (timeoutMinutes <= 0) return false;
    const lastSync = task.lastSyncAt ? Date.parse(task.lastSyncAt) : Number.NaN;
    if (Number.isNaN(lastSync)) return true;
    return Date.now() - lastSync > timeoutMinutes * 60 * 1000;
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
    if (hasField(task, "syncLock")) {
        patch.syncLock = false;
    }
    if (!Object.keys(patch).length) return;
    await bcClient.patchProjectTask(task.systemId, patch);
}

function normalizeProjectKey(value: string | undefined | null) {
    return normalizeProjectNo(value || "");
}

type BcTaskSyncField<T> = {
    present: boolean;
    value: T | null;
};

type BcTaskSyncFields = {
    title: string;
    description: BcTaskSyncField<string>;
    percent: BcTaskSyncField<number>;
    start: BcTaskSyncField<string>;
    finish: BcTaskSyncField<string>;
};

const BC_DESCRIPTION_FIELDS = ["description", "taskDescription", "title", "name"];
const BC_PERCENT_FIELDS = ["percentComplete", "percentcomplete", "percentCompleted", "percentageComplete", "completionPercent"];
const BC_START_FIELDS = ["manualStartDate", "startDate", "plannedStartDate", "plannedStart", "startingDate"];
const BC_FINISH_FIELDS = ["manualEndDate", "endDate", "plannedEndDate", "plannedEnd", "finishDate", "dueDate"];

function hasUsableBcTaskValue(value: unknown) {
    if (value == null) return false;
    if (typeof value === "string") return Boolean(value.trim());
    return true;
}

function readBcTaskField(task: BcProjectTask, candidates: string[]): BcTaskSyncField<unknown> {
    let found = false;
    let firstPresent: unknown = null;
    for (const key of candidates) {
        if (hasField(task, key)) {
            const value = task[key];
            if (!found) {
                found = true;
                firstPresent = value;
            }
            if (hasUsableBcTaskValue(value)) {
                return { present: true, value };
            }
        }
    }
    return found ? { present: true, value: firstPresent } : { present: false, value: null };
}

function toNullableTrimmedString(value: unknown) {
    if (value == null) return null;
    if (typeof value === "string") {
        const trimmed = value.trim();
        return trimmed || null;
    }
    const text = String(value).trim();
    return text || null;
}

function resolveBcDescriptionField(task: BcProjectTask): BcTaskSyncField<string> {
    const raw = readBcTaskField(task, BC_DESCRIPTION_FIELDS);
    return {
        present: raw.present,
        value: toNullableTrimmedString(raw.value),
    };
}

function resolveBcPercentField(task: BcProjectTask): BcTaskSyncField<number> {
    const raw = readBcTaskField(task, BC_PERCENT_FIELDS);
    if (!raw.present) return { present: false, value: null };
    if (raw.value == null) return { present: true, value: null };
    const parsed = typeof raw.value === "number" ? raw.value : Number(String(raw.value).trim());
    return { present: true, value: Number.isFinite(parsed) ? parsed : null };
}

function resolveBcDateField(task: BcProjectTask, candidates: string[]): BcTaskSyncField<string> {
    const raw = readBcTaskField(task, candidates);
    if (!raw.present) return { present: false, value: null };
    const asString = toNullableTrimmedString(raw.value);
    if (!asString) return { present: true, value: null };
    return { present: true, value: resolveTaskDate(asString) };
}

function resolveBcTaskSyncFields(task: BcProjectTask): BcTaskSyncFields {
    const description = resolveBcDescriptionField(task);
    const taskNo = (task.taskNo || "").trim();
    const title = description.value || taskNo || "Untitled Task";
    return {
        title,
        description,
        percent: resolveBcPercentField(task),
        start: resolveBcDateField(task, BC_START_FIELDS),
        finish: resolveBcDateField(task, BC_FINISH_FIELDS),
    };
}

function buildTaskTitle(task: BcProjectTask) {
    return resolveBcTaskSyncFields(task).title;
}

function toDataversePercent(value: unknown, scale: number, min: number, max: number) {
    if (value == null) return null;
    const parsed = typeof value === "number" ? value : Number(String(value).trim());
    if (!Number.isFinite(parsed) || Number.isNaN(parsed)) return null;
    const numericValue = parsed;
    let raw = !scale || scale === 1 ? numericValue : numericValue / scale;
    if (!Number.isFinite(raw)) return null;
    const lower = Number.isFinite(min) ? min : 0;
    const upper = Number.isFinite(max) ? max : 100;
    if (scale === 1 && upper <= 1 && raw > upper && numericValue <= 100) {
        raw = numericValue / 100;
    }
    if (raw < lower) return lower;
    if (raw > upper) return upper;
    return raw;
}

function fromDataversePercent(value: number | null | undefined, scale: number) {
    if (typeof value !== "number" || Number.isNaN(value)) return null;
    if (!scale || scale === 1) return value;
    return value * scale;
}

function normalizeBcPercent(value: number | null) {
    if (typeof value !== "number" || Number.isNaN(value)) return null;
    if (!Number.isFinite(value)) return null;
    const rounded = Math.round(value);
    if (rounded < 0) return 0;
    if (rounded > 100) return 100;
    return rounded;
}

function parseDateMs(value?: string | null) {
    if (!value) return null;
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
}

function resolveBcModifiedMs(task: BcProjectTask) {
    const modified =
        task.systemModifiedAt ||
        task.lastModifiedDateTime ||
        task.modifiedAt;
    return parseDateMs(typeof modified === "string" ? modified : undefined);
}

function resolveDataverseModifiedMs(
    entity: DataverseEntity | null | undefined,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    if (!entity) return null;
    const record = entity as Record<string, unknown>;
    const modifiedField = mapping.taskModifiedField || "modifiedon";
    const raw = record[modifiedField] ?? record.modifiedon ?? record.modifiedOn;
    if (raw == null) return null;
    const text = typeof raw === "string" ? raw : String(raw);
    return parseDateMs(text);
}

async function getDataverseTaskModifiedMs(
    dataverse: DataverseClient,
    taskId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const selectFields = [mapping.taskModifiedField, "modifiedon"].filter(Boolean) as string[];
    const entity = await dataverse.getById<DataverseEntity>(mapping.taskEntitySet, taskId, Array.from(new Set(selectFields)));
    return resolveDataverseModifiedMs(entity, mapping);
}

async function getDataverseTaskPercent(
    dataverse: DataverseClient,
    taskId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const field = mapping.taskPercentField;
    if (!field) return null;
    const entity = await dataverse.getById<DataverseEntity>(mapping.taskEntitySet, taskId, [field]);
    const raw = (entity as Record<string, unknown> | null)?.[field];
    if (typeof raw === "number") return raw;
    if (raw == null) return null;
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : null;
}

async function getDataverseTaskScheduleSnapshot(
    dataverse: DataverseClient,
    taskId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const fields = [mapping.taskPercentField, mapping.taskStartField, mapping.taskFinishField].filter(Boolean) as string[];
    const entity = await dataverse.getById<DataverseEntity>(mapping.taskEntitySet, taskId, Array.from(new Set(fields)));
    const record = entity as Record<string, unknown>;
    const percentRaw = mapping.taskPercentField ? record[mapping.taskPercentField] : null;
    const percent =
        typeof percentRaw === "number" ? percentRaw : percentRaw == null ? null : Number(percentRaw);
    const startRaw = mapping.taskStartField ? record[mapping.taskStartField] : null;
    const finishRaw = mapping.taskFinishField ? record[mapping.taskFinishField] : null;
    const start = typeof startRaw === "string" ? startRaw.trim() || null : null;
    const finish = typeof finishRaw === "string" ? finishRaw.trim() || null : null;
    return {
        percent: Number.isFinite(percent as number) ? (percent as number) : null,
        start,
        finish,
    };
}

function resolveTaskDate(value?: string | null) {
    if (!value) return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
        const year = Number(trimmed.slice(0, 4));
        if (Number.isFinite(year) && year < 1753) return null;
        const [y, m, d] = trimmed.split("-").map((part) => Number(part));
        if (!y || !m || !d) return null;
        return new Date(Date.UTC(y, m - 1, d, 12, 0, 0)).toISOString();
    }
    const parsed = parseDateMs(trimmed);
    if (parsed == null) return null;
    const minCrmDate = Date.UTC(1753, 0, 1);
    if (parsed < minCrmDate) return null;
    return new Date(parsed).toISOString();
}

function toBcDate(value?: string | null) {
    if (!value) return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const match = trimmed.match(/^(\d{4}-\d{2}-\d{2})/);
    if (match) return match[1];
    const parsed = parseDateMs(trimmed);
    if (parsed == null) return null;
    return new Date(parsed).toISOString().slice(0, 10);
}

function buildScheduleTaskEntity(params: {
    taskId: string;
    projectId: string;
    bucketId?: string | null;
    task: BcProjectTask;
    bcFields?: BcTaskSyncFields;
    mapping: ReturnType<typeof getDataverseMappingConfig>;
    dataverse: DataverseClient;
    mode?: "create" | "update";
    percentOverride?: number | null;
    startOverride?: string | null;
    finishOverride?: string | null;
}) {
    const { taskId, projectId, bucketId, task, bcFields, mapping, dataverse, mode, percentOverride, startOverride, finishOverride } =
        params;
    const isCreate = mode === "create";
    const fields = bcFields || resolveBcTaskSyncFields(task);
    const entity: DataverseEntity = {
        "@odata.type": "Microsoft.Dynamics.CRM.msdyn_projecttask",
        [mapping.taskIdField]: taskId,
    };

    const title = fields.title;
    entity[mapping.taskTitleField] = title;

    if (isCreate && mapping.taskBcNoField && task.taskNo) {
        entity[mapping.taskBcNoField] = String(task.taskNo).trim();
    }

    const start = startOverride !== undefined ? startOverride : fields.start.value;
    const finish = finishOverride !== undefined ? finishOverride : fields.finish.value;
    if (start) {
        entity[mapping.taskStartField] = start;
    }
    if (finish) {
        if (!start || Date.parse(finish) >= Date.parse(start)) {
            entity[mapping.taskFinishField] = finish;
        }
    }

    const percent =
        percentOverride !== undefined
            ? percentOverride
            : toDataversePercent(fields.percent.value, mapping.percentScale, mapping.percentMin, mapping.percentMax);
    if (percent != null) {
        entity[mapping.taskPercentField] = percent;
    }

    if (mapping.taskDescriptionField) {
        if (fields.description.value) {
            entity[mapping.taskDescriptionField] = fields.description.value;
        } else if (!isCreate && fields.description.present) {
            entity[mapping.taskDescriptionField] = null;
        }
    }

    if (isCreate) {
        const projectBinding = dataverse.buildLookupBinding(mapping.projectEntitySet, projectId);
        if (projectBinding) {
            entity[`${mapping.taskProjectLookupField}@odata.bind`] = projectBinding;
        }

        if (bucketId) {
            const bucketBinding = dataverse.buildLookupBinding("msdyn_projectbuckets", bucketId);
            if (bucketBinding) {
                entity["msdyn_projectbucket@odata.bind"] = bucketBinding;
            }
        }
    }

    return entity;
}

async function createProjectBucket(dataverse: DataverseClient, projectId: string, name: string) {
    try {
        const projectBinding = dataverse.buildLookupBinding("msdyn_projects", projectId);
        if (!projectBinding) return null;
        const payload: DataverseEntity = {
            msdyn_name: name,
            "msdyn_project@odata.bind": projectBinding,
        };
        const created = await dataverse.create("msdyn_projectbuckets", payload);
        if (!created.entityId) return null;
        return created.entityId;
    } catch (error) {
        logger.warn("Dataverse bucket create failed", { projectId, error: (error as Error)?.message });
        return null;
    }
}

async function getProjectBucketId(
    dataverse: DataverseClient,
    projectId: string,
    cache: Map<string, string | null>
) {
    if (cache.has(projectId)) {
        return cache.get(projectId) || null;
    }
    let lastError: unknown = null;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
        try {
            const result = await dataverse.list<DataverseEntity>("msdyn_projectbuckets", {
                select: ["msdyn_projectbucketid"],
                filter: `_msdyn_project_value eq ${formatODataGuid(projectId)}`,
                top: 1,
            });
            const bucketId = result.value[0]?.msdyn_projectbucketid;
            const resolved = typeof bucketId === "string" && bucketId.trim() ? bucketId.trim() : null;
            if (resolved) {
                await ensureProjectDefaultBucket(dataverse, projectId, resolved);
                cache.set(projectId, resolved);
                return resolved;
            }
            const created = await createProjectBucket(dataverse, projectId, "General");
            if (created) {
                await ensureProjectDefaultBucket(dataverse, projectId, created);
                cache.set(projectId, created);
                return created;
            }
        } catch (error) {
            lastError = error;
            logger.warn("Dataverse bucket lookup failed", {
                projectId,
                attempt,
                error: (error as Error)?.message,
            });
        }
        if (attempt < 3) {
            await sleep(attempt * 500);
        }
    }
    if (lastError) {
        logger.warn("Dataverse bucket unavailable after retries", {
            projectId,
            error: (lastError as Error)?.message,
        });
    }
    cache.set(projectId, null);
    return null;
}

function getAssigneeName(task: BcProjectTask) {
    const name = (task.assignedPersonName || "").trim();
    if (name) return name;
    return (task.assignedPersonCode || "").trim();
}

async function getBookableResourceIdByName(
    dataverse: DataverseClient,
    name: string,
    cache: Map<string, string | null>
) {
    const key = name.toLowerCase();
    if (cache.has(key)) return cache.get(key) || null;
    try {
        const filter = `name eq '${escapeODataString(name)}'`;
        const res = await dataverse.list<DataverseEntity>("bookableresources", {
            select: ["bookableresourceid", "name"],
            filter,
            top: 1,
        });
        const id = res.value[0]?.bookableresourceid;
        const resolved = typeof id === "string" && id.trim() ? id.trim() : null;
        cache.set(key, resolved);
        return resolved;
    } catch (error) {
        logger.debug("Dataverse resource lookup failed", { name, error: (error as Error)?.message });
        cache.set(key, null);
        return null;
    }
}

async function findBookableResourceByAadObjectId(dataverse: DataverseClient, aadObjectId: string) {
    const trimmed = (aadObjectId || "").trim().replace(/^\{/, "").replace(/\}$/, "");
    if (!trimmed) return null;
    const fallbackFields = ["msdyn_aadobjectid", "aadobjectid", "azureactivedirectoryobjectid", "msdyn_azureactivedirectoryobjectid"];
    let fields = fallbackFields;
    try {
        const res = await dataverse.requestRaw(
            "/EntityDefinitions(LogicalName='bookableresource')/Attributes?$select=LogicalName,IsValidForRead"
        );
        const data = (await res.json()) as { value?: Array<Record<string, unknown>> };
        const attrs = Array.isArray(data?.value) ? data.value : [];
        const discovered = attrs
            .map((attr) => ({
                logicalName: String((attr as { LogicalName?: string }).LogicalName || "").trim(),
                readable: (attr as { IsValidForRead?: boolean }).IsValidForRead !== false,
            }))
            .filter((attr) => attr.logicalName && attr.readable)
            .filter((attr) => {
                const logical = attr.logicalName.toLowerCase();
                return logical.includes("aad") || logical.includes("objectid");
            })
            .map((attr) => attr.logicalName);
        fields = Array.from(new Set([...fallbackFields, ...discovered]));
    } catch (error) {
        logger.debug("Dataverse resource metadata lookup failed", { error: (error as Error)?.message });
    }
    for (const field of fields) {
        const filters = [`${field} eq ${formatODataGuid(trimmed)}`, `${field} eq '${escapeODataString(trimmed)}'`];
        for (const filter of filters) {
            try {
                const res = await dataverse.list<DataverseEntity>("bookableresources", {
                    select: ["bookableresourceid", "name", field],
                    filter,
                    top: 1,
                });
                const row = res.value[0];
                if (row?.bookableresourceid) {
                    return {
                        id: String(row.bookableresourceid),
                        name: row.name ? String(row.name) : "",
                    };
                }
            } catch (error) {
                const message = (error as Error)?.message || "";
                if (message.includes("Could not find a property named")) {
                    break;
                }
                if (message.includes("Unrecognized 'Edm.String' literal 'guid'")) {
                    continue;
                }
                logger.debug("Dataverse bookable resource lookup failed", { field, filter, error: message });
            }
        }
    }
    return null;
}

async function getBookableResourceById(
    dataverse: DataverseClient,
    resourceId: string,
    cache: Map<string, { id: string; name: string } | null>
) {
    const trimmed = (resourceId || "").trim().replace(/^\{/, "").replace(/\}$/, "");
    if (!trimmed) return null;
    if (cache.has(trimmed)) return cache.get(trimmed) || null;
    try {
        const res = await dataverse.getById<DataverseEntity>("bookableresources", trimmed, ["bookableresourceid", "name"]);
        const id = res?.bookableresourceid ? String(res.bookableresourceid) : trimmed;
        const name = res?.name ? String(res.name) : "";
        const record = { id, name };
        cache.set(trimmed, record);
        return record;
    } catch (error) {
        logger.debug("Dataverse resource lookup failed", { resourceId: trimmed, error: (error as Error)?.message });
        cache.set(trimmed, null);
        return null;
    }
}

async function findDataverseTeamByAadObjectId(dataverse: DataverseClient, aadObjectId: string) {
    const trimmed = (aadObjectId || "").trim().replace(/^\{/, "").replace(/\}$/, "");
    if (!trimmed) return null;
    const fields = ["azureactivedirectoryobjectid", "aadobjectid", "msdyn_aadobjectid", "msdyn_azureactivedirectoryobjectid"];
    for (const field of fields) {
        const filters = [`${field} eq ${formatODataGuid(trimmed)}`, `${field} eq '${escapeODataString(trimmed)}'`];
        for (const filter of filters) {
            try {
                const res = await dataverse.list<DataverseEntity>("teams", {
                    select: ["teamid", "name", field],
                    filter,
                    top: 1,
                });
                const row = res.value[0];
                if (row?.teamid) {
                    return {
                        id: String(row.teamid).trim(),
                        name: row.name ? String(row.name).trim() : "",
                    };
                }
            } catch (error) {
                const message = (error as Error)?.message || "";
                if (message.includes("Could not find a property named")) {
                    break;
                }
                if (message.includes("Unrecognized 'Edm.String' literal 'guid'")) {
                    continue;
                }
                logger.debug("Dataverse team lookup by AAD object id failed", { field, filter, error: message });
            }
        }
    }
    return null;
}

async function getDataverseTeamById(
    dataverse: DataverseClient,
    teamId: string,
    cache: Map<string, { id: string; name: string } | null>
) {
    const trimmed = (teamId || "").trim().replace(/^\{/, "").replace(/\}$/, "");
    if (!trimmed) return null;
    if (cache.has(trimmed)) return cache.get(trimmed) || null;
    try {
        const team = await dataverse.getById<DataverseEntity>("teams", trimmed, ["teamid", "name"]);
        const record = {
            id: team?.teamid ? String(team.teamid).trim() : trimmed,
            name: team?.name ? String(team.name).trim() : "",
        };
        cache.set(trimmed, record);
        return record;
    } catch (error) {
        logger.debug("Dataverse team lookup failed", { teamId: trimmed, error: (error as Error)?.message });
        cache.set(trimmed, null);
        return null;
    }
}

async function getProjectOwnerId(
    dataverse: DataverseClient,
    projectId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    try {
        const entity = await dataverse.getById<DataverseEntity>(mapping.projectEntitySet, projectId, ["_ownerid_value", "ownerid"]);
        const raw = entity?._ownerid_value ?? entity?.ownerid;
        if (typeof raw !== "string" || !raw.trim()) return null;
        return raw.trim().replace(/^\{/, "").replace(/\}$/, "");
    } catch (error) {
        logger.debug("Dataverse project owner lookup failed", {
            projectId,
            error: (error as Error)?.message,
        });
        return null;
    }
}

async function assignProjectOwnerTeam(
    dataverse: DataverseClient,
    projectId: string,
    ownerTeamId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const normalizedTeamId = (ownerTeamId || "").trim().replace(/^\{/, "").replace(/\}$/, "");
    if (!normalizedTeamId) return false;
    const teamBinding = dataverse.buildLookupBinding("teams", normalizedTeamId);
    if (!teamBinding) return false;
    await dataverse.update(mapping.projectEntitySet, projectId, {
        "ownerid@odata.bind": teamBinding,
    });
    return true;
}

async function getProjectTeamMemberId(
    dataverse: DataverseClient,
    projectId: string,
    resourceId: string,
    cache: Map<string, string | null>
) {
    const key = `${projectId}:${resourceId}`;
    if (cache.has(key)) return cache.get(key) || null;
    try {
        const lookups = await getProjectTeamLookupFields(dataverse);
        const filter = `_${lookups.project}_value eq ${formatODataGuid(projectId)} and _${lookups.resource}_value eq ${formatODataGuid(
            resourceId
        )}`;
        const res = await dataverse.list<DataverseEntity>("msdyn_projectteams", {
            select: ["msdyn_projectteamid"],
            filter,
            top: 1,
        });
        const id = res.value[0]?.msdyn_projectteamid;
        const resolved = typeof id === "string" && id.trim() ? id.trim() : null;
        cache.set(key, resolved);
        return resolved;
    } catch (error) {
        logger.debug("Dataverse project team lookup failed", { projectId, resourceId, error: (error as Error)?.message });
        cache.set(key, null);
        return null;
    }
}

function isDuplicateTeamMemberError(error: unknown) {
    const message = (error as Error)?.message || String(error || "");
    const normalized = message.toLowerCase();
    return normalized.includes("duplicate resource") || normalized.includes("already a member");
}

async function createProjectTeamMember(
    dataverse: DataverseClient,
    projectId: string,
    resourceId: string,
    name: string,
    teamCache?: Map<string, string | null>
) {
    const lookups = await getProjectTeamLookupFields(dataverse);
    const projectBinding = dataverse.buildLookupBinding("msdyn_projects", projectId);
    const resourceBinding = dataverse.buildLookupBinding("bookableresources", resourceId);
    if (!projectBinding || !resourceBinding) return null;
    const entity: DataverseEntity = {
        "@odata.type": "Microsoft.Dynamics.CRM.msdyn_projectteam",
        msdyn_projectteamid: crypto.randomUUID(),
        msdyn_name: name,
        [`${lookups.project}@odata.bind`]: projectBinding,
        [`${lookups.resource}@odata.bind`]: resourceBinding,
    };
    try {
        const created = await dataverse.create("msdyn_projectteams", entity);
        return created.entityId || null;
    } catch (error) {
        if (isDuplicateTeamMemberError(error)) {
            const existing = await getProjectTeamMemberId(dataverse, projectId, resourceId, teamCache || new Map());
            if (existing) {
                return existing;
            }
            logger.debug("Dataverse project team already exists", { projectId, resourceId });
            return null;
        }
        logger.warn("Dataverse project team create failed", { projectId, resourceId, error: (error as Error)?.message });
        return null;
    }
}

async function ensureAssignmentForTask(
    dataverse: DataverseClient,
    task: BcProjectTask,
    projectId: string,
    taskId: string,
    options: {
        operationSetId?: string;
        resourceCache: Map<string, string | null>;
        teamCache: Map<string, string | null>;
        assignmentCache: Set<string>;
        assignmentSnapshotLoaded?: boolean;
        assignmentLookups?: { project: string; task: string; team: string };
    }
) {
    const assignee = getAssigneeName(task);
    if (!assignee) return;
    const resourceId = await getBookableResourceIdByName(dataverse, assignee, options.resourceCache);
    if (!resourceId) {
        logger.debug("Dataverse resource not found for assignee", { projectId, taskId, assignee });
        return;
    }
    let teamId = await getProjectTeamMemberId(dataverse, projectId, resourceId, options.teamCache);
    if (!teamId) {
        teamId = await createProjectTeamMember(dataverse, projectId, resourceId, assignee, options.teamCache);
        if (teamId) {
            options.teamCache.set(`${projectId}:${resourceId}`, teamId);
        }
    }
    if (!teamId) {
        logger.debug("Dataverse project team member missing for assignment", { projectId, taskId, assignee });
        return;
    }
    const assignmentKey = `${taskId}:${teamId}`;
    if (options.assignmentCache.has(assignmentKey)) return;
    const lookups = options.assignmentLookups || (await getAssignmentLookupFields(dataverse));
    if (!options.assignmentSnapshotLoaded) {
        try {
            const filter = `_${lookups.task}_value eq ${formatODataGuid(taskId)} and _${lookups.team}_value eq ${formatODataGuid(
                teamId
            )}`;
            const existing = await dataverse.list<DataverseEntity>("msdyn_resourceassignments", {
                select: ["msdyn_resourceassignmentid"],
                filter,
                top: 1,
            });
            if (existing.value[0]?.msdyn_resourceassignmentid) {
                options.assignmentCache.add(assignmentKey);
                return;
            }
        } catch (error) {
            logger.debug("Dataverse assignment lookup failed", { projectId, taskId, assignee, error: (error as Error)?.message });
        }
    }
    const projectBinding = dataverse.buildLookupBinding("msdyn_projects", projectId);
    const teamBinding = dataverse.buildLookupBinding("msdyn_projectteams", teamId);
    const taskBinding = dataverse.buildLookupBinding("msdyn_projecttasks", taskId);
    if (!projectBinding || !teamBinding || !taskBinding) return;
    const entity: DataverseEntity = {
        "@odata.type": "Microsoft.Dynamics.CRM.msdyn_resourceassignment",
        msdyn_resourceassignmentid: crypto.randomUUID(),
        msdyn_name: assignee,
        [`${lookups.project}@odata.bind`]: projectBinding,
        [`${lookups.team}@odata.bind`]: teamBinding,
        [`${lookups.task}@odata.bind`]: taskBinding,
    };
    try {
        if (options.operationSetId) {
            await dataverse.pssCreate(entity, options.operationSetId);
        } else {
            await dataverse.create("msdyn_resourceassignments", entity);
        }
        options.assignmentCache.add(assignmentKey);
    } catch (error) {
        logger.warn("Dataverse assignment create failed", { projectId, taskId, assignee, error: (error as Error)?.message });
    }
}

async function preloadProjectAssignments(
    dataverse: DataverseClient,
    projectId: string,
    assignmentCache: Set<string>
) {
    try {
        const lookups = await getAssignmentLookupFields(dataverse);
        const taskLookupField = `_${lookups.task}_value`;
        const teamLookupField = `_${lookups.team}_value`;
        const filter = `_${lookups.project}_value eq ${formatODataGuid(projectId)}`;
        const params = new URLSearchParams();
        params.set("$select", [taskLookupField, teamLookupField].join(","));
        params.set("$filter", filter);
        let next: string | null = `/msdyn_resourceassignments?${params.toString()}`;
        while (next) {
            const res = await dataverse.requestRaw(next, {
                headers: {
                    Prefer: "odata.maxpagesize=500",
                },
            });
            const data = (await res.json()) as { value?: DataverseEntity[]; "@odata.nextLink"?: string };
            const rows = Array.isArray(data?.value) ? data.value : [];
            for (const row of rows) {
                const taskId = row?.[taskLookupField];
                const teamId = row?.[teamLookupField];
                if (typeof taskId === "string" && taskId.trim() && typeof teamId === "string" && teamId.trim()) {
                    assignmentCache.add(`${taskId.trim()}:${teamId.trim()}`);
                }
            }
            next = data?.["@odata.nextLink"] || null;
        }
        return { loaded: true, lookups };
    } catch (error) {
        logger.debug("Dataverse assignment preload failed", { projectId, error: (error as Error)?.message });
        return { loaded: false, lookups: undefined };
    }
}

type ProjectAccessStatus = "added" | "already_member" | "resource_not_found";

async function ensureProjectGroupAccess(
    dataverse: DataverseClient,
    projectId: string,
    operationSetId: string | undefined,
    groupId: string,
    teamCache: Map<string, string | null>
): Promise<ProjectAccessStatus> {
    if (!groupId) return "resource_not_found";
    const groupResource = await findBookableResourceByAadObjectId(dataverse, groupId);
    if (!groupResource) {
        logger.warn("Dataverse group resource not found", {
            projectId,
            groupId,
            hint: "Verify bookableresource AAD mapping or set PLANNER_PRIMARY_RESOURCE_ID/PLANNER_GROUP_RESOURCE_IDS to explicit Dataverse resource IDs",
        });
        return "resource_not_found";
    }
    const teamId = await getProjectTeamMemberId(dataverse, projectId, groupResource.id, teamCache);
    if (teamId) return "already_member";
    const created = await createProjectTeamMember(
        dataverse,
        projectId,
        groupResource.id,
        groupResource.name || "Planner Group",
        teamCache
    );
    return created ? "added" : "resource_not_found";
}

async function ensureProjectResourceAccess(
    dataverse: DataverseClient,
    projectId: string,
    operationSetId: string | undefined,
    resourceId: string,
    resourceName: string,
    teamCache: Map<string, string | null>
): Promise<ProjectAccessStatus> {
    if (!resourceId) return "resource_not_found";
    const teamId = await getProjectTeamMemberId(dataverse, projectId, resourceId, teamCache);
    if (teamId) return "already_member";
    const created = await createProjectTeamMember(
        dataverse,
        projectId,
        resourceId,
        resourceName || "Planner Resource",
        teamCache
    );
    if (created) {
        teamCache.set(`${projectId}:${resourceId}`, created);
    }
    return created ? "added" : "resource_not_found";
}

function normalizePlannerGroupResourceIds(values: string[] | undefined) {
    return Array.from(new Set((values || []).map((value) => value.trim()).filter(Boolean)));
}

function resolveProjectAccessTargets(
    options: {
        plannerGroupId?: string;
        plannerGroupResourceIds?: string[];
        plannerOwnerTeamId?: string;
        plannerOwnerTeamAadGroupId?: string;
        plannerOwnerTeamOnly?: boolean;
        plannerPrimaryResourceId?: string;
        plannerPrimaryResourceName?: string;
        plannerShareReminderTaskEnabled?: boolean;
        plannerShareReminderTaskTitle?: string;
    } = {}
) {
    const syncConfig = getPremiumSyncConfig();
    const plannerOwnerTeamOnly =
        options.plannerOwnerTeamOnly !== undefined
            ? Boolean(options.plannerOwnerTeamOnly)
            : Boolean(syncConfig.plannerOwnerTeamOnly);
    let plannerGroupId =
        options.plannerGroupId !== undefined ? options.plannerGroupId.trim() : (syncConfig.plannerGroupId || "").trim();
    let plannerGroupResourceIds =
        options.plannerGroupResourceIds !== undefined
            ? normalizePlannerGroupResourceIds(options.plannerGroupResourceIds)
            : normalizePlannerGroupResourceIds(syncConfig.plannerGroupResourceIds || []);
    const plannerOwnerTeamId =
        options.plannerOwnerTeamId !== undefined
            ? options.plannerOwnerTeamId.trim()
            : (syncConfig.plannerOwnerTeamId || "").trim();
    const plannerOwnerTeamAadGroupId =
        options.plannerOwnerTeamAadGroupId !== undefined
            ? options.plannerOwnerTeamAadGroupId.trim()
            : (syncConfig.plannerOwnerTeamAadGroupId || "").trim();
    let plannerPrimaryResourceId =
        options.plannerPrimaryResourceId !== undefined
            ? options.plannerPrimaryResourceId.trim()
            : (syncConfig.plannerPrimaryResourceId || "").trim();
    let plannerPrimaryResourceName =
        options.plannerPrimaryResourceName !== undefined
            ? options.plannerPrimaryResourceName.trim()
            : (syncConfig.plannerPrimaryResourceName || "").trim();
    const plannerShareReminderTaskEnabled =
        options.plannerShareReminderTaskEnabled !== undefined
            ? Boolean(options.plannerShareReminderTaskEnabled)
            : Boolean(syncConfig.plannerShareReminderTaskEnabled);
    const plannerShareReminderTaskTitle =
        options.plannerShareReminderTaskTitle !== undefined
            ? options.plannerShareReminderTaskTitle.trim()
            : (syncConfig.plannerShareReminderTaskTitle || "").trim();
    if (plannerOwnerTeamOnly) {
        plannerGroupId = "";
        plannerGroupResourceIds = [];
        plannerPrimaryResourceId = "";
        plannerPrimaryResourceName = "";
    }
    return {
        plannerGroupId,
        plannerGroupResourceIds,
        plannerOwnerTeamId,
        plannerOwnerTeamAadGroupId,
        plannerOwnerTeamOnly,
        plannerPrimaryResourceId,
        plannerPrimaryResourceName,
        plannerShareReminderTaskEnabled,
        plannerShareReminderTaskTitle: plannerShareReminderTaskTitle || "Share Project",
    };
}

async function ensureProjectOwnerTeamAccess(
    dataverse: DataverseClient,
    projectId: string,
    options: {
        plannerOwnerTeamId?: string;
        plannerOwnerTeamAadGroupId?: string;
        ownerTeamCache?: Map<string, { id: string; name: string } | null>;
    } = {}
) {
    const { plannerOwnerTeamId, plannerOwnerTeamAadGroupId } = resolveProjectAccessTargets(options);
    const mapping = getDataverseMappingConfig();
    const ownerTeamCache = options.ownerTeamCache || new Map<string, { id: string; name: string } | null>();
    const output = {
        configured: Boolean(plannerOwnerTeamId || plannerOwnerTeamAadGroupId),
        targetTeamId: plannerOwnerTeamId || null,
        targetAadGroupId: plannerOwnerTeamAadGroupId || null,
        resolvedTeamId: null as string | null,
        resolvedTeamName: null as string | null,
        resolvedBy: null as "teamId" | "aadGroupId" | null,
        ownerBefore: null as string | null,
        changed: false,
        alreadyOwned: false,
        error: null as string | null,
    };
    if (!output.configured) {
        return output;
    }

    let team: { id: string; name: string } | null = null;
    if (plannerOwnerTeamId) {
        team = await getDataverseTeamById(dataverse, plannerOwnerTeamId, ownerTeamCache);
        output.resolvedBy = "teamId";
    }
    if (!team && plannerOwnerTeamAadGroupId) {
        team = await findDataverseTeamByAadObjectId(dataverse, plannerOwnerTeamAadGroupId);
        output.resolvedBy = "aadGroupId";
    }
    if (!team) {
        output.error = "owner_team_not_found";
        return output;
    }

    output.resolvedTeamId = team.id;
    output.resolvedTeamName = team.name || null;
    output.targetTeamId = output.targetTeamId || team.id;
    ownerTeamCache.set(team.id, team);
    output.ownerBefore = await getProjectOwnerId(dataverse, projectId, mapping);
    if (output.ownerBefore && output.ownerBefore.toLowerCase() === team.id.toLowerCase()) {
        output.alreadyOwned = true;
        return output;
    }

    await assignProjectOwnerTeam(dataverse, projectId, team.id, mapping);
    output.changed = true;
    return output;
}

async function ensureProjectShareReminderTask(
    dataverse: DataverseClient,
    projectId: string,
    options: {
        requestId?: string;
        projectNo?: string;
        plannerPrimaryResourceId?: string;
        plannerPrimaryResourceName?: string;
        plannerShareReminderTaskEnabled?: boolean;
        plannerShareReminderTaskTitle?: string;
        teamCache?: Map<string, string | null>;
        resourceCache?: Map<string, string | null>;
        resourceNameCache?: Map<string, { id: string; name: string } | null>;
    } = {}
) {
    const {
        plannerPrimaryResourceId,
        plannerPrimaryResourceName,
        plannerShareReminderTaskEnabled,
        plannerShareReminderTaskTitle,
    } = resolveProjectAccessTargets(options);
    const title = plannerShareReminderTaskTitle || "Share Project";
    const output = {
        enabled: Boolean(plannerShareReminderTaskEnabled),
        title,
        taskId: null as string | null,
        created: false,
        assigneeName: null as string | null,
        assignmentAttempted: false,
        error: null as string | null,
    };
    if (!output.enabled) {
        return output;
    }
    const mapping = getDataverseMappingConfig();
    const teamCache = options.teamCache || new Map<string, string | null>();
    const resourceCache = options.resourceCache || new Map<string, string | null>();
    const resourceNameCache = options.resourceNameCache || new Map<string, { id: string; name: string } | null>();

    let assigneeName = plannerPrimaryResourceName || "";
    if (!assigneeName && plannerPrimaryResourceId) {
        const resource = await getBookableResourceById(dataverse, plannerPrimaryResourceId, resourceNameCache);
        assigneeName = (resource?.name || "").trim();
    }
    output.assigneeName = assigneeName || null;

    const existing = await findDataverseTaskByTitle(dataverse, projectId, title, mapping);
    let taskId = resolveTaskId(existing, mapping) || "";
    if (!taskId) {
        const reminderTask: BcProjectTask = {
            projectNo: options.projectNo || "",
            description: title,
            percentComplete: 0,
            assignedPersonName: assigneeName || "",
        };
        const payload = buildTaskPayload(reminderTask, projectId, mapping, dataverse);
        try {
            const created = await dataverse.create(mapping.taskEntitySet, payload);
            taskId = created.entityId || "";
        } catch (error) {
            if (isDirectTaskWriteBlocked(error)) {
                const operationSetId = await createOperationSetWithRecovery({
                    dataverse,
                    projectId,
                    projectNo: options.projectNo || "",
                    requestId: options.requestId,
                    description: `Share reminder ${options.projectNo || projectId} ${new Date().toISOString()}`,
                });
                if (!operationSetId) {
                    throw error;
                }
                const newTaskId = crypto.randomUUID();
                try {
                    const bucketCache = new Map<string, string | null>();
                    const bucketId = await getProjectBucketId(dataverse, projectId, bucketCache);
                    const entity = buildScheduleTaskEntity({
                        taskId: newTaskId,
                        projectId,
                        bucketId,
                        task: reminderTask,
                        mapping,
                        dataverse,
                        mode: "create",
                    });
                    await dataverse.pssCreate(entity, operationSetId);
                    await dataverse.executeOperationSet(operationSetId);
                    taskId = newTaskId;
                } finally {
                    await cleanupOperationSet(dataverse, operationSetId, {
                        projectId,
                        projectNo: options.projectNo || "",
                        requestId: options.requestId,
                        reason: "share_reminder_task",
                    });
                }
            } else {
                throw error;
            }
        }
        output.created = Boolean(taskId);
    }
    output.taskId = taskId || null;
    if (taskId && assigneeName) {
        output.assignmentAttempted = true;
        await ensureAssignmentForTask(
            dataverse,
            {
                projectNo: options.projectNo || "",
                description: title,
                assignedPersonName: assigneeName,
            } as BcProjectTask,
            projectId,
            taskId,
            {
                resourceCache,
                teamCache,
                assignmentCache: new Set<string>(),
            }
        );
    }
    return output;
}

export async function ensurePremiumProjectTeamAccess(
    dataverse: DataverseClient,
    projectId: string,
    options: {
        requestId?: string;
        projectNo?: string;
        plannerGroupId?: string;
        plannerGroupResourceIds?: string[];
        plannerOwnerTeamId?: string;
        plannerOwnerTeamAadGroupId?: string;
        plannerOwnerTeamOnly?: boolean;
        plannerPrimaryResourceId?: string;
        plannerPrimaryResourceName?: string;
        plannerShareReminderTaskEnabled?: boolean;
        teamCache?: Map<string, string | null>;
        resourceCache?: Map<string, string | null>;
        resourceNameCache?: Map<string, { id: string; name: string } | null>;
        ownerTeamCache?: Map<string, { id: string; name: string } | null>;
    } = {}
) {
    const {
        plannerGroupId,
        plannerGroupResourceIds,
        plannerOwnerTeamId,
        plannerOwnerTeamAadGroupId,
        plannerOwnerTeamOnly,
        plannerPrimaryResourceId,
        plannerPrimaryResourceName,
        plannerShareReminderTaskEnabled,
        plannerShareReminderTaskTitle,
    } = resolveProjectAccessTargets(options);
    const teamCache = options.teamCache || new Map<string, string | null>();
    const resourceCache = options.resourceCache || new Map<string, string | null>();
    const resourceNameCache = options.resourceNameCache || new Map<string, { id: string; name: string } | null>();
    const ownerTeamCache = options.ownerTeamCache || new Map<string, { id: string; name: string } | null>();
    const targetResourceIds = new Set<string>(plannerGroupResourceIds);
    if (!plannerOwnerTeamOnly && plannerPrimaryResourceId) {
        targetResourceIds.add(plannerPrimaryResourceId);
    }
    const result = {
        configured: Boolean(
            plannerOwnerTeamId ||
                plannerOwnerTeamAadGroupId ||
                plannerGroupId ||
                targetResourceIds.size ||
                plannerPrimaryResourceName ||
                plannerShareReminderTaskEnabled
        ),
        projectId,
        projectNo: options.projectNo || "",
        plannerGroupId: plannerGroupId || null,
        plannerGroupResourceIds: Array.from(targetResourceIds),
        plannerOwnerTeamId: plannerOwnerTeamId || null,
        plannerOwnerTeamAadGroupId: plannerOwnerTeamAadGroupId || null,
        plannerOwnerTeamOnly: Boolean(plannerOwnerTeamOnly),
        plannerPrimaryResourceId: plannerPrimaryResourceId || null,
        plannerPrimaryResourceName: plannerPrimaryResourceName || null,
        plannerPrimaryResolvedResourceId: null as string | null,
        plannerShareReminderTaskEnabled: Boolean(plannerShareReminderTaskEnabled),
        plannerShareReminderTaskTitle,
        added: 0,
        alreadyMember: 0,
        missing: 0,
        errors: [] as string[],
        ownerTeam: null as null | {
            configured: boolean;
            targetTeamId: string | null;
            targetAadGroupId: string | null;
            resolvedTeamId: string | null;
            resolvedTeamName: string | null;
            resolvedBy: "teamId" | "aadGroupId" | null;
            ownerBefore: string | null;
            changed: boolean;
            alreadyOwned: boolean;
            error: string | null;
        },
        shareReminderTask: null as null | {
            enabled: boolean;
            title: string;
            taskId: string | null;
            created: boolean;
            assigneeName: string | null;
            assignmentAttempted: boolean;
            error: string | null;
        },
    };
    if (!result.configured) {
        return result;
    }

    if (plannerOwnerTeamId || plannerOwnerTeamAadGroupId) {
        try {
            result.ownerTeam = await ensureProjectOwnerTeamAccess(dataverse, projectId, {
                plannerOwnerTeamId,
                plannerOwnerTeamAadGroupId,
                ownerTeamCache,
            });
            if (result.ownerTeam.error) {
                result.errors.push(`owner_team:${result.ownerTeam.error}`);
            }
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            result.errors.push(`owner_team_failed:${message}`);
            result.ownerTeam = {
                configured: true,
                targetTeamId: plannerOwnerTeamId || null,
                targetAadGroupId: plannerOwnerTeamAadGroupId || null,
                resolvedTeamId: null,
                resolvedTeamName: null,
                resolvedBy: null,
                ownerBefore: null,
                changed: false,
                alreadyOwned: false,
                error: message,
            };
            logger.warn("Dataverse owner team share failed", {
                requestId: options.requestId || "",
                projectNo: options.projectNo || "",
                projectId,
                ownerTeamId: plannerOwnerTeamId || "",
                ownerTeamAadGroupId: plannerOwnerTeamAadGroupId || "",
                error: message,
            });
        }
    }

    if (plannerGroupId) {
        try {
            const status = await ensureProjectGroupAccess(dataverse, projectId, undefined, plannerGroupId, teamCache);
            if (status === "added") result.added += 1;
            if (status === "already_member") result.alreadyMember += 1;
            if (status === "resource_not_found") result.missing += 1;
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            result.errors.push(`group:${plannerGroupId}:${message}`);
            logger.warn("Dataverse group share failed", {
                requestId: options.requestId || "",
                projectNo: options.projectNo || "",
                projectId,
                groupId: plannerGroupId,
                error: message,
            });
        }
    }

    if (plannerPrimaryResourceName && !plannerPrimaryResourceId) {
        try {
            const resolved = await getBookableResourceIdByName(dataverse, plannerPrimaryResourceName, resourceCache);
            if (resolved) {
                result.plannerPrimaryResolvedResourceId = resolved;
                targetResourceIds.add(resolved);
            } else {
                result.missing += 1;
                result.errors.push(`primary_resource_name_not_found:${plannerPrimaryResourceName}`);
                logger.warn("Primary planner resource not found by name", {
                    requestId: options.requestId || "",
                    projectNo: options.projectNo || "",
                    projectId,
                    plannerPrimaryResourceName,
                });
            }
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            result.errors.push(`primary_resource_name_lookup_failed:${plannerPrimaryResourceName}:${message}`);
            logger.warn("Primary planner resource lookup failed", {
                requestId: options.requestId || "",
                projectNo: options.projectNo || "",
                projectId,
                plannerPrimaryResourceName,
                error: message,
            });
        }
    }

    for (const resourceId of targetResourceIds) {
        try {
            const resource = await getBookableResourceById(dataverse, resourceId, resourceNameCache);
            if (!resource) {
                result.missing += 1;
                logger.warn("Dataverse resource not found for configured project access resourceId", { projectId, resourceId });
                continue;
            }
            const status = await ensureProjectResourceAccess(
                dataverse,
                projectId,
                undefined,
                resource.id,
                resource.name || "Planner Resource",
                teamCache
            );
            if (status === "added") result.added += 1;
            if (status === "already_member") result.alreadyMember += 1;
            if (status === "resource_not_found") result.missing += 1;
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            result.errors.push(`resource:${resourceId}:${message}`);
            logger.warn("Dataverse resource share failed", {
                requestId: options.requestId || "",
                projectNo: options.projectNo || "",
                projectId,
                resourceId,
                error: message,
            });
        }
    }

    try {
        result.shareReminderTask = await ensureProjectShareReminderTask(dataverse, projectId, {
            requestId: options.requestId,
            projectNo: options.projectNo,
            plannerPrimaryResourceId: result.plannerPrimaryResolvedResourceId || plannerPrimaryResourceId,
            plannerPrimaryResourceName,
            plannerShareReminderTaskEnabled,
            plannerShareReminderTaskTitle,
            teamCache,
            resourceCache,
            resourceNameCache,
        });
    } catch (error) {
        const message = (error as Error)?.message || String(error);
        result.errors.push(`share_reminder_task_failed:${message}`);
        result.shareReminderTask = {
            enabled: Boolean(plannerShareReminderTaskEnabled),
            title: plannerShareReminderTaskTitle || "Share Project",
            taskId: null,
            created: false,
            assigneeName: plannerPrimaryResourceName || null,
            assignmentAttempted: false,
            error: message,
        };
        logger.warn("Share reminder task ensure failed", {
            requestId: options.requestId || "",
            projectNo: options.projectNo || "",
            projectId,
            error: message,
        });
    }

    return result;
}

export async function resolveProjectFromBc(
    bcClient: BusinessCentralClient,
    dataverse: DataverseClient,
    projectNo: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>,
    options: {
        forceCreate?: boolean;
        requestId?: string;
        skipProjectAccess?: boolean;
        plannerGroupId?: string;
        plannerGroupResourceIds?: string[];
        plannerOwnerTeamId?: string;
        plannerOwnerTeamAadGroupId?: string;
    } = {}
) {
    const normalized = (projectNo || "").trim();
    if (!normalized) return null;

    const escaped = escapeODataString(normalized);
    const select = [mapping.projectIdField, mapping.projectTitleField, mapping.projectBcNoField].filter(Boolean) as string[];
    const allowTitleFallback = ["1", "true", "yes", "y", "on"].includes(
        String(process.env.DATAVERSE_ALLOW_PROJECT_TITLE_FALLBACK || "")
            .trim()
            .toLowerCase()
    );

    if (!options.forceCreate && mapping.projectBcNoField) {
        const filter = `${mapping.projectBcNoField} eq '${escaped}'`;
        const result = await dataverse.list<DataverseEntity>(mapping.projectEntitySet, {
            select,
            filter,
            top: 5,
        });
        if (result.value.length) {
            const match = result.value[0];
            logger.info("Dataverse project resolved by BC project number", {
                projectNo: normalized,
                projectId: resolveProjectId(match, mapping),
            });
            return match;
        }
    }

    const titleField = mapping.projectTitleField;
    if (!options.forceCreate && allowTitleFallback && titleField) {
        const filter = `${titleField} eq '${escaped}'`;
        const result = await dataverse.list<DataverseEntity>(mapping.projectEntitySet, {
            select,
            filter,
            top: 5,
        });
        if (result.value.length) {
            const match = result.value[0];
            logger.warn("Dataverse project resolved by title fallback", {
                projectNo: normalized,
                projectId: resolveProjectId(match, mapping),
            });
            return match;
        }
    }

    let bcProject: BcProject | null = null;
    try {
        const projects = await bcClient.listProjects(`projectNo eq '${escaped}'`);
        bcProject = projects[0] || null;
    } catch (error) {
        logger.warn("BC project lookup failed", { projectNo, error: (error as Error)?.message });
    }

    const title = `${normalized}${bcProject?.description ? ` - ${bcProject.description}` : ""}`.trim();
    const payload: Record<string, unknown> = {
        [mapping.projectTitleField]: title,
    };

    const created = await dataverse.createProjectV1(payload);
    if (!created.projectId) return null;
    logger.info("Dataverse project created for BC project", {
        projectNo: normalized,
        projectId: created.projectId,
    });
    if (mapping.projectBcNoField) {
        try {
            await dataverse.update(mapping.projectEntitySet, created.projectId, { [mapping.projectBcNoField]: normalized });
        } catch (error) {
            logger.warn("Dataverse project BC No update failed", {
                projectNo: normalized,
                projectId: created.projectId,
                error: (error as Error)?.message,
            });
        }
    }
    if (!options.skipProjectAccess) {
        await ensurePremiumProjectTeamAccess(dataverse, created.projectId, {
            requestId: options.requestId,
            projectNo: normalized,
            plannerGroupId: options.plannerGroupId,
            plannerGroupResourceIds: options.plannerGroupResourceIds,
            plannerOwnerTeamId: options.plannerOwnerTeamId,
            plannerOwnerTeamAadGroupId: options.plannerOwnerTeamAadGroupId,
        });
    }
    return {
        [mapping.projectIdField]: created.projectId,
        [mapping.projectTitleField]: title,
        ...(mapping.projectBcNoField ? { [mapping.projectBcNoField]: normalized } : {}),
    } as DataverseEntity;
}

export function resolveProjectId(entity: DataverseEntity | null, mapping: ReturnType<typeof getDataverseMappingConfig>) {
    if (!entity) return null;
    const id = entity[mapping.projectIdField];
    if (typeof id === "string" && id.trim()) return id.trim();
    return null;
}

function resolveTaskId(entity: DataverseEntity | null, mapping: ReturnType<typeof getDataverseMappingConfig>) {
    if (!entity) return null;
    const id = entity[mapping.taskIdField];
    if (typeof id === "string" && id.trim()) return id.trim();
    return null;
}

async function validateTaskIdForProject(
    dataverse: DataverseClient,
    taskId: string,
    projectId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    if (!taskId) return false;
    try {
        const fields = Array.from(new Set([mapping.taskIdField, mapping.taskProjectIdField, "_msdyn_project_value"].filter(Boolean))) as string[];
        const entity = await dataverse.getById<DataverseEntity>(mapping.taskEntitySet, taskId, fields);
        if (!entity) return false;
        const projectField = mapping.taskProjectIdField || "_msdyn_project_value";
        const projectCandidates = [projectField, "_msdyn_project_value"];
        for (const field of projectCandidates) {
            const raw = entity[field];
            if (typeof raw === "string" && raw.trim()) {
                return raw.trim().toLowerCase() === projectId.toLowerCase();
            }
        }
        const warnKey = `${projectField}`;
        if (!warnedMissingTaskProjectLookup.has(warnKey)) {
            warnedMissingTaskProjectLookup.add(warnKey);
            logger.warn("Dataverse task project lookup missing; treating cached taskId as invalid", {
                taskId,
                expectedProjectId: projectId,
                projectField,
                availableKeys: Object.keys(entity || {}).slice(0, 20),
            });
        }
        return false;
    } catch (error) {
        const message = (error as Error)?.message || "";
        if (message.includes("Does Not Exist") || message.includes("404")) {
            return false;
        }
        throw error;
    }
}

async function findDataverseTaskByBcNo(
    dataverse: DataverseClient,
    projectId: string,
    taskNo: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    if (!mapping.taskBcNoField) return null;
    const escapedTask = escapeODataString(taskNo);
    const projectFilterField = mapping.taskProjectIdField || `${mapping.taskProjectLookupField}/${mapping.projectIdField}`;
    const projectGuid = formatODataGuid(projectId);
    const filter = `${mapping.taskBcNoField} eq '${escapedTask}' and ${projectFilterField} eq ${projectGuid}`;
    try {
        const res = await dataverse.list<DataverseEntity>(mapping.taskEntitySet, {
            select: [mapping.taskIdField, mapping.taskBcNoField, mapping.taskTitleField].filter(Boolean) as string[],
            filter,
            top: 1,
        });
        return res.value[0] || null;
    } catch (error) {
        logger.warn("Dataverse task lookup failed", { projectId, taskNo, error: (error as Error)?.message });
        return null;
    }
}

async function findDataverseTaskByTitle(
    dataverse: DataverseClient,
    projectId: string,
    title: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const escapedTitle = escapeODataString(title);
    const projectFilterField = mapping.taskProjectIdField || `${mapping.taskProjectLookupField}/${mapping.projectIdField}`;
    const projectGuid = formatODataGuid(projectId);
    const filter = `${mapping.taskTitleField} eq '${escapedTitle}' and ${projectFilterField} eq ${projectGuid}`;
    try {
        const res = await dataverse.list<DataverseEntity>(mapping.taskEntitySet, {
            select: [mapping.taskIdField, mapping.taskTitleField].filter(Boolean) as string[],
            filter,
            top: 1,
        });
        return res.value[0] || null;
    } catch (error) {
        logger.warn("Dataverse task title lookup failed", { projectId, title, error: (error as Error)?.message });
        return null;
    }
}

function buildTaskPayload(
    task: BcProjectTask,
    projectId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>,
    dataverse: DataverseClient,
    bcFields?: BcTaskSyncFields
) {
    const fields = bcFields || resolveBcTaskSyncFields(task);
    const payload: Record<string, unknown> = {};
    const title = fields.title;
    payload[mapping.taskTitleField] = title;

    const start = fields.start.value;
    const finish = fields.finish.value;
    if (start) payload[mapping.taskStartField] = start;
    else if (fields.start.present) payload[mapping.taskStartField] = null;
    if (finish) {
        if (!start || Date.parse(finish) >= Date.parse(start)) {
            payload[mapping.taskFinishField] = finish;
        }
    } else if (fields.finish.present) {
        payload[mapping.taskFinishField] = null;
    }

    const percent = toDataversePercent(fields.percent.value, mapping.percentScale, mapping.percentMin, mapping.percentMax);
    if (percent != null) payload[mapping.taskPercentField] = percent;
    else if (fields.percent.present) payload[mapping.taskPercentField] = null;

    if (mapping.taskDescriptionField) {
        if (fields.description.value) {
            payload[mapping.taskDescriptionField] = fields.description.value;
        } else if (fields.description.present) {
            payload[mapping.taskDescriptionField] = null;
        }
    }

    if (mapping.taskBcNoField && task.taskNo) {
        payload[mapping.taskBcNoField] = String(task.taskNo).trim();
    }

    const lookupBinding = dataverse.buildLookupBinding(mapping.projectEntitySet, projectId);
    if (lookupBinding) {
        payload[`${mapping.taskProjectLookupField}@odata.bind`] = lookupBinding;
    }

    return payload;
}

function shouldSkipTaskForSection(task: BcProjectTask, currentSection: { name: string | null }) {
    const description = String(task.description || "").trim();
    if (description.toUpperCase() === "TOTAL") return true;
    const taskNumber = parseTaskNumber(task.taskNo);
    if (Number.isFinite(taskNumber) && HEADING_TASK_SECTIONS.has(taskNumber)) {
        currentSection.name = HEADING_TASK_SECTIONS.get(taskNumber) || null;
        return true;
    }
    if (currentSection.name === "Revenue") return true;
    return false;
}

function sortTasksByTaskNo(tasks: BcProjectTask[]) {
    const parseTaskNoParts = (value: string | number | null | undefined) => {
        const raw = String(value || "").trim();
        if (!raw) return [] as number[];
        const matches = raw.match(/\d+/g);
        if (!matches?.length) return [] as number[];
        return matches
            .map((part) => Number(part))
            .filter((part) => Number.isFinite(part));
    };

    const compareTaskNoParts = (aParts: number[], bParts: number[]) => {
        const len = Math.max(aParts.length, bParts.length);
        for (let idx = 0; idx < len; idx += 1) {
            const aVal = aParts[idx];
            const bVal = bParts[idx];
            const aHas = Number.isFinite(aVal);
            const bHas = Number.isFinite(bVal);
            if (!aHas && !bHas) return 0;
            if (!aHas) return -1;
            if (!bHas) return 1;
            if (aVal !== bVal) return aVal - bVal;
        }
        return 0;
    };

    return [...tasks].sort((a, b) => {
        const aParts = parseTaskNoParts(a.taskNo);
        const bParts = parseTaskNoParts(b.taskNo);
        const numericCompare = compareTaskNoParts(aParts, bParts);
        if (numericCompare !== 0) {
            return numericCompare;
        }
        const aStr = String(a.taskNo || "").trim();
        const bStr = String(b.taskNo || "").trim();
        return aStr.localeCompare(bStr, undefined, { numeric: true, sensitivity: "base" });
    });
}

function hasLikelyExistingDataverseTask(
    task: BcProjectTask,
    taskIndex: TaskIndex,
    taskOnly: boolean | undefined
) {
    const plannerTaskId = (task.plannerTaskId || "").trim();
    if (plannerTaskId && isGuid(plannerTaskId)) {
        return true;
    }
    const taskNo = (task.taskNo || "").trim();
    if (taskNo && taskIndex.byTaskNo.has(taskNo)) {
        return true;
    }
    if (!taskOnly) {
        const titleKey = buildTaskTitle(task).toLowerCase();
        if (titleKey && taskIndex.byTitle.has(titleKey)) {
            return true;
        }
    }
    return false;
}

function buildBcUpdateFromPremium(
    bcTask: BcProjectTask,
    dataverseTask: DataverseEntity,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const updates: Record<string, unknown> = {
        lastSyncAt: new Date().toISOString(),
        lastPlannerEtag: dataverseTask["@odata.etag"],
    };

    const title = dataverseTask[mapping.taskTitleField];
    if (typeof title === "string" && title.trim()) {
        updates.description = title.trim();
    }

    const percentRaw = dataverseTask[mapping.taskPercentField];
    const percent = typeof percentRaw === "number" ? fromDataversePercent(percentRaw, mapping.percentScale) : null;
    const bcPercent = normalizeBcPercent(percent);
    if (bcPercent != null) {
        updates.percentComplete = bcPercent;
    }

    const start = dataverseTask[mapping.taskStartField];
    if (Object.prototype.hasOwnProperty.call(dataverseTask, mapping.taskStartField)) {
        if (typeof start === "string") {
            const trimmed = start.trim();
            const bcDate = toBcDate(trimmed);
            updates.manualStartDate = bcDate || null;
        } else if (start == null) {
            updates.manualStartDate = null;
        }
    }

    const finish = dataverseTask[mapping.taskFinishField];
    if (Object.prototype.hasOwnProperty.call(dataverseTask, mapping.taskFinishField)) {
        if (typeof finish === "string") {
            const trimmed = finish.trim();
            const bcDate = toBcDate(trimmed);
            updates.manualEndDate = bcDate || null;
        } else if (finish == null) {
            updates.manualEndDate = null;
        }
    }

    return buildBcPatch(bcTask, updates);
}

function isBcChangedSinceLastSync(bcTask: BcProjectTask, graceMs: number) {
    const lastSync = parseDateMs(bcTask.lastSyncAt);
    const modifiedMs =
        parseDateMs(bcTask.systemModifiedAt) ?? parseDateMs(bcTask.lastModifiedDateTime) ?? parseDateMs(bcTask.modifiedAt);
    if (lastSync == null || modifiedMs == null) return false;
    return modifiedMs - lastSync > graceMs;
}

function isDirectTaskWriteBlocked(error: unknown) {
    const message = (error as Error)?.message || String(error || "");
    const normalized = message.toLowerCase();
    if (normalized.includes("cannot directly do 'update' operation")) return true;
    if (normalized.includes("cannot directly do 'create' operation")) return true;
    if (normalized.includes("resource editing ui via project")) return true;
    return normalized.includes("msdyn_projecttask") && normalized.includes("not supported");
}

async function runScheduleUpdateFallback(options: {
    bcClient: BusinessCentralClient;
    dataverse: DataverseClient;
    task: BcProjectTask;
    taskId: string;
    projectId: string;
    requestId?: string;
    mapping: ReturnType<typeof getDataverseMappingConfig>;
    resourceCache: Map<string, string | null>;
    teamCache: Map<string, string | null>;
    assignmentCache: Set<string>;
    assignmentSnapshotLoaded?: boolean;
    assignmentLookups?: { project: string; task: string; team: string };
    percentOverride?: number | null;
    startOverride?: string | null;
    finishOverride?: string | null;
}) {
    const operationSetId = await createOperationSetWithRecovery({
        dataverse: options.dataverse,
        projectId: options.projectId,
        projectNo: options.task.projectNo,
        requestId: options.requestId,
        description: `BC sync fallback ${options.projectId} ${new Date().toISOString()}`,
    });
    if (!operationSetId) {
        throw new Error("Dataverse schedule API unavailable for fallback update");
    }
    await markPremiumTaskWrite(options.taskId, { projectNo: options.task.projectNo, taskNo: options.task.taskNo });
    try {
        const entity = buildScheduleTaskEntity({
            taskId: options.taskId,
            projectId: options.projectId,
            bucketId: null,
            task: options.task,
            mapping: options.mapping,
            dataverse: options.dataverse,
            mode: "update",
            percentOverride: options.percentOverride ?? undefined,
            startOverride: options.startOverride ?? undefined,
            finishOverride: options.finishOverride ?? undefined,
        });
        await options.dataverse.pssUpdate(entity, operationSetId);
        await ensureAssignmentForTask(options.dataverse, options.task, options.projectId, options.taskId, {
            operationSetId,
            resourceCache: options.resourceCache,
            teamCache: options.teamCache,
            assignmentCache: options.assignmentCache,
            assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
            assignmentLookups: options.assignmentLookups,
        });
        await options.dataverse.executeOperationSet(operationSetId);
        await updateBcTaskWithSyncLock(options.bcClient, options.task, {
            plannerTaskId: options.taskId,
            plannerPlanId: options.projectId,
            lastPlannerEtag: options.task.lastPlannerEtag || "",
            lastSyncAt: new Date().toISOString(),
        });
        return { action: "updated" as const, taskId: options.taskId };
    } finally {
        await cleanupOperationSet(options.dataverse, operationSetId, { projectId: options.projectId });
    }
}

async function runScheduleCreateFallback(options: {
    bcClient: BusinessCentralClient;
    dataverse: DataverseClient;
    task: BcProjectTask;
    projectId: string;
    requestId?: string;
    mapping: ReturnType<typeof getDataverseMappingConfig>;
    bucketCache: Map<string, string | null>;
    resourceCache: Map<string, string | null>;
    teamCache: Map<string, string | null>;
    assignmentCache: Set<string>;
    assignmentSnapshotLoaded?: boolean;
    assignmentLookups?: { project: string; task: string; team: string };
}) {
    const operationSetId = await createOperationSetWithRecovery({
        dataverse: options.dataverse,
        projectId: options.projectId,
        projectNo: options.task.projectNo,
        requestId: options.requestId,
        description: `BC sync fallback ${options.projectId} ${new Date().toISOString()}`,
    });
    if (!operationSetId) {
        throw new Error("Dataverse schedule API unavailable for fallback create");
    }
    try {
        const newTaskId = crypto.randomUUID();
        await markPremiumTaskWrite(newTaskId, { projectNo: options.task.projectNo, taskNo: options.task.taskNo });
        const bucketId = await getProjectBucketId(options.dataverse, options.projectId, options.bucketCache);
        const entity = buildScheduleTaskEntity({
            taskId: newTaskId,
            projectId: options.projectId,
            bucketId,
            task: options.task,
            mapping: options.mapping,
            dataverse: options.dataverse,
            mode: "create",
        });
        await options.dataverse.pssCreate(entity, operationSetId);
        await ensureAssignmentForTask(options.dataverse, options.task, options.projectId, newTaskId, {
            operationSetId,
            resourceCache: options.resourceCache,
            teamCache: options.teamCache,
            assignmentCache: options.assignmentCache,
            assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
            assignmentLookups: options.assignmentLookups,
        });
        await options.dataverse.executeOperationSet(operationSetId);
        await updateBcTaskWithSyncLock(options.bcClient, options.task, {
            plannerTaskId: newTaskId,
            plannerPlanId: options.projectId,
            lastPlannerEtag: "",
            lastSyncAt: new Date().toISOString(),
        });
        return { action: "created" as const, taskId: newTaskId };
    } finally {
        await cleanupOperationSet(options.dataverse, operationSetId, { projectId: options.projectId });
    }
}

async function cleanupOperationSet(
    dataverse: DataverseClient,
    operationSetId: string,
    meta: Record<string, unknown> = {}
) {
    if (!operationSetId) return;
    const cleanupFlag = String(process.env.DATAVERSE_CLEANUP_OPERATION_SETS || "")
        .trim()
        .toLowerCase();
    if (!["1", "true", "yes", "y", "on"].includes(cleanupFlag)) {
        return;
    }
    const cleanupMinAgeRaw = Number(process.env.DATAVERSE_CLEANUP_OPERATION_SETS_MIN_AGE_MINUTES || 15);
    const cleanupMinAgeMinutes = Number.isFinite(cleanupMinAgeRaw) ? Math.max(1, Math.floor(cleanupMinAgeRaw)) : 15;
    try {
        const entitySet = await resolveOperationSetEntitySet(dataverse);
        let row: DataverseEntity | null = null;
        try {
            row = await dataverse.getById<DataverseEntity>(entitySet, operationSetId, [
                "msdyn_operationsetid",
                "msdyn_completedon",
                "msdyn_executedon",
                "createdon",
                "modifiedon",
                "statecode",
                "statuscode",
                "msdyn_status",
            ]);
        } catch (error) {
            if (isOperationSetMissingError(error)) {
                return;
            }
            logger.warn("Dataverse operation set cleanup lookup failed", {
                operationSetId,
                entitySet,
                ...meta,
                error: (error as Error)?.message,
            });
            return;
        }
        if (!row || !isTerminalOperationSet(row)) {
            return;
        }

        const terminalMs =
            parseDateMs(String(row.msdyn_completedon || "")) ??
            parseDateMs(String(row.msdyn_executedon || "")) ??
            parseDateMs(String(row.modifiedon || "")) ??
            parseDateMs(String(row.createdon || ""));
        if (terminalMs == null) return;

        const ageMs = Date.now() - terminalMs;
        if (ageMs < cleanupMinAgeMinutes * 60 * 1000) {
            return;
        }

        await dataverse.delete(entitySet, operationSetId);
    } catch (error) {
        if (isOperationSetMissingError(error)) return;
        logger.warn("Dataverse operation set cleanup failed", {
            operationSetId,
            ...meta,
            error: (error as Error)?.message,
        });
    }
}

async function cleanupUnusedOperationSet(
    dataverse: DataverseClient,
    operationSetId: string,
    meta: Record<string, unknown> = {}
) {
    if (!operationSetId) return;
    try {
        const entitySet = await resolveOperationSetEntitySet(dataverse);
        await dataverse.delete(entitySet, operationSetId);
    } catch (error) {
        if (isOperationSetMissingError(error)) return;
        logger.warn("Dataverse unused operation set cleanup failed", {
            operationSetId,
            ...meta,
            error: (error as Error)?.message,
        });
    }
}

async function clearStaleSyncLockIfNeeded(bcClient: BusinessCentralClient, task: BcProjectTask, timeoutMinutes: number) {
    if (!task.syncLock) return task;
    if (!isStaleSyncLock(task, timeoutMinutes)) return task;
    if (!task.systemId) return task;
    try {
        await bcClient.patchProjectTask(task.systemId, { syncLock: false });
        return { ...task, syncLock: false };
    } catch (error) {
        logger.warn("Failed to clear stale syncLock", { projectNo: task.projectNo, taskNo: task.taskNo, error: (error as Error)?.message });
        return task;
    }
}

async function syncTaskToDataverse(
    bcClient: BusinessCentralClient,
    dataverse: DataverseClient,
    task: BcProjectTask,
    projectId: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>,
    options: {
        requestId?: string;
        useScheduleApi: boolean;
        operationSetId?: string;
        bucketCache: Map<string, string | null>;
        resourceCache: Map<string, string | null>;
        teamCache: Map<string, string | null>;
        assignmentCache: Set<string>;
        assignmentSnapshotLoaded?: boolean;
        assignmentLookups?: { project: string; task: string; team: string };
        touchOperationSet?: () => void;
        taskOnly?: boolean;
        preferPlanner?: boolean;
        plannerModifiedGraceMs?: number;
        taskIndex?: TaskIndex;
        scheduleFallbackState?: ScheduleFallbackState;
        requireScheduleApi?: boolean;
        forceTaskRecreate?: boolean;
    }
) {
    const bcFields = resolveBcTaskSyncFields(task);
    const payload = buildTaskPayload(task, projectId, mapping, dataverse, bcFields);
    const payloadTitle = payload[mapping.taskTitleField];
    const titleKey = typeof payloadTitle === "string" && payloadTitle.trim() ? payloadTitle.trim().toLowerCase() : "";
    const requireScheduleApi = Boolean(options.requireScheduleApi);
    const forceTaskRecreate = Boolean(options.forceTaskRecreate);
    const existingId = forceTaskRecreate ? "" : task.plannerTaskId ? task.plannerTaskId.trim() : "";
    let taskId = existingId || "";
    if (taskId && !isGuid(taskId)) {
        warnNonGuidPlannerTaskId(task.projectNo, task.taskNo, taskId);
        taskId = "";
    }

    if (taskId && options.taskIndex?.byId?.has(taskId)) {
        // task already known for project
    } else if (taskId) {
        const valid = await validateTaskIdForProject(dataverse, taskId, projectId, mapping);
        if (!valid) {
            logger.warn("Planner taskId invalid for project; will recreate", {
                projectNo: task.projectNo,
                taskNo: task.taskNo,
                taskId,
                projectId,
            });
            taskId = "";
        }
    }

    let existingTask: DataverseEntity | null = null;
    if (!forceTaskRecreate && !taskId && task.taskNo) {
        const indexed = options.taskIndex?.byTaskNo?.get(String(task.taskNo).trim());
        if (indexed) {
            taskId = indexed;
        } else {
            existingTask = await findDataverseTaskByBcNo(dataverse, projectId, String(task.taskNo), mapping);
            taskId = resolveTaskId(existingTask, mapping) || "";
        }
    }

    if (!forceTaskRecreate && !taskId && !options.taskOnly) {
        if (titleKey && options.taskIndex?.byTitle?.has(titleKey)) {
            taskId = options.taskIndex.byTitle.get(titleKey) || "";
        } else {
            existingTask = await findDataverseTaskByTitle(dataverse, projectId, payload[mapping.taskTitleField] as string, mapping);
            taskId = resolveTaskId(existingTask, mapping) || "";
        }
    }

    if (taskId) {
        const bcPercent = toDataversePercent(
            bcFields.percent.value,
            mapping.percentScale,
            mapping.percentMin,
            mapping.percentMax
        );
        let percentOverride: number | null | undefined;
        let startOverride: string | null | undefined;
        let finishOverride: string | null | undefined;

        if (options.useScheduleApi && (!bcFields.percent.present || !bcFields.start.present || !bcFields.finish.present)) {
            try {
                const snapshot = await getDataverseTaskScheduleSnapshot(dataverse, taskId, mapping);
                if (!bcFields.percent.present) percentOverride = snapshot.percent ?? null;
                if (!bcFields.start.present) startOverride = snapshot.start ?? null;
                if (!bcFields.finish.present) finishOverride = snapshot.finish ?? null;
            } catch (error) {
                logger.debug("Dataverse schedule snapshot lookup failed; proceeding with BC update", {
                    requestId: options.requestId,
                    projectNo: task.projectNo,
                    taskNo: task.taskNo,
                    error: (error as Error)?.message,
                });
            }
        }

        if (options.preferPlanner) {
            try {
                const plannerModifiedMs = await getDataverseTaskModifiedMs(dataverse, taskId, mapping);
                const bcModifiedMs = resolveBcModifiedMs(task);
                const graceMs = options.plannerModifiedGraceMs ?? 0;
                if (
                    plannerModifiedMs != null &&
                    bcModifiedMs != null &&
                    plannerModifiedMs - bcModifiedMs > graceMs
                ) {
                    logger.info("BC -> Premium skipped (Planner newer)", {
                        requestId: options.requestId,
                        projectNo: task.projectNo,
                        taskNo: task.taskNo,
                        plannerModifiedAt: new Date(plannerModifiedMs).toISOString(),
                        bcModifiedAt: new Date(bcModifiedMs).toISOString(),
                    });
                    return { action: "skipped", taskId };
                }
            } catch (error) {
                logger.debug("Dataverse modified check failed; proceeding with BC update", {
                    requestId: options.requestId,
                    projectNo: task.projectNo,
                    taskNo: task.taskNo,
                    error: (error as Error)?.message,
                });
            }
        }

        if (options.useScheduleApi && options.operationSetId) {
            options.touchOperationSet?.();
            await markPremiumTaskWrite(taskId, { requestId: options.requestId, projectNo: task.projectNo, taskNo: task.taskNo });
            const entity = buildScheduleTaskEntity({
                taskId,
                projectId,
                bucketId: null,
                task,
                bcFields,
                mapping,
                dataverse,
                mode: "update",
                percentOverride: percentOverride ?? bcPercent ?? undefined,
                startOverride: startOverride ?? undefined,
                finishOverride: finishOverride ?? undefined,
            });
            await dataverse.pssUpdate(entity, options.operationSetId);
            if (!options.taskOnly) {
                await ensureAssignmentForTask(dataverse, task, projectId, taskId, {
                    operationSetId: options.operationSetId,
                    resourceCache: options.resourceCache,
                    teamCache: options.teamCache,
                    assignmentCache: options.assignmentCache,
                    assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
                    assignmentLookups: options.assignmentLookups,
                });
            }
            const updates = {
                plannerTaskId: taskId,
                plannerPlanId: projectId,
                lastPlannerEtag: task.lastPlannerEtag || "",
                lastSyncAt: new Date().toISOString(),
            };
            addToTaskIndex(options.taskIndex, taskId, task, payloadTitle as string);
            return {
                action: "updated",
                taskId,
                pendingUpdate: { task, updates },
            };
        }

        if (requireScheduleApi) {
            if (options.scheduleFallbackState?.unavailable) {
                throw new Error(
                    `Dataverse schedule API unavailable for BC -> Premium update (${options.scheduleFallbackState.reason || "unknown"})`
                );
            }
            return runScheduleUpdateFallback({
                bcClient,
                dataverse,
                task,
                taskId,
                projectId,
                requestId: options.requestId,
                mapping,
                resourceCache: options.resourceCache,
                teamCache: options.teamCache,
                assignmentCache: options.assignmentCache,
                assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
                assignmentLookups: options.assignmentLookups,
                percentOverride: percentOverride ?? bcPercent ?? undefined,
                startOverride: startOverride ?? undefined,
                finishOverride: finishOverride ?? undefined,
            });
        }

        if (options.taskOnly) {
            const ifMatch = task.lastPlannerEtag ? String(task.lastPlannerEtag) : undefined;
            await markPremiumTaskWrite(taskId, { requestId: options.requestId, projectNo: task.projectNo, taskNo: task.taskNo });
            const updateResult = await dataverse.update(mapping.taskEntitySet, taskId, payload, { ifMatch });
            const updates = {
                plannerTaskId: taskId,
                plannerPlanId: projectId,
                lastPlannerEtag: updateResult.etag || task.lastPlannerEtag || "",
                lastSyncAt: new Date().toISOString(),
            };
            await updateBcTaskWithSyncLock(bcClient, task, updates);
            addToTaskIndex(options.taskIndex, taskId, task, payloadTitle as string);
            return { action: "updated", taskId, etag: updateResult.etag || task.lastPlannerEtag };
        }

        const ifMatch = task.lastPlannerEtag ? String(task.lastPlannerEtag) : undefined;
        try {
            await markPremiumTaskWrite(taskId, { requestId: options.requestId, projectNo: task.projectNo, taskNo: task.taskNo });
            const updateResult = await dataverse.update(mapping.taskEntitySet, taskId, payload, { ifMatch });
            const updates = {
                plannerTaskId: taskId,
                plannerPlanId: projectId,
                lastPlannerEtag: updateResult.etag || task.lastPlannerEtag,
                lastSyncAt: new Date().toISOString(),
            };
            await updateBcTaskWithSyncLock(bcClient, task, updates);
            addToTaskIndex(options.taskIndex, taskId, task, payloadTitle as string);
            return { action: "updated", taskId };
        } catch (error) {
            if (isDirectTaskWriteBlocked(error)) {
                if (options.scheduleFallbackState?.unavailable) {
                    if (!options.scheduleFallbackState.warned) {
                        options.scheduleFallbackState.warned = true;
                        logger.warn("Skipping direct Dataverse task writes; schedule fallback unavailable for project", {
                            requestId: options.requestId,
                            projectNo: task.projectNo,
                            reason: options.scheduleFallbackState.reason,
                        });
                    }
                    return { action: "skipped", taskId };
                }
                try {
                    return await runScheduleUpdateFallback({
                        bcClient,
                        dataverse,
                        task,
                        taskId,
                        projectId,
                        requestId: options.requestId,
                        mapping,
                        resourceCache: options.resourceCache,
                        teamCache: options.teamCache,
                        assignmentCache: options.assignmentCache,
                        assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
                        assignmentLookups: options.assignmentLookups,
                        percentOverride: percentOverride ?? bcPercent ?? undefined,
                        startOverride: startOverride ?? undefined,
                        finishOverride: finishOverride ?? undefined,
                    });
                } catch (fallbackError) {
                    if (isOperationSetLimitError(fallbackError)) {
                        if (options.scheduleFallbackState) {
                            options.scheduleFallbackState.unavailable = true;
                            options.scheduleFallbackState.reason = "operation_set_capacity";
                        }
                        logger.warn("Schedule fallback disabled for project after operation set limit error", {
                            requestId: options.requestId,
                            projectNo: task.projectNo,
                            taskNo: task.taskNo,
                            projectId,
                            error: (fallbackError as Error)?.message,
                        });
                        return { action: "skipped", taskId };
                    }
                    logger.warn("Schedule update fallback failed", {
                        projectId,
                        taskId,
                        error: (fallbackError as Error)?.message,
                    });
                }
            }
            throw error;
        }
    }

    if (!mapping.allowTaskCreate || options.taskOnly) {
        return { action: "skipped", taskId: "" };
    }

    if (options.useScheduleApi && options.operationSetId) {
        options.touchOperationSet?.();
        const newTaskId = crypto.randomUUID();
        await markPremiumTaskWrite(newTaskId, { requestId: options.requestId, projectNo: task.projectNo, taskNo: task.taskNo });
        const bucketId = await getProjectBucketId(dataverse, projectId, options.bucketCache);
        const entity = buildScheduleTaskEntity({
            taskId: newTaskId,
            projectId,
            bucketId,
            task,
            bcFields,
            mapping,
            dataverse,
            mode: "create",
        });
        await dataverse.pssCreate(entity, options.operationSetId);
        await ensureAssignmentForTask(dataverse, task, projectId, newTaskId, {
            operationSetId: options.operationSetId,
            resourceCache: options.resourceCache,
            teamCache: options.teamCache,
            assignmentCache: options.assignmentCache,
            assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
            assignmentLookups: options.assignmentLookups,
        });
        const updates = {
            plannerTaskId: newTaskId,
            plannerPlanId: projectId,
            lastPlannerEtag: "",
            lastSyncAt: new Date().toISOString(),
        };
        addToTaskIndex(options.taskIndex, newTaskId, task, payloadTitle as string);
        return { action: "created", taskId: newTaskId, pendingUpdate: { task, updates } };
    }

    if (requireScheduleApi) {
        if (options.scheduleFallbackState?.unavailable) {
            throw new Error(
                `Dataverse schedule API unavailable for BC -> Premium create (${options.scheduleFallbackState.reason || "unknown"})`
            );
        }
        return runScheduleCreateFallback({
            bcClient,
            dataverse,
            task,
            projectId,
            requestId: options.requestId,
            mapping,
            bucketCache: options.bucketCache,
            resourceCache: options.resourceCache,
            teamCache: options.teamCache,
            assignmentCache: options.assignmentCache,
            assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
            assignmentLookups: options.assignmentLookups,
        });
    }

    try {
        const created = await dataverse.create(mapping.taskEntitySet, payload);
        if (!created.entityId) {
            return { action: "error", taskId: "" };
        }
        await markPremiumTaskWrite(created.entityId, { requestId: options.requestId, projectNo: task.projectNo, taskNo: task.taskNo });
        await updateBcTaskWithSyncLock(bcClient, task, {
            plannerTaskId: created.entityId,
            plannerPlanId: projectId,
            lastPlannerEtag: created.etag,
            lastSyncAt: new Date().toISOString(),
        });
        addToTaskIndex(options.taskIndex, created.entityId, task, payloadTitle as string);
        await ensureAssignmentForTask(dataverse, task, projectId, created.entityId, {
            resourceCache: options.resourceCache,
            teamCache: options.teamCache,
            assignmentCache: options.assignmentCache,
            assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
            assignmentLookups: options.assignmentLookups,
        });
        return { action: "created", taskId: created.entityId };
    } catch (error) {
        if (isDirectTaskWriteBlocked(error)) {
            if (options.scheduleFallbackState?.unavailable) {
                if (!options.scheduleFallbackState.warned) {
                    options.scheduleFallbackState.warned = true;
                    logger.warn("Skipping direct Dataverse task writes; schedule fallback unavailable for project", {
                        requestId: options.requestId,
                        projectNo: task.projectNo,
                        reason: options.scheduleFallbackState.reason,
                    });
                }
                return { action: "skipped", taskId: "" };
            }
            try {
                return await runScheduleCreateFallback({
                    bcClient,
                    dataverse,
                    task,
                    projectId,
                    requestId: options.requestId,
                    mapping,
                    bucketCache: options.bucketCache,
                    resourceCache: options.resourceCache,
                    teamCache: options.teamCache,
                    assignmentCache: options.assignmentCache,
                    assignmentSnapshotLoaded: options.assignmentSnapshotLoaded,
                    assignmentLookups: options.assignmentLookups,
                });
            } catch (fallbackError) {
                if (isOperationSetLimitError(fallbackError)) {
                    if (options.scheduleFallbackState) {
                        options.scheduleFallbackState.unavailable = true;
                        options.scheduleFallbackState.reason = "operation_set_capacity";
                    }
                    logger.warn("Schedule fallback disabled for project after operation set limit error", {
                        requestId: options.requestId,
                        projectNo: task.projectNo,
                        taskNo: task.taskNo,
                        projectId,
                        error: (fallbackError as Error)?.message,
                    });
                    return { action: "skipped", taskId: "" };
                }
                logger.warn("Schedule create fallback failed", {
                    projectId,
                    error: (fallbackError as Error)?.message,
                });
            }
        }
        throw error;
    }
}

function normalizeTaskSystemId(raw: string) {
    let value = (raw || "").trim();
    if (!value) return "";
    if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"'))) {
        value = value.slice(1, -1);
    }
    if (value.startsWith("{") && value.endsWith("}")) {
        value = value.slice(1, -1);
    }
    return value.trim();
}

function canonicalTaskSystemId(raw: string) {
    const normalized = normalizeTaskSystemId(raw);
    return normalized ? normalized.toLowerCase() : "";
}

type BcQueueTargetInfo = {
    queueEntries: Array<Record<string, unknown>>;
    queueRows: number;
    projectNos: string[];
    taskSystemIdsByProject: Map<string, Set<string>>;
    queueEntriesByProject: Map<string, BcQueueEntryRef[]>;
    fullSyncProjects: Set<string>;
    latestChangedMs: number | null;
};

type BcQueueEntryRef = {
    queueEntryId: string;
    projectNo: string;
    taskSystemId: string;
};

function toTrimmedString(value: unknown): string {
    if (typeof value === "string") return value.trim();
    if (value == null) return "";
    return String(value).trim();
}

function extractLatestTimestampMs(record: Record<string, unknown>): number | null {
    for (const field of BC_QUEUE_TIME_FIELDS) {
        const ms = parseTimestamp(record[field]);
        if (ms != null) return ms;
    }
    return null;
}

function shouldDeleteQueueEntryAfterSync() {
    const raw = (process.env.BC_SYNC_QUEUE_DELETE_AFTER || "").trim().toLowerCase();
    if (!raw) return true;
    return !["0", "false", "no", "n", "off"].includes(raw);
}

async function loadBcQueueEntries(
    bcClient: BusinessCentralClient,
    options: { requestId?: string } = {}
): Promise<Array<Record<string, unknown>>> {
    if (!BC_QUEUE_ENTITY_SET) return [];
    let nextLink: string | null = null;
    let page = 0;
    const entries: Array<Record<string, unknown>> = [];

    while (page < BC_QUEUE_MAX_PAGES) {
        const path = nextLink || `/${BC_QUEUE_ENTITY_SET}?$top=${BC_QUEUE_PAGE_SIZE}`;
        const res = await (bcClient as unknown as { request: (path: string) => Promise<Response> }).request(path);
        const data = (await res.json()) as { value?: Array<Record<string, unknown>>; "@odata.nextLink"?: string };
        const value = Array.isArray(data?.value) ? data.value : [];
        for (const item of value) {
            entries.push(item || {});
        }
        nextLink = typeof data?.["@odata.nextLink"] === "string" ? data["@odata.nextLink"] : null;
        page += 1;
        if (!nextLink) break;
    }

    if (nextLink) {
        logger.warn("BC queue read truncated at max pages", {
            requestId: options.requestId || "",
            entitySet: BC_QUEUE_ENTITY_SET,
            maxPages: BC_QUEUE_MAX_PAGES,
        });
    }
    return entries;
}

async function resolveBcQueueTargets(
    bcClient: BusinessCentralClient,
    options: { requestId?: string } = {}
): Promise<BcQueueTargetInfo> {
    const queueEntries = await loadBcQueueEntries(bcClient, options);
    const projectNos = new Set<string>();
    const taskSystemIdsByProject = new Map<string, Set<string>>();
    const queueEntriesByProject = new Map<string, BcQueueEntryRef[]>();
    const fullSyncProjects = new Set<string>();
    const taskCache = new Map<string, BcProjectTask | null>();
    let latestChangedMs: number | null = null;

    for (const entry of queueEntries) {
        const record = entry || {};
        const queueProjectNo = toTrimmedString(record[BC_QUEUE_PROJECT_NO_FIELD]);
        const queueTaskSystemId = canonicalTaskSystemId(toTrimmedString(record[BC_QUEUE_TASK_SYSTEM_ID_FIELD]));
        const entryMs = extractLatestTimestampMs(record);
        if (entryMs != null && (latestChangedMs == null || entryMs > latestChangedMs)) {
            latestChangedMs = entryMs;
        }

        let resolvedProjectNo = queueProjectNo;
        if (queueTaskSystemId) {
            let task = taskCache.get(queueTaskSystemId);
            if (task === undefined) {
                try {
                    task = await bcClient.getProjectTask(queueTaskSystemId);
                } catch (error) {
                    const message = (error as Error)?.message || "";
                    if (message.includes("-> 404")) {
                        task = null;
                    } else {
                        throw error;
                    }
                }
                taskCache.set(queueTaskSystemId, task || null);
            }
            const taskMs = task ? extractLatestTimestampMs(task as Record<string, unknown>) : null;
            if (taskMs != null && (latestChangedMs == null || taskMs > latestChangedMs)) {
                latestChangedMs = taskMs;
            }
            if (task?.projectNo) {
                const taskProjectNo = toTrimmedString(task.projectNo);
                if (taskProjectNo) {
                    resolvedProjectNo = taskProjectNo;
                }
            }
        }

        if (!resolvedProjectNo) continue;
        projectNos.add(resolvedProjectNo);
        const queueEntryId = normalizeTaskSystemId(toTrimmedString(record.systemId || record.id || record.entryId));
        if (queueEntryId) {
            const refs = queueEntriesByProject.get(resolvedProjectNo) || [];
            refs.push({
                queueEntryId,
                projectNo: resolvedProjectNo,
                taskSystemId: queueTaskSystemId,
            });
            queueEntriesByProject.set(resolvedProjectNo, refs);
        }

        if (!queueTaskSystemId) {
            fullSyncProjects.add(resolvedProjectNo);
            continue;
        }
        if (fullSyncProjects.has(resolvedProjectNo)) continue;

        const set = taskSystemIdsByProject.get(resolvedProjectNo) || new Set<string>();
        set.add(queueTaskSystemId);
        taskSystemIdsByProject.set(resolvedProjectNo, set);
    }

    return {
        queueEntries,
        queueRows: queueEntries.length,
        projectNos: Array.from(projectNos),
        taskSystemIdsByProject,
        queueEntriesByProject,
        fullSyncProjects,
        latestChangedMs,
    };
}

async function markPremiumTaskWrite(taskId: string, meta: Record<string, unknown> = {}) {
    if (!taskId) return;
    try {
        await markPremiumTaskIdsFromBc([taskId]);
    } catch (error) {
        logger.warn("Failed to mark premium task write", { taskId, ...meta, error: (error as Error)?.message });
    }
}

async function markBcTaskWrite(systemId: string, meta: Record<string, unknown> = {}) {
    const normalizedSystemId = canonicalTaskSystemId(systemId);
    if (!normalizedSystemId) return;
    try {
        await markBcTaskSystemIdsFromPremium([normalizedSystemId]);
    } catch (error) {
        logger.warn("Failed to mark BC task write", {
            systemId: normalizedSystemId,
            ...meta,
            error: (error as Error)?.message,
        });
    }
}

export async function syncBcToPremium(
    projectNo?: string,
    options: {
        requestId?: string;
        projectNos?: string[];
        taskSystemIds?: string[];
        queueEntries?: BcQueueEntryRef[];
        skipProjectAccess?: boolean;
        taskOnly?: boolean;
        preferPlanner?: boolean;
        forceProjectCreate?: boolean;
        forceTaskRecreate?: boolean;
        disableProjectConcurrency?: boolean;
    } = {}
) {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
    const allowedTaskNumbers = buildAllowedTaskNumberSet(syncConfig);
    const mapping = getDataverseMappingConfig();
    const requireScheduleApi = Boolean(syncConfig.requireScheduleApi && isScheduleManagedTaskEntity(mapping));
    const bcClient = new BusinessCentralClient();
    const dataverse = new DataverseClient();
    const plannerGroupId = syncConfig.plannerGroupId;
    const plannerGroupResourceIds = Array.from(
        new Set((syncConfig.plannerGroupResourceIds || []).map((value) => value.trim()).filter(Boolean))
    );
    const plannerOwnerTeamId = (syncConfig.plannerOwnerTeamId || "").trim();
    const plannerOwnerTeamAadGroupId = (syncConfig.plannerOwnerTeamAadGroupId || "").trim();

    const settings = await listProjectSyncSettings();
    const disabled = buildDisabledProjectSet(settings);

    let projectNos: string[] = [];
    if (projectNo) projectNos = [projectNo];
    if (options.projectNos && options.projectNos.length) projectNos = options.projectNos;
    const taskSystemIdSet = options.taskSystemIds?.length
        ? new Set(options.taskSystemIds.map((value) => canonicalTaskSystemId(value)).filter(Boolean))
        : null;
    let taskSystemIdsByProject: Map<string, Set<string>> | null = null;
    let queueEntriesByProject: Map<string, BcQueueEntryRef[]> | null = null;
    let fullSyncProjects: Set<string> | null = null;

    if (!projectNos.length && !taskSystemIdSet) {
        try {
            const queueTargets = await resolveBcQueueTargets(bcClient, { requestId });
            projectNos = queueTargets.projectNos;
            taskSystemIdsByProject = queueTargets.taskSystemIdsByProject;
            queueEntriesByProject = queueTargets.queueEntriesByProject;
            fullSyncProjects = queueTargets.fullSyncProjects;
        } catch (error) {
            logger.warn("BC queue load failed", {
                requestId,
                entitySet: BC_QUEUE_ENTITY_SET,
                error: (error as Error)?.message,
            });
            return {
                projects: 0,
                tasks: 0,
                created: 0,
                updated: 0,
                skipped: 0,
                errors: 1,
                projectNos: [] as string[],
            };
        }
    }

    if (
        !options.disableProjectConcurrency &&
        syncConfig.projectConcurrency > 1 &&
        projectNos.length > 1
    ) {
        const runs = await runWithConcurrency(projectNos, syncConfig.projectConcurrency, async (projNo) => {
            const projectTaskIds = taskSystemIdSet
                ? Array.from(taskSystemIdSet)
                : taskSystemIdsByProject?.get(projNo)
                  ? Array.from(taskSystemIdsByProject.get(projNo) || [])
                  : undefined;
            const forceFullSync = !taskSystemIdSet && !!fullSyncProjects?.has(projNo);
            return syncBcToPremium(projNo, {
                requestId,
                taskSystemIds: forceFullSync ? undefined : projectTaskIds,
                queueEntries: queueEntriesByProject?.get(projNo) || [],
                skipProjectAccess: options.skipProjectAccess,
                taskOnly: options.taskOnly,
                preferPlanner: options.preferPlanner,
                forceProjectCreate: options.forceProjectCreate,
                forceTaskRecreate: options.forceTaskRecreate,
                disableProjectConcurrency: true,
            });
        });
        const mergedProjectNos = new Set<string>();
        let projects = 0;
        let tasks = 0;
        let created = 0;
        let updated = 0;
        let skipped = 0;
        let errors = 0;
        for (const run of runs) {
            projects += run.projects || 0;
            tasks += run.tasks || 0;
            created += run.created || 0;
            updated += run.updated || 0;
            skipped += run.skipped || 0;
            errors += run.errors || 0;
            for (const proj of run.projectNos || []) {
                if (proj) mergedProjectNos.add(proj);
            }
        }
        return {
            projects,
            tasks,
            created,
            updated,
            skipped,
            errors,
            projectNos: Array.from(mergedProjectNos),
        };
    }

    const result = {
        projects: 0,
        tasks: 0,
        created: 0,
        updated: 0,
        skipped: 0,
        errors: 0,
        projectNos: [] as string[],
    };

    for (const projNoRaw of projectNos) {
        const projNo = (projNoRaw || "").trim();
        if (!projNo) continue;
        if (disabled.has(normalizeProjectKey(projNo))) {
            logger.info("Premium sync skipped for disabled project", { requestId, projectNo: projNo });
            continue;
        }
        const scopedQueueEntries =
            options.queueEntries ||
            (queueEntriesByProject && queueEntriesByProject.get(projNo) ? queueEntriesByProject.get(projNo) || [] : []);
        const successfulTaskSystemIds = new Set<string>();
        const projectErrorsStart = result.errors;
        const projectTasksStart = result.tasks;
        const projectActivityStart = result.created + result.updated + result.skipped;
        const projectCreatedStart = result.created;
        const projectUpdatedStart = result.updated;
        const projectSkippedStart = result.skipped;

        let tasks: BcProjectTask[] = [];
        let scopedTaskSystemIdSet =
            taskSystemIdSet ||
            (taskSystemIdsByProject && !fullSyncProjects?.has(projNo)
                ? taskSystemIdsByProject.get(projNo) || null
                : null);
        try {
            if (scopedTaskSystemIdSet) {
                const resolved: BcProjectTask[] = [];
                for (const systemId of scopedTaskSystemIdSet) {
                    if (!systemId) continue;
                    try {
                        const task = await bcClient.getProjectTask(systemId);
                        if (!task) continue;
                        if ((task.projectNo || "").trim() !== projNo) continue;
                        resolved.push(task);
                    } catch (error) {
                        const message = (error as Error)?.message || "";
                        if (message.includes("-> 404")) continue;
                        throw error;
                    }
                }
                tasks = resolved;
            } else {
                const filter = `projectNo eq '${escapeODataString(projNo)}'`;
                tasks = await bcClient.listProjectTasks(filter);
            }
        } catch (error) {
            logger.warn("BC task load failed", { requestId, projectNo: projNo, error: (error as Error)?.message });
            result.errors += 1;
            continue;
        }

        if (!tasks.length && scopedQueueEntries.length) {
            const reloaded = await reloadProjectTasksAfterPropagationDelay(bcClient, projNo, {
                requestId,
                attempts: 3,
                baseDelayMs: 800,
                minTaskCount: 1,
                reason: "queue_project_sync",
            });
            if (reloaded.tasks.length) {
                tasks = reloaded.tasks;
                // Queue events can arrive before task rows are queryable. Once rows appear, seed the whole project.
                scopedTaskSystemIdSet = null;
                logger.info("Queue-triggered sync expanded after BC task propagation delay", {
                    requestId,
                    projectNo: projNo,
                    attempt: reloaded.attempt,
                    loadedTasks: reloaded.tasks.length,
                });
            }
        }

        if (!tasks.length && !scopedTaskSystemIdSet && !scopedQueueEntries.length && BC_PROJECT_BOOTSTRAP_RETRY_ATTEMPTS > 0) {
            const reloaded = await reloadProjectTasksAfterPropagationDelay(bcClient, projNo, {
                requestId,
                attempts: BC_PROJECT_BOOTSTRAP_RETRY_ATTEMPTS,
                baseDelayMs: BC_PROJECT_BOOTSTRAP_RETRY_DELAY_MS,
                minTaskCount: 1,
                reason: "project_bootstrap_sync",
            });
            if (reloaded.tasks.length) {
                tasks = reloaded.tasks;
                logger.info("Project bootstrap sync expanded after BC task propagation delay", {
                    requestId,
                    projectNo: projNo,
                    attempt: reloaded.attempt,
                    loadedTasks: reloaded.tasks.length,
                });
            }
        }

        let sorted = tasks.length ? sortTasksByTaskNo(tasks) : [];
        const currentSection = { name: null as string | null };

        let cachedProjectId = tasks.find((task) => (task.plannerPlanId || "").trim())?.plannerPlanId?.trim() || "";
        if (cachedProjectId && !isGuid(cachedProjectId)) {
            warnNonGuidPlannerPlanId(projNo, cachedProjectId);
            cachedProjectId = "";
        }
        if (cachedProjectId) {
            try {
                await dataverse.getById(mapping.projectEntitySet, cachedProjectId, [mapping.projectIdField]);
            } catch (error) {
                logger.debug("Cached Dataverse project lookup failed", {
                    requestId,
                    projectNo: projNo,
                    projectId: cachedProjectId,
                    error: (error as Error)?.message,
                });
                cachedProjectId = "";
            }
        }

        let resolvedProjectId = "";
        const shouldResolveProjectFromBc = Boolean(mapping.projectBcNoField) || !cachedProjectId || Boolean(options.forceProjectCreate);
        if (shouldResolveProjectFromBc) {
            try {
                const projectEntity = await resolveProjectFromBc(bcClient, dataverse, projNo, mapping, {
                    forceCreate: options.forceProjectCreate,
                    requestId,
                    skipProjectAccess: options.skipProjectAccess,
                    plannerGroupId,
                    plannerGroupResourceIds,
                    plannerOwnerTeamId,
                    plannerOwnerTeamAadGroupId,
                });
                resolvedProjectId = resolveProjectId(projectEntity, mapping) || "";
            } catch (error) {
                logger.warn("Dataverse project resolve by BC projectNo failed", {
                    requestId,
                    projectNo: projNo,
                    error: (error as Error)?.message,
                });
            }
        }

        let projectId = resolvedProjectId || cachedProjectId;
        if (resolvedProjectId && cachedProjectId && resolvedProjectId.toLowerCase() !== cachedProjectId.toLowerCase()) {
            logger.warn("BC tasks reference stale plannerPlanId; using canonical Dataverse project for sync", {
                requestId,
                projectNo: projNo,
                cachedProjectId,
                resolvedProjectId,
            });
        }
        if (!projectId) {
            logger.warn("Dataverse project not found", { requestId, projectNo: projNo });
            result.errors += 1;
            continue;
        }

        if (scopedTaskSystemIdSet?.size) {
            try {
                const filter = `projectNo eq '${escapeODataString(projNo)}'`;
                const projectTasks = await bcClient.listProjectTasks(filter);
                const projectSorted = projectTasks.length ? sortTasksByTaskNo(projectTasks) : [];
                const bootstrapSection = { name: null as string | null };
                const hasUnlinkedSyncableTasks = projectSorted.some((task) => {
                    const skipForSection = shouldSkipTaskForSection(task, bootstrapSection);
                    if (!options.taskOnly && skipForSection) return false;
                    if (!isAllowedSyncTaskNo(task.taskNo, allowedTaskNumbers)) return false;
                    const plannerTaskId = (task.plannerTaskId || "").trim();
                    const plannerPlanId = (task.plannerPlanId || "").trim();
                    if (!plannerTaskId || !plannerPlanId) return true;
                    if (!isGuid(plannerTaskId)) return true;
                    if (!isGuid(plannerPlanId)) return true;
                    if (plannerPlanId.toLowerCase() !== projectId.toLowerCase()) return true;
                    return false;
                });
                if (hasUnlinkedSyncableTasks) {
                    tasks = projectSorted;
                    sorted = projectSorted;
                    scopedTaskSystemIdSet = null;
                    logger.info("Switching to full project task sync for unlinked project bootstrap", {
                        requestId,
                        projectNo: projNo,
                        projectId,
                        loadedTasks: projectSorted.length,
                    });

                    if (BC_PROJECT_BOOTSTRAP_RETRY_ATTEMPTS > 0) {
                        const expanded = await reloadProjectTasksAfterPropagationDelay(bcClient, projNo, {
                            requestId,
                            attempts: BC_PROJECT_BOOTSTRAP_RETRY_ATTEMPTS,
                            baseDelayMs: BC_PROJECT_BOOTSTRAP_RETRY_DELAY_MS,
                            minTaskCount: projectSorted.length + 1,
                            reason: "bootstrap_full_project_expand",
                        });
                        if (expanded.tasks.length > projectSorted.length) {
                            const expandedSorted = sortTasksByTaskNo(expanded.tasks);
                            tasks = expandedSorted;
                            sorted = expandedSorted;
                            logger.info("Expanded full project bootstrap sync after BC task propagation delay", {
                                requestId,
                                projectNo: projNo,
                                projectId,
                                attempt: expanded.attempt,
                                loadedTasks: expanded.tasks.length,
                                initialTasks: projectSorted.length,
                            });
                        }
                    }
                }
            } catch (error) {
                logger.warn("Failed to evaluate bootstrap full-project sync eligibility", {
                    requestId,
                    projectNo: projNo,
                    error: (error as Error)?.message,
                });
            }
        }

        const shouldLoadTaskIndex = !scopedTaskSystemIdSet;
        const taskIndex = shouldLoadTaskIndex
            ? await loadProjectTaskIndex(dataverse, projectId, mapping)
            : createEmptyTaskIndex();

        const pendingUpdates: Array<{ task: BcProjectTask; updates: Record<string, unknown> }> = [];
        const bucketCache = new Map<string, string | null>();
        const resourceCache = new Map<string, string | null>();
        const resourceNameCache = new Map<string, { id: string; name: string } | null>();
        const teamCache = new Map<string, string | null>();
        const assignmentCache = new Set<string>();
        const scheduleTasks: BcProjectTask[] = [];
        const scheduleCounts = { created: 0, updated: 0, skipped: 0 };
        const scheduleFallbackState: ScheduleFallbackState = { unavailable: false };
        let useScheduleApi = syncConfig.useScheduleApi;
        let operationSetId = "";
        let operationSetTouched = false;
        if (useScheduleApi) {
            const bucketId = await getProjectBucketId(dataverse, projectId, bucketCache);
            if (!bucketId) {
                logger.warn("Dataverse bucket unavailable; continuing with schedule API updates (task creates may fail)", {
                    requestId,
                    projectNo: projNo,
                    projectId,
                });
            }
        }
        if (useScheduleApi) {
            try {
                operationSetId = await createOperationSetWithRecovery({
                    dataverse,
                    projectId,
                    projectNo: projNo,
                    requestId,
                    description: `BC sync ${projNo} ${new Date().toISOString()}`,
                });
                if (!operationSetId) {
                    useScheduleApi = false;
                    logger.warn("Dataverse schedule API unavailable; using per-task schedule fallback", {
                        requestId,
                        projectNo: projNo,
                    });
                }
            } catch (error) {
                useScheduleApi = false;
                if (isOperationSetLimitError(error)) {
                    scheduleFallbackState.unavailable = true;
                    scheduleFallbackState.reason = "operation_set_capacity";
                }
                logger.warn("Dataverse schedule API init failed; using per-task schedule fallback", {
                    requestId,
                    projectNo: projNo,
                    error: (error as Error)?.message,
                });
            }
        }

        try {
            if (!options.skipProjectAccess) {
                await ensurePremiumProjectTeamAccess(dataverse, projectId, {
                    requestId,
                    projectNo: projNo,
                    plannerGroupId,
                    plannerGroupResourceIds,
                    plannerOwnerTeamId,
                    plannerOwnerTeamAadGroupId,
                    teamCache,
                    resourceCache,
                    resourceNameCache,
                });
            }

        if (!sorted.length) {
            const hasProjectQueueEntry = scopedQueueEntries.some(
                (entry) => !canonicalTaskSystemId(String(entry.taskSystemId || ""))
            );
            logger.warn("BC -> Premium found no BC tasks for queued project sync", {
                requestId,
                projectNo: projNo,
                queueEntries: scopedQueueEntries.length,
                hasProjectQueueEntry,
            });
            if (hasProjectQueueEntry) {
                // Keep project-level queue entry for retry when BC task rows become queryable.
                result.errors += 1;
                logger.warn("Retaining project-level queue entry; no BC tasks were loaded yet", {
                    requestId,
                    projectNo: projNo,
                });
            }
            result.projects += 1;
            result.projectNos.push(projNo);
            continue;
        }

        const toSync: BcProjectTask[] = [];
        let skippedByTarget = 0;
        let skippedBySection = 0;
        let skippedByAllowlist = 0;
        let skippedByPremiumWriteback = 0;
        for (const task of sorted) {
            const skipForSection = shouldSkipTaskForSection(task, currentSection);
            const systemId = typeof task.systemId === "string" ? canonicalTaskSystemId(task.systemId) : "";
            const isTarget = !scopedTaskSystemIdSet || (systemId && scopedTaskSystemIdSet.has(systemId));
            if (!isTarget) {
                skippedByTarget += 1;
                continue;
            }
            result.tasks += 1;
            if (!options.taskOnly && skipForSection) {
                skippedBySection += 1;
                result.skipped += 1;
                continue;
            }
            if (!isAllowedSyncTaskNo(task.taskNo, allowedTaskNumbers)) {
                skippedByAllowlist += 1;
                result.skipped += 1;
                continue;
            }
            if (systemId && (await wasBcTaskSystemIdUpdatedByPremium(systemId))) {
                skippedByPremiumWriteback += 1;
                result.skipped += 1;
                successfulTaskSystemIds.add(systemId);
                if (requestId) {
                    logger.info("BC -> Premium skipped (premium-origin writeback window)", {
                        requestId,
                        projectNo: task.projectNo,
                        taskNo: task.taskNo,
                        systemId,
                    });
                }
                continue;
            }
            toSync.push(task);
        }

        if (requestId) {
            logger.info("BC -> Premium task selection summary", {
                requestId,
                projectNo: projNo,
                totalProjectTasks: sorted.length,
                scopedTaskCount: scopedTaskSystemIdSet?.size || 0,
                candidates: toSync.length,
                skippedByTarget,
                skippedBySection,
                skippedByAllowlist,
                skippedByPremiumWriteback,
                forceTaskRecreate: Boolean(options.forceTaskRecreate),
            });
        }

        if (!toSync.length) {
            logger.warn("BC -> Premium found no syncable tasks after filters", {
                requestId,
                projectNo: projNo,
                totalTasks: sorted.length,
                allowlistSize: allowedTaskNumbers.size,
                scopedTaskCount: scopedTaskSystemIdSet?.size || 0,
            });
        }

        let assignmentSnapshotLoaded = false;
        let assignmentLookups: { project: string; task: string; team: string } | undefined;
        if (!options.taskOnly && toSync.length) {
            const preload = await preloadProjectAssignments(dataverse, projectId, assignmentCache);
            assignmentSnapshotLoaded = preload.loaded;
            assignmentLookups = preload.lookups;
        }

        const usingOperationSet = Boolean(useScheduleApi && operationSetId);
        const syncTaskBatch = async (
            batch: BcProjectTask[],
            concurrency: number,
            overrides: {
                useScheduleApi?: boolean;
                operationSetId?: string;
                trackInOperationSet?: boolean;
            } = {}
        ) =>
            runWithConcurrency(batch, concurrency, async (task) => {
                const batchUseScheduleApi =
                    overrides.useScheduleApi !== undefined ? overrides.useScheduleApi : useScheduleApi;
                const batchOperationSetId =
                    overrides.operationSetId !== undefined
                        ? overrides.operationSetId
                        : batchUseScheduleApi
                          ? operationSetId
                          : undefined;
                const trackInOperationSet =
                    overrides.trackInOperationSet !== undefined
                        ? overrides.trackInOperationSet
                        : Boolean(batchUseScheduleApi && batchOperationSetId);
                const cleanTask = await clearStaleSyncLockIfNeeded(bcClient, task, syncConfig.syncLockTimeoutMinutes);
                if (cleanTask.syncLock) {
                    return { action: "skipped" as const, task: cleanTask, batchedInOperationSet: false };
                }
                if (trackInOperationSet) {
                    scheduleTasks.push(cleanTask);
                }
                try {
                    const res = await syncTaskToDataverse(bcClient, dataverse, cleanTask, projectId, mapping, {
                        requestId,
                        useScheduleApi: batchUseScheduleApi,
                        operationSetId: batchOperationSetId,
                        bucketCache,
                        resourceCache,
                        teamCache,
                        assignmentCache,
                        assignmentSnapshotLoaded,
                        assignmentLookups,
                        taskOnly: options.taskOnly,
                        touchOperationSet: () => {
                            operationSetTouched = true;
                        },
                        preferPlanner: options.preferPlanner ?? !syncConfig.preferBc,
                        plannerModifiedGraceMs: syncConfig.premiumModifiedGraceMs,
                        taskIndex,
                        scheduleFallbackState,
                        requireScheduleApi,
                        forceTaskRecreate: options.forceTaskRecreate,
                    });
                    if (res.pendingUpdate) {
                        pendingUpdates.push(res.pendingUpdate);
                    }
                    return {
                        action: res.action as "created" | "updated" | "skipped" | "error",
                        task: cleanTask,
                        batchedInOperationSet: trackInOperationSet,
                    };
                } catch (error) {
                    logger.warn("Premium task sync failed", {
                        requestId,
                        projectNo: projNo,
                        taskNo: cleanTask.taskNo,
                        error: (error as Error)?.message,
                    });
                    return { action: "error" as const, task: cleanTask, batchedInOperationSet: false };
                }
            });

        const preserveTaskOrder = parseBoolEnv(process.env.DATAVERSE_PRESERVE_TASK_ORDER, true);

        let outcomes: Array<{
            action: "created" | "updated" | "skipped" | "error";
            task: BcProjectTask;
            batchedInOperationSet: boolean;
        }> = [];
        if (usingOperationSet) {
            if (preserveTaskOrder) {
                outcomes = await syncTaskBatch(toSync, 1, {
                    // Run one task at a time so Premium task order matches BC order.
                    useScheduleApi: false,
                    operationSetId: undefined,
                    trackInOperationSet: false,
                });
            } else {
            // Keep create operations ordered while allowing updates to parallelize.
            const likelyCreates: BcProjectTask[] = [];
            const likelyUpdates: BcProjectTask[] = [];
            if (options.forceTaskRecreate) {
                likelyCreates.push(...toSync);
            } else {
                for (const task of toSync) {
                    if (hasLikelyExistingDataverseTask(task, taskIndex, options.taskOnly)) {
                        likelyUpdates.push(task);
                    } else {
                        likelyCreates.push(task);
                    }
                }
            }
            const createOutcomes = await syncTaskBatch(likelyCreates, 1, {
                useScheduleApi,
                operationSetId,
                trackInOperationSet: true,
            });
            const updateOutcomes = await syncTaskBatch(likelyUpdates, Math.max(1, syncConfig.taskConcurrency), {
                useScheduleApi,
                operationSetId,
                trackInOperationSet: true,
            });
            outcomes = [...createOutcomes, ...updateOutcomes];
            }
        } else {
            outcomes = await syncTaskBatch(toSync, preserveTaskOrder ? 1 : syncConfig.taskConcurrency);
        }

        for (const outcome of outcomes) {
            if (outcome.action === "error") {
                result.errors += 1;
                continue;
            }
            const outcomeSystemId = canonicalTaskSystemId(String(outcome.task?.systemId || ""));
            if ((outcome.action === "created" || outcome.action === "updated") && outcomeSystemId) {
                successfulTaskSystemIds.add(outcomeSystemId);
            }
            if (usingOperationSet && outcome.batchedInOperationSet) {
                if (outcome.action === "created") scheduleCounts.created += 1;
                else if (outcome.action === "updated") scheduleCounts.updated += 1;
                else scheduleCounts.skipped += 1;
            } else {
                if (outcome.action === "created") result.created += 1;
                else if (outcome.action === "updated") result.updated += 1;
                else result.skipped += 1;
            }
        }

            if (useScheduleApi && operationSetId && (pendingUpdates.length || operationSetTouched)) {
                try {
                    await dataverse.executeOperationSet(operationSetId);
                    if (pendingUpdates.length) {
                        await Promise.all(
                            pendingUpdates.map((entry) => updateBcTaskWithSyncLock(bcClient, entry.task, entry.updates))
                        );
                    }
                    result.created += scheduleCounts.created;
                    result.updated += scheduleCounts.updated;
                    result.skipped += scheduleCounts.skipped;
                } catch (error) {
                    logger.warn("Dataverse schedule execute failed", {
                        requestId,
                        projectNo: projNo,
                        error: (error as Error)?.message,
                    });
                    if (isInvalidDefaultBucketError(error)) {
                        bucketCache.delete(projectId);
                        const recoveredBucket = await getProjectBucketId(dataverse, projectId, bucketCache);
                        logger.warn("Dataverse default bucket invalid; refreshed bucket before retry", {
                            requestId,
                            projectNo: projNo,
                            projectId,
                            recovered: Boolean(recoveredBucket),
                        });
                    }
                    if (scheduleTasks.length) {
                        logger.warn(
                            requireScheduleApi
                                ? "Retrying tasks via per-task schedule operation sets"
                                : "Retrying tasks via direct Dataverse API",
                            {
                                requestId,
                                projectNo: projNo,
                                count: scheduleTasks.length,
                            }
                        );
                        for (const task of scheduleTasks) {
                            try {
                                const res = await syncTaskToDataverse(bcClient, dataverse, task, projectId, mapping, {
                                    requestId,
                                    useScheduleApi: false,
                                    operationSetId: undefined,
                                    bucketCache,
                                    resourceCache,
                                    teamCache,
                                    assignmentCache,
                                    assignmentSnapshotLoaded,
                                    assignmentLookups,
                                    taskOnly: options.taskOnly,
                                    touchOperationSet: undefined,
                                    preferPlanner: options.preferPlanner ?? !syncConfig.preferBc,
                                    plannerModifiedGraceMs: syncConfig.premiumModifiedGraceMs,
                                    taskIndex,
                                    scheduleFallbackState,
                                    requireScheduleApi,
                                    forceTaskRecreate: options.forceTaskRecreate,
                                });
                                if (res.action === "created") {
                                    result.created += 1;
                                    const taskId = canonicalTaskSystemId(String(task.systemId || ""));
                                    if (taskId) successfulTaskSystemIds.add(taskId);
                                } else if (res.action === "updated") {
                                    result.updated += 1;
                                    const taskId = canonicalTaskSystemId(String(task.systemId || ""));
                                    if (taskId) successfulTaskSystemIds.add(taskId);
                                } else {
                                    result.skipped += 1;
                                }
                            } catch (retryError) {
                                result.errors += 1;
                                logger.warn("Premium task retry failed", {
                                    requestId,
                                    projectNo: projNo,
                                    taskNo: task.taskNo,
                                    error: (retryError as Error)?.message,
                                });
                            }
                        }
                    }
                }
            }
            if (shouldDeleteQueueEntryAfterSync() && scopedQueueEntries.length && BC_QUEUE_ENTITY_SET) {
                const seenEntryIds = new Set<string>();
                const projectHadErrors = result.errors > projectErrorsStart;
                const projectHadTaskReads = result.tasks > projectTasksStart;
                const projectHadSyncActivity = result.created + result.updated + result.skipped > projectActivityStart;
                const usedFullProjectScope = !scopedTaskSystemIdSet;
                for (const queueEntry of scopedQueueEntries) {
                    const entryId = normalizeTaskSystemId(queueEntry.queueEntryId);
                    if (!entryId || seenEntryIds.has(entryId)) continue;
                    seenEntryIds.add(entryId);
                    const taskId = canonicalTaskSystemId(queueEntry.taskSystemId);
                    const canDelete = taskId
                        ? successfulTaskSystemIds.has(taskId) ||
                          (usedFullProjectScope && !projectHadErrors && (projectHadTaskReads || projectHadSyncActivity))
                        : !projectHadErrors && (projectHadTaskReads || projectHadSyncActivity);
                    if (!canDelete) continue;
                    try {
                        await bcClient.deleteEntity(BC_QUEUE_ENTITY_SET, entryId);
                    } catch (error) {
                        logger.warn("BC queue entry delete failed", {
                            requestId,
                            projectNo: projNo,
                            queueEntryId: entryId,
                            taskSystemId: taskId || null,
                            error: (error as Error)?.message,
                        });
                    }
                }
            }

            result.projects += 1;
            result.projectNos.push(projNo);
            if (requestId) {
                logger.info("BC -> Premium project sync summary", {
                    requestId,
                    projectNo: projNo,
                    projectId,
                    tasksExamined: result.tasks - projectTasksStart,
                    created: result.created - projectCreatedStart,
                    updated: result.updated - projectUpdatedStart,
                    skipped: result.skipped - projectSkippedStart,
                    errors: result.errors - projectErrorsStart,
                    usedScheduleApi: useScheduleApi,
                    operationSetId: operationSetId || null,
                    forceTaskRecreate: Boolean(options.forceTaskRecreate),
                });
            }
        } finally {
            if (operationSetId && !operationSetTouched && !pendingUpdates.length) {
                await cleanupUnusedOperationSet(dataverse, operationSetId, { projectId, projectNo: projNo });
            }
            await cleanupOperationSet(dataverse, operationSetId, { projectId, projectNo: projNo });
        }
    }

    return result;
}

type PrefetchedBcTaskIndex = {
    byPlannerTaskId: Map<string, BcProjectTask>;
    byProjectTaskNo: Map<string, BcProjectTask>;
};

function buildProjectTaskNoKey(projectNo: string, taskNo: string) {
    return `${projectNo.trim().toLowerCase()}::${taskNo.trim()}`;
}

function addBcTaskToPrefetchIndex(index: PrefetchedBcTaskIndex, task: BcProjectTask) {
    const plannerTaskId = (task.plannerTaskId || "").trim();
    if (plannerTaskId) {
        index.byPlannerTaskId.set(plannerTaskId, task);
    }
    const projectNo = (task.projectNo || "").trim();
    const taskNo = (task.taskNo || "").trim();
    if (projectNo && taskNo) {
        index.byProjectTaskNo.set(buildProjectTaskNoKey(projectNo, taskNo), task);
    }
}

async function resolveDataverseProjectNo(
    dataverse: DataverseClient,
    dataverseTask: DataverseEntity,
    mapping: ReturnType<typeof getDataverseMappingConfig>,
    projectCache: Map<string, string | null>
) {
    if (!mapping.projectBcNoField) return null;

    const directProjectNo = dataverseTask[mapping.projectBcNoField];
    if (typeof directProjectNo === "string" && directProjectNo.trim()) {
        return directProjectNo.trim();
    }

    const projectIdRaw = dataverseTask[mapping.taskProjectIdField];
    const projectId = typeof projectIdRaw === "string" ? projectIdRaw.trim() : "";
    if (!projectId) return null;

    if (projectCache.has(projectId)) {
        return projectCache.get(projectId) || null;
    }

    try {
        const project = await dataverse.getById<DataverseEntity>(mapping.projectEntitySet, projectId, [
            mapping.projectBcNoField,
        ]);
        const projectNo = project?.[mapping.projectBcNoField];
        const resolved = typeof projectNo === "string" && projectNo.trim() ? projectNo.trim() : null;
        projectCache.set(projectId, resolved);
        return resolved;
    } catch (error) {
        logger.warn("Dataverse project lookup failed", {
            projectId,
            error: (error as Error)?.message,
        });
        projectCache.set(projectId, null);
        return null;
    }
}

async function preloadBcTasksForDataverseChanges(
    bcClient: BusinessCentralClient,
    dataverse: DataverseClient,
    items: DataverseEntity[],
    mapping: ReturnType<typeof getDataverseMappingConfig>,
    projectCache: Map<string, string | null>,
    concurrency: number
): Promise<PrefetchedBcTaskIndex> {
    const index: PrefetchedBcTaskIndex = {
        byPlannerTaskId: new Map<string, BcProjectTask>(),
        byProjectTaskNo: new Map<string, BcProjectTask>(),
    };

    const projectNos = new Set<string>();
    const pendingProjectItems: DataverseEntity[] = [];
    for (const item of items) {
        const removed = item["@removed"] as { reason?: string } | undefined;
        if (removed) continue;
        const directProjectNo =
            mapping.projectBcNoField && typeof item[mapping.projectBcNoField] === "string"
                ? String(item[mapping.projectBcNoField]).trim()
                : "";
        if (directProjectNo) {
            projectNos.add(directProjectNo);
            continue;
        }
        pendingProjectItems.push(item);
    }

    if (pendingProjectItems.length) {
        await runWithConcurrency(pendingProjectItems, Math.max(1, concurrency), async (item) => {
            const projectNo = await resolveDataverseProjectNo(dataverse, item, mapping, projectCache);
            if (projectNo) {
                projectNos.add(projectNo);
            }
        });
    }

    const projectList = Array.from(projectNos);
    if (!projectList.length) return index;

    await runWithConcurrency(projectList, Math.max(1, concurrency), async (projectNo) => {
        try {
            const filter = `projectNo eq '${escapeODataString(projectNo)}'`;
            const tasks = await bcClient.listProjectTasks(filter);
            for (const task of tasks || []) {
                addBcTaskToPrefetchIndex(index, task);
            }
        } catch (error) {
            logger.warn("BC task preload failed", {
                projectNo,
                error: (error as Error)?.message,
            });
        }
    });

    return index;
}

async function resolveBcTaskFromDataverse(
    bcClient: BusinessCentralClient,
    dataverse: DataverseClient,
    dataverseTask: DataverseEntity,
    mapping: ReturnType<typeof getDataverseMappingConfig>,
    projectCache: Map<string, string | null>
) {
    const taskId = dataverseTask[mapping.taskIdField];
    if (typeof taskId === "string" && taskId.trim()) {
        const byPlannerId = await bcClient.findProjectTaskByPlannerTaskId(taskId.trim());
        if (byPlannerId) return byPlannerId;
    }

    const taskNo = mapping.taskBcNoField ? dataverseTask[mapping.taskBcNoField] : null;
    const taskNoValue = typeof taskNo === "string" && taskNo.trim() ? taskNo.trim() : null;
    const projectNoValue = await resolveDataverseProjectNo(dataverse, dataverseTask, mapping, projectCache);

    if (taskNoValue && projectNoValue) {
        return bcClient.findProjectTaskByProjectAndTaskNo(projectNoValue, taskNoValue);
    }

    return null;
}

export async function syncPremiumChanges(options: { requestId?: string; deltaLink?: string | null } = {}) {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
    const allowedTaskNumbers = buildAllowedTaskNumberSet(syncConfig);
    const mapping = getDataverseMappingConfig();
    const bcClient = new BusinessCentralClient();
    const dataverse = new DataverseClient();

    const deltaLink = options.deltaLink || (await getDataverseDeltaLink(mapping.taskEntitySet));

    const selectFields = [
        mapping.taskIdField,
        mapping.taskTitleField,
        mapping.taskPercentField,
        mapping.taskStartField,
        mapping.taskFinishField,
        mapping.taskBcNoField,
        mapping.taskProjectIdField,
        mapping.taskModifiedField,
    ].filter(Boolean) as string[];

    const { value, deltaLink: newDelta } = await dataverse.listChanges<DataverseEntity>(mapping.taskEntitySet, {
        select: selectFields,
        deltaLink,
        top: syncConfig.pollPageSize,
        maxPages: syncConfig.pollMaxPages,
    });

    const summary = {
        changed: value.length,
        updated: 0,
        skipped: 0,
        cleared: 0,
        deleted: 0,
        errors: 0,
        deltaLinkSaved: false,
    };

    const projectCache = new Map<string, string | null>();
    const premiumToBcConcurrency = Math.max(
        1,
        Math.floor(Number(process.env.SYNC_PREMIUM_TO_BC_CONCURRENCY || syncConfig.taskConcurrency || 1))
    );
    const prefetchedTasks = await preloadBcTasksForDataverseChanges(
        bcClient,
        dataverse,
        value,
        mapping,
        projectCache,
        premiumToBcConcurrency
    );

    const outcomes = await runWithConcurrency(value, premiumToBcConcurrency, async (item) => {
        const outcome = { updated: 0, skipped: 0, cleared: 0, errors: 0 };
        const removed = item["@removed"] as { reason?: string } | undefined;
        if (removed) {
            if (syncConfig.deleteBehavior === "ignore") {
                outcome.skipped += 1;
                return outcome;
            }
            const taskId = item[mapping.taskIdField];
            if (typeof taskId === "string" && taskId.trim()) {
                const trimmedTaskId = taskId.trim();
                const bcTask =
                    prefetchedTasks.byPlannerTaskId.get(trimmedTaskId) ||
                    (await bcClient.findProjectTaskByPlannerTaskId(trimmedTaskId));
                if (bcTask && bcTask.systemId) {
                    if (!isAllowedSyncTaskNo(bcTask.taskNo, allowedTaskNumbers)) {
                        outcome.skipped += 1;
                        return outcome;
                    }
                    try {
                        await updateBcTaskWithSyncLock(bcClient, bcTask, {
                            plannerTaskId: "",
                            plannerPlanId: "",
                            plannerBucket: "",
                            lastPlannerEtag: "",
                            lastSyncAt: new Date().toISOString(),
                        });
                        await markBcTaskWrite(bcTask.systemId, {
                            requestId,
                            taskId: trimmedTaskId,
                            action: "clear_link",
                        });
                        outcome.cleared += 1;
                    } catch (error) {
                        outcome.errors += 1;
                        logger.warn("Failed clearing BC link for deleted premium task", {
                            requestId,
                            taskId: trimmedTaskId,
                            error: (error as Error)?.message,
                        });
                    }
                } else {
                    outcome.skipped += 1;
                }
            } else {
                outcome.skipped += 1;
            }
            return outcome;
        }

        let bcTask: BcProjectTask | null = null;
        const taskId = item[mapping.taskIdField];
        if (typeof taskId === "string" && taskId.trim()) {
            bcTask = prefetchedTasks.byPlannerTaskId.get(taskId.trim()) || null;
        }
        if (!bcTask && mapping.taskBcNoField) {
            const taskNoRaw = item[mapping.taskBcNoField];
            const taskNo = typeof taskNoRaw === "string" ? taskNoRaw.trim() : "";
            if (taskNo) {
                const projectNo = await resolveDataverseProjectNo(dataverse, item, mapping, projectCache);
                if (projectNo) {
                    bcTask = prefetchedTasks.byProjectTaskNo.get(buildProjectTaskNoKey(projectNo, taskNo)) || null;
                }
            }
        }
        if (!bcTask) {
            bcTask = await resolveBcTaskFromDataverse(bcClient, dataverse, item, mapping, projectCache);
        }

        if (!bcTask) {
            outcome.skipped += 1;
            if (requestId) {
                logger.warn("Premium -> BC skipped (no BC task match)", {
                    requestId,
                    taskId: item[mapping.taskIdField],
                    taskNo: mapping.taskBcNoField ? item[mapping.taskBcNoField] : null,
                    projectId: item[mapping.taskProjectIdField],
                });
            }
            return outcome;
        }
        if (!isAllowedSyncTaskNo(bcTask.taskNo, allowedTaskNumbers)) {
            outcome.skipped += 1;
            if (requestId) {
                logger.warn("Premium -> BC skipped (taskNo not in allowlist)", {
                    requestId,
                    projectNo: bcTask.projectNo,
                    taskNo: bcTask.taskNo,
                });
            }
            return outcome;
        }

        const cleanTask = await clearStaleSyncLockIfNeeded(bcClient, bcTask, syncConfig.syncLockTimeoutMinutes);
        if (cleanTask.syncLock) {
            outcome.skipped += 1;
            if (requestId) {
                logger.warn("Premium -> BC skipped (syncLock)", {
                    requestId,
                    projectNo: cleanTask.projectNo,
                    taskNo: cleanTask.taskNo,
                });
            }
            return outcome;
        }

        if (syncConfig.preferBc && isBcChangedSinceLastSync(cleanTask, syncConfig.bcModifiedGraceMs)) {
            outcome.skipped += 1;
            if (requestId) {
                logger.warn("Premium -> BC skipped (BC newer)", {
                    requestId,
                    projectNo: cleanTask.projectNo,
                    taskNo: cleanTask.taskNo,
                    lastSyncAt: cleanTask.lastSyncAt,
                    systemModifiedAt: cleanTask.systemModifiedAt,
                    lastModifiedDateTime: cleanTask.lastModifiedDateTime,
                    modifiedAt: cleanTask.modifiedAt,
                });
            }
            return outcome;
        }

        try {
            const updates = buildBcUpdateFromPremium(cleanTask, item, mapping);
            const planId = item[mapping.taskProjectIdField];
            const finalUpdates = {
                ...updates,
                plannerTaskId: typeof taskId === "string" ? taskId : cleanTask.plannerTaskId,
                plannerPlanId: typeof planId === "string" ? planId : cleanTask.plannerPlanId,
            };
            await updateBcTaskWithSyncLock(bcClient, cleanTask, finalUpdates);
            await markBcTaskWrite(cleanTask.systemId, {
                requestId,
                taskId: typeof taskId === "string" ? taskId : cleanTask.plannerTaskId,
                action: "update",
            });
            outcome.updated += 1;
        } catch (error) {
            outcome.errors += 1;
            logger.warn("Premium -> BC update failed", {
                requestId,
                taskId: item[mapping.taskIdField],
                error: (error as Error)?.message,
            });
        }
        return outcome;
    });

    for (const outcome of outcomes) {
        summary.updated += outcome.updated;
        summary.skipped += outcome.skipped;
        summary.cleared += outcome.cleared;
        summary.errors += outcome.errors;
    }

    if (newDelta) {
        await saveDataverseDeltaLink(mapping.taskEntitySet, newDelta);
        summary.deltaLinkSaved = true;
    }

    return summary;
}

export async function syncPremiumTaskIds(
    taskIds: string[],
    options: { requestId?: string; respectPreferBc?: boolean } = {}
) {
    const requestId = options.requestId || "";
    const respectPreferBc = options.respectPreferBc === true;
    const syncConfig = getPremiumSyncConfig();
    const allowedTaskNumbers = buildAllowedTaskNumberSet(syncConfig);
    const mapping = getDataverseMappingConfig();
    const bcClient = new BusinessCentralClient();
    const dataverse = new DataverseClient();
    const projectCache = new Map<string, string | null>();
    const uniqueIds = Array.from(
        new Set((taskIds || []).map((value) => (typeof value === "string" ? value.trim() : "")).filter(Boolean))
    );

    const selectFields = [
        mapping.taskIdField,
        mapping.taskTitleField,
        mapping.taskPercentField,
        mapping.taskStartField,
        mapping.taskFinishField,
        mapping.taskBcNoField,
        mapping.taskProjectIdField,
        mapping.taskModifiedField,
    ].filter(Boolean) as string[];

    const summary = {
        changed: uniqueIds.length,
        updated: 0,
        skipped: 0,
        cleared: 0,
        deleted: 0,
        errors: 0,
        taskIds: uniqueIds,
    };

    const premiumToBcConcurrency = Math.max(
        1,
        Math.floor(Number(process.env.SYNC_PREMIUM_TO_BC_CONCURRENCY || syncConfig.taskConcurrency || 1))
    );
    const outcomes = await runWithConcurrency(uniqueIds, premiumToBcConcurrency, async (taskId) => {
        const outcome = { updated: 0, skipped: 0, cleared: 0, errors: 0 };
        let item: DataverseEntity | null = null;
        try {
            item = await dataverse.getById<DataverseEntity>(mapping.taskEntitySet, taskId, selectFields);
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            if (message.includes("-> 404")) {
                if (syncConfig.deleteBehavior === "ignore") {
                    outcome.skipped += 1;
                    return outcome;
                }
                const bcTask = await bcClient.findProjectTaskByPlannerTaskId(taskId);
                if (bcTask && bcTask.systemId) {
                    if (!isAllowedSyncTaskNo(bcTask.taskNo, allowedTaskNumbers)) {
                        outcome.skipped += 1;
                        return outcome;
                    }
                    try {
                        await updateBcTaskWithSyncLock(bcClient, bcTask, {
                            plannerTaskId: "",
                            plannerPlanId: "",
                            plannerBucket: "",
                            lastPlannerEtag: "",
                            lastSyncAt: new Date().toISOString(),
                        });
                        await markBcTaskWrite(bcTask.systemId, {
                            requestId,
                            taskId,
                            action: "clear_link",
                        });
                        outcome.cleared += 1;
                    } catch (clearError) {
                        outcome.errors += 1;
                        logger.warn("Failed clearing BC link for deleted premium task", {
                            requestId,
                            taskId,
                            error: (clearError as Error)?.message,
                        });
                    }
                } else {
                    outcome.skipped += 1;
                }
                return outcome;
            }
            outcome.errors += 1;
            logger.warn("Premium -> BC task lookup failed", { requestId, taskId, error: message });
            return outcome;
        }

        if (!item) {
            outcome.skipped += 1;
            return outcome;
        }

        const bcTask = await resolveBcTaskFromDataverse(bcClient, dataverse, item, mapping, projectCache);
        if (!bcTask) {
            outcome.skipped += 1;
            return outcome;
        }
        if (!isAllowedSyncTaskNo(bcTask.taskNo, allowedTaskNumbers)) {
            outcome.skipped += 1;
            return outcome;
        }

        const cleanTask = await clearStaleSyncLockIfNeeded(bcClient, bcTask, syncConfig.syncLockTimeoutMinutes);
        if (cleanTask.syncLock) {
            outcome.skipped += 1;
            return outcome;
        }

        if (respectPreferBc && syncConfig.preferBc && isBcChangedSinceLastSync(cleanTask, syncConfig.bcModifiedGraceMs)) {
            outcome.skipped += 1;
            return outcome;
        }

        try {
            const updates = buildBcUpdateFromPremium(cleanTask, item, mapping);
            const planId = item[mapping.taskProjectIdField];
            const finalUpdates = {
                ...updates,
                plannerTaskId: taskId,
                plannerPlanId: typeof planId === "string" ? planId : cleanTask.plannerPlanId,
            };
            await updateBcTaskWithSyncLock(bcClient, cleanTask, finalUpdates);
            await markBcTaskWrite(cleanTask.systemId, { requestId, taskId, action: "update" });
            outcome.updated += 1;
        } catch (error) {
            outcome.errors += 1;
            logger.warn("Premium -> BC task update failed", {
                requestId,
                taskId,
                error: (error as Error)?.message,
            });
        }
        return outcome;
    });

    for (const outcome of outcomes) {
        summary.updated += outcome.updated;
        summary.skipped += outcome.skipped;
        summary.cleared += outcome.cleared;
        summary.errors += outcome.errors;
    }

    return summary;
}

export async function runPremiumChangePoll(options: { requestId?: string } = {}) {
    return syncPremiumChanges(options);
}

function parseTimestamp(value: unknown): number | null {
    if (value == null) return null;
    const ms = Date.parse(String(value));
    return Number.isFinite(ms) ? ms : null;
}

function formatTimestamp(ms: number | null): string | null {
    if (ms == null || !Number.isFinite(ms)) return null;
    return new Date(ms).toISOString();
}

export type BcChangePreview = {
    hasChanges: boolean;
    changes: number;
    latestChangedAt: string | null;
    latestChangedMs: number | null;
    lastSeq: number | null;
    projectNos: string[];
    error?: string;
};

export type PremiumChangePreview = {
    hasChanges: boolean;
    changes: number;
    latestModifiedAt: string | null;
    latestModifiedMs: number | null;
    error?: string;
};

export type SyncDecision = {
    decision: "bcToPremium" | "premiumToBc" | "none";
    reason: string;
    decidedAt: string;
    preferBc: boolean;
    graceMs: number;
    bc: BcChangePreview;
    premium: PremiumChangePreview;
};

export async function previewBcChanges(options: { requestId?: string } = {}): Promise<BcChangePreview> {
    const requestId = options.requestId || "";
    const bcClient = new BusinessCentralClient();
    try {
        const queueTargets = await resolveBcQueueTargets(bcClient, { requestId });
        return {
            hasChanges: queueTargets.queueRows > 0,
            changes: queueTargets.queueRows,
            latestChangedAt: formatTimestamp(queueTargets.latestChangedMs),
            latestChangedMs: queueTargets.latestChangedMs,
            lastSeq: null,
            projectNos: queueTargets.projectNos,
        };
    } catch (error) {
        logger.warn("BC queue preview failed", {
            requestId,
            entitySet: BC_QUEUE_ENTITY_SET,
            error: (error as Error)?.message,
        });
        return {
            hasChanges: false,
            changes: 0,
            latestChangedAt: null,
            latestChangedMs: null,
            lastSeq: null,
            projectNos: [],
            error: (error as Error)?.message,
        };
    }
}

export async function previewPremiumChanges(options: { requestId?: string; deltaLink?: string | null } = {}): Promise<PremiumChangePreview> {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
    const mapping = getDataverseMappingConfig();
    const dataverse = new DataverseClient();
    try {
        const deltaLink = options.deltaLink ?? (await getDataverseDeltaLink(mapping.taskEntitySet));
        const modifiedField = mapping.taskModifiedField || "modifiedon";
        const selectFields = [mapping.taskIdField, modifiedField, "modifiedon"].filter(Boolean) as string[];
        const { value } = await dataverse.listChanges<DataverseEntity>(mapping.taskEntitySet, {
            select: Array.from(new Set(selectFields)),
            deltaLink,
            orderBy: modifiedField ? `${modifiedField} desc` : undefined,
            maxPages: syncConfig.previewMaxPages,
            top: syncConfig.previewPageSize,
        });
        let latestMs: number | null = null;
        for (const item of value) {
            const record = item as Record<string, unknown>;
            const raw = record[modifiedField] ?? record.modifiedon ?? record.modifiedOn;
            const ms = parseTimestamp(raw);
            if (ms != null && (latestMs == null || ms > latestMs)) {
                latestMs = ms;
            }
        }
        return {
            hasChanges: value.length > 0,
            changes: value.length,
            latestModifiedAt: formatTimestamp(latestMs),
            latestModifiedMs: latestMs,
        };
    } catch (error) {
        logger.warn("Premium change preview failed", { requestId, error: (error as Error)?.message });
        return {
            hasChanges: false,
            changes: 0,
            latestModifiedAt: null,
            latestModifiedMs: null,
            error: (error as Error)?.message,
        };
    }
}

export async function decidePremiumSync(options: { requestId?: string; preferBc?: boolean; graceMs?: number } = {}): Promise<SyncDecision> {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
    const preferBc = options.preferBc ?? syncConfig.preferBc;
    const graceMs = Number.isFinite(options.graceMs ?? NaN) ? Number(options.graceMs) : syncConfig.bcModifiedGraceMs;
    const [bc, premium] = await Promise.all([previewBcChanges({ requestId }), previewPremiumChanges({ requestId })]);

    let decision: SyncDecision["decision"] = "none";
    let reason = "No changes detected in BC or Premium.";

    if (bc.hasChanges && !premium.hasChanges) {
        decision = "bcToPremium";
        reason = "BC has changes and Premium does not.";
    } else if (!bc.hasChanges && premium.hasChanges) {
        decision = "premiumToBc";
        reason = "Premium has changes and BC does not.";
    } else if (bc.hasChanges && premium.hasChanges) {
        const bcMs = bc.latestChangedMs;
        const premiumMs = premium.latestModifiedMs;
        if (bcMs != null && premiumMs != null) {
            const diff = bcMs - premiumMs;
            if (Math.abs(diff) <= graceMs) {
                decision = preferBc ? "bcToPremium" : "premiumToBc";
                reason = `Changes within ${graceMs}ms; preferBc=${preferBc}.`;
            } else if (diff > 0) {
                decision = "bcToPremium";
                reason = "BC changes are more recent than Premium changes.";
            } else {
                decision = "premiumToBc";
                reason = "Premium changes are more recent than BC changes.";
            }
        } else if (preferBc) {
            decision = "bcToPremium";
            reason = "Changes detected on both sides without comparable timestamps; preferBc=true.";
        } else if (premiumMs != null) {
            decision = "premiumToBc";
            reason = "Premium changes include timestamps while BC does not; preferBc=false.";
        } else {
            decision = "premiumToBc";
            reason = "Changes detected on both sides without comparable timestamps; preferBc=false.";
        }
    }

    return {
        decision,
        reason,
        decidedAt: new Date().toISOString(),
        preferBc,
        graceMs,
        bc,
        premium,
    };
}

export async function runPremiumSyncDecision(options: {
    requestId?: string;
    dryRun?: boolean;
    preferBc?: boolean;
    graceMs?: number;
} = {}) {
    const requestId = options.requestId || "";
    const decision = await decidePremiumSync({
        requestId,
        preferBc: options.preferBc,
        graceMs: options.graceMs,
    });
    if (options.dryRun || decision.decision === "none") {
        return { decision, result: null };
    }
    if (decision.decision === "bcToPremium") {
        const bcResult = await syncBcToPremium(undefined, { requestId });
        return { decision, result: { bcToPremium: bcResult } };
    }
    const premiumResult = await syncPremiumChanges({ requestId });
    return { decision, result: { premiumToBc: premiumResult } };
}
