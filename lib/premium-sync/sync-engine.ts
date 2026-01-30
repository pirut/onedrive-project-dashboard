import { BusinessCentralClient, BcProject, BcProjectTask } from "../planner-sync/bc-client.js";
import { logger } from "../planner-sync/logger.js";
import { getBcProjectChangeCursor, saveBcProjectChangeCursor } from "../planner-sync/bc-change-store.js";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "../planner-sync/project-sync-store.js";
import { DataverseClient, DataverseEntity } from "../dataverse-client.js";
import { getDataverseDeltaLink, saveDataverseDeltaLink } from "./delta-store.js";
import { getDataverseMappingConfig, getPremiumSyncConfig } from "./config.js";
import crypto from "crypto";

const HEADING_TASK_SECTIONS = new Map<number, string | null>([
    [1000, "Pre-Construction"],
    [2000, "Installation"],
    [3000, "Revenue"],
    [4000, "Change Orders"],
]);

function hasField(task: BcProjectTask, field: string) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function escapeODataString(value: string) {
    return value.replace(/'/g, "''");
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
    if (!Object.keys(patch).length) return;

    if (hasField(task, "syncLock")) {
        await bcClient.patchProjectTask(task.systemId, { syncLock: true });
    }
    await bcClient.patchProjectTask(task.systemId, {
        ...patch,
        ...(hasField(task, "syncLock") ? { syncLock: false } : {}),
    });
}

function normalizeProjectKey(value: string | undefined | null) {
    return normalizeProjectNo(value || "");
}

function buildTaskTitle(task: BcProjectTask) {
    const description = (task.description || "").trim();
    const taskNo = (task.taskNo || "").trim();
    return description || taskNo || "Untitled Task";
}

function toDataversePercent(value: number | null | undefined, scale: number, min: number, max: number) {
    if (typeof value !== "number" || Number.isNaN(value)) return null;
    let raw = !scale || scale === 1 ? value : value / scale;
    if (!Number.isFinite(raw)) return null;
    const lower = Number.isFinite(min) ? min : 0;
    const upper = Number.isFinite(max) ? max : 100;
    if (scale === 1 && upper <= 1 && raw > upper && value <= 100) {
        raw = value / 100;
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

function parseDateMs(value?: string | null) {
    if (!value) return null;
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
}

function resolveTaskDate(value?: string | null) {
    if (!value) return null;
    const parsed = parseDateMs(value);
    if (parsed == null) return null;
    const minCrmDate = Date.UTC(1753, 0, 1);
    if (parsed < minCrmDate) return null;
    return new Date(parsed).toISOString();
}

function buildScheduleTaskEntity(params: {
    taskId: string;
    projectId: string;
    bucketId?: string | null;
    task: BcProjectTask;
    mapping: ReturnType<typeof getDataverseMappingConfig>;
    dataverse: DataverseClient;
}) {
    const { taskId, projectId, bucketId, task, mapping, dataverse } = params;
    const entity: DataverseEntity = {
        "@odata.type": "Microsoft.Dynamics.CRM.msdyn_projecttask",
        [mapping.taskIdField]: taskId,
    };

    const title = buildTaskTitle(task);
    entity[mapping.taskTitleField] = title;

    if (mapping.taskBcNoField && task.taskNo) {
        entity[mapping.taskBcNoField] = String(task.taskNo).trim();
    }

    const start = resolveTaskDate(task.manualStartDate || null);
    const finish = resolveTaskDate(task.manualEndDate || null);
    if (start) {
        entity[mapping.taskStartField] = start;
    }
    if (finish) {
        entity[mapping.taskFinishField] = finish;
    }

    const percent = toDataversePercent(task.percentComplete ?? null, mapping.percentScale, mapping.percentMin, mapping.percentMax);
    if (percent != null) entity[mapping.taskPercentField] = percent;

    if (mapping.taskDescriptionField && task.description) {
        entity[mapping.taskDescriptionField] = task.description;
    }

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
    try {
        const result = await dataverse.list<DataverseEntity>("msdyn_projectbuckets", {
            select: ["msdyn_projectbucketid"],
            filter: `_msdyn_project_value eq ${formatODataGuid(projectId)}`,
            top: 1,
        });
        const bucketId = result.value[0]?.msdyn_projectbucketid;
        const resolved = typeof bucketId === "string" && bucketId.trim() ? bucketId.trim() : null;
        if (resolved) {
            cache.set(projectId, resolved);
            return resolved;
        }
        const created = await createProjectBucket(dataverse, projectId, "General");
        cache.set(projectId, created);
        return created;
    } catch (error) {
        logger.warn("Dataverse bucket lookup failed", { projectId, error: (error as Error)?.message });
        cache.set(projectId, null);
        return null;
    }
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
        logger.warn("Dataverse resource lookup failed", { name, error: (error as Error)?.message });
        cache.set(key, null);
        return null;
    }
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
        const filter = `_msdyn_projectid_value eq ${formatODataGuid(projectId)} and _msdyn_bookableresourceid_value eq ${formatODataGuid(
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
        logger.warn("Dataverse project team lookup failed", { projectId, resourceId, error: (error as Error)?.message });
        cache.set(key, null);
        return null;
    }
}

async function createProjectTeamMember(
    dataverse: DataverseClient,
    projectId: string,
    resourceId: string,
    name: string,
    operationSetId?: string
) {
    const projectBinding = dataverse.buildLookupBinding("msdyn_projects", projectId);
    const resourceBinding = dataverse.buildLookupBinding("bookableresources", resourceId);
    if (!projectBinding || !resourceBinding) return null;
    const entity: DataverseEntity = {
        "@odata.type": "Microsoft.Dynamics.CRM.msdyn_projectteam",
        msdyn_projectteamid: crypto.randomUUID(),
        msdyn_name: name,
        "msdyn_projectid@odata.bind": projectBinding,
        "msdyn_bookableresourceid@odata.bind": resourceBinding,
    };
    try {
        if (operationSetId) {
            await dataverse.pssCreate(entity, operationSetId);
            return entity.msdyn_projectteamid as string;
        }
        const created = await dataverse.create("msdyn_projectteams", entity);
        return created.entityId || null;
    } catch (error) {
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
    }
) {
    const assignee = getAssigneeName(task);
    if (!assignee) return;
    const resourceId = await getBookableResourceIdByName(dataverse, assignee, options.resourceCache);
    if (!resourceId) {
        logger.warn("Dataverse resource not found for assignee", { projectId, taskId, assignee });
        return;
    }
    let teamId = await getProjectTeamMemberId(dataverse, projectId, resourceId, options.teamCache);
    if (!teamId) {
        teamId = await createProjectTeamMember(dataverse, projectId, resourceId, assignee, options.operationSetId);
        if (teamId) {
            options.teamCache.set(`${projectId}:${resourceId}`, teamId);
        }
    }
    if (!teamId) {
        logger.warn("Dataverse project team member missing for assignment", { projectId, taskId, assignee });
        return;
    }
    const assignmentKey = `${taskId}:${teamId}`;
    if (options.assignmentCache.has(assignmentKey)) return;
    try {
        const filter = `_msdyn_taskid_value eq ${formatODataGuid(taskId)} and _msdyn_projectteamid_value eq ${formatODataGuid(teamId)}`;
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
        logger.warn("Dataverse assignment lookup failed", { projectId, taskId, assignee, error: (error as Error)?.message });
    }
    const projectBinding = dataverse.buildLookupBinding("msdyn_projects", projectId);
    const teamBinding = dataverse.buildLookupBinding("msdyn_projectteams", teamId);
    const taskBinding = dataverse.buildLookupBinding("msdyn_projecttasks", taskId);
    if (!projectBinding || !teamBinding || !taskBinding) return;
    const entity: DataverseEntity = {
        "@odata.type": "Microsoft.Dynamics.CRM.msdyn_resourceassignment",
        msdyn_resourceassignmentid: crypto.randomUUID(),
        msdyn_name: assignee,
        "msdyn_projectid@odata.bind": projectBinding,
        "msdyn_projectteamid@odata.bind": teamBinding,
        "msdyn_taskid@odata.bind": taskBinding,
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

async function resolveProjectFromBc(
    bcClient: BusinessCentralClient,
    dataverse: DataverseClient,
    projectNo: string,
    mapping: ReturnType<typeof getDataverseMappingConfig>
) {
    const normalized = (projectNo || "").trim();
    if (!normalized) return null;

    const escaped = escapeODataString(normalized);
    const select = [mapping.projectIdField, mapping.projectTitleField, mapping.projectBcNoField].filter(Boolean) as string[];

    if (mapping.projectBcNoField) {
        const filter = `${mapping.projectBcNoField} eq '${escaped}'`;
        const result = await dataverse.list<DataverseEntity>(mapping.projectEntitySet, {
            select,
            filter,
            top: 5,
        });
        if (result.value.length) {
            const match = result.value[0];
            return match;
        }
    }

    const titleField = mapping.projectTitleField;
    if (titleField) {
        const filter = `startswith(${titleField}, '${escaped}')`;
        const result = await dataverse.list<DataverseEntity>(mapping.projectEntitySet, {
            select,
            filter,
            top: 5,
        });
        if (result.value.length) {
            const match = result.value[0];
            return match;
        }
    }

    if (!mapping.allowProjectCreate) {
        return null;
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
    if (mapping.projectBcNoField) {
        payload[mapping.projectBcNoField] = normalized;
    }

    const created = await dataverse.create(mapping.projectEntitySet, payload);
    if (!created.entityId) return null;
    return {
        [mapping.projectIdField]: created.entityId,
        [mapping.projectTitleField]: title,
        ...(mapping.projectBcNoField ? { [mapping.projectBcNoField]: normalized } : {}),
    } as DataverseEntity;
}

function resolveProjectId(entity: DataverseEntity | null, mapping: ReturnType<typeof getDataverseMappingConfig>) {
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
    dataverse: DataverseClient
) {
    const payload: Record<string, unknown> = {};
    const title = buildTaskTitle(task);
    payload[mapping.taskTitleField] = title;

    const start = resolveTaskDate(task.manualStartDate || null);
    const finish = resolveTaskDate(task.manualEndDate || null);
    if (start) payload[mapping.taskStartField] = start;
    if (finish) payload[mapping.taskFinishField] = finish;

    const percent = toDataversePercent(task.percentComplete ?? null, mapping.percentScale, mapping.percentMin, mapping.percentMax);
    if (percent != null) payload[mapping.taskPercentField] = percent;

    if (mapping.taskDescriptionField && task.description) {
        payload[mapping.taskDescriptionField] = task.description;
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
    return [...tasks].sort((a, b) => {
        const aNum = parseTaskNumber(a.taskNo);
        const bNum = parseTaskNumber(b.taskNo);
        if (Number.isFinite(aNum) && Number.isFinite(bNum) && aNum !== bNum) {
            return aNum - bNum;
        }
        const aStr = String(a.taskNo || "").trim();
        const bStr = String(b.taskNo || "").trim();
        return aStr.localeCompare(bStr);
    });
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
    if (percent != null) {
        updates.percentComplete = percent;
    }

    const start = dataverseTask[mapping.taskStartField];
    if (typeof start === "string") {
        updates.manualStartDate = start;
        updates.startDate = start;
    }

    const finish = dataverseTask[mapping.taskFinishField];
    if (typeof finish === "string") {
        updates.manualEndDate = finish;
        updates.endDate = finish;
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
        useScheduleApi: boolean;
        operationSetId?: string;
        bucketCache: Map<string, string | null>;
        resourceCache: Map<string, string | null>;
        teamCache: Map<string, string | null>;
        assignmentCache: Set<string>;
    }
) {
    const payload = buildTaskPayload(task, projectId, mapping, dataverse);
    const existingId = task.plannerTaskId ? task.plannerTaskId.trim() : "";
    let taskId = existingId || "";
    if (taskId && !isGuid(taskId)) {
        logger.warn("Ignoring non-GUID plannerTaskId; will resolve by BC keys", {
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            taskId,
        });
        taskId = "";
    }

    let existingTask: DataverseEntity | null = null;
    if (!taskId && task.taskNo) {
        existingTask = await findDataverseTaskByBcNo(dataverse, projectId, String(task.taskNo), mapping);
        taskId = resolveTaskId(existingTask, mapping) || "";
    }

    if (!taskId) {
        existingTask = await findDataverseTaskByTitle(dataverse, projectId, payload[mapping.taskTitleField] as string, mapping);
        taskId = resolveTaskId(existingTask, mapping) || "";
    }

    if (taskId) {
        if (options.useScheduleApi && options.operationSetId) {
            const entity = buildScheduleTaskEntity({
                taskId,
                projectId,
                bucketId: null,
                task,
                mapping,
                dataverse,
            });
            await dataverse.pssUpdate(entity, options.operationSetId);
            await ensureAssignmentForTask(dataverse, task, projectId, taskId, {
                operationSetId: options.operationSetId,
                resourceCache: options.resourceCache,
                teamCache: options.teamCache,
                assignmentCache: options.assignmentCache,
            });
            const updates = {
                plannerTaskId: taskId,
                plannerPlanId: projectId,
                lastPlannerEtag: task.lastPlannerEtag || "",
                lastSyncAt: new Date().toISOString(),
            };
            return { action: "updated", taskId, pendingUpdate: { task, updates } };
        }

        const ifMatch = task.lastPlannerEtag ? String(task.lastPlannerEtag) : undefined;
        const updateResult = await dataverse.update(mapping.taskEntitySet, taskId, payload, { ifMatch });
        const updates = {
            plannerTaskId: taskId,
            plannerPlanId: projectId,
            lastPlannerEtag: updateResult.etag || task.lastPlannerEtag,
            lastSyncAt: new Date().toISOString(),
        };
        await updateBcTaskWithSyncLock(bcClient, task, updates);
        return { action: "updated", taskId };
    }

    if (!mapping.allowTaskCreate) {
        return { action: "skipped", taskId: "" };
    }

    if (options.useScheduleApi && options.operationSetId) {
        const newTaskId = crypto.randomUUID();
        const bucketId = await getProjectBucketId(dataverse, projectId, options.bucketCache);
        const entity = buildScheduleTaskEntity({
            taskId: newTaskId,
            projectId,
            bucketId,
            task,
            mapping,
            dataverse,
        });
        await dataverse.pssCreate(entity, options.operationSetId);
        await ensureAssignmentForTask(dataverse, task, projectId, newTaskId, {
            operationSetId: options.operationSetId,
            resourceCache: options.resourceCache,
            teamCache: options.teamCache,
            assignmentCache: options.assignmentCache,
        });
        const updates = {
            plannerTaskId: newTaskId,
            plannerPlanId: projectId,
            lastPlannerEtag: "",
            lastSyncAt: new Date().toISOString(),
        };
        return { action: "created", taskId: newTaskId, pendingUpdate: { task, updates } };
    }

    const created = await dataverse.create(mapping.taskEntitySet, payload);
    if (!created.entityId) {
        return { action: "error", taskId: "" };
    }
    await updateBcTaskWithSyncLock(bcClient, task, {
        plannerTaskId: created.entityId,
        plannerPlanId: projectId,
        lastPlannerEtag: created.etag,
        lastSyncAt: new Date().toISOString(),
    });
    await ensureAssignmentForTask(dataverse, task, projectId, created.entityId, {
        resourceCache: options.resourceCache,
        teamCache: options.teamCache,
        assignmentCache: options.assignmentCache,
    });
    return { action: "created", taskId: created.entityId };
}

export async function syncBcToPremium(projectNo?: string, options: { requestId?: string; projectNos?: string[] } = {}) {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
    const mapping = getDataverseMappingConfig();
    const bcClient = new BusinessCentralClient();
    const dataverse = new DataverseClient();

    const settings = await listProjectSyncSettings();
    const disabled = buildDisabledProjectSet(settings);

    let projectNos: string[] = [];
    if (projectNo) projectNos = [projectNo];
    if (options.projectNos && options.projectNos.length) projectNos = options.projectNos;

    if (!projectNos.length) {
        try {
            const cursor = await getBcProjectChangeCursor("premium");
            const { items, lastSeq } = await bcClient.listProjectChangesSince(cursor);
            const changed = new Set<string>();
            for (const item of items) {
                const proj = (item.projectNo || "").trim();
                if (proj) changed.add(proj);
            }
            projectNos = Array.from(changed);
            if (lastSeq != null) {
                await saveBcProjectChangeCursor("premium", lastSeq);
            }
        } catch (error) {
            logger.warn("BC change feed unavailable; falling back to project list", { error: (error as Error)?.message });
        }
    }

    if (!projectNos.length && syncConfig.maxProjectsPerRun > 0) {
        const projects = await bcClient.listProjects();
        projectNos = projects.map((project) => project.projectNo || "").filter(Boolean).slice(0, syncConfig.maxProjectsPerRun);
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

        let tasks: BcProjectTask[] = [];
        try {
            const filter = `projectNo eq '${escapeODataString(projNo)}'`;
            tasks = await bcClient.listProjectTasks(filter);
        } catch (error) {
            logger.warn("BC task load failed", { requestId, projectNo: projNo, error: (error as Error)?.message });
            result.errors += 1;
            continue;
        }

        if (!tasks.length) continue;
        const sorted = sortTasksByTaskNo(tasks);
        const currentSection = { name: null as string | null };

        let projectId = tasks.find((task) => (task.plannerPlanId || "").trim())?.plannerPlanId?.trim() || "";
        if (projectId && !isGuid(projectId)) {
            logger.warn("Ignoring non-GUID plannerPlanId; will resolve project by BC data", {
                requestId,
                projectNo: projNo,
                projectId,
            });
            projectId = "";
        }
        if (projectId) {
            try {
                await dataverse.getById(mapping.projectEntitySet, projectId, [mapping.projectIdField]);
            } catch (error) {
                logger.warn("Cached Dataverse project lookup failed", { requestId, projectNo: projNo, projectId, error: (error as Error)?.message });
                projectId = "";
            }
        }
        if (!projectId) {
            const projectEntity = await resolveProjectFromBc(bcClient, dataverse, projNo, mapping);
            projectId = resolveProjectId(projectEntity, mapping) || "";
        }
        if (!projectId) {
            logger.warn("Dataverse project not found", { requestId, projectNo: projNo });
            result.errors += 1;
            continue;
        }

        const pendingUpdates: Array<{ task: BcProjectTask; updates: Record<string, unknown> }> = [];
        const bucketCache = new Map<string, string | null>();
        const resourceCache = new Map<string, string | null>();
        const teamCache = new Map<string, string | null>();
        const assignmentCache = new Set<string>();
        let useScheduleApi = syncConfig.useScheduleApi;
        let operationSetId = "";
        if (useScheduleApi) {
            try {
                operationSetId = await dataverse.createOperationSet(
                    projectId,
                    `BC sync ${projNo} ${new Date().toISOString()}`
                );
                if (!operationSetId) {
                    useScheduleApi = false;
                    logger.warn("Dataverse schedule API unavailable; falling back to direct updates", {
                        requestId,
                        projectNo: projNo,
                    });
                }
            } catch (error) {
                useScheduleApi = false;
                logger.warn("Dataverse schedule API init failed; falling back to direct updates", {
                    requestId,
                    projectNo: projNo,
                    error: (error as Error)?.message,
                });
            }
        }

        for (const task of sorted) {
            result.tasks += 1;
            const cleanTask = await clearStaleSyncLockIfNeeded(bcClient, task, syncConfig.syncLockTimeoutMinutes);
            if (cleanTask.syncLock) {
                result.skipped += 1;
                continue;
            }
            if (shouldSkipTaskForSection(cleanTask, currentSection)) {
                result.skipped += 1;
                continue;
            }
            try {
                const res = await syncTaskToDataverse(bcClient, dataverse, cleanTask, projectId, mapping, {
                    useScheduleApi,
                    operationSetId: useScheduleApi ? operationSetId : undefined,
                    bucketCache,
                    resourceCache,
                    teamCache,
                    assignmentCache,
                });
                if (res.action === "created") result.created += 1;
                else if (res.action === "updated") result.updated += 1;
                else result.skipped += 1;
                if (res.pendingUpdate) {
                    pendingUpdates.push(res.pendingUpdate);
                }
            } catch (error) {
                logger.warn("Premium task sync failed", {
                    requestId,
                    projectNo: projNo,
                    taskNo: cleanTask.taskNo,
                    error: (error as Error)?.message,
                });
                result.errors += 1;
            }
        }

        if (useScheduleApi && operationSetId && pendingUpdates.length) {
            try {
                await dataverse.executeOperationSet(operationSetId);
                for (const entry of pendingUpdates) {
                    await updateBcTaskWithSyncLock(bcClient, entry.task, entry.updates);
                }
            } catch (error) {
                logger.warn("Dataverse schedule execute failed", {
                    requestId,
                    projectNo: projNo,
                    error: (error as Error)?.message,
                });
                result.errors += pendingUpdates.length;
            }
        }

        result.projects += 1;
        result.projectNos.push(projNo);
    }

    return result;
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
    let projectNoValue: string | null = null;

    if (mapping.projectBcNoField) {
        const directProjectNo = dataverseTask[mapping.projectBcNoField];
        if (typeof directProjectNo === "string" && directProjectNo.trim()) {
            projectNoValue = directProjectNo.trim();
        }
    }

    if (!projectNoValue && mapping.projectBcNoField) {
        const projectIdRaw = dataverseTask[mapping.taskProjectIdField];
        const projectId = typeof projectIdRaw === "string" ? projectIdRaw.trim() : "";
        if (projectId) {
            if (projectCache.has(projectId)) {
                projectNoValue = projectCache.get(projectId) || null;
            } else {
                try {
                    const project = await dataverse.getById<DataverseEntity>(mapping.projectEntitySet, projectId, [
                        mapping.projectBcNoField,
                    ]);
                    const projectNo = project?.[mapping.projectBcNoField];
                    projectNoValue = typeof projectNo === "string" && projectNo.trim() ? projectNo.trim() : null;
                } catch (error) {
                    logger.warn("Dataverse project lookup failed", {
                        projectId,
                        error: (error as Error)?.message,
                    });
                    projectNoValue = null;
                }
                projectCache.set(projectId, projectNoValue);
            }
        }
    }

    if (taskNoValue && projectNoValue) {
        return bcClient.findProjectTaskByProjectAndTaskNo(projectNoValue, taskNoValue);
    }

    return null;
}

export async function syncPremiumChanges(options: { requestId?: string; deltaLink?: string | null } = {}) {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
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

    for (const item of value) {
        const removed = item["@removed"] as { reason?: string } | undefined;
        if (removed) {
            if (syncConfig.deleteBehavior === "ignore") {
                summary.skipped += 1;
                continue;
            }
            const taskId = item[mapping.taskIdField];
            if (typeof taskId === "string" && taskId.trim()) {
                const bcTask = await bcClient.findProjectTaskByPlannerTaskId(taskId.trim());
                if (bcTask && bcTask.systemId) {
                    try {
                        await updateBcTaskWithSyncLock(bcClient, bcTask, {
                            plannerTaskId: "",
                            plannerPlanId: "",
                            plannerBucket: "",
                            lastPlannerEtag: "",
                            lastSyncAt: new Date().toISOString(),
                        });
                        summary.cleared += 1;
                    } catch (error) {
                        summary.errors += 1;
                        logger.warn("Failed clearing BC link for deleted premium task", {
                            requestId,
                            taskId,
                            error: (error as Error)?.message,
                        });
                    }
                } else {
                    summary.skipped += 1;
                }
            } else {
                summary.skipped += 1;
            }
            continue;
        }

        const bcTask = await resolveBcTaskFromDataverse(bcClient, dataverse, item, mapping, projectCache);
        if (!bcTask) {
            summary.skipped += 1;
            continue;
        }

        const cleanTask = await clearStaleSyncLockIfNeeded(bcClient, bcTask, syncConfig.syncLockTimeoutMinutes);
        if (cleanTask.syncLock) {
            summary.skipped += 1;
            continue;
        }

        if (syncConfig.preferBc && isBcChangedSinceLastSync(cleanTask, syncConfig.bcModifiedGraceMs)) {
            summary.skipped += 1;
            continue;
        }

        try {
            const updates = buildBcUpdateFromPremium(cleanTask, item, mapping);
            const taskId = item[mapping.taskIdField];
            const planId = item[mapping.taskProjectIdField];
            const finalUpdates = {
                ...updates,
                plannerTaskId: typeof taskId === "string" ? taskId : cleanTask.plannerTaskId,
                plannerPlanId: typeof planId === "string" ? planId : cleanTask.plannerPlanId,
            };
            await updateBcTaskWithSyncLock(bcClient, cleanTask, finalUpdates);
            summary.updated += 1;
        } catch (error) {
            summary.errors += 1;
            logger.warn("Premium -> BC update failed", {
                requestId,
                taskId: item[mapping.taskIdField],
                error: (error as Error)?.message,
            });
        }
    }

    if (newDelta) {
        await saveDataverseDeltaLink(mapping.taskEntitySet, newDelta);
        summary.deltaLinkSaved = true;
    }

    return summary;
}

export async function runPremiumChangePoll(options: { requestId?: string } = {}) {
    return syncPremiumChanges(options);
}
