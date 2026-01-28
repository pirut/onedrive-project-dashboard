import { BusinessCentralClient } from "../planner-sync/bc-client.js";
import { logger } from "../planner-sync/logger.js";
import { getBcProjectChangeCursor, saveBcProjectChangeCursor } from "../planner-sync/bc-change-store.js";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "../planner-sync/project-sync-store.js";
import { DataverseClient } from "../dataverse-client.js";
import { getDataverseDeltaLink, saveDataverseDeltaLink } from "./delta-store.js";
import { getDataverseMappingConfig, getPremiumSyncConfig } from "./config.js";

const HEADING_TASK_SECTIONS = new Map([
    [1000, "Pre-Construction"],
    [2000, "Installation"],
    [3000, "Revenue"],
    [4000, "Change Orders"],
]);

function hasField(task, field) {
    return Object.prototype.hasOwnProperty.call(task, field);
}

function escapeODataString(value) {
    return value.replace(/'/g, "''");
}

function formatODataGuid(value) {
    const trimmed = value.trim().replace(/^\{/, "").replace(/\}$/, "");
    return `guid'${trimmed}'`;
}

function isGuid(value) {
    const trimmed = value.trim().replace(/^\{/, "").replace(/\}$/, "");
    return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(trimmed);
}

function parseTaskNumber(taskNo) {
    const raw = String(taskNo || "");
    const match = raw.match(/\d+/);
    if (!match) return Number.NaN;
    return Number(match[0]);
}

function isStaleSyncLock(task, timeoutMinutes) {
    if (!task.syncLock) return false;
    if (timeoutMinutes <= 0) return false;
    const lastSync = task.lastSyncAt ? Date.parse(task.lastSyncAt) : Number.NaN;
    if (Number.isNaN(lastSync)) return true;
    return Date.now() - lastSync > timeoutMinutes * 60 * 1000;
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

function normalizeProjectKey(value) {
    return normalizeProjectNo(value || "");
}

function buildTaskTitle(task) {
    const description = (task.description || "").trim();
    const taskNo = (task.taskNo || "").trim();
    return description || taskNo || "Untitled Task";
}

function toDataversePercent(value, scale) {
    if (typeof value !== "number" || Number.isNaN(value)) return null;
    if (!scale || scale === 1) return value;
    return value / scale;
}

function fromDataversePercent(value, scale) {
    if (typeof value !== "number" || Number.isNaN(value)) return null;
    if (!scale || scale === 1) return value;
    return value * scale;
}

function parseDateMs(value) {
    if (!value) return null;
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
}

function resolveTaskDate(value) {
    if (!value) return null;
    return value;
}

async function resolveProjectFromBc(bcClient, dataverse, projectNo, mapping) {
    const normalized = (projectNo || "").trim();
    if (!normalized) return null;

    const escaped = escapeODataString(normalized);
    const select = [mapping.projectIdField, mapping.projectTitleField, mapping.projectBcNoField].filter(Boolean);

    if (mapping.projectBcNoField) {
        const filter = `${mapping.projectBcNoField} eq '${escaped}'`;
        const result = await dataverse.list(mapping.projectEntitySet, {
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
        const result = await dataverse.list(mapping.projectEntitySet, {
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

    let bcProject = null;
    try {
        const projects = await bcClient.listProjects(`projectNo eq '${escaped}'`);
        bcProject = projects[0] || null;
    } catch (error) {
        logger.warn("BC project lookup failed", { projectNo, error: error?.message || String(error) });
    }

    const title = `${normalized}${bcProject?.description ? ` - ${bcProject.description}` : ""}`.trim();
    const payload = {
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
    };
}

function resolveProjectId(entity, mapping) {
    if (!entity) return null;
    const id = entity[mapping.projectIdField];
    if (typeof id === "string" && id.trim()) return id.trim();
    return null;
}

function resolveTaskId(entity, mapping) {
    if (!entity) return null;
    const id = entity[mapping.taskIdField];
    if (typeof id === "string" && id.trim()) return id.trim();
    return null;
}

async function findDataverseTaskByBcNo(dataverse, projectId, taskNo, mapping) {
    if (!mapping.taskBcNoField) return null;
    const escapedTask = escapeODataString(taskNo);
    const projectFilterField = mapping.taskProjectIdField || `${mapping.taskProjectLookupField}/${mapping.projectIdField}`;
    const projectGuid = formatODataGuid(projectId);
    const filter = `${mapping.taskBcNoField} eq '${escapedTask}' and ${projectFilterField} eq ${projectGuid}`;
    try {
        const res = await dataverse.list(mapping.taskEntitySet, {
            select: [mapping.taskIdField, mapping.taskBcNoField, mapping.taskTitleField].filter(Boolean),
            filter,
            top: 1,
        });
        return res.value[0] || null;
    } catch (error) {
        logger.warn("Dataverse task lookup failed", { projectId, taskNo, error: error?.message || String(error) });
        return null;
    }
}

async function findDataverseTaskByTitle(dataverse, projectId, title, mapping) {
    const escapedTitle = escapeODataString(title);
    const projectFilterField = mapping.taskProjectIdField || `${mapping.taskProjectLookupField}/${mapping.projectIdField}`;
    const projectGuid = formatODataGuid(projectId);
    const filter = `${mapping.taskTitleField} eq '${escapedTitle}' and ${projectFilterField} eq ${projectGuid}`;
    try {
        const res = await dataverse.list(mapping.taskEntitySet, {
            select: [mapping.taskIdField, mapping.taskTitleField].filter(Boolean),
            filter,
            top: 1,
        });
        return res.value[0] || null;
    } catch (error) {
        logger.warn("Dataverse task title lookup failed", { projectId, title, error: error?.message || String(error) });
        return null;
    }
}

function buildTaskPayload(task, projectId, mapping, dataverse) {
    const payload = {};
    const title = buildTaskTitle(task);
    payload[mapping.taskTitleField] = title;

    const start = resolveTaskDate(task.manualStartDate || task.startDate || null);
    const finish = resolveTaskDate(task.manualEndDate || task.endDate || null);
    if (start) payload[mapping.taskStartField] = start;
    if (finish) payload[mapping.taskFinishField] = finish;

    const percent = toDataversePercent(task.percentComplete ?? null, mapping.percentScale);
    if (percent != null) payload[mapping.taskPercentField] = percent;

    if (mapping.taskDescriptionField && task.description) {
        payload[mapping.taskDescriptionField] = task.description;
    }

    if (mapping.taskBcNoField && task.taskNo) {
        payload[mapping.taskBcNoField] = String(task.taskNo).trim();
    }

    if (mapping.projectBcNoField && task.projectNo) {
        payload[mapping.projectBcNoField] = String(task.projectNo).trim();
    }

    const lookupBinding = dataverse.buildLookupBinding(mapping.projectEntitySet, projectId);
    if (lookupBinding) {
        payload[`${mapping.taskProjectLookupField}@odata.bind`] = lookupBinding;
    }

    return payload;
}

function shouldSkipTaskForSection(task, currentSection) {
    const taskNumber = parseTaskNumber(task.taskNo);
    if (Number.isFinite(taskNumber) && HEADING_TASK_SECTIONS.has(taskNumber)) {
        currentSection.name = HEADING_TASK_SECTIONS.get(taskNumber) || null;
        return true;
    }
    if (currentSection.name === "Revenue") return true;
    return false;
}

function sortTasksByTaskNo(tasks) {
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

function buildBcUpdateFromPremium(bcTask, dataverseTask, mapping) {
    const updates = {
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

function isBcChangedSinceLastSync(bcTask, graceMs) {
    const lastSync = parseDateMs(bcTask.lastSyncAt);
    const modifiedMs =
        parseDateMs(bcTask.systemModifiedAt) ?? parseDateMs(bcTask.lastModifiedDateTime) ?? parseDateMs(bcTask.modifiedAt);
    if (lastSync == null || modifiedMs == null) return false;
    return modifiedMs - lastSync > graceMs;
}

async function clearStaleSyncLockIfNeeded(bcClient, task, timeoutMinutes) {
    if (!task.syncLock) return task;
    if (!isStaleSyncLock(task, timeoutMinutes)) return task;
    if (!task.systemId) return task;
    try {
        await bcClient.patchProjectTask(task.systemId, { syncLock: false });
        return { ...task, syncLock: false };
    } catch (error) {
        logger.warn("Failed to clear stale syncLock", {
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            error: error?.message || String(error),
        });
        return task;
    }
}

async function syncTaskToDataverse(bcClient, dataverse, task, projectId, mapping) {
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

    let existingTask = null;
    if (!taskId && task.taskNo) {
        existingTask = await findDataverseTaskByBcNo(dataverse, projectId, String(task.taskNo), mapping);
        taskId = resolveTaskId(existingTask, mapping) || "";
    }

    if (!taskId) {
        existingTask = await findDataverseTaskByTitle(dataverse, projectId, payload[mapping.taskTitleField], mapping);
        taskId = resolveTaskId(existingTask, mapping) || "";
    }

    if (taskId) {
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
    return { action: "created", taskId: created.entityId };
}

export async function syncBcToPremium(projectNo, options = {}) {
    const requestId = options.requestId || "";
    const syncConfig = getPremiumSyncConfig();
    const mapping = getDataverseMappingConfig();
    const bcClient = new BusinessCentralClient();
    const dataverse = new DataverseClient();

    const settings = await listProjectSyncSettings();
    const disabled = buildDisabledProjectSet(settings);

    let projectNos = [];
    if (projectNo) projectNos = [projectNo];
    if (options.projectNos && options.projectNos.length) projectNos = options.projectNos;

    if (!projectNos.length) {
        try {
            const cursor = await getBcProjectChangeCursor("premium");
            const { items, lastSeq } = await bcClient.listProjectChangesSince(cursor);
            const changed = new Set();
            for (const item of items) {
                const proj = (item.projectNo || "").trim();
                if (proj) changed.add(proj);
            }
            projectNos = Array.from(changed);
            if (lastSeq != null) {
                await saveBcProjectChangeCursor("premium", lastSeq);
            }
        } catch (error) {
            logger.warn("BC change feed unavailable; falling back to project list", { error: error?.message || String(error) });
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
        projectNos: [],
    };

    for (const projNoRaw of projectNos) {
        const projNo = (projNoRaw || "").trim();
        if (!projNo) continue;
        if (disabled.has(normalizeProjectKey(projNo))) {
            logger.info("Premium sync skipped for disabled project", { requestId, projectNo: projNo });
            continue;
        }

        let tasks = [];
        try {
            const filter = `projectNo eq '${escapeODataString(projNo)}'`;
            tasks = await bcClient.listProjectTasks(filter);
        } catch (error) {
            logger.warn("BC task load failed", { requestId, projectNo: projNo, error: error?.message || String(error) });
            result.errors += 1;
            continue;
        }

        if (!tasks.length) continue;
        const sorted = sortTasksByTaskNo(tasks);
        const currentSection = { name: null };

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
                logger.warn("Cached Dataverse project lookup failed", {
                    requestId,
                    projectNo: projNo,
                    projectId,
                    error: error?.message || String(error),
                });
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
                const res = await syncTaskToDataverse(bcClient, dataverse, cleanTask, projectId, mapping);
                if (res.action === "created") result.created += 1;
                else if (res.action === "updated") result.updated += 1;
                else result.skipped += 1;
            } catch (error) {
                logger.warn("Premium task sync failed", {
                    requestId,
                    projectNo: projNo,
                    taskNo: cleanTask.taskNo,
                    error: error?.message || String(error),
                });
                result.errors += 1;
            }
        }

        result.projects += 1;
        result.projectNos.push(projNo);
    }

    return result;
}

async function resolveBcTaskFromDataverse(bcClient, dataverse, dataverseTask, mapping, projectCache) {
    const taskId = dataverseTask[mapping.taskIdField];
    if (typeof taskId === "string" && taskId.trim()) {
        const byPlannerId = await bcClient.findProjectTaskByPlannerTaskId(taskId.trim());
        if (byPlannerId) return byPlannerId;
    }

    const taskNo = mapping.taskBcNoField ? dataverseTask[mapping.taskBcNoField] : null;
    const taskNoValue = typeof taskNo === "string" && taskNo.trim() ? taskNo.trim() : null;
    let projectNoValue = null;

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
                    const project = await dataverse.getById(mapping.projectEntitySet, projectId, [
                        mapping.projectBcNoField,
                    ]);
                    const projectNo = project?.[mapping.projectBcNoField];
                    projectNoValue = typeof projectNo === "string" && projectNo.trim() ? projectNo.trim() : null;
                } catch (error) {
                    logger.warn("Dataverse project lookup failed", {
                        projectId,
                        error: error?.message || String(error),
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

export async function syncPremiumChanges(options = {}) {
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
    ].filter(Boolean);

    const { value, deltaLink: newDelta } = await dataverse.listChanges(mapping.taskEntitySet, {
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

    const projectCache = new Map();
    for (const item of value) {
        const removed = item["@removed"];
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
                            error: error?.message || String(error),
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
                error: error?.message || String(error),
            });
        }
    }

    if (newDelta) {
        await saveDataverseDeltaLink(mapping.taskEntitySet, newDelta);
        summary.deltaLinkSaved = true;
    }

    return summary;
}

export async function runPremiumChangePoll(options = {}) {
    return syncPremiumChanges(options);
}
