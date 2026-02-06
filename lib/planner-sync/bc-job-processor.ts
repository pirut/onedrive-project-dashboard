import { BusinessCentralClient } from "./bc-client.js";
import { logger } from "./logger.js";
import { syncBcToPremium } from "../premium-sync/index.js";
import { popBcJobs, BcWebhookJob } from "./bc-webhook-store.js";

export type BcJobProcessSummary = {
    jobs: number;
    projects: number;
    processed: number;
    skipped: number;
    errors: number;
    projectNos: string[];
    skipReasons?: Record<string, number>;
};

type ResolveResult = {
    projectNo: string;
    systemId: string;
    skipped: boolean;
    reason?: string;
    queueEntryId?: string;
    forceFullSync?: boolean;
};

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

function normalizeSystemId(raw: string) {
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

function parseBool(raw: string | undefined | null, fallback: boolean) {
    if (raw == null || raw === "") return fallback;
    const normalized = String(raw).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return fallback;
}

const QUEUE_ENTITY_SET = (process.env.BC_SYNC_QUEUE_ENTITY_SET || "premiumSyncQueue").trim();
const QUEUE_PROJECT_NO_FIELD = (process.env.BC_SYNC_QUEUE_PROJECTNO_FIELD || "projectNo").trim();
const QUEUE_TASK_SYSTEM_ID_FIELD = (process.env.BC_SYNC_QUEUE_TASKSYSTEMID_FIELD || "projectTaskSystemId").trim();
const PROCESS_QUEUE_ONLY = parseBool(
    process.env.BC_WEBHOOK_PROCESS_QUEUE_ONLY,
    Boolean(QUEUE_ENTITY_SET && QUEUE_ENTITY_SET.toLowerCase() !== "projecttasks")
);
const QUEUE_FORCE_FULL_SYNC = parseBool(process.env.BC_QUEUE_FORCE_FULL_SYNC, true);

async function resolveProjectNo(bcClient: BusinessCentralClient, job: BcWebhookJob): Promise<ResolveResult> {
    const entitySet = (job.entitySet || "").trim();
    const systemId = normalizeSystemId(job.systemId || "");
    if (!entitySet || !systemId) return { projectNo: "", systemId: "", skipped: true, reason: "missing_resource" };
    if (PROCESS_QUEUE_ONLY && QUEUE_ENTITY_SET && entitySet.toLowerCase() !== QUEUE_ENTITY_SET.toLowerCase()) {
        return { projectNo: "", systemId: "", skipped: true, reason: "queue_only_entity" };
    }

    if (QUEUE_ENTITY_SET && entitySet.toLowerCase() === QUEUE_ENTITY_SET.toLowerCase()) {
        try {
            const record = await bcClient.getEntity(QUEUE_ENTITY_SET, systemId);
            if (!record) return { projectNo: "", systemId: "", skipped: true, reason: "not_found" };
            const projectNoRaw = record?.[QUEUE_PROJECT_NO_FIELD];
            const projectNo = typeof projectNoRaw === "string" ? projectNoRaw.trim() : String(projectNoRaw || "").trim();
            const taskSystemRaw = record?.[QUEUE_TASK_SYSTEM_ID_FIELD];
            const taskSystemId = normalizeSystemId(
                typeof taskSystemRaw === "string" ? taskSystemRaw : String(taskSystemRaw || "")
            );
            const forceFullSync = QUEUE_FORCE_FULL_SYNC || !taskSystemId;
            return {
                projectNo,
                systemId: taskSystemId,
                skipped: !projectNo,
                reason: projectNo ? undefined : "queue_missing_project_no",
                queueEntryId: systemId,
                forceFullSync,
            };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("-> 404")) {
                return { projectNo: "", systemId: "", skipped: true, reason: "not_found" };
            }
            throw error;
        }
    }

    if (entitySet === "projectTasks") {
        try {
            const task = await bcClient.getProjectTask(systemId);
            if (!task) return { projectNo: "", systemId: "", skipped: true, reason: "not_found" };
            if (task.syncLock) return { projectNo: "", systemId: "", skipped: true, reason: "locked" };
            const graceMs = Number(process.env.SYNC_BC_MODIFIED_GRACE_MS || 0);
            if (graceMs > 0) {
                const lastSyncMs = task.lastSyncAt ? Date.parse(task.lastSyncAt) : Number.NaN;
                const modifiedMs = Date.parse(
                    task.systemModifiedAt || task.lastModifiedDateTime || task.modifiedAt || ""
                );
                if (Number.isFinite(lastSyncMs) && Number.isFinite(modifiedMs) && modifiedMs - lastSyncMs <= graceMs) {
                    return { projectNo: "", systemId: "", skipped: true, reason: "grace" };
                }
            }
            const projectNo = (task.projectNo || "").trim();
            return {
                projectNo,
                systemId: normalizeSystemId(task.systemId || systemId),
                skipped: !projectNo,
                reason: projectNo ? undefined : "missing_project_no",
            };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("-> 404")) {
                return { projectNo: "", systemId: "", skipped: true, reason: "not_found" };
            }
            throw error;
        }
    }

    if (entitySet === "projects") {
        try {
            const project = await bcClient.getProject(systemId);
            const projectNo = (project?.projectNo || "").trim();
            return {
                projectNo,
                systemId: "",
                skipped: !projectNo,
                reason: projectNo ? undefined : "missing_project_no",
                forceFullSync: Boolean(projectNo),
            };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("-> 404")) {
                return { projectNo: "", systemId: "", skipped: true, reason: "not_found" };
            }
            throw error;
        }
    }

    return { projectNo: "", systemId: "", skipped: true, reason: "unsupported_entity" };
}

export async function processBcJobQueue(options: { maxJobs?: number; requestId?: string } = {}): Promise<BcJobProcessSummary> {
    const maxJobs = options.maxJobs ?? 25;
    const requestId = options.requestId || "";
    const jobs = await popBcJobs(maxJobs);
    const projectNos = new Set<string>();
    const taskIdsByProject = new Map<string, Set<string>>();
    const fullSyncProjects = new Set<string>();
    const queueEntriesByProject = new Map<string, Array<{ queueEntryId: string; projectNo: string; taskSystemId: string }>>();
    let skipped = 0;
    let errors = 0;
    const skipReasons: Record<string, number> = {};
    const bumpReason = (reason?: string) => {
        const key = reason || "skipped";
        skipReasons[key] = (skipReasons[key] || 0) + 1;
    };

    if (!jobs.length) {
        return { jobs: 0, projects: 0, processed: 0, skipped: 0, errors: 0, projectNos: [], skipReasons: {} };
    }

    const bcClient = new BusinessCentralClient();
    for (const job of jobs) {
        try {
            const result = await resolveProjectNo(bcClient, job);
            if (result.skipped) {
                skipped += 1;
                bumpReason(result.reason);
                continue;
            }
            if (result.projectNo) {
                projectNos.add(result.projectNo);
                if (result.forceFullSync) {
                    fullSyncProjects.add(result.projectNo);
                }
                if (result.systemId) {
                    const set = taskIdsByProject.get(result.projectNo) || new Set<string>();
                    set.add(result.systemId);
                    taskIdsByProject.set(result.projectNo, set);
                }
                if (result.queueEntryId) {
                    const list = queueEntriesByProject.get(result.projectNo) || [];
                    if (!list.some((entry) => entry.queueEntryId === result.queueEntryId)) {
                        list.push({
                            queueEntryId: result.queueEntryId,
                            projectNo: result.projectNo,
                            taskSystemId: result.systemId,
                        });
                    }
                    queueEntriesByProject.set(result.projectNo, list);
                }
            }
        } catch (error) {
            errors += 1;
            bumpReason("error");
            logger.warn("BC webhook job resolution failed", {
                requestId,
                entitySet: job.entitySet,
                systemId: job.systemId,
                error: (error as Error)?.message,
            });
        }
    }

    let processed = 0;
    const projectConcurrency = Math.max(1, Math.floor(Number(process.env.BC_JOB_PROJECT_CONCURRENCY || 3)));
    const projectList = Array.from(projectNos);
    const projectOutcomes = await runWithConcurrency(projectList, projectConcurrency, async (projectNo) => {
        const taskIds = taskIdsByProject.get(projectNo) || new Set<string>();
        const forceFullSync = fullSyncProjects.has(projectNo);
        if (!taskIds.size && !forceFullSync) {
            return { processed: false, errors: 0 };
        }
        try {
            const syncResult = await syncBcToPremium(projectNo, {
                requestId,
                taskSystemIds: taskIds.size && !forceFullSync ? Array.from(taskIds) : undefined,
                queueEntries: queueEntriesByProject.get(projectNo) || [],
                skipProjectAccess: true,
                taskOnly: false,
                // Webhook jobs are BC-originated changes and should not be dropped by "planner newer" arbitration.
                preferPlanner: false,
            });
            if ((syncResult.errors || 0) > 0) {
                logger.warn("BC webhook sync completed with task errors", {
                    requestId,
                    projectNo,
                    errors: syncResult.errors,
                    created: syncResult.created,
                    updated: syncResult.updated,
                    skipped: syncResult.skipped,
                });
                return { processed: false, errors: syncResult.errors || 1 };
            }
            return { processed: true, errors: 0 };
        } catch (error) {
            logger.warn("BC webhook sync failed", {
                requestId,
                projectNo,
                error: (error as Error)?.message,
            });
            return { processed: false, errors: 1 };
        }
    });
    for (const outcome of projectOutcomes) {
        if (outcome.processed) processed += 1;
        if (outcome.errors) errors += outcome.errors;
    }

    return {
        jobs: jobs.length,
        projects: projectNos.size,
        processed,
        skipped,
        errors,
        projectNos: Array.from(projectNos),
        skipReasons,
    };
}
