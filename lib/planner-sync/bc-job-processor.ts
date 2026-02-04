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
};

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

async function resolveProjectNo(bcClient: BusinessCentralClient, job: BcWebhookJob): Promise<ResolveResult> {
    const entitySet = (job.entitySet || "").trim();
    const systemId = normalizeSystemId(job.systemId || "");
    if (!entitySet || !systemId) return { projectNo: "", systemId: "", skipped: true, reason: "missing_resource" };

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
                if (result.systemId) {
                    const set = taskIdsByProject.get(result.projectNo) || new Set<string>();
                    set.add(result.systemId);
                    taskIdsByProject.set(result.projectNo, set);
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
    for (const projectNo of projectNos) {
        const taskIds = taskIdsByProject.get(projectNo) || new Set<string>();
        if (!taskIds.size) {
            continue;
        }
        try {
            await syncBcToPremium(projectNo, {
                requestId,
                taskSystemIds: Array.from(taskIds),
                skipProjectAccess: true,
                taskOnly: true,
            });
            processed += 1;
        } catch (error) {
            errors += 1;
            logger.warn("BC webhook sync failed", {
                requestId,
                projectNo,
                error: (error as Error)?.message,
            });
        }
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
