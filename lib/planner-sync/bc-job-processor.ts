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

async function resolveProjectNo(bcClient: BusinessCentralClient, job: BcWebhookJob) {
    const entitySet = (job.entitySet || "").trim();
    const systemId = normalizeSystemId(job.systemId || "");
    if (!entitySet || !systemId) return { projectNo: "", systemId: "", skipped: true };

    if (entitySet === "projectTasks") {
        try {
            const task = await bcClient.getProjectTask(systemId);
            if (!task) return { projectNo: "", systemId: "", skipped: true };
            if (task.syncLock) return { projectNo: "", systemId: "", skipped: true };
            const projectNo = (task.projectNo || "").trim();
            return { projectNo, systemId: normalizeSystemId(task.systemId || systemId), skipped: !projectNo };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("-> 404")) return { projectNo: "", systemId: "", skipped: true };
            throw error;
        }
    }

    if (entitySet === "projects") {
        try {
            const project = await bcClient.getProject(systemId);
            const projectNo = (project?.projectNo || "").trim();
            return { projectNo, systemId: "", skipped: !projectNo };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (message.includes("-> 404")) return { projectNo: "", systemId: "", skipped: true };
            throw error;
        }
    }

    return { projectNo: "", systemId: "", skipped: true };
}

export async function processBcJobQueue(options: { maxJobs?: number; requestId?: string } = {}): Promise<BcJobProcessSummary> {
    const maxJobs = options.maxJobs ?? 25;
    const requestId = options.requestId || "";
    const jobs = await popBcJobs(maxJobs);
    const projectNos = new Set<string>();
    const taskIdsByProject = new Map<string, Set<string>>();
    let skipped = 0;
    let errors = 0;

    if (!jobs.length) {
        return { jobs: 0, projects: 0, processed: 0, skipped: 0, errors: 0, projectNos: [] };
    }

    const bcClient = new BusinessCentralClient();
    for (const job of jobs) {
        try {
            const result = await resolveProjectNo(bcClient, job);
            if (result.skipped) {
                skipped += 1;
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
    };
}
