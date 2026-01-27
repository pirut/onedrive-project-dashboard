import { BusinessCentralClient } from "./bc-client.js";
import { logger } from "./logger.js";
import { syncBcToPlanner } from "./sync-engine.js";
import { popBcJobs } from "./bc-webhook-store.js";

function normalizeSystemId(raw) {
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

async function resolveProjectNo(bcClient, job) {
    const entitySet = (job.entitySet || "").trim();
    const systemId = normalizeSystemId(job.systemId || "");
    if (!entitySet || !systemId) return { projectNo: "", skipped: true };

    if (entitySet === "projectTasks") {
        try {
            const task = await bcClient.getProjectTask(systemId);
            if (!task) return { projectNo: "", skipped: true };
            if (task.syncLock) return { projectNo: "", skipped: true };
            const projectNo = (task.projectNo || "").trim();
            return { projectNo, skipped: !projectNo };
        } catch (error) {
            const message = error?.message || String(error);
            if (message.includes("-> 404")) return { projectNo: "", skipped: true };
            throw error;
        }
    }

    if (entitySet === "projects") {
        try {
            const project = await bcClient.getProject(systemId);
            const projectNo = (project?.projectNo || "").trim();
            return { projectNo, skipped: !projectNo };
        } catch (error) {
            const message = error?.message || String(error);
            if (message.includes("-> 404")) return { projectNo: "", skipped: true };
            throw error;
        }
    }

    return { projectNo: "", skipped: true };
}

export async function processBcJobQueue(options = {}) {
    const maxJobs = options.maxJobs ?? 25;
    const requestId = options.requestId || "";
    const jobs = await popBcJobs(maxJobs);
    const projectNos = new Set();
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
            if (result.projectNo) projectNos.add(result.projectNo);
        } catch (error) {
            errors += 1;
            logger.warn("BC webhook job resolution failed", {
                requestId,
                entitySet: job.entitySet,
                systemId: job.systemId,
                error: error?.message || String(error),
            });
        }
    }

    let processed = 0;
    for (const projectNo of projectNos) {
        try {
            await syncBcToPlanner(projectNo);
            processed += 1;
        } catch (error) {
            errors += 1;
            logger.warn("BC webhook sync failed", {
                requestId,
                projectNo,
                error: error?.message || String(error),
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
