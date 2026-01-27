import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

const DEFAULT_BUCKET_NAME = "General";
const HEADING_TASK_BUCKETS = new Map([
    [1000, "Pre-Construction"],
    [2000, "Installation"],
    [3000, null],
    [4000, "Change Orders"],
]);

function normalizeBucketName(name) {
    const trimmed = (name || "").trim();
    return trimmed || DEFAULT_BUCKET_NAME;
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

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const includePremium = req.url?.includes("includePremium=1");

    const body = await readJsonBody(req);
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const taskNo = body?.taskNo ? String(body.taskNo).trim() : "";

    if (!projectNo || !taskNo) {
        res.status(400).json({ ok: false, error: "projectNo and taskNo are required" });
        return;
    }

    const bcClient = new BusinessCentralClient();
    const filter = `projectNo eq '${projectNo.replace(/'/g, "''")}'`;

    try {
        const tasks = await bcClient.listProjectTasks(filter);
        const orderedTasks = [...tasks].sort((a, b) => {
            const aKey = (a.taskNo || "").toString();
            const bKey = (b.taskNo || "").toString();
            return aKey.localeCompare(bKey, undefined, { numeric: true, sensitivity: "base" });
        });

        let currentBucket = DEFAULT_BUCKET_NAME;
        let skipSection = false;
        let currentHeading = null;
        const normalizedTaskNo = taskNo.toLowerCase();
        let targetInfo = null;

        for (const task of orderedTasks) {
            const taskType = (task.taskType || "").toLowerCase();
            const taskNoValue = (task.taskNo || "").toString().trim().toLowerCase();
            const isTarget = taskNoValue === normalizedTaskNo;

            if (taskType === "heading") {
                currentHeading = (task.description || "").trim() || null;
                const resolved = resolveBucketFromHeading(task.taskNo, task.description);
                if (resolved.skip) {
                    currentBucket = null;
                    skipSection = true;
                } else {
                    currentBucket = resolved.bucket;
                    skipSection = false;
                }

                if (isTarget) {
                    targetInfo = {
                        task,
                        heading: currentHeading,
                        bucket: currentBucket,
                        skipSection,
                        reasons: ["taskType is heading"],
                    };
                    break;
                }
                continue;
            }

            if (isTarget) {
                const reasons = [];
                if (taskType !== "posting") reasons.push(`taskType is ${task.taskType || "unknown"}`);
                if (skipSection) reasons.push(`skipped heading ${currentHeading || "unknown"}`);
                if (!currentBucket) reasons.push("no bucket resolved");
                if (task.syncLock) reasons.push("syncLock is true");
                targetInfo = {
                    task,
                    heading: currentHeading,
                    bucket: currentBucket,
                    skipSection,
                    reasons,
                };
                break;
            }
        }

        if (!targetInfo) {
            res.status(404).json({
                ok: false,
                error: "Task not found in project tasks",
                projectNo,
                taskNo,
                tasks: orderedTasks.length,
            });
            return;
        }

        const reasons = targetInfo.reasons || [];
        let premiumTask = null;
        if (includePremium && targetInfo.task.plannerTaskId) {
            try {
                const { DataverseClient } = await import("../../lib/dataverse-client.js");
                const { getDataverseMappingConfig } = await import("../../lib/premium-sync/config.js");
                const mapping = getDataverseMappingConfig();
                const dataverse = new DataverseClient();
                premiumTask = await dataverse.getById(mapping.taskEntitySet, targetInfo.task.plannerTaskId);
            } catch (error) {
                logger.warn("Premium task lookup failed in debug", {
                    taskId: targetInfo.task.plannerTaskId,
                    error: error?.message || String(error),
                });
            }
        }
        const bcModified =
            targetInfo.task.systemModifiedAt ||
            targetInfo.task.lastModifiedDateTime ||
            targetInfo.task.lastModifiedAt ||
            targetInfo.task.modifiedAt ||
            targetInfo.task.modifiedOn ||
            targetInfo.task.lastModifiedOn ||
            targetInfo.task.systemModifiedOn ||
            null;
        const lastSyncAt = targetInfo.task.lastSyncAt || null;
        const premiumModifiedAt = premiumTask?.modifiedon || premiumTask?.modifiedOn || premiumTask?.lastModifiedDateTime || null;
        const lastPlannerEtag = targetInfo.task.lastPlannerEtag || null;
        const premiumEtag = premiumTask?.["@odata.etag"] || null;
        const bcModifiedMs = bcModified ? Date.parse(String(bcModified)) : NaN;
        const lastSyncMs = lastSyncAt ? Date.parse(String(lastSyncAt)) : NaN;
        const premiumModifiedMs = premiumModifiedAt ? Date.parse(String(premiumModifiedAt)) : NaN;
        const bcChangedSinceSync =
            Number.isNaN(bcModifiedMs) || Number.isNaN(lastSyncMs) ? null : bcModifiedMs > lastSyncMs;
        const premiumEtagChanged = premiumEtag && lastPlannerEtag ? premiumEtag !== lastPlannerEtag : null;
        const premiumChangedSinceSync = Number.isNaN(lastSyncMs)
            ? premiumEtagChanged
            : Number.isNaN(premiumModifiedMs)
                ? premiumEtagChanged
                : premiumModifiedMs > lastSyncMs;

        res.status(200).json({
            ok: true,
            projectNo,
            taskNo,
            tasks: orderedTasks.length,
            target: {
                taskNo: targetInfo.task.taskNo,
                description: targetInfo.task.description,
                taskType: targetInfo.task.taskType,
                projectNo: targetInfo.task.projectNo,
                systemId: targetInfo.task.systemId,
                plannerTaskId: targetInfo.task.plannerTaskId,
                plannerPlanId: targetInfo.task.plannerPlanId,
                syncLock: targetInfo.task.syncLock,
                assignedPerson: targetInfo.task.assignedPersonCode,
                assignedPersonName: targetInfo.task.assignedPersonName,
                manualStartDate: targetInfo.task.manualStartDate,
                manualEndDate: targetInfo.task.manualEndDate,
                startDate: targetInfo.task.startDate,
                endDate: targetInfo.task.endDate,
                budgetTotalCost: targetInfo.task.budgetTotalCost,
                actualTotalCost: targetInfo.task.actualTotalCost,
                systemModifiedAt: targetInfo.task.systemModifiedAt,
                lastSyncAt: targetInfo.task.lastSyncAt,
                lastPlannerEtag: targetInfo.task.lastPlannerEtag,
            },
            context: {
                heading: targetInfo.heading,
                bucket: targetInfo.bucket,
                skipSection: targetInfo.skipSection,
            },
            timestamps: {
                bcModifiedAt: bcModified,
                lastSyncAt,
                bcChangedSinceSync,
                premiumModifiedAt,
                premiumChangedSinceSync,
                premiumEtagChanged,
            },
            premium: premiumTask
                ? {
                      id: premiumTask.msdyn_projecttaskid || premiumTask.id,
                      title: premiumTask.msdyn_subject || premiumTask.subject || premiumTask.title,
                      percentComplete: premiumTask.msdyn_percentcomplete ?? premiumTask.percentComplete,
                      startDate: premiumTask.msdyn_start || premiumTask.startDateTime || premiumTask.startDate,
                      endDate: premiumTask.msdyn_finish || premiumTask.dueDateTime || premiumTask.endDate,
                      modifiedOn: premiumTask.modifiedon || premiumTask.modifiedOn || premiumTask.lastModifiedDateTime,
                      etag: premiumTask["@odata.etag"],
                      lastPlannerEtag: targetInfo.task.lastPlannerEtag,
                  }
                : null,
            decision: {
                willSync: reasons.length === 0,
                reasons,
            },
        });
    } catch (error) {
        logger.error("BC task debug failed", { projectNo, taskNo, error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
