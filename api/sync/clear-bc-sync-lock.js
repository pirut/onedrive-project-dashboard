import "../../lib/planner-sync/bootstrap.js";
import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

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

function escapeODataString(value) {
    return String(value || "").replace(/'/g, "''");
}

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const body = await readJsonBody(req);
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const taskNo = body?.taskNo ? String(body.taskNo).trim() : "";
    const systemId = body?.systemId ? String(body.systemId).trim() : "";
    const plannerTaskId = body?.plannerTaskId ? String(body.plannerTaskId).trim() : "";

    if (!systemId && !(projectNo && taskNo) && !plannerTaskId && !projectNo) {
        res.status(400).json({ ok: false, error: "Provide systemId, plannerTaskId, projectNo + taskNo, or projectNo" });
        return;
    }

    try {
        const bcClient = new BusinessCentralClient();
        let task = null;
        if (projectNo && !taskNo && !systemId && !plannerTaskId) {
            const escapedProject = escapeODataString(projectNo);
            const filter = `projectNo eq '${escapedProject}' and syncLock eq true`;
            const tasks = await bcClient.listProjectTasks(filter);
            const results = [];
            for (const entry of tasks) {
                if (!entry.systemId) {
                    results.push({ taskNo: entry.taskNo || null, ok: false, error: "Missing systemId" });
                    continue;
                }
                try {
                    await bcClient.patchProjectTask(entry.systemId, { syncLock: false });
                    results.push({ taskNo: entry.taskNo || null, ok: true });
                } catch (err) {
                    results.push({ taskNo: entry.taskNo || null, ok: false, error: err?.message || String(err) });
                }
            }
            res.status(200).json({
                ok: true,
                projectNo,
                cleared: results.filter((row) => row.ok).length,
                errors: results.filter((row) => !row.ok).length,
                results,
            });
            return;
        }
        if (systemId) {
            task = { systemId };
        } else if (plannerTaskId) {
            task = await bcClient.findProjectTaskByPlannerTaskId(plannerTaskId);
        } else {
            task = await bcClient.findProjectTaskByProjectAndTaskNo(projectNo, taskNo);
        }

        if (!task || !task.systemId) {
            res.status(404).json({ ok: false, error: "BC task not found" });
            return;
        }

        await bcClient.patchProjectTask(task.systemId, { syncLock: false });
        res.status(200).json({
            ok: true,
            systemId: task.systemId,
            projectNo: task.projectNo || projectNo || null,
            taskNo: task.taskNo || taskNo || null,
            plannerTaskId: task.plannerTaskId || plannerTaskId || null,
            cleared: true,
        });
    } catch (error) {
        logger.error("Clear BC sync lock failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
