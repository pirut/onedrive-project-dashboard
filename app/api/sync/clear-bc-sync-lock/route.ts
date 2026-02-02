import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { logger } from "../../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

export async function POST(request: Request) {
    const body = await readJsonBody(request);
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const taskNo = body?.taskNo ? String(body.taskNo).trim() : "";
    const systemId = body?.systemId ? String(body.systemId).trim() : "";
    const plannerTaskId = body?.plannerTaskId ? String(body.plannerTaskId).trim() : "";

    if (!systemId && !(projectNo && taskNo) && !plannerTaskId) {
        return new Response(JSON.stringify({ ok: false, error: "Provide systemId, plannerTaskId, or projectNo + taskNo" }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        const bcClient = new BusinessCentralClient();
        let task: Record<string, unknown> | null = null;
        if (systemId) {
            task = { systemId };
        } else if (plannerTaskId) {
            task = await bcClient.findProjectTaskByPlannerTaskId(plannerTaskId) as Record<string, unknown> | null;
        } else {
            task = await bcClient.findProjectTaskByProjectAndTaskNo(projectNo, taskNo) as Record<string, unknown> | null;
        }

        if (!task || !task.systemId) {
            return new Response(JSON.stringify({ ok: false, error: "BC task not found" }, null, 2), {
                status: 404,
                headers: { "Content-Type": "application/json" },
            });
        }

        await bcClient.patchProjectTask(String(task.systemId), { syncLock: false });
        return new Response(JSON.stringify({
            ok: true,
            systemId: task.systemId,
            projectNo: task.projectNo || projectNo || null,
            taskNo: task.taskNo || taskNo || null,
            plannerTaskId: task.plannerTaskId || plannerTaskId || null,
            cleared: true,
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Clear BC sync lock failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}
