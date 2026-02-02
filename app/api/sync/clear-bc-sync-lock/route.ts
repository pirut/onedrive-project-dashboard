import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { logger } from "../../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

function escapeODataString(value: unknown) {
    return String(value || "").replace(/'/g, "''");
}

export async function POST(request: Request) {
    const body = await readJsonBody(request);
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const taskNo = body?.taskNo ? String(body.taskNo).trim() : "";
    const systemId = body?.systemId ? String(body.systemId).trim() : "";
    const plannerTaskId = body?.plannerTaskId ? String(body.plannerTaskId).trim() : "";

    if (!systemId && !(projectNo && taskNo) && !plannerTaskId && !projectNo) {
        return new Response(JSON.stringify({ ok: false, error: "Provide systemId, plannerTaskId, projectNo + taskNo, or projectNo" }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        const bcClient = new BusinessCentralClient();
        let task: Record<string, unknown> | null = null;
        if (projectNo && !taskNo && !systemId && !plannerTaskId) {
            const escapedProject = escapeODataString(projectNo);
            const filter = `projectNo eq '${escapedProject}' and syncLock eq true`;
            const tasks = await bcClient.listProjectTasks(filter);
            const results: Array<Record<string, unknown>> = [];
            for (const entry of tasks as Array<Record<string, unknown>>) {
                const systemIdEntry = entry.systemId ? String(entry.systemId) : "";
                if (!systemIdEntry) {
                    results.push({ taskNo: entry.taskNo || null, ok: false, error: "Missing systemId" });
                    continue;
                }
                try {
                    await bcClient.patchProjectTask(systemIdEntry, { syncLock: false });
                    results.push({ taskNo: entry.taskNo || null, ok: true });
                } catch (err) {
                    results.push({ taskNo: entry.taskNo || null, ok: false, error: (err as Error)?.message || String(err) });
                }
            }
            return new Response(JSON.stringify({
                ok: true,
                projectNo,
                cleared: results.filter((row) => row.ok).length,
                errors: results.filter((row) => !row.ok).length,
                results,
            }, null, 2), {
                status: 200,
                headers: { "Content-Type": "application/json" },
            });
        }
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
