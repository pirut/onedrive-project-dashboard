import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const BC_MODIFIED_FIELDS = [
    "systemModifiedAt",
    "lastModifiedDateTime",
    "lastModifiedAt",
    "modifiedAt",
    "modifiedOn",
    "lastModifiedOn",
    "systemModifiedOn",
] as const;

function parseNumber(value: string | null, fallback: number) {
    const num = Number(value);
    if (!Number.isFinite(num)) return fallback;
    return num;
}

export async function GET(request: Request) {
    const url = new URL(request.url);
    const limit = Math.max(1, Math.min(200, parseNumber(url.searchParams.get("limit"), 25)));
    const includeAll = url.searchParams.get("all") === "1";
    const bcClient = new BusinessCentralClient();

    try {
        const tasks = await bcClient.listProjectTasks(includeAll ? undefined : "plannerTaskId ne ''");
        const sample = tasks.slice(0, limit);

        const foundFields = BC_MODIFIED_FIELDS.filter((field) => sample.some((task) => (task as Record<string, unknown>)?.[field] != null));
        const sampleItems = sample.map((task) => {
            const modified: Record<string, unknown> = {};
            for (const field of BC_MODIFIED_FIELDS) {
                const value = (task as Record<string, unknown>)[field];
                if (value != null) {
                    modified[field] = value;
                }
            }
            return {
                projectNo: task.projectNo,
                taskNo: task.taskNo,
                systemId: task.systemId,
                lastSyncAt: task.lastSyncAt,
                modified,
            };
        });

        return new Response(
            JSON.stringify({
                ok: true,
                filter: includeAll ? "all tasks" : "plannerTaskId ne ''",
                total: tasks.length,
                sampleCount: sample.length,
                foundFields,
                sample: sampleItems,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } }
        );
    } catch (error) {
        logger.error("BC timestamp debug failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function POST() {
    return new Response(JSON.stringify({ ok: false, error: "Method not allowed" }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
    });
}
