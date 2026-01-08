import { syncBcToPlanner } from "../../../../lib/planner-sync";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST(request: Request) {
    let body: { projectNo?: string } | null = null;
    try {
        body = await request.json();
    } catch {
        body = null;
    }
    const projectNo = body?.projectNo?.trim();

    try {
        const result = await syncBcToPlanner(projectNo || undefined);
        return new Response(JSON.stringify({ ok: true, result }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("BC to Planner sync failed", { error: (error as Error)?.message });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
