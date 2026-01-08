import { runPollingSync } from "../../../../lib/planner-sync";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function POST() {
    try {
        const result = await runPollingSync();
        return new Response(JSON.stringify({ ok: true, result }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Polling sync failed", { error: (error as Error)?.message });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}
