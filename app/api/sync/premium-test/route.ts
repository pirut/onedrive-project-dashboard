import { DataverseClient } from "../../../../lib/dataverse-client";
import { getDataverseMappingConfig } from "../../../../lib/premium-sync/config";
import { logger } from "../../../../lib/planner-sync/logger";

export async function GET() {
    try {
        const dataverse = new DataverseClient();
        const mapping = getDataverseMappingConfig();
        const who = await dataverse.whoAmI();
        return new Response(JSON.stringify({ ok: true, whoAmI: who, mapping }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Premium test failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
