import { GraphClient } from "../../lib/planner-sync/graph-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

async function collectDeltaSummary(graphClient) {
    let nextLink = null;
    let deltaLink = null;
    let count = 0;
    let firstId = null;
    let pageCount = 0;

    while (true) {
        const page = await graphClient.listPlannerTasksDelta(nextLink || undefined);
        pageCount += 1;
        const values = page?.value || [];
        if (!firstId && values.length) {
            firstId = values[0]?.id || null;
        }
        count += values.length;
        if (page?.nextLink) {
            nextLink = page.nextLink;
            continue;
        }
        deltaLink = page?.deltaLink || null;
        break;
    }

    return {
        count,
        firstId,
        pageCount,
        hasDeltaLink: Boolean(deltaLink),
    };
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const startTime = Date.now();
    try {
        const graphClient = new GraphClient();
        const summary = await collectDeltaSummary(graphClient);
        res.status(200).json({
            ok: true,
            durationMs: Date.now() - startTime,
            ...summary,
        });
    } catch (error) {
        const message = error?.message || String(error);
        logger.error("GET /api/sync/planner-delta-test - Failed", { error: message });
        res.status(500).json({
            ok: false,
            error: message,
            durationMs: Date.now() - startTime,
        });
    }
}
