import { GraphClient } from "../../lib/planner-sync/graph-client.js";
import { getPlannerConfig } from "../../lib/planner-sync/config.js";
import { logger } from "../../lib/planner-sync/logger.js";

async function resolvePlanId(graphClient, planIdParam) {
    const trimmed = (planIdParam || "").toString().trim();
    if (trimmed) return trimmed;
    const plannerConfig = getPlannerConfig();
    if (plannerConfig.defaultPlanId) return plannerConfig.defaultPlanId;
    try {
        const plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
        return plans[0]?.id || null;
    } catch (error) {
        logger.warn("Planner delta test failed to resolve planId", {
            error: error?.message || String(error),
        });
        return null;
    }
}

async function collectDeltaSummary(graphClient, planId, selectOverride) {
    let nextLink = null;
    let deltaLink = null;
    let count = 0;
    let firstId = null;
    let pageCount = 0;

    while (true) {
        const page =
            pageCount === 0 && selectOverride
                ? await graphClient.listPlannerPlanTasksDeltaWithSelect(planId, selectOverride)
                : await graphClient.listPlannerPlanTasksDelta(planId, nextLink || undefined);
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
        const selectOverride = (req.query.select || "").toString().trim() || undefined;
        const planId = await resolvePlanId(graphClient, req.query.planId);
        if (!planId) {
            res.status(400).json({
                ok: false,
                error: "No planId resolved from query, default plan, or group.",
                durationMs: Date.now() - startTime,
            });
            return;
        }
        const summary = await collectDeltaSummary(graphClient, planId, selectOverride);
        res.status(200).json({
            ok: true,
            durationMs: Date.now() - startTime,
            planId,
            select: selectOverride || null,
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
