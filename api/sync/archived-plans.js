import { GraphClient } from "../../lib/planner-sync/graph-client.js";
import { getPlannerConfig } from "../../lib/planner-sync/config.js";
import { logger } from "../../lib/planner-sync/logger.js";

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const plannerConfig = getPlannerConfig();
    if (!plannerConfig.groupId) {
        res.status(400).json({ ok: false, error: "PLANNER_GROUP_ID is required" });
        return;
    }

    const url = new URL(req.url, `http://${req.headers.host}`);
    const deep = url.searchParams.get("deep") === "1" || url.searchParams.get("deep") === "true";

    try {
        const graphClient = new GraphClient();
        const plans = await graphClient.listPlansForGroupDetailed(plannerConfig.groupId, {
            useBeta: true,
            select: "id,title,createdDateTime,isArchived",
        });
        const archived = [];
        for (const plan of plans) {
            const flag = plan?.isArchived === true || plan?.archived === true;
            if (flag) {
                archived.push(plan);
            } else if (deep) {
                try {
                    const details = await graphClient.getPlan(plan.id);
                    if (details?.isArchived === true || details?.archived === true) {
                        archived.push(details);
                    }
                } catch (error) {
                    logger.warn("Failed to inspect Planner plan archive state", {
                        planId: plan.id,
                        error: (error && error.message) ? error.message : String(error),
                    });
                }
            }
        }
        const items = archived.map((plan) => ({
            id: plan.id,
            title: plan.title,
            createdDateTime: plan.createdDateTime,
            isArchived: plan.isArchived === true || plan.archived === true,
        }));
        res.status(200).json({
            ok: true,
            groupId: plannerConfig.groupId,
            deep,
            totalPlans: plans.length,
            archivedCount: items.length,
            items,
        });
    } catch (error) {
        logger.error("Failed to list archived Planner plans", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
