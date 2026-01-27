import { GraphClient } from "../../../../lib/planner-sync/graph-client";
import { getPlannerConfig } from "../../../../lib/planner-sync/config";
import { logger } from "../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
    const url = new URL(request.url);
    const deep = url.searchParams.get("deep") === "1" || url.searchParams.get("deep") === "true";
    const plannerConfig = getPlannerConfig();
    if (!plannerConfig.groupId) {
        return new Response(JSON.stringify({ ok: false, error: "PLANNER_GROUP_ID is required" }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

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
                        error: error instanceof Error ? error.message : String(error),
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
        return new Response(
            JSON.stringify({
                ok: true,
                groupId: plannerConfig.groupId,
                deep,
                totalPlans: plans.length,
                archivedCount: items.length,
                items,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } }
        );
    } catch (error) {
        logger.error("Failed to list archived Planner plans", {
            error: error instanceof Error ? error.message : String(error),
        });
        return new Response(JSON.stringify({ ok: false, error: error instanceof Error ? error.message : String(error) }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
