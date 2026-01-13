import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { getPlannerConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

type DeltaSummary = {
    count: number;
    firstId: string | null;
    pageCount: number;
    hasDeltaLink: boolean;
};

async function resolvePlanId(graphClient: GraphClient, planIdParam: string | null) {
    const trimmed = (planIdParam || "").trim();
    if (trimmed) return trimmed;
    const plannerConfig = getPlannerConfig();
    if (plannerConfig.defaultPlanId) return plannerConfig.defaultPlanId;
    try {
        const plans = await graphClient.listPlansForGroup(plannerConfig.groupId);
        return plans[0]?.id || null;
    } catch (error) {
        logger.warn("Planner delta test failed to resolve planId", {
            error: error instanceof Error ? error.message : String(error),
        });
        return null;
    }
}

async function collectDeltaSummary(
    graphClient: GraphClient,
    planId: string,
    selectOverride?: string
): Promise<DeltaSummary> {
    let nextLink: string | null = null;
    let deltaLink: string | null = null;
    let count = 0;
    let firstId: string | null = null;
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

export async function GET(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();
    const selectOverride = (url.searchParams.get("select") || "").trim() || undefined;
    const planIdParam = url.searchParams.get("planId");

    logger.info("GET /api/sync/planner-delta-test - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const graphClient = new GraphClient();
        const planId = await resolvePlanId(graphClient, planIdParam);
        if (!planId) {
            return new Response(JSON.stringify({
                ok: false,
                error: "No planId resolved from query, default plan, or group.",
                requestId,
            }, null, 2), {
                status: 400,
                headers: {
                    "Content-Type": "application/json",
                    "X-Request-ID": requestId,
                },
            });
        }

        const summary = await collectDeltaSummary(graphClient, planId, selectOverride);
        const duration = Date.now() - startTime;

        logger.info("GET /api/sync/planner-delta-test - Completed", {
            requestId,
            duration,
            count: summary.count,
            pageCount: summary.pageCount,
        });

        return new Response(JSON.stringify({
            ok: true,
            requestId,
            duration,
            planId,
            select: selectOverride || null,
            ...summary,
        }, null, 2), {
            status: 200,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const message = error instanceof Error ? error.message : String(error);
        logger.error("GET /api/sync/planner-delta-test - Failed", {
            requestId,
            duration,
            error: message,
        });

        return new Response(JSON.stringify({
            ok: false,
            error: message,
            requestId,
            duration,
        }, null, 2), {
            status: 500,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    }
}

export async function POST() {
    return new Response(JSON.stringify({ ok: false, error: "Method not allowed. Use GET." }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
    });
}
