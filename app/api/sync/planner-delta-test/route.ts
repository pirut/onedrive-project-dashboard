import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

type DeltaSummary = {
    count: number;
    firstId: string | null;
    pageCount: number;
    hasDeltaLink: boolean;
};

async function collectDeltaSummary(graphClient: GraphClient, selectOverride?: string): Promise<DeltaSummary> {
    let nextLink: string | null = null;
    let deltaLink: string | null = null;
    let count = 0;
    let firstId: string | null = null;
    let pageCount = 0;

    while (true) {
        const page =
            pageCount === 0 && selectOverride
                ? await graphClient.listPlannerTasksDeltaWithSelect(selectOverride)
                : await graphClient.listPlannerTasksDelta(nextLink || undefined);
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

    logger.info("GET /api/sync/planner-delta-test - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const graphClient = new GraphClient();
        const summary = await collectDeltaSummary(graphClient, selectOverride);
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
