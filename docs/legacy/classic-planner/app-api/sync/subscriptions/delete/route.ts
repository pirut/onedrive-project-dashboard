import { GraphClient } from "../../../../../lib/planner-sync/graph-client";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

async function readBody(request: Request) {
    try {
        const text = await request.text();
        if (!text) return null;
        return JSON.parse(text);
    } catch {
        return null;
    }
}

export async function POST(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    logger.info("POST /api/sync/subscriptions/delete - Request received", {
        requestId,
        url: url.toString(),
        pathname: url.pathname,
        searchParams: Object.fromEntries(url.searchParams),
    });

    try {
        const body = await readBody(request);
        const ids = Array.isArray(body?.ids) ? body.ids.map((id: unknown) => String(id).trim()).filter(Boolean) : [];
        const deleteAll = Boolean(body?.all);

        const graphClient = new GraphClient();
        const allSubs = await graphClient.listSubscriptions();
        const plannerSubs = allSubs.filter((sub) => (sub.resource || "").includes("/planner/"));

        const targetSubs = deleteAll
            ? plannerSubs
            : plannerSubs.filter((sub) => ids.includes(sub.id));

        const deleted: string[] = [];
        const failed: { id: string; error: string }[] = [];

        for (const sub of targetSubs) {
            try {
                await graphClient.deleteSubscription(sub.id);
                deleted.push(sub.id);
            } catch (error) {
                failed.push({ id: sub.id, error: error instanceof Error ? error.message : String(error) });
            }
        }

        const duration = Date.now() - startTime;
        logger.info("POST /api/sync/subscriptions/delete - Completed", {
            requestId,
            duration,
            deletedCount: deleted.length,
            failedCount: failed.length,
        });

        return new Response(JSON.stringify({
            ok: failed.length === 0,
            deleted,
            failed,
            totalPlanner: plannerSubs.length,
            requestId,
            duration,
        }, null, 2), {
            status: failed.length ? 207 : 200,
            headers: {
                "Content-Type": "application/json",
                "X-Request-ID": requestId,
            },
        });
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/subscriptions/delete - Failed", {
            requestId,
            duration,
            error: errorMessage,
        });
        return new Response(JSON.stringify({
            ok: false,
            error: errorMessage,
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
