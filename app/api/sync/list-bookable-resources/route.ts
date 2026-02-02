import { DataverseClient } from "../../../../../lib/dataverse-client";
import { logger } from "../../../../../lib/planner-sync/logger";

function parseNumber(value: unknown, fallback: number) {
    if (value == null || value === "") return fallback;
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
}

async function handle(request: Request) {
    const url = new URL(request.url);
    const pageSize = Math.max(1, Math.min(500, parseNumber(url.searchParams.get("pageSize"), 200)));
    const maxPages = Math.max(1, Math.min(20, parseNumber(url.searchParams.get("maxPages"), 5)));
    const limit = Math.max(1, Math.min(5000, parseNumber(url.searchParams.get("limit"), pageSize * maxPages)));

    try {
        const dataverse = new DataverseClient();
        const items: Array<{ id: string | null; name: string | null }> = [];
        let nextLink: string | null = null;
        let pages = 0;

        while (pages < maxPages && items.length < limit) {
            const path = nextLink || `/bookableresources?$select=bookableresourceid,name&$orderby=name asc&$top=${pageSize}`;
            const resRaw = await dataverse.requestRaw(path);
            const data = (await resRaw.json()) as { value?: Array<Record<string, unknown>>; "@odata.nextLink"?: string };
            const value = Array.isArray(data?.value) ? data.value : [];
            for (const row of value) {
                items.push({
                    id: (row as { bookableresourceid?: string }).bookableresourceid || null,
                    name: (row as { name?: string }).name || null,
                });
                if (items.length >= limit) break;
            }
            nextLink = data?.["@odata.nextLink"] || null;
            pages += 1;
            if (!nextLink) break;
        }

        return new Response(JSON.stringify({
            ok: true,
            pageSize,
            maxPages,
            limit,
            pages,
            count: items.length,
            nextLink: nextLink || null,
            items,
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("List bookable resources failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function GET(request: Request) {
    return handle(request);
}
