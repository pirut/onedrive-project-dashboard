import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

function parseNumber(value, fallback) {
    if (value == null || value === "") return fallback;
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const pageSize = Math.max(1, Math.min(500, parseNumber(url.searchParams.get("pageSize"), 200)));
    const maxPages = Math.max(1, Math.min(20, parseNumber(url.searchParams.get("maxPages"), 5)));
    const limit = Math.max(1, Math.min(5000, parseNumber(url.searchParams.get("limit"), pageSize * maxPages)));

    try {
        const dataverse = new DataverseClient();
        const items = [];
        let nextLink = null;
        let pages = 0;

        while (pages < maxPages && items.length < limit) {
            const path = nextLink || `/bookableresources?$select=bookableresourceid,name&$orderby=name asc&$top=${pageSize}`;
            const resRaw = await dataverse.requestRaw(path);
            const data = await resRaw.json();
            const value = Array.isArray(data?.value) ? data.value : [];
            for (const row of value) {
                items.push({
                    id: row.bookableresourceid || row.bookableResourceId || row.bookableResourceID || null,
                    name: row.name || null,
                });
                if (items.length >= limit) break;
            }
            nextLink = data?.["@odata.nextLink"] || null;
            pages += 1;
            if (!nextLink) break;
        }

        res.status(200).json({
            ok: true,
            pageSize,
            maxPages,
            limit,
            pages,
            count: items.length,
            nextLink: nextLink || null,
            items,
        });
    } catch (error) {
        logger.error("List bookable resources failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
