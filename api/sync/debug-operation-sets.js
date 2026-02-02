import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

function parseNumber(value, fallback) {
    if (value == null || value === "") return fallback;
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
}

function parseBool(value) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

async function resolveOperationSetEntitySet(dataverse, override) {
    if (override) return override;
    try {
        const res = await dataverse.requestRaw("/EntityDefinitions(LogicalName='msdyn_operationset')?$select=EntitySetName,LogicalName");
        const data = await res.json();
        const entitySet = data?.EntitySetName || data?.entitySetName;
        if (entitySet) return entitySet;
    } catch (error) {
        logger.warn("Operation set metadata lookup failed", { error: error?.message || String(error) });
    }
    return "msdyn_operationsets";
}

function extractOperationSetId(row) {
    if (!row || typeof row !== "object") return null;
    if (row.msdyn_operationsetid) return row.msdyn_operationsetid;
    if (row.OperationSetId) return row.OperationSetId;
    if (row.operationSetId) return row.operationSetId;
    for (const key of Object.keys(row)) {
        if (key.toLowerCase().endsWith("operationsetid")) return row[key];
    }
    return null;
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const pageSize = Math.max(1, Math.min(500, parseNumber(url.searchParams.get("pageSize"), 50)));
    const maxPages = Math.max(1, Math.min(20, parseNumber(url.searchParams.get("maxPages"), 5)));
    const limit = Math.max(1, Math.min(5000, parseNumber(url.searchParams.get("limit"), pageSize * maxPages)));
    const includeRaw = parseBool(url.searchParams.get("include")) === true;
    const entitySetOverride = (url.searchParams.get("entitySet") || "").trim();

    try {
        const dataverse = new DataverseClient();
        const entitySet = await resolveOperationSetEntitySet(dataverse, entitySetOverride);
        const items = [];
        let nextLink = null;
        let pages = 0;

        while (pages < maxPages && items.length < limit) {
            const path = nextLink || `/${entitySet}?$top=${pageSize}`;
            const resRaw = await dataverse.requestRaw(path);
            const data = await resRaw.json();
            const value = Array.isArray(data?.value) ? data.value : [];
            for (const row of value) {
                const id = extractOperationSetId(row);
                items.push({
                    id: id || null,
                    name: row.msdyn_name || row.name || null,
                    createdon: row.createdon || null,
                    modifiedon: row.modifiedon || null,
                    raw: includeRaw ? row : undefined,
                });
                if (items.length >= limit) break;
            }
            nextLink = data?.["@odata.nextLink"] || null;
            pages += 1;
            if (!nextLink) break;
        }

        res.status(200).json({
            ok: true,
            entitySet,
            pageSize,
            maxPages,
            limit,
            pages,
            count: items.length,
            nextLink: nextLink || null,
            items,
        });
    } catch (error) {
        logger.error("Debug operation sets failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
