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

function escapeODataString(value) {
    return String(value || "").replace(/'/g, "''");
}

async function resolvePssErrorEntitySet(dataverse, override) {
    if (override) return override;
    try {
        const res = await dataverse.requestRaw("/EntityDefinitions(LogicalName='msdyn_psserrorlog')?$select=EntitySetName,LogicalName");
        const data = await res.json();
        const entitySet = data?.EntitySetName || data?.entitySetName;
        if (entitySet) return entitySet;
    } catch (error) {
        logger.warn("PSS error log metadata lookup failed", { error: error?.message || String(error) });
    }
    return "msdyn_psserrorlogs";
}

function extractErrorLogId(row) {
    if (!row || typeof row !== "object") return null;
    if (row.msdyn_psserrorlogid) return row.msdyn_psserrorlogid;
    if (row.msdyn_psserrorlogId) return row.msdyn_psserrorlogId;
    for (const key of Object.keys(row)) {
        if (key.toLowerCase().endsWith("psserrorlogid")) return row[key];
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
    const includeLog = parseBool(url.searchParams.get("includeLog")) === true;
    const entitySetOverride = (url.searchParams.get("entitySet") || "").trim();
    const correlationId = (url.searchParams.get("correlationId") || "").trim();
    const projectId = (url.searchParams.get("projectId") || "").trim();

    try {
        const dataverse = new DataverseClient();
        const entitySet = await resolvePssErrorEntitySet(dataverse, entitySetOverride);
        const items = [];
        let nextLink = null;
        let pages = 0;

        const selectFields = [
            "msdyn_psserrorlogid",
            "msdyn_correlationid",
            "msdyn_errorcode",
            "msdyn_sessionid",
            "createdon",
            "modifiedon",
            "_msdyn_project_value",
        ];
        if (includeLog) {
            selectFields.push("msdyn_log", "msdyn_callstack", "msdyn_helplink");
        }

        const filters = [];
        if (correlationId) {
            filters.push(`msdyn_correlationid eq '${escapeODataString(correlationId)}'`);
        }
        if (projectId) {
            filters.push(`_msdyn_project_value eq ${projectId}`);
        }
        const filterQuery = filters.length ? `&$filter=${filters.join(" and ")}` : "";
        const selectQuery = `$select=${selectFields.join(",")}`;
        const orderQuery = "$orderby=createdon desc";

        while (pages < maxPages && items.length < limit) {
            const path = nextLink || `/${entitySet}?${selectQuery}&${orderQuery}&$top=${pageSize}${filterQuery}`;
            const resRaw = await dataverse.requestRaw(path);
            const data = await resRaw.json();
            const value = Array.isArray(data?.value) ? data.value : [];
            for (const row of value) {
                const id = extractErrorLogId(row);
                items.push({
                    id: id || null,
                    correlationId: row.msdyn_correlationid || null,
                    errorCode: row.msdyn_errorcode || null,
                    sessionId: row.msdyn_sessionid || null,
                    projectId: row._msdyn_project_value || null,
                    createdon: row.createdon || null,
                    modifiedon: row.modifiedon || null,
                    log: includeLog ? row.msdyn_log || null : undefined,
                    callstack: includeLog ? row.msdyn_callstack || null : undefined,
                    helpLink: includeLog ? row.msdyn_helplink || null : undefined,
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
        logger.error("Debug PSS error logs failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
