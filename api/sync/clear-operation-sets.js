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

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
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

function parseTimestamp(value) {
    if (!value) return null;
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.getTime();
}

async function listOperationSets(dataverse, entitySet, pageSize, maxPages, limit) {
    const items = [];
    let nextLink = null;
    let pages = 0;

    while (pages < maxPages && items.length < limit) {
        const path = nextLink || `/${entitySet}?$top=${pageSize}`;
        const resRaw = await dataverse.requestRaw(path);
        const data = await resRaw.json();
        const value = Array.isArray(data?.value) ? data.value : [];
        for (const row of value) {
            items.push({
                id: extractOperationSetId(row),
                createdon: row.createdon || null,
                modifiedon: row.modifiedon || null,
            });
            if (items.length >= limit) break;
        }
        nextLink = data?.["@odata.nextLink"] || null;
        pages += 1;
        if (!nextLink) break;
    }

    return items;
}

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const body = await readJsonBody(req);

    const entitySetOverride = String(body?.entitySet || url.searchParams.get("entitySet") || "").trim();
    const pageSize = Math.max(1, Math.min(500, parseNumber(body?.pageSize ?? url.searchParams.get("pageSize"), 50)));
    const maxPages = Math.max(1, Math.min(20, parseNumber(body?.maxPages ?? url.searchParams.get("maxPages"), 5)));
    const limit = Math.max(1, Math.min(5000, parseNumber(body?.limit ?? url.searchParams.get("limit"), pageSize * maxPages)));
    const olderThanMinutes = Math.max(0, parseNumber(body?.olderThanMinutes ?? url.searchParams.get("olderThanMinutes"), 0));
    const dryRun = parseBool(body?.dryRun ?? url.searchParams.get("dryRun")) === true;

    const ids = Array.isArray(body?.ids)
        ? body.ids.map((value) => String(value).trim()).filter(Boolean)
        : [];

    try {
        const dataverse = new DataverseClient();
        const entitySet = await resolveOperationSetEntitySet(dataverse, entitySetOverride);
        const cutoff = olderThanMinutes ? Date.now() - olderThanMinutes * 60 * 1000 : null;

        let targets = [];
        if (ids.length) {
            targets = ids.map((id) => ({ id }));
        } else {
            targets = await listOperationSets(dataverse, entitySet, pageSize, maxPages, limit);
        }

        if (cutoff != null) {
            targets = targets.filter((item) => {
                const ts = parseTimestamp(item.modifiedon) ?? parseTimestamp(item.createdon);
                if (ts == null) return false;
                return ts <= cutoff;
            });
        }

        const results = [];
        for (const item of targets) {
            if (!item.id) {
                results.push({ id: null, ok: false, error: "Missing operation set id" });
                continue;
            }
            if (dryRun) {
                results.push({ id: item.id, ok: true, dryRun: true });
                continue;
            }
            try {
                await dataverse.delete(entitySet, String(item.id));
                results.push({ id: item.id, ok: true });
            } catch (error) {
                results.push({ id: item.id, ok: false, error: error?.message || String(error) });
            }
        }

        res.status(200).json({
            ok: true,
            entitySet,
            dryRun,
            olderThanMinutes,
            count: results.length,
            results,
        });
    } catch (error) {
        logger.error("Clear operation sets failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
