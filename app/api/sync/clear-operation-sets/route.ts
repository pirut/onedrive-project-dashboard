import { DataverseClient } from "../../../../../lib/dataverse-client";
import { logger } from "../../../../../lib/planner-sync/logger";

function parseNumber(value: unknown, fallback: number) {
    if (value == null || value === "") return fallback;
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
}

function parseBool(value: unknown) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

async function resolveOperationSetEntitySet(dataverse: DataverseClient, override: string) {
    if (override) return override;
    try {
        const res = await dataverse.requestRaw("/EntityDefinitions(LogicalName='msdyn_operationset')?$select=EntitySetName,LogicalName");
        const data = (await res.json()) as { EntitySetName?: string; entitySetName?: string };
        const entitySet = data?.EntitySetName || data?.entitySetName;
        if (entitySet) return entitySet;
    } catch (error) {
        logger.warn("Operation set metadata lookup failed", { error: (error as Error)?.message || String(error) });
    }
    return "msdyn_operationsets";
}

function extractOperationSetId(row: Record<string, unknown>) {
    if (!row) return null;
    if (row.msdyn_operationsetid) return row.msdyn_operationsetid;
    if ((row as { OperationSetId?: unknown }).OperationSetId) return (row as { OperationSetId?: unknown }).OperationSetId;
    if ((row as { operationSetId?: unknown }).operationSetId) return (row as { operationSetId?: unknown }).operationSetId;
    for (const key of Object.keys(row)) {
        if (key.toLowerCase().endsWith("operationsetid")) return row[key];
    }
    return null;
}

function parseTimestamp(value: unknown) {
    if (!value) return null;
    const date = new Date(String(value));
    return Number.isNaN(date.getTime()) ? null : date.getTime();
}

async function listOperationSets(dataverse: DataverseClient, entitySet: string, pageSize: number, maxPages: number, limit: number) {
    const items: Array<{ id: unknown; createdon?: unknown; modifiedon?: unknown }> = [];
    let nextLink: string | null = null;
    let pages = 0;

    while (pages < maxPages && items.length < limit) {
        const path = nextLink || `/${entitySet}?$top=${pageSize}`;
        const resRaw = await dataverse.requestRaw(path);
        const data = (await resRaw.json()) as { value?: Array<Record<string, unknown>>; "@odata.nextLink"?: string };
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

async function handle(request: Request) {
    const url = new URL(request.url);
    const body = await readJsonBody(request);

    const entitySetOverride = String(body?.entitySet || url.searchParams.get("entitySet") || "").trim();
    const pageSize = Math.max(1, Math.min(500, parseNumber(body?.pageSize ?? url.searchParams.get("pageSize"), 50)));
    const maxPages = Math.max(1, Math.min(20, parseNumber(body?.maxPages ?? url.searchParams.get("maxPages"), 5)));
    const limit = Math.max(1, Math.min(5000, parseNumber(body?.limit ?? url.searchParams.get("limit"), pageSize * maxPages)));
    const olderThanMinutes = Math.max(0, parseNumber(body?.olderThanMinutes ?? url.searchParams.get("olderThanMinutes"), 0));
    const dryRun = parseBool(body?.dryRun ?? url.searchParams.get("dryRun")) === true;

    const ids = Array.isArray(body?.ids)
        ? body.ids.map((value: unknown) => String(value).trim()).filter(Boolean)
        : [];

    try {
        const dataverse = new DataverseClient();
        const entitySet = await resolveOperationSetEntitySet(dataverse, entitySetOverride);
        const cutoff = olderThanMinutes ? Date.now() - olderThanMinutes * 60 * 1000 : null;

        let targets: Array<{ id?: unknown; createdon?: unknown; modifiedon?: unknown }> = [];
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

        const results: Array<Record<string, unknown>> = [];
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
                results.push({ id: item.id, ok: false, error: (error as Error)?.message || String(error) });
            }
        }

        return new Response(JSON.stringify({
            ok: true,
            entitySet,
            dryRun,
            olderThanMinutes,
            count: results.length,
            results,
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Clear operation sets failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function POST(request: Request) {
    return handle(request);
}
