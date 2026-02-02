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

async function handle(request: Request) {
    const url = new URL(request.url);
    const pageSize = Math.max(1, Math.min(500, parseNumber(url.searchParams.get("pageSize"), 50)));
    const maxPages = Math.max(1, Math.min(20, parseNumber(url.searchParams.get("maxPages"), 5)));
    const limit = Math.max(1, Math.min(5000, parseNumber(url.searchParams.get("limit"), pageSize * maxPages)));
    const includeRaw = parseBool(url.searchParams.get("include")) === true;
    const entitySetOverride = (url.searchParams.get("entitySet") || "").trim();

    try {
        const dataverse = new DataverseClient();
        const entitySet = await resolveOperationSetEntitySet(dataverse, entitySetOverride);
        const items: Array<Record<string, unknown>> = [];
        let nextLink: string | null = null;
        let pages = 0;

        while (pages < maxPages && items.length < limit) {
            const path = nextLink || `/${entitySet}?$top=${pageSize}`;
            const resRaw = await dataverse.requestRaw(path);
            const data = (await resRaw.json()) as { value?: Array<Record<string, unknown>>; "@odata.nextLink"?: string };
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

        return new Response(JSON.stringify({
            ok: true,
            entitySet,
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
        logger.error("Debug operation sets failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function GET(request: Request) {
    return handle(request);
}
