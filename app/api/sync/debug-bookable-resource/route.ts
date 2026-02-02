import { DataverseClient } from "../../../../../lib/dataverse-client";
import { logger } from "../../../../../lib/planner-sync/logger";

function normalizeGuid(value: unknown) {
    if (!value) return "";
    let raw = String(value).trim();
    if (!raw) return "";
    if ((raw.startsWith("{") && raw.endsWith("}")) || (raw.startsWith("(") && raw.endsWith(")"))) {
        raw = raw.slice(1, -1);
    }
    return raw.trim();
}

function formatODataGuid(value: unknown) {
    const trimmed = normalizeGuid(value);
    if (!trimmed) return "";
    return `guid'${trimmed}'`;
}

function escapeODataString(value: unknown) {
    return String(value || "").replace(/'/g, "''");
}

function parseBool(value: unknown) {
    if (value == null) return false;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(normalized);
}

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

async function listMetadataHints(dataverse: DataverseClient) {
    try {
        const res = await dataverse.requestRaw(
            "/EntityDefinitions(LogicalName='bookableresource')/Attributes?$select=LogicalName,SchemaName,AttributeType,IsValidForRead"
        );
        const data = (await res.json()) as { value?: Array<Record<string, unknown>> };
        const attrs = Array.isArray(data?.value) ? data.value : [];
        const filtered = attrs.filter((attr) => {
            const logical = String((attr as { LogicalName?: string }).LogicalName || "").toLowerCase();
            const schema = String((attr as { SchemaName?: string }).SchemaName || "").toLowerCase();
            return logical.includes("aad") || schema.includes("aad") || logical.includes("objectid") || schema.includes("objectid");
        });
        return {
            ok: true,
            total: attrs.length,
            count: filtered.length,
            attributes: filtered.map((attr) => ({
                logicalName: (attr as { LogicalName?: string }).LogicalName || null,
                schemaName: (attr as { SchemaName?: string }).SchemaName || null,
                type: (attr as { AttributeType?: string }).AttributeType || null,
                readable: (attr as { IsValidForRead?: boolean }).IsValidForRead ?? null,
            })),
        };
    } catch (error) {
        return { ok: false, error: (error as Error)?.message || String(error) };
    }
}

async function checkTable(dataverse: DataverseClient) {
    try {
        const res = await dataverse.requestRaw("/EntityDefinitions(LogicalName='bookableresource')?$select=LogicalName,SchemaName,EntitySetName");
        const data = (await res.json()) as { LogicalName?: string; SchemaName?: string; EntitySetName?: string };
        return {
            exists: true,
            logicalName: data?.LogicalName || "bookableresource",
            schemaName: data?.SchemaName || null,
            entitySetName: data?.EntitySetName || null,
        };
    } catch (error) {
        return {
            exists: false,
            error: (error as Error)?.message || String(error),
        };
    }
}

async function queryByAadObjectId(dataverse: DataverseClient, aadObjectId: string) {
    const fields = ["msdyn_aadobjectid", "aadobjectid"] as const;
    const guid = formatODataGuid(aadObjectId);
    if (!guid) return { ok: false, error: "Missing or invalid aadObjectId" };
    for (const field of fields) {
        try {
            const filter = `${field} eq ${guid}`;
            const res = await dataverse.list("bookableresources", {
                select: ["bookableresourceid", "name", field],
                filter,
                top: 5,
            });
            return {
                ok: true,
                field,
                count: res.value.length,
                first: res.value[0] || null,
            };
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            if (message.includes("Could not find a property named")) {
                continue;
            }
            return { ok: false, error: message, field };
        }
    }
    return { ok: false, error: "No aadobjectid fields available" };
}

async function queryByName(dataverse: DataverseClient, name: string) {
    const trimmed = String(name || "").trim();
    if (!trimmed) return { ok: false, error: "Missing name" };
    try {
        const filter = `name eq '${escapeODataString(trimmed)}'`;
        const res = await dataverse.list("bookableresources", {
            select: ["bookableresourceid", "name"],
            filter,
            top: 5,
        });
        return { ok: true, count: res.value.length, first: res.value[0] || null };
    } catch (error) {
        return { ok: false, error: (error as Error)?.message || String(error) };
    }
}

async function handle(request: Request) {
    const url = new URL(request.url);
    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const aadObjectId = String(body?.aadObjectId || body?.groupId || url.searchParams.get("aadObjectId") || url.searchParams.get("groupId") || "").trim();
    const name = String(body?.name || url.searchParams.get("name") || "").trim();
    const includeMetadata = parseBool(body?.includeMetadata ?? url.searchParams.get("includeMetadata") ?? url.searchParams.get("metadata"));

    try {
        const dataverse = new DataverseClient();
        const table = await checkTable(dataverse);
        const byAadObjectId = aadObjectId ? await queryByAadObjectId(dataverse, aadObjectId) : null;
        const byName = name ? await queryByName(dataverse, name) : null;
        const metadata = includeMetadata ? await listMetadataHints(dataverse) : null;

        return new Response(JSON.stringify({
            ok: true,
            query: { aadObjectId: aadObjectId || null, name: name || null },
            table,
            metadata,
            results: {
                byAadObjectId,
                byName,
            },
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Debug bookable resource failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function GET(request: Request) {
    return handle(request);
}

export async function POST(request: Request) {
    return handle(request);
}
