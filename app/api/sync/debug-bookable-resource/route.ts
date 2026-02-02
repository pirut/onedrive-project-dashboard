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

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
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

    try {
        const dataverse = new DataverseClient();
        const table = await checkTable(dataverse);
        const byAadObjectId = aadObjectId ? await queryByAadObjectId(dataverse, aadObjectId) : null;
        const byName = name ? await queryByName(dataverse, name) : null;

        return new Response(JSON.stringify({
            ok: true,
            query: { aadObjectId: aadObjectId || null, name: name || null },
            table,
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
