import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

function normalizeGuid(value) {
    if (!value) return "";
    let raw = String(value).trim();
    if (!raw) return "";
    if ((raw.startsWith("{") && raw.endsWith("}")) || (raw.startsWith("(") && raw.endsWith(")"))) {
        raw = raw.slice(1, -1);
    }
    return raw.trim();
}

function formatODataGuid(value) {
    const trimmed = normalizeGuid(value);
    if (!trimmed) return "";
    return `guid'${trimmed}'`;
}

function escapeODataString(value) {
    return String(value || "").replace(/'/g, "''");
}

function parseBool(value) {
    if (value == null) return false;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(normalized);
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

async function listMetadataHints(dataverse) {
    try {
        const res = await dataverse.requestRaw(
            "/EntityDefinitions(LogicalName='bookableresource')/Attributes?$select=LogicalName,SchemaName,AttributeType,IsValidForRead"
        );
        const data = await res.json();
        const attrs = Array.isArray(data?.value) ? data.value : [];
        const filtered = attrs.filter((attr) => {
            const logical = String(attr.LogicalName || "").toLowerCase();
            const schema = String(attr.SchemaName || "").toLowerCase();
            return logical.includes("aad") || schema.includes("aad") || logical.includes("objectid") || schema.includes("objectid");
        });
        return {
            ok: true,
            total: attrs.length,
            count: filtered.length,
            attributes: filtered.map((attr) => ({
                logicalName: attr.LogicalName || null,
                schemaName: attr.SchemaName || null,
                type: attr.AttributeType || null,
                readable: attr.IsValidForRead ?? null,
            })),
        };
    } catch (error) {
        return { ok: false, error: error?.message || String(error) };
    }
}

async function checkTable(dataverse) {
    try {
        const res = await dataverse.requestRaw("/EntityDefinitions(LogicalName='bookableresource')?$select=LogicalName,SchemaName,EntitySetName");
        const data = await res.json();
        return {
            exists: true,
            logicalName: data?.LogicalName || "bookableresource",
            schemaName: data?.SchemaName || null,
            entitySetName: data?.EntitySetName || null,
        };
    } catch (error) {
        return {
            exists: false,
            error: error?.message || String(error),
        };
    }
}

async function queryByAadObjectId(dataverse, aadObjectId) {
    const fields = ["msdyn_aadobjectid", "aadobjectid"];
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
            const message = error?.message || String(error);
            if (message.includes("Could not find a property named")) {
                continue;
            }
            return { ok: false, error: message, field };
        }
    }
    return { ok: false, error: "No aadobjectid fields available" };
}

async function queryByName(dataverse, name) {
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
        return { ok: false, error: error?.message || String(error) };
    }
}

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const body = req.method === "POST" ? await readJsonBody(req) : null;

    const aadObjectId = (body?.aadObjectId || body?.groupId || url.searchParams.get("aadObjectId") || url.searchParams.get("groupId") || "").trim();
    const name = (body?.name || url.searchParams.get("name") || "").trim();
    const includeMetadata = parseBool(body?.includeMetadata ?? url.searchParams.get("includeMetadata") ?? url.searchParams.get("metadata"));

    try {
        const dataverse = new DataverseClient();
        const table = await checkTable(dataverse);
        const byAadObjectId = aadObjectId ? await queryByAadObjectId(dataverse, aadObjectId) : null;
        const byName = name ? await queryByName(dataverse, name) : null;
        const metadata = includeMetadata ? await listMetadataHints(dataverse) : null;

        res.status(200).json({
            ok: true,
            query: { aadObjectId: aadObjectId || null, name: name || null },
            table,
            metadata,
            results: {
                byAadObjectId,
                byName,
            },
        });
    } catch (error) {
        logger.error("Debug bookable resource failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
