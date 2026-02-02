import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { getDataverseMappingConfig } from "../../lib/premium-sync/config.js";
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

async function resolveLookupField(dataverse, entityLogicalName, targetLogicalName) {
    try {
        const relRes = await dataverse.requestRaw(
            `/EntityDefinitions(LogicalName='${entityLogicalName}')/ManyToOneRelationships?$select=ReferencingAttribute,ReferencedEntity`
        );
        const relData = await relRes.json();
        const rels = Array.isArray(relData?.value) ? relData.value : [];
        const relMatch = rels.find(
            (rel) => String(rel.ReferencedEntity || "").toLowerCase() === targetLogicalName.toLowerCase()
        );
        if (relMatch?.ReferencingAttribute) {
            return String(relMatch.ReferencingAttribute);
        }
    } catch (error) {
        logger.warn("Dataverse lookup field resolve failed", {
            entityLogicalName,
            targetLogicalName,
            error: error?.message || String(error),
        });
    }
    return null;
}

async function resolveProjectId(dataverse, projectNo) {
    const mapping = getDataverseMappingConfig();
    const normalized = (projectNo || "").trim();
    if (!normalized) return null;
    const escaped = escapeODataString(normalized);
    const select = [mapping.projectIdField, mapping.projectTitleField, mapping.projectBcNoField].filter(Boolean);

    if (mapping.projectBcNoField) {
        const filter = `${mapping.projectBcNoField} eq '${escaped}'`;
        const res = await dataverse.list(mapping.projectEntitySet, { select, filter, top: 5 });
        if (res.value?.length) {
            const match = res.value[0];
            const id = match?.[mapping.projectIdField];
            if (typeof id === "string" && id.trim()) return id.trim();
        }
    }

    if (mapping.projectTitleField) {
        const filter = `startswith(${mapping.projectTitleField}, '${escaped}')`;
        const res = await dataverse.list(mapping.projectEntitySet, { select, filter, top: 5 });
        if (res.value?.length) {
            const match = res.value[0];
            const id = match?.[mapping.projectIdField];
            if (typeof id === "string" && id.trim()) return id.trim();
        }
    }

    return null;
}

async function resolveResourceName(dataverse, resourceId, cache) {
    const trimmed = normalizeGuid(resourceId);
    if (!trimmed) return null;
    if (cache.has(trimmed)) return cache.get(trimmed);
    try {
        const res = await dataverse.getById("bookableresources", trimmed, ["bookableresourceid", "name"]);
        const name = res?.name ? String(res.name) : null;
        cache.set(trimmed, name);
        return name;
    } catch (error) {
        logger.warn("Dataverse resource lookup failed", { resourceId: trimmed, error: error?.message || String(error) });
        cache.set(trimmed, null);
        return null;
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
    const projectIdInput = (body?.projectId || url.searchParams.get("projectId") || "").trim();
    const projectNo = (body?.projectNo || url.searchParams.get("projectNo") || "").trim();

    try {
        const dataverse = new DataverseClient();
        const mapping = getDataverseMappingConfig();
        let projectId = normalizeGuid(projectIdInput);
        if (!projectId && projectNo) {
            projectId = await resolveProjectId(dataverse, projectNo);
        }
        if (!projectId) {
            res.status(400).json({ ok: false, error: "projectId or projectNo is required" });
            return;
        }

        const projectLookup = (await resolveLookupField(dataverse, "msdyn_projectteam", "msdyn_project")) || "msdyn_projectid";
        const resourceLookup = (await resolveLookupField(dataverse, "msdyn_projectteam", "bookableresource")) || "msdyn_bookableresourceid";

        const filter = `_${projectLookup}_value eq ${formatODataGuid(projectId)}`;
        const select = ["msdyn_projectteamid", "msdyn_name", `_${projectLookup}_value`, `_${resourceLookup}_value`];
        const teamRes = await dataverse.list("msdyn_projectteams", { select, filter, top: 500 });

        const resourceCache = new Map();
        const members = [];
        for (const row of teamRes.value || []) {
            const resourceId = row?.[`_${resourceLookup}_value`];
            const resourceName = await resolveResourceName(dataverse, resourceId, resourceCache);
            members.push({
                teamId: row.msdyn_projectteamid || null,
                teamName: row.msdyn_name || null,
                resourceId: resourceId || null,
                resourceName: resourceName || null,
            });
        }

        res.status(200).json({
            ok: true,
            projectId,
            projectNo: projectNo || null,
            mapping: {
                projectLookup,
                resourceLookup,
                projectEntitySet: mapping.projectEntitySet,
            },
            count: members.length,
            members,
        });
    } catch (error) {
        logger.error("Debug project team failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
