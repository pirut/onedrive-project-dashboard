import { DataverseClient } from "../../../../../lib/dataverse-client";
import { getDataverseMappingConfig } from "../../../../../lib/premium-sync/config";
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

function formatODataGuidRaw(value: unknown) {
    const trimmed = normalizeGuid(value);
    if (!trimmed) return "";
    return trimmed;
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

async function resolveLookupField(dataverse: DataverseClient, entityLogicalName: string, targetLogicalName: string) {
    try {
        const relRes = await dataverse.requestRaw(
            `/EntityDefinitions(LogicalName='${entityLogicalName}')/ManyToOneRelationships?$select=ReferencingAttribute,ReferencedEntity`
        );
        const relData = (await relRes.json()) as { value?: Array<Record<string, unknown>> };
        const rels = Array.isArray(relData?.value) ? relData.value : [];
        const relMatch = rels.find(
            (rel) => String((rel as { ReferencedEntity?: string }).ReferencedEntity || "").toLowerCase() === targetLogicalName.toLowerCase()
        );
        if (relMatch && (relMatch as { ReferencingAttribute?: string }).ReferencingAttribute) {
            return String((relMatch as { ReferencingAttribute?: string }).ReferencingAttribute);
        }
    } catch (error) {
        logger.warn("Dataverse lookup field resolve failed", {
            entityLogicalName,
            targetLogicalName,
            error: (error as Error)?.message || String(error),
        });
    }
    return null;
}

async function resolveProjectId(dataverse: DataverseClient, projectNo: string) {
    const mapping = getDataverseMappingConfig();
    const normalized = (projectNo || "").trim();
    if (!normalized) return null;
    const escaped = escapeODataString(normalized);
    const select = [mapping.projectIdField, mapping.projectTitleField, mapping.projectBcNoField].filter(Boolean) as string[];

    if (mapping.projectBcNoField) {
        const filter = `${mapping.projectBcNoField} eq '${escaped}'`;
        const res = await dataverse.list(mapping.projectEntitySet, { select, filter, top: 5 });
        if (res.value?.length) {
            const match = res.value[0] as Record<string, unknown>;
            const id = match?.[mapping.projectIdField];
            if (typeof id === "string" && id.trim()) return id.trim();
        }
    }

    if (mapping.projectTitleField) {
        const filter = `startswith(${mapping.projectTitleField}, '${escaped}')`;
        const res = await dataverse.list(mapping.projectEntitySet, { select, filter, top: 5 });
        if (res.value?.length) {
            const match = res.value[0] as Record<string, unknown>;
            const id = match?.[mapping.projectIdField];
            if (typeof id === "string" && id.trim()) return id.trim();
        }
    }

    return null;
}

async function resolveResourceName(dataverse: DataverseClient, resourceId: string, cache: Map<string, string | null>) {
    const trimmed = normalizeGuid(resourceId);
    if (!trimmed) return null;
    if (cache.has(trimmed)) return cache.get(trimmed) || null;
    try {
        const res = await dataverse.getById("bookableresources", trimmed, ["bookableresourceid", "name"]);
        const name = (res as { name?: string })?.name ? String((res as { name?: string }).name) : null;
        cache.set(trimmed, name);
        return name;
    } catch (error) {
        logger.warn("Dataverse resource lookup failed", { resourceId: trimmed, error: (error as Error)?.message || String(error) });
        cache.set(trimmed, null);
        return null;
    }
}

async function handle(request: Request) {
    const url = new URL(request.url);
    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const projectIdInput = String(body?.projectId || url.searchParams.get("projectId") || "").trim();
    const projectNo = String(body?.projectNo || url.searchParams.get("projectNo") || "").trim();

    try {
        const dataverse = new DataverseClient();
        const mapping = getDataverseMappingConfig();
        let projectId = normalizeGuid(projectIdInput);
        if (!projectId && projectNo) {
            projectId = await resolveProjectId(dataverse, projectNo);
        }
        if (!projectId) {
            return new Response(JSON.stringify({ ok: false, error: "projectId or projectNo is required" }, null, 2), {
                status: 400,
                headers: { "Content-Type": "application/json" },
            });
        }

        const projectLookup = (await resolveLookupField(dataverse, "msdyn_projectteam", "msdyn_project")) || "msdyn_projectid";
        const resourceLookup = (await resolveLookupField(dataverse, "msdyn_projectteam", "bookableresource")) || "msdyn_bookableresourceid";

        const select = ["msdyn_projectteamid", "msdyn_name", `_${projectLookup}_value`, `_${resourceLookup}_value`];
        let teamRes;
        const guidFilter = `_${projectLookup}_value eq ${formatODataGuid(projectId)}`;
        try {
            teamRes = await dataverse.list("msdyn_projectteams", { select, filter: guidFilter, top: 500 });
        } catch (error) {
            const message = (error as Error)?.message || String(error);
            if (message.includes("0x80060888") || message.includes("Unrecognized 'Edm.String' literal 'guid'")) {
                const rawFilter = `_${projectLookup}_value eq ${formatODataGuidRaw(projectId)}`;
                teamRes = await dataverse.list("msdyn_projectteams", { select, filter: rawFilter, top: 500 });
            } else {
                throw error;
            }
        }

        const resourceCache = new Map<string, string | null>();
        const members = [] as Array<Record<string, unknown>>;
        for (const row of teamRes.value || []) {
            const record = row as Record<string, unknown>;
            const resourceId = record[`_${resourceLookup}_value`];
            const resourceName = typeof resourceId === "string" ? await resolveResourceName(dataverse, resourceId, resourceCache) : null;
            members.push({
                teamId: record.msdyn_projectteamid || null,
                teamName: record.msdyn_name || null,
                resourceId: resourceId || null,
                resourceName: resourceName || null,
            });
        }

        return new Response(JSON.stringify({
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
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Debug project team failed", { error: (error as Error)?.message || String(error) });
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
