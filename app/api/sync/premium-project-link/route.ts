import { DataverseClient } from "../../../../../lib/dataverse-client";
import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getDataverseMappingConfig } from "../../../../../lib/premium-sync/config";
import { buildPremiumProjectUrl, getPremiumProjectUrlTemplate, getTenantIdForUrl } from "../../../../../lib/premium-sync/premium-url";
import { resolveProjectFromBc, resolveProjectId } from "../../../../../lib/premium-sync/sync-engine";
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

async function handle(request: Request) {
    const url = new URL(request.url);
    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const projectNo = String(body?.projectNo || url.searchParams.get("projectNo") || "").trim();
    const projectIdInput = String(body?.projectId || url.searchParams.get("projectId") || "").trim();
    const redirectParam = parseBool(body?.redirect ?? url.searchParams.get("redirect"));
    const redirect = redirectParam === true;

    if (!projectNo && !projectIdInput) {
        return new Response(JSON.stringify({ ok: false, error: "projectNo or projectId is required" }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    try {
        const dataverse = new DataverseClient();
        const mapping = getDataverseMappingConfig();

        let projectId = normalizeGuid(projectIdInput);
        let projectEntity: Record<string, unknown> | null = null;

        if (projectId) {
            try {
                projectEntity = await dataverse.getById(mapping.projectEntitySet, projectId, [
                    mapping.projectIdField,
                    mapping.projectTitleField,
                    mapping.projectBcNoField,
                ].filter(Boolean) as string[]);
            } catch (error) {
                logger.warn("Premium project lookup failed", {
                    projectId,
                    error: (error as Error)?.message || String(error),
                });
                projectId = "";
            }
        }

        if (!projectId) {
            if (!projectNo) {
                return new Response(JSON.stringify({ ok: false, error: "projectNo is required when projectId is missing" }, null, 2), {
                    status: 400,
                    headers: { "Content-Type": "application/json" },
                });
            }
            const bcClient = new BusinessCentralClient();
            projectEntity = await resolveProjectFromBc(bcClient, dataverse, projectNo, mapping);
            projectId = resolveProjectId(projectEntity as Record<string, unknown> | null, mapping) || "";
        }

        if (!projectId) {
            return new Response(JSON.stringify({ ok: false, error: "Premium project not found" }, null, 2), {
                status: 404,
                headers: { "Content-Type": "application/json" },
            });
        }

        const who = await dataverse.whoAmI().catch(() => null);
        const orgId = who && (who as { OrganizationId?: string }).OrganizationId ? String((who as { OrganizationId?: string }).OrganizationId) : "";
        const template = getPremiumProjectUrlTemplate({ tenantId: getTenantIdForUrl(), orgId });
        const premiumUrl = buildPremiumProjectUrl(template, projectId);

        if (!premiumUrl) {
            return new Response(JSON.stringify({ ok: false, error: "Premium project URL template is missing" }, null, 2), {
                status: 500,
                headers: { "Content-Type": "application/json" },
            });
        }

        const projectNoOut = projectNo || (mapping.projectBcNoField && projectEntity?.[mapping.projectBcNoField]) || null;
        if (redirect) {
            return Response.redirect(premiumUrl, 302);
        }
        return new Response(JSON.stringify({
            ok: true,
            projectId,
            projectNo: projectNoOut,
            url: premiumUrl,
        }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Premium project link lookup failed", { error: (error as Error)?.message || String(error) });
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
