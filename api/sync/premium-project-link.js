import "../../lib/planner-sync/bootstrap.js";
import { DataverseClient } from "../../lib/dataverse-client.js";
import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import { getDataverseMappingConfig } from "../../lib/premium-sync/config.js";
import { buildPremiumProjectUrl, getPremiumProjectUrlTemplate, getTenantIdForUrl } from "../../lib/premium-sync/premium-url.js";
import { resolveProjectFromBc, resolveProjectId } from "../../lib/premium-sync/sync-engine.js";
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

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const body = req.method === "POST" ? await readJsonBody(req) : null;

    const projectNo = String(body?.projectNo || url.searchParams.get("projectNo") || "").trim();
    const projectIdInput = String(body?.projectId || url.searchParams.get("projectId") || "").trim();

    if (!projectNo && !projectIdInput) {
        res.status(400).json({ ok: false, error: "projectNo or projectId is required" });
        return;
    }

    try {
        const dataverse = new DataverseClient();
        const mapping = getDataverseMappingConfig();

        let projectId = normalizeGuid(projectIdInput);
        let projectEntity = null;

        if (projectId) {
            try {
                projectEntity = await dataverse.getById(mapping.projectEntitySet, projectId, [
                    mapping.projectIdField,
                    mapping.projectTitleField,
                    mapping.projectBcNoField,
                ].filter(Boolean));
            } catch (error) {
                logger.warn("Premium project lookup failed", {
                    projectId,
                    error: error?.message || String(error),
                });
                projectId = "";
            }
        }

        if (!projectId) {
            if (!projectNo) {
                res.status(400).json({ ok: false, error: "projectNo is required when projectId is missing" });
                return;
            }
            const bcClient = new BusinessCentralClient();
            projectEntity = await resolveProjectFromBc(bcClient, dataverse, projectNo, mapping);
            projectId = resolveProjectId(projectEntity, mapping) || "";
        }

        if (!projectId) {
            res.status(404).json({ ok: false, error: "Premium project not found" });
            return;
        }

        const who = await dataverse.whoAmI().catch(() => null);
        const orgId = who && who.OrganizationId ? String(who.OrganizationId) : "";
        const template = getPremiumProjectUrlTemplate({ tenantId: getTenantIdForUrl(), orgId });
        const premiumUrl = buildPremiumProjectUrl(template, projectId);

        if (!premiumUrl) {
            res.status(500).json({ ok: false, error: "Premium project URL template is missing" });
            return;
        }

        const projectNoOut = projectNo || (mapping.projectBcNoField && projectEntity?.[mapping.projectBcNoField]) || null;
        res.status(200).json({
            ok: true,
            projectId,
            projectNo: projectNoOut,
            url: premiumUrl,
        });
    } catch (error) {
        logger.error("Premium project link lookup failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
