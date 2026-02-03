import "isomorphic-fetch";
import crypto from "crypto";
import { ConfidentialClientApplication } from "@azure/msal-node";
import { DataverseClient } from "../lib/dataverse-client.js";
import { getDataverseMappingConfig } from "../lib/premium-sync/config.js";
import { getPremiumProjectUrlTemplate, getTenantIdForUrl } from "../lib/premium-sync/premium-url.js";
import { logger } from "../lib/planner-sync/logger.js";

const ADMIN_SESSION_SECRET = process.env.ADMIN_SESSION_SECRET || "";

function signSession(data) {
    if (!ADMIN_SESSION_SECRET) return null;
    const h = crypto.createHmac("sha256", ADMIN_SESSION_SECRET);
    h.update(data);
    return h.digest("hex");
}

function safeEqual(a = "", b = "") {
    const ab = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    if (ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

function parseCookies(req) {
    const hdr = req.headers["cookie"];
    const out = {};
    if (!hdr) return out;
    String(hdr)
        .split(";")
        .map((s) => s.trim())
        .forEach((pair) => {
            const eq = pair.indexOf("=");
            if (eq === -1) return;
            const k = decodeURIComponent(pair.slice(0, eq).trim());
            const v = decodeURIComponent(pair.slice(eq + 1).trim());
            out[k] = v;
        });
    return out;
}

function verifySession(cookieVal) {
    try {
        if (!cookieVal) return false;
        const decoded = Buffer.from(cookieVal, "base64url").toString("utf8");
        const parts = decoded.split("|");
        if (parts.length !== 3) return false;
        const [u, ts, sig] = parts;
        const raw = `${u}|${ts}`;
        const expect = signSession(raw);
        if (!expect) return false;
        if (!safeEqual(sig, expect)) return false;
        const ageMs = Date.now() - Number(ts);
        if (!Number.isFinite(ageMs) || ageMs > 7 * 24 * 60 * 60 * 1000) return false;
        return { username: u };
    } catch {
        return false;
    }
}

function requireAdmin(req, res) {
    const cookies = parseCookies(req);
    const sess = verifySession(cookies["admin_session"]);
    if (!sess) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
        return false;
    }
    return true;
}

function readJsonBody(req) {
    return new Promise((resolve) => {
        const chunks = [];
        req.on("data", (chunk) => chunks.push(chunk));
        req.on("end", () => {
            if (!chunks.length) return resolve(null);
            const text = Buffer.concat(chunks).toString("utf8");
            try {
                resolve(JSON.parse(text));
            } catch {
                resolve(null);
            }
        });
    });
}

function getGraphConfig() {
    const tenantId = process.env.TENANT_ID || "";
    const clientId = process.env.GRAPH_CLIENT_ID || process.env.MSAL_CLIENT_ID || "";
    const clientSecret = process.env.MICROSOFT_CLIENT_SECRET || "";
    const scope = process.env.MS_GRAPH_SCOPE || "https://graph.microsoft.com/.default";
    const groupId = process.env.PLANNER_GROUP_ID || "";
    return { tenantId, clientId, clientSecret, scope, groupId };
}


function createGraphClient(config) {
    if (!config.tenantId || !config.clientId || !config.clientSecret) {
        return null;
    }
    return new ConfidentialClientApplication({
        auth: {
            clientId: config.clientId,
            authority: `https://login.microsoftonline.com/${config.tenantId}`,
            clientSecret: config.clientSecret,
        },
    });
}

async function getGraphToken(config, msalApp) {
    if (!msalApp) throw new Error("Graph app not configured");
    const result = await msalApp.acquireTokenByClientCredential({ scopes: [config.scope] });
    if (!result?.accessToken) {
        throw new Error("Graph token missing accessToken");
    }
    return result.accessToken;
}

async function graphRequest(config, msalApp, path, options = {}) {
    const token = await getGraphToken(config, msalApp);
    const url = path.startsWith("http") ? path : `https://graph.microsoft.com/v1.0${path}`;
    const res = await fetch(url, {
        method: options.method || "GET",
        headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
            ...(options.headers || {}),
        },
        body: options.body ? JSON.stringify(options.body) : undefined,
    });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`Graph ${options.method || "GET"} ${url} -> ${res.status}: ${text}`);
    }
    if (options.method === "DELETE") return null;
    return res.json();
}

async function graphDeletePlan(config, msalApp, planId) {
    const token = await getGraphToken(config, msalApp);
    const url = `https://graph.microsoft.com/v1.0/planner/plans/${planId}`;
    const getRes = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!getRes.ok) {
        const text = await getRes.text();
        throw new Error(`Graph GET ${url} -> ${getRes.status}: ${text}`);
    }
    let etag = getRes.headers.get("etag") || getRes.headers.get("ETag") || "";
    if (!etag) {
        const data = await getRes.json();
        etag = data?.["@odata.etag"] || data?.["@odata.etag".toString()] || "";
    }
    if (!etag) {
        throw new Error("Missing ETag for planner plan delete");
    }
    const delRes = await fetch(url, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}`, "If-Match": etag },
    });
    if (!delRes.ok) {
        const text = await delRes.text();
        throw new Error(`Graph DELETE ${url} -> ${delRes.status}: ${text}`);
    }
    return true;
}

function escapeODataString(value) {
    return String(value).replace(/'/g, "''");
}

function extractBcProjectNo(title) {
    if (!title) return "";
    const raw = String(title).trim();
    const dashIndex = raw.indexOf("-");
    if (dashIndex > 0) {
        const left = raw.slice(0, dashIndex).trim();
        if (/^[A-Za-z]{2}\d{3,}$/.test(left)) return left.toUpperCase();
    }
    const match = raw.match(/[A-Za-z]{2}\d{3,}/);
    return match ? match[0].toUpperCase() : "";
}


async function listDataverseProjects(dataverse, mapping) {
    const select = [mapping.projectIdField, mapping.projectTitleField, mapping.projectBcNoField, "modifiedon"].filter(Boolean);
    const res = await dataverse.list(mapping.projectEntitySet, { select, top: 200 });
    const items = Array.isArray(res.value) ? res.value : [];
    if (!res.nextLink) return items;
    let next = res.nextLink;
    while (next) {
        const pageRes = await dataverse.request(next);
        const data = await pageRes.json();
        if (Array.isArray(data?.value)) items.push(...data.value);
        next = data?.["@odata.nextLink"] || null;
    }
    return items;
}

async function listGraphPlans(config, msalApp) {
    if (!config.groupId) {
        throw new Error("Missing PLANNER_GROUP_ID");
    }
    const data = await graphRequest(config, msalApp, `/groups/${config.groupId}/planner/plans?$select=id,title,createdDateTime,owner`);
    return Array.isArray(data?.value) ? data.value : [];
}

async function findDataverseProjectByBcNo(dataverse, mapping, bcNo) {
    if (!bcNo || !mapping.projectBcNoField) return null;
    const filter = `${mapping.projectBcNoField} eq '${escapeODataString(bcNo)}'`;
    const res = await dataverse.list(mapping.projectEntitySet, {
        select: [mapping.projectIdField, mapping.projectBcNoField, mapping.projectTitleField],
        filter,
        top: 1,
    });
    return res.value && res.value[0] ? res.value[0] : null;
}

async function findDataverseProjectByTitle(dataverse, mapping, title) {
    if (!title) return null;
    const filter = `${mapping.projectTitleField} eq '${escapeODataString(title)}'`;
    const res = await dataverse.list(mapping.projectEntitySet, {
        select: [mapping.projectIdField, mapping.projectTitleField],
        filter,
        top: 1,
    });
    return res.value && res.value[0] ? res.value[0] : null;
}

export default async function handler(req, res) {
    if (!requireAdmin(req, res)) return;

    if (req.method === "GET") {
        try {
            const dataverse = new DataverseClient();
            const mapping = getDataverseMappingConfig();
            const graphConfig = getGraphConfig();
            const msalApp = createGraphClient(graphConfig);
            const graphEnabled = Boolean(msalApp && graphConfig.groupId);
            const graphPlansPromise = graphEnabled ? listGraphPlans(graphConfig, msalApp) : Promise.resolve([]);
            const whoPromise = dataverse.whoAmI().catch(() => null);
            const [dataverseProjects, graphPlans, who] = await Promise.all([
                listDataverseProjects(dataverse, mapping),
                graphPlansPromise,
                whoPromise,
            ]);
            const orgId = who && who.OrganizationId ? String(who.OrganizationId) : "";
            res.status(200).json({
                ok: true,
                mapping,
                dataverseProjects,
                graphPlans,
                graphEnabled,
                projectUrlTemplate: getPremiumProjectUrlTemplate({ tenantId: getTenantIdForUrl(), orgId }),
            });
        } catch (error) {
            logger.warn("Admin planner assets list failed", { error: error?.message || String(error) });
            res.status(400).json({ ok: false, error: error?.message || String(error) });
        }
        return;
    }

    if (req.method === "POST") {
        const body = await readJsonBody(req);
        const action = body?.action ? String(body.action) : "";
        try {
            const dataverse = new DataverseClient();
            const mapping = getDataverseMappingConfig();
            const graphConfig = getGraphConfig();
            const msalApp = createGraphClient(graphConfig);
            if (action === "delete-dataverse") {
                const ids = Array.isArray(body?.ids) ? body.ids : [];
                const results = [];
                for (const id of ids) {
                    try {
                        await dataverse.delete(mapping.projectEntitySet, String(id));
                        results.push({ id, ok: true });
                    } catch (error) {
                        results.push({ id, ok: false, error: error?.message || String(error) });
                    }
                }
                res.status(200).json({ ok: true, results });
                return;
            }

            if (action === "delete-graph") {
                if (!msalApp || !graphConfig.groupId) throw new Error("Graph not configured");
                const ids = Array.isArray(body?.ids) ? body.ids : [];
                const results = [];
                for (const id of ids) {
                    try {
                        await graphDeletePlan(graphConfig, msalApp, String(id));
                        results.push({ id, ok: true });
                    } catch (error) {
                        results.push({ id, ok: false, error: error?.message || String(error) });
                    }
                }
                res.status(200).json({ ok: true, results });
                return;
            }

            if (action === "convert-graph") {
                const plans = Array.isArray(body?.plans) ? body.plans : [];
                const results = [];
                for (const plan of plans) {
                    const planId = plan?.id ? String(plan.id) : "";
                    const title = plan?.title ? String(plan.title) : "";
                    if (!planId) {
                        results.push({ id: planId, ok: false, error: "Missing plan id" });
                        continue;
                    }
                    const bcNo = extractBcProjectNo(title);
                    try {
                        let existing = null;
                        if (bcNo) {
                            existing = await findDataverseProjectByBcNo(dataverse, mapping, bcNo);
                        }
                        if (!existing && title) {
                            existing = await findDataverseProjectByTitle(dataverse, mapping, title);
                        }
                        if (existing) {
                            results.push({ id: planId, ok: true, skipped: true, projectId: existing[mapping.projectIdField] });
                            continue;
                        }
                        const payload = { [mapping.projectTitleField]: title || planId };
                        const createResult = await dataverse.createProjectV1(payload);
                        const projectId = createResult.projectId;
                        if (!projectId) {
                            results.push({ id: planId, ok: false, error: "Project service did not return ProjectId" });
                            continue;
                        }
                        if (projectId && bcNo && mapping.projectBcNoField) {
                            try {
                                await dataverse.update(mapping.projectEntitySet, projectId, { [mapping.projectBcNoField]: bcNo });
                            } catch (error) {
                                results.push({ id: planId, ok: false, error: `BC No update failed: ${error?.message || String(error)}` });
                                continue;
                            }
                        }
                        results.push({ id: planId, ok: true, projectId });
                    } catch (error) {
                        results.push({ id: planId, ok: false, error: error?.message || String(error) });
                    }
                }
                res.status(200).json({ ok: true, results });
                return;
            }

            res.status(400).json({ ok: false, error: "Unknown action" });
        } catch (error) {
            logger.warn("Admin planner assets action failed", { error: error?.message || String(error), action });
            res.status(400).json({ ok: false, error: error?.message || String(error) });
        }
        return;
    }

    res.status(405).json({ ok: false, error: "Method not allowed" });
}
