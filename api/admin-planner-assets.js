import "isomorphic-fetch";
import crypto from "crypto";
import { ConfidentialClientApplication } from "@azure/msal-node";
import { BusinessCentralClient } from "../lib/planner-sync/bc-client.js";
import { DataverseClient } from "../lib/dataverse-client.js";
import { getDataverseMappingConfig, getPremiumSyncConfig } from "../lib/premium-sync/config.js";
import { getPremiumProjectUrlTemplate, getTenantIdForUrl } from "../lib/premium-sync/premium-url.js";
import { syncBcToPremium } from "../lib/premium-sync/index.js";
import { logger } from "../lib/planner-sync/logger.js";
import { buildDisabledProjectSet, listProjectSyncSettings, normalizeProjectNo } from "../lib/planner-sync/project-sync-store.js";

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

function parseTaskNumber(taskNo) {
    const raw = String(taskNo || "");
    const match = raw.match(/\d+/);
    if (!match) return Number.NaN;
    return Number(match[0]);
}

function isAllowedSyncTaskNo(taskNo, allowlist) {
    if (!allowlist.size) return true;
    const taskNumber = parseTaskNumber(taskNo);
    if (!Number.isFinite(taskNumber)) return false;
    return allowlist.has(taskNumber);
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

function isGuid(value) {
    if (!value) return false;
    const trimmed = String(value).trim().replace(/^\{/, "").replace(/\}$/, "");
    return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
        trimmed
    );
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

async function buildSyncedProjectSet(bcClient) {
    const tasks = await bcClient.listProjectTasks();
    const synced = new Set();
    for (const task of tasks || []) {
        const projectNo = (task?.projectNo || "").trim();
        if (!projectNo) continue;
        const planId = (task?.plannerPlanId || "").trim();
        if (planId && isGuid(planId)) {
            synced.add(projectNo);
        }
    }
    return synced;
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
            const syncConfig = getPremiumSyncConfig();
            const allowedTaskNumbers = new Set(
                Array.isArray(syncConfig.allowedTaskNumbers)
                    ? syncConfig.allowedTaskNumbers.filter((value) => Number.isFinite(value))
                    : []
            );
            const graphConfig = getGraphConfig();
            const msalApp = createGraphClient(graphConfig);
            const graphEnabled = Boolean(msalApp && graphConfig.groupId);
            const bcClient = new BusinessCentralClient();
            const graphPlansPromise = graphEnabled ? listGraphPlans(graphConfig, msalApp) : Promise.resolve([]);
            const whoPromise = dataverse.whoAmI().catch(() => null);
            const bcProjectsPromise = bcClient.listProjects().catch((error) => {
                logger.warn("BC project list failed", { error: error?.message || String(error) });
                return [];
            });
            const bcTasksPromise = bcClient.listProjectTasks().catch((error) => {
                logger.warn("BC task list failed", { error: error?.message || String(error) });
                return [];
            });
            const syncSettingsPromise = listProjectSyncSettings().catch(() => []);
            const [dataverseProjects, graphPlans, who, bcProjectsRaw, bcTasks, syncSettings] = await Promise.all([
                listDataverseProjects(dataverse, mapping),
                graphPlansPromise,
                whoPromise,
                bcProjectsPromise,
                bcTasksPromise,
                syncSettingsPromise,
            ]);
            const disabledSet = buildDisabledProjectSet(syncSettings);
            const premiumProjectSet = new Set();
            if (mapping.projectBcNoField) {
                for (const project of dataverseProjects || []) {
                    const bcNo = project?.[mapping.projectBcNoField];
                    if (typeof bcNo === "string" && bcNo.trim()) {
                        premiumProjectSet.add(normalizeProjectNo(bcNo));
                    }
                }
            }
            const taskStats = new Map();
            for (const task of bcTasks || []) {
                const projectNo = (task?.projectNo || "").trim();
                if (!projectNo) continue;
                if (!isAllowedSyncTaskNo(task?.taskNo, allowedTaskNumbers)) continue;
                const key = normalizeProjectNo(projectNo);
                const entry = taskStats.get(key) || { projectNo, total: 0, linked: 0, lastSyncAt: "" };
                entry.total += 1;
                const planId = (task?.plannerPlanId || "").trim();
                if (planId && isGuid(planId)) {
                    entry.linked += 1;
                }
                const lastSyncAt = typeof task?.lastSyncAt === "string" ? task.lastSyncAt : "";
                if (lastSyncAt) {
                    if (!entry.lastSyncAt || Date.parse(lastSyncAt) > Date.parse(entry.lastSyncAt)) {
                        entry.lastSyncAt = lastSyncAt;
                    }
                }
                taskStats.set(key, entry);
            }
            const bcProjects = [];
            const seen = new Set();
            for (const project of bcProjectsRaw || []) {
                const projectNo = (project?.projectNo || "").trim();
                if (!projectNo) continue;
                const key = normalizeProjectNo(projectNo);
                seen.add(key);
                const stats = taskStats.get(key) || { total: 0, linked: 0, lastSyncAt: "" };
                const total = stats.total || 0;
                const linked = stats.linked || 0;
                const syncState = total === 0 ? "empty" : linked === 0 ? "none" : linked >= total ? "linked" : "partial";
                bcProjects.push({
                    projectNo,
                    description: project?.description || "",
                    status: project?.status || "",
                    syncEnabled: !disabledSet.has(key),
                    tasksTotal: total,
                    tasksLinked: linked,
                    lastSyncAt: stats.lastSyncAt || "",
                    syncState,
                    hasPremiumProject: premiumProjectSet.has(key),
                });
            }
            for (const [key, stats] of taskStats.entries()) {
                if (seen.has(key)) continue;
                const total = stats.total || 0;
                const linked = stats.linked || 0;
                const syncState = total === 0 ? "empty" : linked === 0 ? "none" : linked >= total ? "linked" : "partial";
                bcProjects.push({
                    projectNo: stats.projectNo || key,
                    description: "",
                    status: "",
                    syncEnabled: !disabledSet.has(key),
                    tasksTotal: total,
                    tasksLinked: linked,
                    lastSyncAt: stats.lastSyncAt || "",
                    syncState,
                    hasPremiumProject: premiumProjectSet.has(key),
                });
            }
            const orgId = who && who.OrganizationId ? String(who.OrganizationId) : "";
            res.status(200).json({
                ok: true,
                mapping,
                dataverseProjects,
                graphPlans,
                graphEnabled,
                bcProjects,
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

            if (action === "sync-bc-projects") {
                const projectNos = Array.isArray(body?.projectNos)
                    ? body.projectNos.map((value) => String(value || "").trim()).filter(Boolean)
                    : [];
                if (!projectNos.length) {
                    res.status(400).json({ ok: false, error: "projectNos required" });
                    return;
                }
                const requestId = body?.requestId ? String(body.requestId) : undefined;
                const skipProjectAccess = body?.skipProjectAccess !== false;
                const forceProjectCreate = body?.forceProjectCreate !== false;
                const result = await syncBcToPremium(undefined, {
                    requestId,
                    projectNos,
                    forceProjectCreate,
                    skipProjectAccess,
                });
                res.status(200).json({ ok: true, projectNos, result });
                return;
            }

            if (action === "recreate-all") {
                const bcClient = new BusinessCentralClient();
                let projectNos = Array.isArray(body?.projectNos)
                    ? body.projectNos.map((value) => String(value || "").trim()).filter(Boolean)
                    : [];
                if (!projectNos.length) {
                    const projects = await bcClient.listProjects();
                    projectNos = projects.map((project) => String(project.projectNo || "").trim()).filter(Boolean);
                }
                const skipSynced = body?.skipSynced !== false;
                let skipped = [];
                if (skipSynced) {
                    const synced = await buildSyncedProjectSet(bcClient);
                    skipped = projectNos.filter((projectNo) => synced.has(projectNo));
                    projectNos = projectNos.filter((projectNo) => !synced.has(projectNo));
                }
                const requestId = body?.requestId ? String(body.requestId) : undefined;
                const skipProjectAccess = body?.skipProjectAccess === true;
                const result = await syncBcToPremium(undefined, {
                    requestId,
                    projectNos,
                    forceProjectCreate: true,
                    skipProjectAccess,
                });
                res.status(200).json({
                    ok: true,
                    projectNos,
                    skippedAlreadySynced: skipped,
                    skippedCount: skipped.length,
                    result,
                });
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
