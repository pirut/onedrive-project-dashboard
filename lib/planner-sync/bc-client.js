import { fetchWithRetry, readResponseJson, readResponseText } from "./http.js";
import { logger } from "./logger.js";
import { getBcConfig } from "./config.js";

function parseDateMs(value) {
    if (!value) return null;
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
}

function buildPlannerTaskScore(task) {
    const hasPlanId = !!(task.plannerPlanId || "").trim();
    const lastSyncMs = parseDateMs(task.lastSyncAt);
    const modifiedMs = parseDateMs(task.systemModifiedAt) ?? parseDateMs(task.lastModifiedDateTime) ?? parseDateMs(task.modifiedAt);
    return {
        hasPlanId: hasPlanId ? 1 : 0,
        lastSyncMs: lastSyncMs ?? -1,
        modifiedMs: modifiedMs ?? -1,
    };
}

function isBetterPlannerTask(candidate, current) {
    const candidateScore = buildPlannerTaskScore(candidate);
    const currentScore = buildPlannerTaskScore(current);
    if (candidateScore.hasPlanId !== currentScore.hasPlanId) {
        return candidateScore.hasPlanId > currentScore.hasPlanId;
    }
    if (candidateScore.lastSyncMs !== currentScore.lastSyncMs) {
        return candidateScore.lastSyncMs > currentScore.lastSyncMs;
    }
    if (candidateScore.modifiedMs !== currentScore.modifiedMs) {
        return candidateScore.modifiedMs > currentScore.modifiedMs;
    }
    return false;
}

function selectPrimaryPlannerTask(tasks) {
    let best = tasks[0];
    for (const task of tasks.slice(1)) {
        if (isBetterPlannerTask(task, best)) {
            best = task;
        }
    }
    return best;
}

async function clearDuplicatePlannerLink(bcClient, task, plannerTaskId, primaryTask) {
    if (!task.systemId) {
        logger.warn("Duplicate Planner linkage missing systemId; skipping", {
            plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
        });
        return false;
    }
    try {
        await bcClient.patchProjectTask(task.systemId, {
            plannerTaskId: "",
            plannerPlanId: "",
            plannerBucket: "",
            lastPlannerEtag: "",
            syncLock: false,
        });
        logger.warn("Cleared duplicate Planner linkage", {
            plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            keptProjectNo: primaryTask?.projectNo,
            keptTaskNo: primaryTask?.taskNo,
        });
        return true;
    } catch (error) {
        logger.warn("Failed to clear duplicate Planner linkage", {
            plannerTaskId,
            projectNo: task.projectNo,
            taskNo: task.taskNo,
            error: error?.message || String(error),
        });
        return false;
    }
}

export class BusinessCentralClient {
    constructor() {
        this.config = getBcConfig();
        this.tokenCache = null;
        this.projectChangesEntitySet = null;
    }

    baseUrl() {
        const { apiBase, tenantId, environment, publisher, group, version, companyId } = this.config;
        return `${apiBase}/${tenantId}/${environment}/api/${publisher}/${group}/${version}/companies(${companyId})`;
    }

    apiRootUrl() {
        const { apiBase, tenantId, environment, publisher, group, version } = this.config;
        return `${apiBase}/${tenantId}/${environment}/api/${publisher}/${group}/${version}`;
    }

    buildWebhookResource(entitySet) {
        const { publisher, group, version, companyId } = this.config;
        const trimmed = (entitySet || "").trim().replace(/^\/+/, "");
        if (!trimmed) return "";
        if (trimmed.includes("companies(")) {
            return trimmed.startsWith("api/") ? `/${trimmed}` : `/${trimmed}`;
        }
        return `/api/${publisher}/${group}/${version}/companies(${companyId})/${trimmed}`;
    }

    async getAccessToken() {
        const now = Date.now();
        if (this.tokenCache && now < this.tokenCache.expiresAt - 60_000) {
            return this.tokenCache.token;
        }
        const { tenantId, clientId, clientSecret } = this.config;
        const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
        const body = new URLSearchParams({
            grant_type: "client_credentials",
            client_id: clientId,
            client_secret: clientSecret,
            scope: "https://api.businesscentral.dynamics.com/.default",
        });
        const res = await fetchWithRetry(tokenUrl, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body,
        });
        if (!res.ok) {
            const text = await readResponseText(res);
            throw new Error(`BC token error ${res.status}: ${text}`);
        }
        const data = await readResponseJson(res);
        if (!data?.access_token) {
            throw new Error("BC token response missing access_token");
        }
        this.tokenCache = {
            token: data.access_token,
            expiresAt: now + (data.expires_in || 3600) * 1000,
        };
        return this.tokenCache.token;
    }

    async request(path, options = {}) {
        const token = await this.getAccessToken();
        const url = path.startsWith("http") ? path : `${this.baseUrl()}${path}`;
        const res = await fetchWithRetry(url, {
            method: options.method || "GET",
            headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
                ...(options.headers || {}),
            },
            body: options.body,
        });
        if (!res.ok) {
            const text = await readResponseText(res);
            throw new Error(`BC ${options.method || "GET"} ${path} -> ${res.status}: ${text}`);
        }
        return res;
    }

    parseEntitySets(metadataText) {
        if (!metadataText) return [];
        const entitySets = [];
        const regex = /<EntitySet\s+Name="([^"]+)"/g;
        let match;
        while ((match = regex.exec(metadataText))) {
            entitySets.push(match[1]);
        }
        return entitySets;
    }

    resolveChangeEntitySetName(entitySets) {
        let best = null;
        for (const name of entitySets) {
            const lower = name.toLowerCase();
            let score = Number.POSITIVE_INFINITY;
            if (lower === "projectchanges") score = 0;
            else if (lower.includes("projectchanges")) score = 1;
            else if (lower.includes("projectchange")) score = 2;
            else if (lower.includes("project") && lower.includes("change")) score = 3;
            if (Number.isFinite(score) && (!best || score < best.score)) {
                best = { name, score };
            }
        }
        return best?.name || null;
    }

    async resolveProjectChangesEntitySet() {
        if (this.projectChangesEntitySet) return this.projectChangesEntitySet;
        const explicit = (this.config.projectChangesEntitySet || "").trim();
        if (explicit) {
            this.projectChangesEntitySet = explicit;
            return explicit;
        }
        try {
            const metadataUrl = `${this.apiRootUrl()}/$metadata`;
            const res = await this.request(metadataUrl);
            const text = await readResponseText(res);
            const entitySets = this.parseEntitySets(text);
            const resolved = this.resolveChangeEntitySetName(entitySets);
            if (resolved) {
                this.projectChangesEntitySet = resolved;
                logger.info("Resolved BC project change feed entity set", { entitySet: resolved });
                return resolved;
            }
        } catch (error) {
            logger.warn("BC project change feed metadata lookup failed", {
                error: error?.message || String(error),
            });
        }
        this.projectChangesEntitySet = "projectChanges";
        return this.projectChangesEntitySet;
    }

    async listProjectTasks(filter) {
        const path = filter ? `/projectTasks?$filter=${encodeURIComponent(filter)}` : "/projectTasks";
        const res = await this.request(path);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async getProjectTask(systemId) {
        const trimmed = (systemId || "").trim();
        if (!trimmed) return null;
        const res = await this.request(`/projectTasks(${trimmed})`);
        return readResponseJson(res);
    }

    async listProjects(filter) {
        const path = filter ? `/projects?$filter=${encodeURIComponent(filter)}` : "/projects";
        const res = await this.request(path);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async getProject(systemId) {
        const trimmed = (systemId || "").trim();
        if (!trimmed) return null;
        const res = await this.request(`/projects(${trimmed})`);
        return readResponseJson(res);
    }

    async createWebhookSubscription(options) {
        const resource = this.buildWebhookResource(options.entitySet);
        if (!resource) throw new Error("BC webhook resource could not be resolved");
        const url = `${this.apiRootUrl()}/subscriptions`;
        const res = await this.request(url, {
            method: "POST",
            body: JSON.stringify({
                notificationUrl: options.notificationUrl,
                resource,
                clientState: options.clientState,
                ...(options.expirationDateTime ? { expirationDateTime: options.expirationDateTime } : {}),
            }),
        });
        return readResponseJson(res);
    }

    async renewWebhookSubscription(subscriptionId, expirationDateTime) {
        const trimmed = (subscriptionId || "").trim();
        if (!trimmed) throw new Error("BC webhook subscription id is required");
        const url = `${this.apiRootUrl()}/subscriptions(${trimmed})`;
        const res = await this.request(url, {
            method: "PATCH",
            body: JSON.stringify({ expirationDateTime }),
        });
        return readResponseJson(res);
    }

    async deleteWebhookSubscription(subscriptionId) {
        const trimmed = (subscriptionId || "").trim();
        if (!trimmed) return;
        const url = `${this.apiRootUrl()}/subscriptions(${trimmed})`;
        await this.request(url, { method: "DELETE" });
    }

    async listProjectChangesSince(lastSeq) {
        const items = [];
        let maxSeq = null;
        let pageCount = 0;
        let nextLink = null;
        let cursor = lastSeq;
        const top = 5000;
        const entitySet = await this.resolveProjectChangesEntitySet();

        while (true) {
            const params = new URLSearchParams();
            if (cursor != null) params.set("$filter", `sequenceNo gt ${cursor}`);
            params.set("$orderby", "sequenceNo asc");
            params.set("$top", String(top));
            const path = nextLink || `/${entitySet}?${params.toString()}`;
            const res = await this.request(path);
            const data = await readResponseJson(res);
            const pageItems = Array.isArray(data?.value) ? data?.value || [] : [];
            for (const raw of pageItems) {
                const rawSeq = raw?.sequenceNo;
                const seq = typeof rawSeq === "number" ? rawSeq : Number(rawSeq);
                if (!Number.isFinite(seq)) {
                    logger.warn("BC project change missing sequenceNo", { sequenceNo: rawSeq });
                    continue;
                }
                const change = { ...raw, sequenceNo: seq };
                items.push(change);
                if (maxSeq == null || seq > maxSeq) maxSeq = seq;
            }
            pageCount += 1;
            const next = data?.["@odata.nextLink"];
            if (next) {
                nextLink = next;
                continue;
            }
            const shouldContinue = pageItems.length >= top && maxSeq != null && (cursor == null || maxSeq > cursor);
            if (shouldContinue) {
                cursor = maxSeq;
                nextLink = null;
                continue;
            }
            break;
        }

        return { items, lastSeq: maxSeq, pageCount };
    }

    async patchProjectTask(systemId, payload) {
        if (!systemId) {
            throw new Error("BC patchProjectTask requires systemId");
        }
        return this.request(`/projectTasks(${systemId})`, {
            method: "PATCH",
            headers: {
                "If-Match": "*",
            },
            body: JSON.stringify(payload),
        });
    }

    async findProjectTaskByPlannerTaskId(plannerTaskId) {
        const escaped = plannerTaskId.replace(/'/g, "''");
        const filter = `plannerTaskId eq '${escaped}'`;
        const tasks = await this.listProjectTasks(filter);
        if (!tasks.length) return null;
        if (tasks.length === 1) return tasks[0];
        const primary = selectPrimaryPlannerTask(tasks);
        logger.warn("Duplicate Planner linkage detected; clearing extras", {
            plannerTaskId,
            count: tasks.length,
            keepProjectNo: primary?.projectNo,
            keepTaskNo: primary?.taskNo,
        });
        for (const task of tasks) {
            if (task === primary) continue;
            await clearDuplicatePlannerLink(this, task, plannerTaskId, primary);
        }
        return primary;
    }
}
