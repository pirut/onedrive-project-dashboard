import { fetchWithRetry, readResponseJson, readResponseText } from "./http";
import { logger } from "./logger";
import { getBcConfig } from "./config";

function parseDateMs(value: string | null | undefined) {
    if (!value) return null;
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
}

function buildPlannerTaskScore(task: BcProjectTask) {
    const hasPlanId = !!(task.plannerPlanId || "").trim();
    const lastSyncMs = parseDateMs(task.lastSyncAt);
    const modifiedMs = parseDateMs(task.systemModifiedAt) ?? parseDateMs(task.lastModifiedDateTime) ?? parseDateMs(task.modifiedAt);
    return {
        hasPlanId: hasPlanId ? 1 : 0,
        lastSyncMs: lastSyncMs ?? -1,
        modifiedMs: modifiedMs ?? -1,
    };
}

function isBetterPlannerTask(candidate: BcProjectTask, current: BcProjectTask) {
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

function selectPrimaryPlannerTask(tasks: BcProjectTask[]) {
    let best = tasks[0];
    for (const task of tasks.slice(1)) {
        if (isBetterPlannerTask(task, best)) {
            best = task;
        }
    }
    return best;
}

async function clearDuplicatePlannerLink(
    bcClient: BusinessCentralClient,
    task: BcProjectTask,
    plannerTaskId: string,
    primaryTask: BcProjectTask
) {
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
            error: (error as Error)?.message,
        });
        return false;
    }
}

export type BcProjectTask = {
    systemId?: string;
    projectNo?: string;
    taskNo?: string;
    description?: string;
    taskType?: string;
    percentComplete?: number;
    startDate?: string;
    endDate?: string;
    manualStartDate?: string;
    manualEndDate?: string;
    assignedPersonCode?: string;
    assignedPersonName?: string;
    budgetTotalCost?: number;
    actualTotalCost?: number;
    plannerPlanId?: string;
    plannerTaskId?: string;
    plannerBucket?: string;
    lastSyncAt?: string;
    lastPlannerEtag?: string;
    syncLock?: boolean;
    systemModifiedAt?: string;
    lastModifiedDateTime?: string;
    modifiedAt?: string;
    [key: string]: unknown;
};

export type BcProject = {
    systemId?: string;
    projectNo?: string;
    description?: string;
    status?: string;
    [key: string]: unknown;
};

export type BcProjectChange = {
    sequenceNo: number;
    projectNo?: string;
    changedAt?: string;
    changeType?: string;
    [key: string]: unknown;
};

export type BcWebhookSubscription = {
    id?: string;
    notificationUrl?: string;
    resource?: string;
    clientState?: string;
    expirationDateTime?: string;
    [key: string]: unknown;
};

type TokenCache = {
    token: string;
    expiresAt: number;
};

export class BusinessCentralClient {
    private config = getBcConfig();
    private tokenCache: TokenCache | null = null;
    private projectChangesEntitySet: string | null = null;

    private baseUrl() {
        const { apiBase, tenantId, environment, publisher, group, version, companyId } = this.config;
        return `${apiBase}/${tenantId}/${environment}/api/${publisher}/${group}/${version}/companies(${companyId})`;
    }

    private apiRootUrl() {
        const { apiBase, tenantId, environment, publisher, group, version } = this.config;
        return `${apiBase}/${tenantId}/${environment}/api/${publisher}/${group}/${version}`;
    }

    private buildWebhookResource(entitySet: string) {
        const { publisher, group, version, companyId } = this.config;
        const trimmed = (entitySet || "").trim().replace(/^\/+/, "");
        if (!trimmed) return "";
        if (trimmed.includes("companies(")) {
            return trimmed.startsWith("api/") ? `/${trimmed}` : `/${trimmed}`;
        }
        return `/api/${publisher}/${group}/${version}/companies(${companyId})/${trimmed}`;
    }

    private async getAccessToken() {
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
        const data = await readResponseJson<{ access_token: string; expires_in: number }>(res);
        if (!data?.access_token) {
            throw new Error("BC token response missing access_token");
        }
        this.tokenCache = {
            token: data.access_token,
            expiresAt: now + (data.expires_in || 3600) * 1000,
        };
        return this.tokenCache.token;
    }

    private async request(path: string, options: RequestInit = {}) {
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

    private parseEntitySets(metadataText: string) {
        if (!metadataText) return [];
        const entitySets: string[] = [];
        const regex = /<EntitySet\s+Name="([^"]+)"/g;
        let match: RegExpExecArray | null;
        while ((match = regex.exec(metadataText))) {
            entitySets.push(match[1]);
        }
        return entitySets;
    }

    private resolveChangeEntitySetName(entitySets: string[]) {
        let best: { name: string; score: number } | null = null;
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

    private async resolveProjectChangesEntitySet() {
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
                error: (error as Error)?.message,
            });
        }
        this.projectChangesEntitySet = "projectChanges";
        return this.projectChangesEntitySet;
    }

    async listProjectTasks(filter?: string) {
        const path = filter ? `/projectTasks?$filter=${encodeURIComponent(filter)}` : "/projectTasks";
        const res = await this.request(path);
        const data = await readResponseJson<{ value: BcProjectTask[] }>(res);
        return data?.value || [];
    }

    async getProjectTask(systemId: string) {
        const trimmed = (systemId || "").trim();
        if (!trimmed) return null;
        const res = await this.request(`/projectTasks(${trimmed})`);
        return readResponseJson<BcProjectTask>(res);
    }

    async listProjects(filter?: string) {
        const path = filter ? `/projects?$filter=${encodeURIComponent(filter)}` : "/projects";
        const res = await this.request(path);
        const data = await readResponseJson<{ value: BcProject[] }>(res);
        return data?.value || [];
    }

    async getProject(systemId: string) {
        const trimmed = (systemId || "").trim();
        if (!trimmed) return null;
        const res = await this.request(`/projects(${trimmed})`);
        return readResponseJson<BcProject>(res);
    }

    async createWebhookSubscription(options: {
        entitySet: string;
        notificationUrl: string;
        clientState?: string;
        expirationDateTime?: string;
    }) {
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
        return readResponseJson<BcWebhookSubscription>(res);
    }

    async listWebhookSubscriptions() {
        const url = `${this.apiRootUrl()}/subscriptions`;
        const res = await this.request(url);
        const data = await readResponseJson<{ value?: BcWebhookSubscription[] }>(res);
        return Array.isArray(data?.value) ? data?.value || [] : [];
    }

    async renewWebhookSubscription(subscriptionId: string, expirationDateTime: string) {
        const trimmed = (subscriptionId || "").trim();
        if (!trimmed) throw new Error("BC webhook subscription id is required");
        const url = `${this.apiRootUrl()}/subscriptions(${trimmed})`;
        const res = await this.request(url, {
            method: "PATCH",
            body: JSON.stringify({ expirationDateTime }),
        });
        return readResponseJson<BcWebhookSubscription>(res);
    }

    async deleteWebhookSubscription(subscriptionId: string) {
        const trimmed = (subscriptionId || "").trim();
        if (!trimmed) return;
        const url = `${this.apiRootUrl()}/subscriptions(${trimmed})`;
        await this.request(url, { method: "DELETE" });
    }

    async listProjectChangesSince(lastSeq: number | null) {
        const items: BcProjectChange[] = [];
        let maxSeq: number | null = null;
        let pageCount = 0;
        let nextLink: string | null = null;
        let cursor: number | null = lastSeq;
        const top = 5000;
        const entitySet = await this.resolveProjectChangesEntitySet();

        while (true) {
            const params = new URLSearchParams();
            if (cursor != null) params.set("$filter", `sequenceNo gt ${cursor}`);
            params.set("$orderby", "sequenceNo asc");
            params.set("$top", String(top));
            const path = nextLink || `/${entitySet}?${params.toString()}`;
            const res = await this.request(path);
            const data = await readResponseJson<{ value?: Record<string, unknown>[]; "@odata.nextLink"?: string }>(res);
            const pageItems = Array.isArray(data?.value) ? data?.value || [] : [];
            for (const raw of pageItems) {
                const rawSeq = (raw as Record<string, unknown>)?.sequenceNo;
                const seq = typeof rawSeq === "number" ? rawSeq : Number(rawSeq);
                if (!Number.isFinite(seq)) {
                    logger.warn("BC project change missing sequenceNo", { sequenceNo: rawSeq });
                    continue;
                }
                const change = { ...(raw as Record<string, unknown>), sequenceNo: seq } as BcProjectChange;
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

    async patchProjectTask(systemId: string, payload: Record<string, unknown>) {
        if (!systemId) {
            throw new Error("BC patchProjectTask requires systemId");
        }
        const res = await this.request(`/projectTasks(${systemId})`, {
            method: "PATCH",
            headers: {
                "If-Match": "*",
            },
            body: JSON.stringify(payload),
        });
        return res;
    }

    async findProjectTaskByPlannerTaskId(plannerTaskId: string) {
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
