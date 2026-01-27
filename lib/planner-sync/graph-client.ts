import { fetchWithRetry, readResponseJson, readResponseText } from "./http";
import { logger } from "./logger";
import { getGraphConfig } from "./config";

export type PlannerTask = {
    id: string;
    title?: string;
    planId?: string;
    bucketId?: string;
    createdDateTime?: string | null;
    startDateTime?: string | null;
    dueDateTime?: string | null;
    lastModifiedDateTime?: string | null;
    percentComplete?: number;
    assignments?: Record<string, unknown>;
    "@odata.etag"?: string;
    [key: string]: unknown;
};

export type PlannerTaskDelta = PlannerTask & {
    "@removed"?: { reason?: string };
};

type PlannerTaskDeltaPage = {
    value?: PlannerTaskDelta[];
    "@odata.nextLink"?: string;
    "@odata.deltaLink"?: string;
};

export type PlannerTaskDetails = {
    description?: string;
    "@odata.etag"?: string;
    [key: string]: unknown;
};

export type PlannerBucket = {
    id: string;
    name?: string;
    orderHint?: string;
    planId?: string;
    [key: string]: unknown;
};

export type PlannerPlan = {
    id: string;
    title?: string;
    createdDateTime?: string;
    "@odata.etag"?: string;
    [key: string]: unknown;
};

export type GraphOrganization = {
    id?: string;
    displayName?: string;
    verifiedDomains?: { name?: string; isDefault?: boolean; isInitial?: boolean }[];
    [key: string]: unknown;
};

export type GraphUser = {
    id?: string;
    displayName?: string;
    mail?: string;
    userPrincipalName?: string;
    "@odata.type"?: string;
    [key: string]: unknown;
};

export type GraphSubscription = {
    id: string;
    resource?: string;
    expirationDateTime?: string;
    clientState?: string;
    [key: string]: unknown;
};

type TokenCache = {
    token: string;
    expiresAt: number;
};

export class GraphClient {
    private config = getGraphConfig();
    private tokenCache: TokenCache | null = null;
    private plannerDeltaSelectResolved: string | null = null;
    private baseUrl = "https://graph.microsoft.com/v1.0";
    private betaBaseUrl = "https://graph.microsoft.com/beta";

    private sanitizeUrl(rawUrl: string) {
        try {
            const url = new URL(rawUrl);
            const redactKeys = new Set([
                "$deltatoken",
                "$skiptoken",
                "deltatoken",
                "skiptoken",
                "token",
                "access_token",
                "client_secret",
            ]);
            for (const key of redactKeys) {
                if (url.searchParams.has(key)) {
                    url.searchParams.set(key, "<redacted>");
                }
            }
            return url.toString();
        } catch {
            return rawUrl;
        }
    }

    private safeParseJson(text: string) {
        try {
            return JSON.parse(text) as unknown;
        } catch {
            return null;
        }
    }

    private normalizePlannerDeltaSelect(raw: string) {
        return raw
            .split(",")
            .map((value) => value.trim())
            .filter(Boolean)
            .join(",");
    }

    private buildPlannerDeltaSelectCandidates() {
        if (this.plannerDeltaSelectResolved) return [this.plannerDeltaSelectResolved];
        const candidates = [
            "id,planId,title,bucketId,percentComplete,startDateTime,dueDateTime,lastModifiedDateTime,assignments",
            "id,planId,title,bucketId,percentComplete,startDateTime,dueDateTime,lastModifiedDateTime",
            "id,planId,title,bucketId,createdBy,lastModifiedBy,createdDateTime,lastModifiedDateTime",
            "id,planId,title,bucketId,createdDateTime,dueDateTime,percentComplete",
            "id,planId,title,bucketId,creationSource,createdBy,lastModifiedBy",
            "id,planId,title,bucketId,orderHint",
            "id,planId,title,bucketId",
        ];
        const normalizedCandidates = candidates.map((value) => this.normalizePlannerDeltaSelect(value));
        if (!this.config.plannerDeltaSelect) return normalizedCandidates;
        const preferred = this.normalizePlannerDeltaSelect(this.config.plannerDeltaSelect);
        const seen = new Set<string>();
        const ordered: string[] = [];
        for (const value of [preferred, ...normalizedCandidates]) {
            if (!value || seen.has(value)) continue;
            seen.add(value);
            ordered.push(value);
        }
        return ordered;
    }

    private isPlannerDeltaSelectError(error: unknown) {
        const msg = error instanceof Error ? error.message : String(error);
        const lowered = msg.toLowerCase();
        return msg.includes("-> 405") && (lowered.includes("certain fields") || lowered.includes("publication"));
    }

    async listPlannerPlanTasksDeltaWithSelect(planId: string, select: string) {
        if (!planId) {
            throw new Error("Planner delta requires planId");
        }
        const normalized = this.normalizePlannerDeltaSelect(select);
        const url = `${this.betaBaseUrl}/planner/plans/${planId}/tasks/delta?$select=${normalized}`;
        const res = await this.request(url);
        const data = await readResponseJson<PlannerTaskDeltaPage>(res);
        return {
            value: data?.value || [],
            nextLink: data?.["@odata.nextLink"],
            deltaLink: data?.["@odata.deltaLink"],
        };
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
            scope: "https://graph.microsoft.com/.default",
        });
        const res = await fetchWithRetry(tokenUrl, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body,
        });
        if (!res.ok) {
            const text = await readResponseText(res);
            throw new Error(`Graph token error ${res.status}: ${text}`);
        }
        const data = await readResponseJson<{ access_token: string; expires_in: number }>(res);
        if (!data?.access_token) {
            throw new Error("Graph token response missing access_token");
        }
        this.tokenCache = {
            token: data.access_token,
            expiresAt: now + (data.expires_in || 3600) * 1000,
        };
        return this.tokenCache.token;
    }

    private async request(path: string, options: RequestInit = {}) {
        const token = await this.getAccessToken();
        const url = path.startsWith("http") ? path : `${this.baseUrl}${path}`;
        const method = options.method || "GET";
        const headers = {
            Authorization: `Bearer ${token}`,
            ...(options.headers || {}),
        } as Record<string, string>;
        const hasBody = options.body !== undefined && options.body !== null;
        if (hasBody && !("Content-Type" in headers)) {
            headers["Content-Type"] = "application/json";
        }
        const requestInit: RequestInit = { method, headers };
        if (hasBody) requestInit.body = options.body;
        const res = await fetchWithRetry(url, requestInit);
        if (!res.ok) {
            const text = await readResponseText(res);
            const errorJson = text ? this.safeParseJson(text) : null;
            const errorCode = (errorJson as { error?: { code?: string; message?: string } })?.error?.code;
            const errorMessage = (errorJson as { error?: { code?: string; message?: string } })?.error?.message;
            const isDeltaCookie = res.status === 410 && (errorCode === "UnknownCookie" || /sync point cookie/i.test(errorMessage || ""));
            const logMeta = {
                method,
                url: this.sanitizeUrl(url),
                status: res.status,
                errorText: text || null,
                errorJson: errorJson || undefined,
            };
            if (isDeltaCookie) {
                logger.info("Graph request failed", logMeta);
            } else {
                logger.error("Graph request failed", logMeta);
            }
            throw new Error(`Graph ${method} ${this.sanitizeUrl(url)} -> ${res.status}: ${text}`);
        }
        return res;
    }

    async listPlansForGroup(groupId: string) {
        const res = await this.request(`/groups/${groupId}/planner/plans`);
        const data = await readResponseJson<{ value: PlannerPlan[] }>(res);
        return data?.value || [];
    }

    async getPlan(planId: string) {
        const res = await this.request(`/planner/plans/${planId}`);
        const data = await readResponseJson<PlannerPlan>(res);
        return data || null;
    }

    async listTasks(planId: string) {
        const res = await this.request(`/planner/plans/${planId}/tasks`);
        const data = await readResponseJson<{ value: PlannerTask[] }>(res);
        return data?.value || [];
    }

    async listPlannerPlanTasksDelta(planId: string, deltaLink?: string) {
        if (deltaLink) {
            const res = await this.request(deltaLink);
            const data = await readResponseJson<PlannerTaskDeltaPage>(res);
            return {
                value: data?.value || [],
                nextLink: data?.["@odata.nextLink"],
                deltaLink: data?.["@odata.deltaLink"],
            };
        }

        if (!planId) {
            throw new Error("Planner delta requires planId");
        }

        const candidates = this.buildPlannerDeltaSelectCandidates();
        let lastError: Error | null = null;
        for (let i = 0; i < candidates.length; i += 1) {
            const select = candidates[i];
            const url = `${this.betaBaseUrl}/planner/plans/${planId}/tasks/delta?$select=${select}`;
            try {
                const res = await this.request(url);
                this.plannerDeltaSelectResolved = select;
                if (i > 0) {
                    logger.warn("Planner delta select fallback succeeded", { select, attempt: i + 1, planId });
                }
                const data = await readResponseJson<PlannerTaskDeltaPage>(res);
                return {
                    value: data?.value || [],
                    nextLink: data?.["@odata.nextLink"],
                    deltaLink: data?.["@odata.deltaLink"],
                };
            } catch (error) {
                lastError = error instanceof Error ? error : new Error(String(error));
                if (this.isPlannerDeltaSelectError(error) && i < candidates.length - 1) {
                    logger.warn("Planner delta select rejected; trying fallback", {
                        select,
                        attempt: i + 1,
                        planId,
                    });
                    continue;
                }
                throw error;
            }
        }
        throw lastError ?? new Error("Planner delta request failed");
    }

    async createPlan(groupId: string, title: string) {
        const res = await this.request(`/planner/plans`, {
            method: "POST",
            body: JSON.stringify({ title, owner: groupId }),
        });
        const data = await readResponseJson<PlannerPlan>(res);
        if (!data) throw new Error("Graph createPlan response empty");
        return data;
    }

    async deletePlan(planId: string) {
        const plan = await this.getPlan(planId);
        const etag = plan?.["@odata.etag"] as string | undefined;
        if (!etag) {
            throw new Error("Planner plan delete missing @odata.etag");
        }
        const res = await this.request(`/planner/plans/${planId}`, {
            method: "DELETE",
            headers: { "If-Match": etag },
        });
        return res.status === 204 || res.status === 202 || res.status === 200;
    }

    async updatePlan(planId: string, payload: Record<string, unknown>, etag?: string) {
        let planEtag = etag;
        if (!planEtag) {
            const plan = await this.getPlan(planId);
            planEtag = plan?.["@odata.etag"] as string | undefined;
        }
        if (!planEtag) {
            throw new Error("Planner plan update missing @odata.etag");
        }
        const res = await this.request(`/planner/plans/${planId}`, {
            method: "PATCH",
            headers: { "If-Match": planEtag },
            body: JSON.stringify(payload),
        });
        return res;
    }

    async getOrganizations() {
        const res = await this.request(`/organization`);
        const data = await readResponseJson<{ value: GraphOrganization[] }>(res);
        return data?.value || [];
    }

    async getDefaultDomain() {
        const orgs = await this.getOrganizations();
        const org = orgs[0];
        if (!org?.verifiedDomains?.length) return null;
        const domains = org.verifiedDomains;
        const preferred = domains.find((domain) => domain?.isDefault) || domains.find((domain) => domain?.isInitial);
        return (preferred || domains[0])?.name || null;
    }

    async listGroupMembers(groupId: string) {
        const members: GraphUser[] = [];
        let path = `/groups/${groupId}/members?$select=id,displayName,mail,userPrincipalName&$top=999`;
        while (path) {
            const res = await this.request(path);
            const data = await readResponseJson<{ value: GraphUser[]; "@odata.nextLink"?: string }>(res);
            if (data?.value?.length) members.push(...data.value);
            const next = data?.["@odata.nextLink"];
            if (next && next.startsWith(this.baseUrl)) {
                path = next.slice(this.baseUrl.length);
                continue;
            }
            path = "";
        }
        return members;
    }

    async findUserIdByIdentity(identity: string) {
        const trimmed = identity.trim();
        if (!trimmed) return null;
        const escaped = trimmed.replace(/'/g, "''");
        const isEmail = trimmed.includes("@");
        const filter = isEmail
            ? `userPrincipalName eq '${escaped}' or mail eq '${escaped}'`
            : `displayName eq '${escaped}'`;
        let path = `/users?$filter=${encodeURIComponent(filter)}&$select=id,displayName,mail,userPrincipalName&$top=5`;
        let res = await this.request(path);
        let data = await readResponseJson<{ value: GraphUser[] }>(res);
        let users = data?.value || [];
        if (!users.length && !isEmail) {
            const fallback = `startswith(displayName,'${escaped}')`;
            path = `/users?$filter=${encodeURIComponent(fallback)}&$select=id,displayName,mail,userPrincipalName&$top=5`;
            res = await this.request(path);
            data = await readResponseJson<{ value: GraphUser[] }>(res);
            users = data?.value || [];
        }
        if (users.length > 1) {
            logger.warn("Multiple Graph users matched identity", { identity: trimmed, count: users.length });
        }
        return users[0]?.id || null;
    }

    async listBuckets(planId: string) {
        const res = await this.request(`/planner/plans/${planId}/buckets`);
        const data = await readResponseJson<{ value: PlannerBucket[] }>(res);
        return data?.value || [];
    }

    async createBucket(planId: string, name: string) {
        const res = await this.request(`/planner/buckets`, {
            method: "POST",
            body: JSON.stringify({ name, planId, orderHint: " !" }),
        });
        const data = await readResponseJson<PlannerBucket>(res);
        if (!data) throw new Error("Graph createBucket response empty");
        return data;
    }

    async getBucket(bucketId: string) {
        const res = await this.request(`/planner/buckets/${bucketId}`);
        const data = await readResponseJson<PlannerBucket>(res);
        return data || null;
    }

    async getTask(taskId: string) {
        const res = await this.request(`/planner/tasks/${taskId}`);
        const data = await readResponseJson<PlannerTask>(res);
        return data || null;
    }

    async getTaskDetails(taskId: string) {
        const res = await this.request(`/planner/tasks/${taskId}/details`);
        const data = await readResponseJson<PlannerTaskDetails>(res);
        return data || null;
    }

    async createTask(payload: Record<string, unknown>) {
        const res = await this.request(`/planner/tasks`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        const data = await readResponseJson<PlannerTask>(res);
        if (!data?.id) throw new Error("Graph createTask response missing id");
        return data;
    }

    async updateTask(taskId: string, payload: Record<string, unknown>, etag: string) {
        const res = await this.request(`/planner/tasks/${taskId}`, {
            method: "PATCH",
            headers: { "If-Match": etag },
            body: JSON.stringify(payload),
        });
        const responseEtag = res.headers.get("etag") || res.headers.get("ETag") || undefined;
        return responseEtag;
    }

    async updateTaskDetails(taskId: string, payload: Record<string, unknown>, etag: string) {
        const res = await this.request(`/planner/tasks/${taskId}/details`, {
            method: "PATCH",
            headers: { "If-Match": etag },
            body: JSON.stringify(payload),
        });
        const responseEtag = res.headers.get("etag") || res.headers.get("ETag") || undefined;
        return responseEtag;
    }

    async createSubscription(payload: Record<string, unknown>) {
        const res = await this.request(`/subscriptions`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        const data = await readResponseJson<GraphSubscription>(res);
        if (!data?.id) throw new Error("Graph createSubscription response missing id");
        logger.info("Graph subscription created", { subscriptionId: data.id, resource: data.resource });
        return data;
    }

    async listSubscriptions() {
        const res = await this.request(`/subscriptions`);
        const data = await readResponseJson<{ value: GraphSubscription[] }>(res);
        return data?.value || [];
    }

    async deleteSubscription(id: string) {
        const res = await this.request(`/subscriptions/${id}`, {
            method: "DELETE",
        });
        return res.status === 204 || res.status === 202 || res.status === 200;
    }

    async renewSubscription(id: string, expirationDateTime: string) {
        const res = await this.request(`/subscriptions/${id}`, {
            method: "PATCH",
            body: JSON.stringify({ expirationDateTime }),
        });
        const data = await readResponseJson<GraphSubscription>(res);
        return data || null;
    }
}
