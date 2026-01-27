import { fetchWithRetry, readResponseJson, readResponseText } from "./http.js";
import { logger } from "./logger.js";
import { getGraphConfig } from "./config.js";

export class GraphClient {
    constructor() {
        this.config = getGraphConfig();
        this.tokenCache = null;
        this.plannerDeltaSelectResolved = null;
        this.baseUrl = "https://graph.microsoft.com/v1.0";
        this.betaBaseUrl = "https://graph.microsoft.com/beta";
    }

    sanitizeUrl(rawUrl) {
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

    safeParseJson(text) {
        try {
            return JSON.parse(text);
        } catch {
            return null;
        }
    }

    normalizePlannerDeltaSelect(raw) {
        return raw
            .split(",")
            .map((value) => value.trim())
            .filter(Boolean)
            .join(",");
    }

    buildPlannerDeltaSelectCandidates() {
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
        const seen = new Set();
        const ordered = [];
        for (const value of [preferred, ...normalizedCandidates]) {
            if (!value || seen.has(value)) continue;
            seen.add(value);
            ordered.push(value);
        }
        return ordered;
    }

    isPlannerDeltaSelectError(error) {
        const msg = error instanceof Error ? error.message : String(error);
        const lowered = msg.toLowerCase();
        return msg.includes("-> 405") && (lowered.includes("certain fields") || lowered.includes("publication"));
    }

    async listPlannerPlanTasksDeltaWithSelect(planId, select) {
        if (!planId) {
            throw new Error("Planner delta requires planId");
        }
        const normalized = this.normalizePlannerDeltaSelect(select);
        const url = `${this.betaBaseUrl}/planner/plans/${planId}/tasks/delta?$select=${normalized}`;
        const res = await this.request(url);
        const data = await readResponseJson(res);
        return {
            value: data?.value || [],
            nextLink: data?.["@odata.nextLink"],
            deltaLink: data?.["@odata.deltaLink"],
        };
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
        const data = await readResponseJson(res);
        if (!data?.access_token) {
            throw new Error("Graph token response missing access_token");
        }
        this.tokenCache = {
            token: data.access_token,
            expiresAt: now + (data.expires_in || 3600) * 1000,
        };
        return this.tokenCache.token;
    }

    async request(path, options = {}) {
        const token = await this.getAccessToken();
        const url = path.startsWith("http") ? path : `${this.baseUrl}${path}`;
        const method = options.method || "GET";
        const headers = {
            Authorization: `Bearer ${token}`,
            ...(options.headers || {}),
        };
        const hasBody = options.body !== undefined && options.body !== null;
        if (hasBody && !("Content-Type" in headers)) {
            headers["Content-Type"] = "application/json";
        }
        const requestInit = { method, headers };
        if (hasBody) requestInit.body = options.body;
        const res = await fetchWithRetry(url, requestInit);
        if (!res.ok) {
            const text = await readResponseText(res);
            const errorJson = text ? this.safeParseJson(text) : null;
            const errorCode = errorJson?.error?.code;
            const errorMessage = errorJson?.error?.message;
            const isDeltaCookie = res.status === 410 && (errorCode === "UnknownCookie" || /sync point cookie/i.test(errorMessage || ""));
            const isArchivedEntity =
                errorCode === "ArchivedEntityCanNotBeUpdated" || /archived entity/i.test(errorMessage || "");
            const logMeta = {
                method,
                url: this.sanitizeUrl(url),
                status: res.status,
                errorText: text || null,
                errorJson: errorJson || undefined,
            };
            if (isDeltaCookie) {
                logger.info("Graph request failed", logMeta);
            } else if (isArchivedEntity) {
                logger.info("Graph request failed", logMeta);
            } else {
                logger.error("Graph request failed", logMeta);
            }
            throw new Error(`Graph ${method} ${this.sanitizeUrl(url)} -> ${res.status}: ${text}`);
        }
        return res;
    }

    async listPlansForGroup(groupId) {
        const res = await this.request(`/groups/${groupId}/planner/plans`);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async getPlan(planId) {
        const res = await this.request(`/planner/plans/${planId}`);
        const data = await readResponseJson(res);
        return data || null;
    }

    async listTasks(planId) {
        const res = await this.request(`/planner/plans/${planId}/tasks`);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async listPlannerPlanTasksDelta(planId, deltaLink) {
        if (deltaLink) {
            const res = await this.request(deltaLink);
            const data = await readResponseJson(res);
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
        let lastError = null;
        for (let i = 0; i < candidates.length; i += 1) {
            const select = candidates[i];
            const url = `${this.betaBaseUrl}/planner/plans/${planId}/tasks/delta?$select=${select}`;
            try {
                const res = await this.request(url);
                this.plannerDeltaSelectResolved = select;
                if (i > 0) {
                    logger.warn("Planner delta select fallback succeeded", { select, attempt: i + 1, planId });
                }
                const data = await readResponseJson(res);
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

    async createPlan(groupId, title) {
        const res = await this.request(`/planner/plans`, {
            method: "POST",
            body: JSON.stringify({ title, owner: groupId }),
        });
        const data = await readResponseJson(res);
        if (!data) throw new Error("Graph createPlan response empty");
        return data;
    }

    async deletePlan(planId) {
        const plan = await this.getPlan(planId);
        const etag = plan?.["@odata.etag"];
        if (!etag) {
            throw new Error("Planner plan delete missing @odata.etag");
        }
        const res = await this.request(`/planner/plans/${planId}`, {
            method: "DELETE",
            headers: { "If-Match": etag },
        });
        return res.status === 204 || res.status === 202 || res.status === 200;
    }

    async updatePlan(planId, payload, etag) {
        let planEtag = etag;
        if (!planEtag) {
            const plan = await this.getPlan(planId);
            planEtag = plan?.["@odata.etag"];
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
        const data = await readResponseJson(res);
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

    async listGroupMembers(groupId) {
        const members = [];
        let path = `/groups/${groupId}/members?$select=id,displayName,mail,userPrincipalName&$top=999`;
        while (path) {
            const res = await this.request(path);
            const data = await readResponseJson(res);
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

    async findUserIdByIdentity(identity) {
        const trimmed = identity.trim();
        if (!trimmed) return null;
        const escaped = trimmed.replace(/'/g, "''");
        const isEmail = trimmed.includes("@");
        const filter = isEmail
            ? `userPrincipalName eq '${escaped}' or mail eq '${escaped}'`
            : `displayName eq '${escaped}'`;
        let path = `/users?$filter=${encodeURIComponent(filter)}&$select=id,displayName,mail,userPrincipalName&$top=5`;
        let res = await this.request(path);
        let data = await readResponseJson(res);
        let users = data?.value || [];
        if (!users.length && !isEmail) {
            const fallback = `startswith(displayName,'${escaped}')`;
            path = `/users?$filter=${encodeURIComponent(fallback)}&$select=id,displayName,mail,userPrincipalName&$top=5`;
            res = await this.request(path);
            data = await readResponseJson(res);
            users = data?.value || [];
        }
        if (users.length > 1) {
            logger.warn("Multiple Graph users matched identity", { identity: trimmed, count: users.length });
        }
        return users[0]?.id || null;
    }

    async listBuckets(planId) {
        const res = await this.request(`/planner/plans/${planId}/buckets`);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async createBucket(planId, name) {
        const res = await this.request(`/planner/buckets`, {
            method: "POST",
            body: JSON.stringify({ name, planId, orderHint: " !" }),
        });
        const data = await readResponseJson(res);
        if (!data) throw new Error("Graph createBucket response empty");
        return data;
    }

    async getBucket(bucketId) {
        const res = await this.request(`/planner/buckets/${bucketId}`);
        const data = await readResponseJson(res);
        return data || null;
    }

    async getTask(taskId) {
        const res = await this.request(`/planner/tasks/${taskId}`);
        const data = await readResponseJson(res);
        return data || null;
    }

    async getTaskDetails(taskId) {
        const res = await this.request(`/planner/tasks/${taskId}/details`);
        const data = await readResponseJson(res);
        return data || null;
    }

    async createTask(payload) {
        const res = await this.request(`/planner/tasks`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        const data = await readResponseJson(res);
        if (!data?.id) throw new Error("Graph createTask response missing id");
        return data;
    }

    async updateTask(taskId, payload, etag) {
        const res = await this.request(`/planner/tasks/${taskId}`, {
            method: "PATCH",
            headers: { "If-Match": etag },
            body: JSON.stringify(payload),
        });
        return res.headers.get("etag") || res.headers.get("ETag") || undefined;
    }

    async updateTaskDetails(taskId, payload, etag) {
        const res = await this.request(`/planner/tasks/${taskId}/details`, {
            method: "PATCH",
            headers: { "If-Match": etag },
            body: JSON.stringify(payload),
        });
        return res.headers.get("etag") || res.headers.get("ETag") || undefined;
    }

    async createSubscription(payload) {
        const res = await this.request(`/subscriptions`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        const data = await readResponseJson(res);
        if (!data?.id) throw new Error("Graph createSubscription response missing id");
        logger.info("Graph subscription created", { subscriptionId: data.id, resource: data.resource });
        return data;
    }

    async listSubscriptions() {
        const res = await this.request(`/subscriptions`);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async deleteSubscription(id) {
        const res = await this.request(`/subscriptions/${id}`, {
            method: "DELETE",
        });
        return res.status === 204 || res.status === 202 || res.status === 200;
    }

    async renewSubscription(id, expirationDateTime) {
        const res = await this.request(`/subscriptions/${id}`, {
            method: "PATCH",
            body: JSON.stringify({ expirationDateTime }),
        });
        const data = await readResponseJson(res);
        return data || null;
    }
}
