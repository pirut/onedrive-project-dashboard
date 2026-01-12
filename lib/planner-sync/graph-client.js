import { fetchWithRetry, readResponseJson, readResponseText } from "./http.js";
import { logger } from "./logger.js";
import { getGraphConfig } from "./config.js";

export class GraphClient {
    constructor() {
        this.config = getGraphConfig();
        this.tokenCache = null;
        this.baseUrl = "https://graph.microsoft.com/v1.0";
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
        const url = `${this.baseUrl}${path}`;
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
            throw new Error(`Graph ${options.method || "GET"} ${path} -> ${res.status}: ${text}`);
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

    async createPlan(groupId, title) {
        const res = await this.request(`/planner/plans`, {
            method: "POST",
            body: JSON.stringify({ title, owner: groupId }),
        });
        const data = await readResponseJson(res);
        if (!data) throw new Error("Graph createPlan response empty");
        return data;
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

    async renewSubscription(id, expirationDateTime) {
        const res = await this.request(`/subscriptions/${id}`, {
            method: "PATCH",
            body: JSON.stringify({ expirationDateTime }),
        });
        const data = await readResponseJson(res);
        return data || null;
    }
}
