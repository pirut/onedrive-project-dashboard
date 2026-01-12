import { fetchWithRetry, readResponseJson, readResponseText } from "./http";
import { logger } from "./logger";
import { getGraphConfig } from "./config";

export type PlannerTask = {
    id: string;
    title?: string;
    planId?: string;
    bucketId?: string;
    startDateTime?: string | null;
    dueDateTime?: string | null;
    lastModifiedDateTime?: string | null;
    percentComplete?: number;
    "@odata.etag"?: string;
    [key: string]: unknown;
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
    [key: string]: unknown;
};

export type GraphOrganization = {
    id?: string;
    displayName?: string;
    verifiedDomains?: { name?: string; isDefault?: boolean; isInitial?: boolean }[];
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
    private baseUrl = "https://graph.microsoft.com/v1.0";

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

    async createPlan(groupId: string, title: string) {
        const res = await this.request(`/planner/plans`, {
            method: "POST",
            body: JSON.stringify({ title, owner: groupId }),
        });
        const data = await readResponseJson<PlannerPlan>(res);
        if (!data) throw new Error("Graph createPlan response empty");
        return data;
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
