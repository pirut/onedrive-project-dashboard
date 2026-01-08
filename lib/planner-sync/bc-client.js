import { fetchWithRetry, readResponseJson, readResponseText } from "./http.js";
import { logger } from "./logger.js";
import { getBcConfig } from "./config.js";

export class BusinessCentralClient {
    constructor() {
        this.config = getBcConfig();
        this.tokenCache = null;
    }

    baseUrl() {
        const { apiBase, tenantId, environment, publisher, group, version, companyId } = this.config;
        return `${apiBase}/${tenantId}/${environment}/api/${publisher}/${group}/${version}/companies(${companyId})`;
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
        const url = `${this.baseUrl()}${path}`;
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

    async listProjectTasks(filter) {
        const path = filter ? `/projectTasks?$filter=${encodeURIComponent(filter)}` : "/projectTasks";
        const res = await this.request(path);
        const data = await readResponseJson(res);
        return data?.value || [];
    }

    async listProjects(filter) {
        const path = filter ? `/projects?$filter=${encodeURIComponent(filter)}` : "/projects";
        const res = await this.request(path);
        const data = await readResponseJson(res);
        return data?.value || [];
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
        if (tasks.length > 1) {
            logger.warn("Multiple BC tasks found for Planner task", { plannerTaskId, count: tasks.length });
        }
        return tasks[0];
    }
}
