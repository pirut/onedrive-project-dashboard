import { fetchWithRetry, readResponseJson, readResponseText } from "./planner-sync/http.js";
import { logger } from "./planner-sync/logger.js";
import { getDataverseRefreshToken, saveDataverseRefreshToken } from "./dataverse-auth-store.js";

function readEnv(name, required = false) {
    const value = process.env[name];
    if (required && !value) {
        throw new Error(`Missing env ${name}`);
    }
    return value;
}

function normalizeBaseUrl(raw) {
    return raw.replace(/\/+$/, "");
}

function normalizeAuthMode(raw) {
    const mode = (raw || "client_credentials").trim().toLowerCase();
    return mode === "delegated" ? "delegated" : "client_credentials";
}

function normalizeAuthScopes(scopes, baseUrl) {
    const trimmed = String(scopes || "").trim().replace(/\s+/g, " ");
    if (trimmed) return trimmed;
    return `${baseUrl}/user_impersonation offline_access`;
}

export function getDataverseConfig() {
    const baseUrl = normalizeBaseUrl(readEnv("DATAVERSE_BASE_URL", true));
    const apiVersion = readEnv("DATAVERSE_API_VERSION") || "v9.2";
    const tenantId = readEnv("DATAVERSE_TENANT_ID", true);
    const clientId = readEnv("DATAVERSE_CLIENT_ID", true);
    const clientSecret = readEnv("DATAVERSE_CLIENT_SECRET", true);
    const resourceScope = readEnv("DATAVERSE_RESOURCE_SCOPE") || `${baseUrl}/.default`;
    const authMode = normalizeAuthMode(readEnv("DATAVERSE_AUTH_MODE"));
    const authClientId = readEnv("DATAVERSE_AUTH_CLIENT_ID") || clientId;
    const authClientSecret = readEnv("DATAVERSE_AUTH_CLIENT_SECRET") || clientSecret;
    const authScopes = normalizeAuthScopes(readEnv("DATAVERSE_AUTH_SCOPES") || "", baseUrl);
    return {
        baseUrl,
        apiVersion,
        tenantId,
        clientId,
        clientSecret,
        resourceScope,
        authMode,
        authClientId,
        authClientSecret,
        authScopes,
    };
}

function sanitizeUrl(rawUrl) {
    try {
        const url = new URL(rawUrl);
        const redactKeys = new Set(["token", "access_token", "client_secret", "$deltatoken", "deltatoken", "$skiptoken"]);
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

function parseEntityIdHeader(headerValue) {
    if (!headerValue) return null;
    const match = headerValue.match(/\(([^)]+)\)$/);
    return match ? match[1] : null;
}

export class DataverseClient {
    constructor() {
        this.config = getDataverseConfig();
        this.tokenCache = null;
    }

    apiRoot() {
        const { baseUrl, apiVersion } = this.config;
        return `${baseUrl}/api/data/${apiVersion}`;
    }

    async getAccessToken() {
        const now = Date.now();
        if (this.tokenCache && now < this.tokenCache.expiresAt - 60000) {
            return this.tokenCache.token;
        }
        const { tenantId, clientId, clientSecret, resourceScope, authMode, authClientId, authClientSecret, authScopes } = this.config;
        const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
        if (authMode === "delegated") {
            const refreshToken = await getDataverseRefreshToken();
            if (!refreshToken) {
                throw new Error("Missing Dataverse refresh token. Visit /api/auth/dataverse/login to authorize.");
            }
            const body = new URLSearchParams({
                grant_type: "refresh_token",
                client_id: authClientId || clientId,
                refresh_token: refreshToken,
            });
            const secret = authClientSecret || clientSecret;
            if (secret) body.set("client_secret", secret);
            if (authScopes) body.set("scope", authScopes);
            const res = await fetchWithRetry(tokenUrl, {
                method: "POST",
                headers: { "Content-Type": "application/x-www-form-urlencoded" },
                body,
            });
            if (!res.ok) {
                const text = await readResponseText(res);
                throw new Error(`Dataverse refresh token error ${res.status}: ${text}`);
            }
            const data = await readResponseJson(res);
            if (!data || !data.access_token) {
                throw new Error("Dataverse token response missing access_token");
            }
            if (data.refresh_token) {
                await saveDataverseRefreshToken(data.refresh_token);
            }
            this.tokenCache = {
                token: data.access_token,
                expiresAt: now + (data.expires_in || 3600) * 1000,
            };
            return this.tokenCache.token;
        }

        const body = new URLSearchParams({
            grant_type: "client_credentials",
            client_id: clientId,
            client_secret: clientSecret,
            scope: resourceScope,
        });
        const res = await fetchWithRetry(tokenUrl, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body,
        });
        if (!res.ok) {
            const text = await readResponseText(res);
            throw new Error(`Dataverse token error ${res.status}: ${text}`);
        }
        const data = await readResponseJson(res);
        if (!data || !data.access_token) {
            throw new Error("Dataverse token response missing access_token");
        }
        this.tokenCache = {
            token: data.access_token,
            expiresAt: now + (data.expires_in || 3600) * 1000,
        };
        return this.tokenCache.token;
    }

    async request(pathOrUrl, options = {}) {
        const token = await this.getAccessToken();
        const url = pathOrUrl.startsWith("http") ? pathOrUrl : `${this.apiRoot()}${pathOrUrl}`;
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
            throw new Error(`Dataverse ${options.method || "GET"} ${sanitizeUrl(url)} -> ${res.status}: ${text}`);
        }
        return res;
    }

    async executeAction(actionName, payload) {
        const res = await this.request(`/${actionName}`, {
            method: "POST",
            body: payload == null ? undefined : JSON.stringify(payload),
        });
        return readResponseJson(res);
    }

    async createOperationSet(projectId, description) {
        const payload = { ProjectId: projectId };
        if (description)
            payload.Description = description;
        const data = await this.executeAction("msdyn_CreateOperationSetV1", payload);
        const opId = (data === null || data === void 0 ? void 0 : data.OperationSetId) || (data === null || data === void 0 ? void 0 : data.operationSetId);
        return opId || "";
    }

    async executeOperationSet(operationSetId) {
        const payload = { OperationSetId: operationSetId };
        return this.executeAction("msdyn_ExecuteOperationSetV1", payload);
    }

    async pssCreate(entity, operationSetId) {
        const payload = {
            OperationSetId: operationSetId,
            Entity: entity,
        };
        return this.executeAction("msdyn_PssCreateV1", payload);
    }

    async pssUpdate(entity, operationSetId) {
        const payload = {
            OperationSetId: operationSetId,
            Entity: entity,
        };
        return this.executeAction("msdyn_PssUpdateV1", payload);
    }

    async whoAmI() {
        const res = await this.request("/WhoAmI");
        return readResponseJson(res);
    }

    buildLookupBinding(entitySet, id) {
        const trimmed = (entitySet || "").trim().replace(/^\/+/, "");
        const cleanId = id.trim().replace(/^\{/, "").replace(/\}$/, "");
        if (!trimmed || !cleanId) return "";
        return `/${trimmed}(${cleanId})`;
    }

    async list(entitySet, options = {}) {
        const params = new URLSearchParams();
        if (options.select && options.select.length) params.set("$select", options.select.join(","));
        if (options.filter) params.set("$filter", options.filter);
        if (options.orderBy) params.set("$orderby", options.orderBy);
        if (options.expand) params.set("$expand", options.expand);
        if (typeof options.top === "number") params.set("$top", String(options.top));
        const query = params.toString();
        const path = `/${entitySet}${query ? `?${query}` : ""}`;
        const res = await this.request(path);
        const data = await readResponseJson(res);
        return {
            value: Array.isArray(data && data.value) ? data.value : [],
            nextLink: data && data["@odata.nextLink"],
            deltaLink: data && data["@odata.deltaLink"],
        };
    }

    async listChanges(entitySet, options = {}) {
        const items = [];
        let nextLink = options.deltaLink || null;
        let deltaLink;
        const maxPages = options.maxPages ?? 10;
        let pages = 0;

        while (pages < maxPages) {
            const res = await this.request(
                nextLink ||
                    `/${entitySet}?${new URLSearchParams({
                        ...(options.select && options.select.length ? { $select: options.select.join(",") } : {}),
                        ...(options.filter ? { $filter: options.filter } : {}),
                        ...(options.orderBy ? { $orderby: options.orderBy } : {}),
                    }).toString()}`,
                {
                    headers: {
                        Prefer: "odata.track-changes",
                    },
                }
            );
            const data = await readResponseJson(res);
            const pageItems = Array.isArray(data && data.value) ? data.value : [];
            items.push(...pageItems);
            nextLink = (data && data["@odata.nextLink"]) || null;
            if (data && data["@odata.deltaLink"]) {
                deltaLink = data["@odata.deltaLink"];
            }
            pages += 1;
            if (!nextLink) break;
        }

        if (pages >= maxPages && nextLink) {
            logger.warn("Dataverse change tracking page limit reached", { entitySet, pages, nextLink: sanitizeUrl(nextLink) });
        }

        return { value: items, deltaLink };
    }

    async getById(entitySet, id, select) {
        const params = new URLSearchParams();
        if (select && select.length) params.set("$select", select.join(","));
        const query = params.toString();
        const path = `/${entitySet}(${id})${query ? `?${query}` : ""}`;
        const res = await this.request(path);
        return readResponseJson(res);
    }

    async create(entitySet, payload) {
        const res = await this.request(`/${entitySet}`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        const entityId = parseEntityIdHeader(res.headers.get("OData-EntityId") || res.headers.get("odata-entityid"));
        const etag = res.headers.get("ETag") || res.headers.get("etag") || undefined;
        return { entityId, etag };
    }

    async update(entitySet, id, payload, options = {}) {
        const headers = {};
        if (options.ifMatch) headers["If-Match"] = options.ifMatch;
        const res = await this.request(`/${entitySet}(${id})`, {
            method: "PATCH",
            headers,
            body: JSON.stringify(payload),
        });
        const etag = res.headers.get("ETag") || res.headers.get("etag") || undefined;
        return { etag };
    }

    async delete(entitySet, id) {
        await this.request(`/${entitySet}(${id})`, { method: "DELETE" });
    }
}
