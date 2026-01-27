import { fetchWithRetry, readResponseJson, readResponseText } from "./planner-sync/http";
import { logger } from "./planner-sync/logger";

export type DataverseConfig = {
    baseUrl: string;
    apiVersion: string;
    tenantId: string;
    clientId: string;
    clientSecret: string;
    resourceScope: string;
};

export type DataverseEntity = Record<string, unknown>;

export type DataverseListResponse<T extends DataverseEntity> = {
    value: T[];
    nextLink?: string;
    deltaLink?: string;
};

type TokenCache = {
    token: string;
    expiresAt: number;
};

function readEnv(name: string, required = false) {
    const value = process.env[name];
    if (required && !value) {
        throw new Error(`Missing env ${name}`);
    }
    return value;
}

function normalizeBaseUrl(raw: string) {
    return raw.replace(/\/+$/, "");
}

export function getDataverseConfig(): DataverseConfig {
    const baseUrl = normalizeBaseUrl(readEnv("DATAVERSE_BASE_URL", true) as string);
    const apiVersion = readEnv("DATAVERSE_API_VERSION") || "v9.2";
    const tenantId = readEnv("DATAVERSE_TENANT_ID", true) as string;
    const clientId = readEnv("DATAVERSE_CLIENT_ID", true) as string;
    const clientSecret = readEnv("DATAVERSE_CLIENT_SECRET", true) as string;
    const resourceScope = readEnv("DATAVERSE_RESOURCE_SCOPE") || `${baseUrl}/.default`;
    return { baseUrl, apiVersion, tenantId, clientId, clientSecret, resourceScope };
}

function sanitizeUrl(rawUrl: string) {
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

function parseEntityIdHeader(headerValue: string | null) {
    if (!headerValue) return null;
    const match = headerValue.match(/\(([^)]+)\)$/);
    return match ? match[1] : null;
}

export class DataverseClient {
    private config = getDataverseConfig();
    private tokenCache: TokenCache | null = null;

    private apiRoot() {
        const { baseUrl, apiVersion } = this.config;
        return `${baseUrl}/api/data/${apiVersion}`;
    }

    private async getAccessToken() {
        const now = Date.now();
        if (this.tokenCache && now < this.tokenCache.expiresAt - 60_000) {
            return this.tokenCache.token;
        }
        const { tenantId, clientId, clientSecret, resourceScope } = this.config;
        const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
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
        const data = await readResponseJson<{ access_token: string; expires_in: number }>(res);
        if (!data?.access_token) {
            throw new Error("Dataverse token response missing access_token");
        }
        this.tokenCache = {
            token: data.access_token,
            expiresAt: now + (data.expires_in || 3600) * 1000,
        };
        return this.tokenCache.token;
    }

    private async request(pathOrUrl: string, options: RequestInit = {}) {
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

    async whoAmI() {
        const res = await this.request("/WhoAmI");
        return readResponseJson<{ UserId?: string; BusinessUnitId?: string; OrganizationId?: string }>(res);
    }

    buildLookupBinding(entitySet: string, id: string) {
        const trimmed = (entitySet || "").trim().replace(/^\/+/, "");
        const cleanId = id.trim().replace(/^\{/, "").replace(/\}$/, "");
        if (!trimmed || !cleanId) return "";
        return `/${trimmed}(${cleanId})`;
    }

    async list<T extends DataverseEntity>(entitySet: string, options: {
        select?: string[];
        filter?: string;
        orderBy?: string;
        expand?: string;
        top?: number;
    } = {}): Promise<DataverseListResponse<T>> {
        const params = new URLSearchParams();
        if (options.select?.length) params.set("$select", options.select.join(","));
        if (options.filter) params.set("$filter", options.filter);
        if (options.orderBy) params.set("$orderby", options.orderBy);
        if (options.expand) params.set("$expand", options.expand);
        if (typeof options.top === "number") params.set("$top", String(options.top));
        const query = params.toString();
        const path = `/${entitySet}${query ? `?${query}` : ""}`;
        const res = await this.request(path);
        const data = await readResponseJson<{ value?: T[]; "@odata.nextLink"?: string; "@odata.deltaLink"?: string }>(res);
        return {
            value: Array.isArray(data?.value) ? (data?.value as T[]) : [],
            nextLink: data?.["@odata.nextLink"],
            deltaLink: data?.["@odata.deltaLink"],
        };
    }

    async listChanges<T extends DataverseEntity>(entitySet: string, options: {
        select?: string[];
        filter?: string;
        orderBy?: string;
        top?: number;
        deltaLink?: string | null;
        maxPages?: number;
    } = {}): Promise<{ value: T[]; deltaLink?: string }> {
        const items: T[] = [];
        let nextLink = options.deltaLink || null;
        let deltaLink: string | undefined;
        const maxPages = options.maxPages ?? 10;
        let pages = 0;

        while (pages < maxPages) {
            const res = await this.request(
                nextLink || `/${entitySet}?${new URLSearchParams({
                    ...(options.select?.length ? { $select: options.select.join(",") } : {}),
                    ...(options.filter ? { $filter: options.filter } : {}),
                    ...(options.orderBy ? { $orderby: options.orderBy } : {}),
                    ...(typeof options.top === "number" ? { $top: String(options.top) } : {}),
                } as Record<string, string>).toString()}`,
                {
                    headers: {
                        Prefer: "odata.track-changes",
                    },
                }
            );
            const data = await readResponseJson<{ value?: T[]; "@odata.nextLink"?: string; "@odata.deltaLink"?: string }>(res);
            const pageItems = Array.isArray(data?.value) ? (data?.value as T[]) : [];
            items.push(...pageItems);
            nextLink = data?.["@odata.nextLink"] || null;
            if (data?.["@odata.deltaLink"]) {
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

    async getById<T extends DataverseEntity>(entitySet: string, id: string, select?: string[]) {
        const params = new URLSearchParams();
        if (select?.length) params.set("$select", select.join(","));
        const query = params.toString();
        const path = `/${entitySet}(${id})${query ? `?${query}` : ""}`;
        const res = await this.request(path);
        return readResponseJson<T>(res);
    }

    async create(entitySet: string, payload: DataverseEntity) {
        const res = await this.request(`/${entitySet}`, {
            method: "POST",
            body: JSON.stringify(payload),
        });
        const entityId = parseEntityIdHeader(res.headers.get("OData-EntityId") || res.headers.get("odata-entityid"));
        const etag = res.headers.get("ETag") || res.headers.get("etag") || undefined;
        return { entityId, etag };
    }

    async update(entitySet: string, id: string, payload: DataverseEntity, options: { ifMatch?: string } = {}) {
        const headers: Record<string, string> = {};
        if (options.ifMatch) headers["If-Match"] = options.ifMatch;
        const res = await this.request(`/${entitySet}(${id})`, {
            method: "PATCH",
            headers,
            body: JSON.stringify(payload),
        });
        const etag = res.headers.get("ETag") || res.headers.get("etag") || undefined;
        return { etag };
    }

    async delete(entitySet: string, id: string) {
        await this.request(`/${entitySet}(${id})`, { method: "DELETE" });
    }
}
