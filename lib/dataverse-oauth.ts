import crypto from "crypto";
import { fetchWithRetry, readResponseJson, readResponseText } from "./planner-sync/http";
import { logger } from "./planner-sync/logger";

export type DataverseOAuthConfig = {
    baseUrl: string;
    tenantId: string;
    clientId: string;
    clientSecret: string;
    scopes: string;
    redirectUri: string;
    authorizeUrl: string;
    tokenUrl: string;
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

function normalizeScopes(scopes: string, baseUrl: string) {
    const trimmed = (scopes || "").trim().replace(/\s+/g, " ");
    if (trimmed) return trimmed;
    return `${baseUrl}/user_impersonation offline_access`;
}

export function getDataverseAuthStateSecret() {
    return (process.env.DATAVERSE_AUTH_STATE_SECRET || process.env.ADMIN_SESSION_SECRET || "").trim();
}

export function createDataverseAuthState(secret: string) {
    const nonce = crypto.randomBytes(16).toString("base64url");
    const ts = Date.now().toString();
    const payload = `${nonce}.${ts}`;
    if (!secret) return payload;
    const sig = crypto.createHmac("sha256", secret).update(payload).digest("base64url");
    return `${payload}.${sig}`;
}

export function createDataversePkcePair() {
    const verifier = crypto.randomBytes(32).toString("base64url");
    const challenge = crypto.createHash("sha256").update(verifier).digest("base64url");
    return { verifier, challenge };
}

export function verifyDataverseAuthState(state: string, secret: string, maxAgeMs = 10 * 60 * 1000) {
    if (!secret) return true;
    const parts = state.split(".");
    if (parts.length !== 3) return false;
    const [nonce, ts, sig] = parts;
    const payload = `${nonce}.${ts}`;
    const expected = crypto.createHmac("sha256", secret).update(payload).digest("base64url");
    const sigBuf = Buffer.from(sig);
    const expBuf = Buffer.from(expected);
    if (sigBuf.length !== expBuf.length) return false;
    if (!crypto.timingSafeEqual(sigBuf, expBuf)) return false;
    const ageMs = Date.now() - Number(ts);
    if (!Number.isFinite(ageMs) || ageMs < 0 || ageMs > maxAgeMs) return false;
    return true;
}

export function getDataverseOAuthConfig(origin?: string): DataverseOAuthConfig {
    const baseUrl = normalizeBaseUrl(readEnv("DATAVERSE_BASE_URL", true) as string);
    const tenantId = readEnv("DATAVERSE_TENANT_ID", true) as string;
    const clientId = (readEnv("DATAVERSE_AUTH_CLIENT_ID") || readEnv("DATAVERSE_CLIENT_ID", true)) as string;
    const clientSecret = (readEnv("DATAVERSE_AUTH_CLIENT_SECRET") || readEnv("DATAVERSE_CLIENT_SECRET") || "") as string;
    const redirectUri = readEnv("DATAVERSE_AUTH_REDIRECT_URI") || (origin ? new URL("/api/auth/dataverse/callback", origin).toString() : "");
    if (!redirectUri) {
        throw new Error("Missing DATAVERSE_AUTH_REDIRECT_URI");
    }
    const scopes = normalizeScopes(readEnv("DATAVERSE_AUTH_SCOPES") || "", baseUrl);
    const authorizeUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/authorize`;
    const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
    return { baseUrl, tenantId, clientId, clientSecret, scopes, redirectUri, authorizeUrl, tokenUrl };
}

export function buildDataverseAuthorizeUrl(
    config: DataverseOAuthConfig,
    state: string,
    prompt?: string,
    codeChallenge?: string
) {
    const params = new URLSearchParams({
        client_id: config.clientId,
        response_type: "code",
        redirect_uri: config.redirectUri,
        response_mode: "query",
        scope: config.scopes,
        state,
    });
    if (codeChallenge) {
        params.set("code_challenge", codeChallenge);
        params.set("code_challenge_method", "S256");
    }
    if (prompt) params.set("prompt", prompt);
    return `${config.authorizeUrl}?${params.toString()}`;
}

export async function exchangeDataverseCode(config: DataverseOAuthConfig, code: string, codeVerifier?: string) {
    const body = new URLSearchParams({
        grant_type: "authorization_code",
        client_id: config.clientId,
        code,
        redirect_uri: config.redirectUri,
        scope: config.scopes,
    });
    if (codeVerifier) body.set("code_verifier", codeVerifier);
    if (config.clientSecret) body.set("client_secret", config.clientSecret);
    const res = await fetchWithRetry(config.tokenUrl, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
    });
    if (!res.ok) {
        const text = await readResponseText(res);
        throw new Error(`Dataverse auth token error ${res.status}: ${text}`);
    }
    const data = await readResponseJson<Record<string, unknown>>(res);
    if (!data?.access_token) {
        throw new Error("Dataverse auth token response missing access_token");
    }
    return data as {
        access_token: string;
        refresh_token?: string;
        expires_in?: number;
        scope?: string;
        token_type?: string;
        id_token?: string;
    };
}

export async function exchangeDataverseRefreshToken(config: DataverseOAuthConfig, refreshToken: string) {
    const body = new URLSearchParams({
        grant_type: "refresh_token",
        client_id: config.clientId,
        refresh_token: refreshToken,
    });
    if (config.clientSecret) body.set("client_secret", config.clientSecret);
    if (config.scopes) body.set("scope", config.scopes);
    const res = await fetchWithRetry(config.tokenUrl, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
    });
    if (!res.ok) {
        const text = await readResponseText(res);
        throw new Error(`Dataverse refresh token error ${res.status}: ${text}`);
    }
    const data = await readResponseJson<Record<string, unknown>>(res);
    if (!data?.access_token) {
        throw new Error("Dataverse refresh token response missing access_token");
    }
    return data as {
        access_token: string;
        refresh_token?: string;
        expires_in?: number;
        scope?: string;
        token_type?: string;
        id_token?: string;
    };
}

export function warnIfMissingAuthSecret() {
    if (getDataverseAuthStateSecret()) return;
    logger.warn("DATAVERSE_AUTH_STATE_SECRET is missing; OAuth state validation is disabled");
}
