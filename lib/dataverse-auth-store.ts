import { promises as fs } from "fs";
import path from "path";
import crypto from "crypto";
import { getRedis } from "./planner-sync/redis";
import { logger } from "./planner-sync/logger";

const FILE_PATH =
    process.env.DATAVERSE_REFRESH_TOKEN_FILE || path.join(process.cwd(), ".dataverse-refresh-token.json");
const KV_KEY = "dataverse:refresh-token";

function resolveSecret() {
    return (process.env.DATAVERSE_TOKEN_ENCRYPTION_SECRET || process.env.ADMIN_SESSION_SECRET || "").trim();
}

function deriveKey(secret: string) {
    return crypto.createHash("sha256").update(secret).digest();
}

function encryptToken(token: string) {
    const secret = resolveSecret();
    if (!secret) return token;
    const key = deriveKey(secret);
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
    const enc = Buffer.concat([cipher.update(token, "utf8"), cipher.final()]);
    const tag = cipher.getAuthTag();
    return `enc:${iv.toString("base64url")}.${tag.toString("base64url")}.${enc.toString("base64url")}`;
}

function decryptToken(value: string) {
    const secret = resolveSecret();
    if (!secret) return value;
    if (!value.startsWith("enc:")) return value;
    const payload = value.slice(4);
    const [ivB64, tagB64, dataB64] = payload.split(".");
    if (!ivB64 || !tagB64 || !dataB64) return value;
    try {
        const key = deriveKey(secret);
        const iv = Buffer.from(ivB64, "base64url");
        const tag = Buffer.from(tagB64, "base64url");
        const data = Buffer.from(dataB64, "base64url");
        const decipher = crypto.createDecipheriv("aes-256-gcm", key, iv);
        decipher.setAuthTag(tag);
        const dec = Buffer.concat([decipher.update(data), decipher.final()]);
        return dec.toString("utf8");
    } catch (error) {
        logger.warn("Failed to decrypt Dataverse refresh token", { error: (error as Error)?.message });
        return "";
    }
}

async function readFileStore() {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        return JSON.parse(raw) as { refreshToken?: string } | null;
    } catch {
        return null;
    }
}

async function writeFileStore(payload: unknown) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write Dataverse refresh token", { error: (error as Error)?.message });
    }
}

async function readStore() {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return null;
            if (typeof raw === "string") return JSON.parse(raw) as Record<string, unknown> | null;
            if (typeof raw === "object") return raw as Record<string, unknown> | null;
        } catch (error) {
            logger.warn("KV read failed for Dataverse refresh token; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    return readFileStore();
}

async function writeStore(payload: unknown) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(payload));
            return;
        } catch (error) {
            logger.warn("KV write failed for Dataverse refresh token; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    await writeFileStore(payload);
}

export async function getDataverseRefreshToken() {
    const store = (await readStore()) as { refreshToken?: string } | null;
    if (!store?.refreshToken) return "";
    return decryptToken(store.refreshToken);
}

export async function saveDataverseRefreshToken(refreshToken: string) {
    if (!refreshToken) return;
    const store = (await readStore()) as Record<string, unknown> | null;
    const payload = {
        ...(store || {}),
        refreshToken: encryptToken(refreshToken),
        updatedAt: new Date().toISOString(),
    };
    await writeStore(payload);
}

export async function clearDataverseRefreshToken() {
    const store = (await readStore()) as Record<string, unknown> | null;
    await writeStore({ ...(store || {}), refreshToken: "", clearedAt: new Date().toISOString() });
}

const AUTH_STATE_MAX_AGE_MS = 10 * 60 * 1000;

export async function saveDataverseAuthState(state: string, codeVerifier: string) {
    if (!state || !codeVerifier) return;
    const store = (await readStore()) as Record<string, unknown> | null;
    const payload = {
        ...(store || {}),
        authState: {
            state,
            codeVerifier: encryptToken(codeVerifier),
            createdAt: new Date().toISOString(),
        },
    };
    await writeStore(payload);
}

export async function consumeDataverseAuthState(state: string) {
    if (!state) return "";
    const store = (await readStore()) as { authState?: { state?: string; codeVerifier?: string; createdAt?: string } } | null;
    const authState = store?.authState;
    if (!authState?.state || authState.state !== state) return "";
    const createdAt = authState.createdAt ? Date.parse(authState.createdAt) : NaN;
    if (!Number.isFinite(createdAt) || Date.now() - createdAt > AUTH_STATE_MAX_AGE_MS) {
        await writeStore({ ...(store || {}), authState: undefined });
        return "";
    }
    const verifier = authState.codeVerifier ? decryptToken(authState.codeVerifier) : "";
    await writeStore({ ...(store || {}), authState: undefined });
    return verifier;
}
