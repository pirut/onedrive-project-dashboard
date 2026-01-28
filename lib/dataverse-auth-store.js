import { promises as fs } from "fs";
import path from "path";
import crypto from "crypto";
import { getRedis } from "./planner-sync/redis.js";
import { logger } from "./planner-sync/logger.js";

const FILE_PATH = process.env.DATAVERSE_REFRESH_TOKEN_FILE || path.join(process.cwd(), ".dataverse-refresh-token.json");
const KV_KEY = "dataverse:refresh-token";

function resolveSecret() {
    return (process.env.DATAVERSE_TOKEN_ENCRYPTION_SECRET || process.env.ADMIN_SESSION_SECRET || "").trim();
}

function deriveKey(secret) {
    return crypto.createHash("sha256").update(secret).digest();
}

function encryptToken(token) {
    const secret = resolveSecret();
    if (!secret) return token;
    const key = deriveKey(secret);
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
    const enc = Buffer.concat([cipher.update(token, "utf8"), cipher.final()]);
    const tag = cipher.getAuthTag();
    return `enc:${iv.toString("base64url")}.${tag.toString("base64url")}.${enc.toString("base64url")}`;
}

function decryptToken(value) {
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
        logger.warn("Failed to decrypt Dataverse refresh token", { error: error?.message || String(error) });
        return "";
    }
}

async function readFileStore() {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

async function writeFileStore(payload) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write Dataverse refresh token", { error: error?.message || String(error) });
    }
}

async function readStore() {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return null;
            if (typeof raw === "string") return JSON.parse(raw);
            if (typeof raw === "object") return raw;
        } catch (error) {
            logger.warn("KV read failed for Dataverse refresh token; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    return readFileStore();
}

async function writeStore(payload) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(payload));
            return;
        } catch (error) {
            logger.warn("KV write failed for Dataverse refresh token; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    await writeFileStore(payload);
}

export async function getDataverseRefreshToken() {
    const store = await readStore();
    if (!(store && store.refreshToken)) return "";
    return decryptToken(store.refreshToken);
}

export async function saveDataverseRefreshToken(refreshToken) {
    if (!refreshToken) return;
    const payload = {
        refreshToken: encryptToken(refreshToken),
        updatedAt: new Date().toISOString(),
    };
    await writeStore(payload);
}

export async function clearDataverseRefreshToken() {
    await writeStore({ refreshToken: "", clearedAt: new Date().toISOString() });
}
