import { Redis } from "@upstash/redis";

function resolveKvCredentials() {
    // Use ONLY Vercel KV official envs to avoid cross-integration mismatches
    const url = process.env.KV_REST_API_URL || "";
    const token = process.env.KV_REST_API_TOKEN || process.env.KV_REST_API_READ_ONLY_TOKEN || "";
    return { url, token };
}

function isKvConfigured() {
    const { url, token } = resolveKvCredentials();
    return Boolean(url && token);
}

function getRedis() {
    if (!isKvConfigured()) return null;
    const { url, token } = resolveKvCredentials();
    return new Redis({ url, token });
}

export async function logSubmission(record) {
    try {
        const redis = getRedis();
        if (!redis) {
            // Fallback: best-effort log to console in case KV is not configured
            // eslint-disable-next-line no-console
            console.warn("KV not configured; submission:", record);
            return false;
        }
        const payload = {
            ...record,
            loggedAt: new Date().toISOString(),
        };
        try {
            await redis.lpush("submissions", JSON.stringify(payload));
        } catch (e) {
            // eslint-disable-next-line no-console
            console.warn("KV write failed (possible read-only token):", e?.message || String(e));
            return false;
        }
        // keep last 500 (ignore errors here to not fail the main write)
        try {
            await redis.ltrim("submissions", 0, 499);
        } catch {}
        return true;
    } catch {
        return false;
    }
}

export async function listSubmissions(limit = 100) {
    const redis = getRedis();
    if (!redis) return [];
    // Force bypass any potential caching layers by reading a distinct key first
    try {
        await redis.get("__kv_warm__");
    } catch {}
    const items = await redis.lrange("submissions", 0, Math.max(0, limit - 1));
    return items
        .map((s) => {
            try {
                return JSON.parse(s);
            } catch {
                return null;
            }
        })
        .filter(Boolean);
}

export async function listSubmissionsRaw(limit = 100) {
    const redis = getRedis();
    if (!redis) return { itemsRaw: [], info: getKvInfo() };
    try {
        await redis.get("__kv_warm__");
    } catch {}
    const itemsRaw = await redis.lrange("submissions", 0, Math.max(0, limit - 1));
    return { itemsRaw, info: getKvInfo() };
}

export function getKvInfo() {
    const { url, token } = resolveKvCredentials();
    return {
        configured: Boolean(url && token),
        provider: "kv_rest_only",
        urlPresent: Boolean(url),
        tokenPresent: Boolean(token),
        tokenIsReadOnly: Boolean(!process.env.KV_REST_API_TOKEN && !!process.env.KV_REST_API_READ_ONLY_TOKEN),
    };
}

export async function kvDiagnostics() {
    const info = getKvInfo();
    if (!info.configured) return { ok: false, info, error: "KV not configured" };
    try {
        const redis = getRedis();
        if (!redis) return { ok: false, info, error: "KV getRedis() returned null" };
        // Basic connectivity
        try {
            await redis.ping();
        } catch (e) {
            return { ok: false, info, error: `PING failed: ${e?.message || String(e)}` };
        }
        // Write test
        try {
            const key = `submissions:diag:${Date.now()}`;
            await redis.lpush(key, "ok");
            await redis.del(key);
        } catch (e) {
            return { ok: false, info, error: `Write failed (read-only token?): ${e?.message || String(e)}` };
        }
        return { ok: true, info };
    } catch (e) {
        return { ok: false, info, error: e?.message || String(e) };
    }
}
