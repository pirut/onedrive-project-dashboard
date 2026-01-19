import { Redis } from "@upstash/redis";

const SUBMISSIONS_KEY = "submissions";
const SUBMISSIONS_RETENTION_DAYS = 4;
const SUBMISSIONS_RETENTION_MS = SUBMISSIONS_RETENTION_DAYS * 24 * 60 * 60 * 1000;
const KV_LOGGING_DISABLED = process.env.KV_LOGGING_DISABLED !== "false";

function isWrongTypeError(error) {
    return String(error?.message || error || "").includes("WRONGTYPE");
}

async function resetSubmissionsKey(redis) {
    try {
        await redis.del(SUBMISSIONS_KEY);
    } catch {}
}

function resolveKvCredentials() {
    // Use ONLY Vercel KV official envs to avoid cross-integration mismatches
    const url = process.env.KV_REST_API_URL || "";
    const token = process.env.KV_REST_API_TOKEN || process.env.KV_REST_API_READ_ONLY_TOKEN || "";
    return { url, token };
}

function isKvConfigured() {
    if (KV_LOGGING_DISABLED) return false;
    const { url, token } = resolveKvCredentials();
    return Boolean(url && token);
}

function getRedis() {
    if (KV_LOGGING_DISABLED) return null;
    if (!isKvConfigured()) return null;
    const { url, token } = resolveKvCredentials();
    return new Redis({ url, token });
}

export async function logSubmission(record) {
    if (KV_LOGGING_DISABLED) return false;
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
        const now = Date.now();
        const cutoff = now - SUBMISSIONS_RETENTION_MS;
        try {
            const attemptWrite = () =>
                redis
                    .pipeline()
                    .zadd(SUBMISSIONS_KEY, { score: now, member: JSON.stringify(payload) })
                    .zremrangebyscore(SUBMISSIONS_KEY, 0, cutoff)
                    .exec();
            try {
                await attemptWrite();
            } catch (e) {
                if (isWrongTypeError(e)) {
                    await resetSubmissionsKey(redis);
                    await attemptWrite();
                } else {
                    throw e;
                }
            }
        } catch (e) {
            // eslint-disable-next-line no-console
            console.warn("KV write failed (possible read-only token):", e?.message || String(e));
            return false;
        }
        return true;
    } catch {
        return false;
    }
}

export async function listSubmissions(limit = 100) {
    if (KV_LOGGING_DISABLED) return [];
    const redis = getRedis();
    if (!redis) return [];
    // Force bypass any potential caching layers by reading a distinct key first
    try {
        await redis.get("__kv_warm__");
    } catch {}
    const cutoff = Date.now() - SUBMISSIONS_RETENTION_MS;
    const fetchItems = () =>
        redis
            .pipeline()
            .zremrangebyscore(SUBMISSIONS_KEY, 0, cutoff)
            .zrange(SUBMISSIONS_KEY, 0, Math.max(0, limit - 1), { rev: true })
            .exec();
    let items;
    try {
        [, items] = await fetchItems();
    } catch (e) {
        if (isWrongTypeError(e)) {
            await resetSubmissionsKey(redis);
            [, items] = await fetchItems();
        } else {
            throw e;
        }
    }
    return items
        .map((entry) => {
            if (entry == null) return null;
            if (typeof entry === "object") return entry; // already parsed by client
            if (typeof entry === "string") {
                try {
                    return JSON.parse(entry);
                } catch {
                    return null;
                }
            }
            return null;
        })
        .filter(Boolean);
}

export async function listSubmissionsRaw(limit = 100) {
    if (KV_LOGGING_DISABLED) return { itemsRaw: [], info: getKvInfo() };
    const redis = getRedis();
    if (!redis) return { itemsRaw: [], info: getKvInfo() };
    try {
        await redis.get("__kv_warm__");
    } catch {}
    const cutoff = Date.now() - SUBMISSIONS_RETENTION_MS;
    const fetchItemsRaw = () =>
        redis
            .pipeline()
            .zremrangebyscore(SUBMISSIONS_KEY, 0, cutoff)
            .zrange(SUBMISSIONS_KEY, 0, Math.max(0, limit - 1), { rev: true })
            .exec();
    let itemsRaw;
    try {
        [, itemsRaw] = await fetchItemsRaw();
    } catch (e) {
        if (isWrongTypeError(e)) {
            await resetSubmissionsKey(redis);
            [, itemsRaw] = await fetchItemsRaw();
        } else {
            throw e;
        }
    }
    return { itemsRaw, info: getKvInfo() };
}

export function getKvInfo() {
    const { url, token } = resolveKvCredentials();
    return {
        configured: Boolean(url && token) && !KV_LOGGING_DISABLED,
        loggingDisabled: KV_LOGGING_DISABLED,
        provider: "kv_rest_only",
        urlPresent: Boolean(url),
        tokenPresent: Boolean(token),
        tokenIsReadOnly: Boolean(!process.env.KV_REST_API_TOKEN && !!process.env.KV_REST_API_READ_ONLY_TOKEN),
    };
}

export async function kvDiagnostics() {
    const info = getKvInfo();
    if (KV_LOGGING_DISABLED) return { ok: false, info, error: "KV logging disabled" };
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
