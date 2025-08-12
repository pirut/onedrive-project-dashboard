import { Redis } from "@upstash/redis";

function isKvConfigured() {
    const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
    return Boolean(url && token);
}

function getRedis() {
    if (!isKvConfigured()) return null;
    const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
    return new Redis({ url, token });
}

export async function logSubmission(record) {
    try {
        const redis = getRedis();
        if (!redis) return false;
        const payload = {
            ...record,
            loggedAt: new Date().toISOString(),
        };
        await redis.lpush("submissions", JSON.stringify(payload));
        // keep last 500
        await redis.ltrim("submissions", 0, 499);
        return true;
    } catch {
        return false;
    }
}

export async function listSubmissions(limit = 100) {
    const redis = getRedis();
    if (!redis) return [];
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
