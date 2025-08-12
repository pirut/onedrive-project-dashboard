import { Redis } from "@upstash/redis";

function resolveKvCredentials() {
  // Prefer explicit REST credentials; fall back to Upstash aliases
  const url =
    process.env.KV_REST_API_URL ||
    process.env.UPSTASH_REDIS_REST_URL ||
    process.env.KV_URL ||
    process.env.REDIS_URL ||
    "";
  const token =
    process.env.KV_REST_API_TOKEN ||
    process.env.UPSTASH_REDIS_REST_TOKEN ||
    process.env.KV_REST_API_READ_ONLY_TOKEN ||
    "";
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
