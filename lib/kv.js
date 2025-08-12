import { Redis } from "@upstash/redis";

function isKvConfigured() {
  return Boolean(process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN);
}

function getRedis() {
  if (!isKvConfigured()) return null;
  return new Redis({ url: process.env.KV_REST_API_URL, token: process.env.KV_REST_API_TOKEN });
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


