import { Redis } from "@upstash/redis";

function resolveRedisConfig() {
    const url = process.env.KV_REST_API_URL || "";
    const token = process.env.KV_REST_API_TOKEN || process.env.KV_REST_API_READ_ONLY_TOKEN || "";
    if (!url || !token) return null;
    return {
        url,
        token,
        readOnly: !process.env.KV_REST_API_TOKEN && !!process.env.KV_REST_API_READ_ONLY_TOKEN,
    };
}

export function getRedis({ requireWrite }) {
    const config = resolveRedisConfig();
    if (!config) return null;
    if (requireWrite && config.readOnly) return null;
    return new Redis({ url: config.url, token: config.token });
}
