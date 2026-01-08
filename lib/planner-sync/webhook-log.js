import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

const LOG_KEY = "planner:webhook:log";
const MAX_LOG = 100;
const inMemoryLog = [];

export async function appendWebhookLog(entry) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.lpush(LOG_KEY, JSON.stringify(entry));
            await redis.ltrim(LOG_KEY, 0, MAX_LOG - 1);
            return;
        } catch (error) {
            logger.warn("Webhook log write failed; using memory log", {
                error: error?.message || String(error),
            });
        }
    }
    inMemoryLog.unshift(entry);
    if (inMemoryLog.length > MAX_LOG) {
        inMemoryLog.length = MAX_LOG;
    }
}

export async function listWebhookLog(limit = 50) {
    const safeLimit = Math.max(1, Math.min(limit, MAX_LOG));
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const rows = await redis.lrange(LOG_KEY, 0, safeLimit - 1);
            return rows
                .map((row) => {
                    if (typeof row === "string") {
                        try {
                            return JSON.parse(row);
                        } catch {
                            return null;
                        }
                    }
                    return row;
                })
                .filter(Boolean);
        } catch (error) {
            logger.warn("Webhook log read failed; using memory log", {
                error: error?.message || String(error),
            });
        }
    }
    return inMemoryLog.slice(0, safeLimit);
}
