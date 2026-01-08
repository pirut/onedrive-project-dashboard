import { getRedis } from "./redis";
import { logger } from "./logger";

export type WebhookLogEntry = {
    ts: string;
    requestId: string;
    type: "validation" | "notification" | "invalid_json" | "error";
    notificationCount?: number;
    validCount?: number;
    invalidCount?: number;
    clientStateMismatchCount?: number;
    missingTaskIdCount?: number;
    taskIds?: string[];
    error?: string;
};

const LOG_KEY = "planner:webhook:log";
const MAX_LOG = 100;
const inMemoryLog: WebhookLogEntry[] = [];

export async function appendWebhookLog(entry: WebhookLogEntry) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.lpush(LOG_KEY, JSON.stringify(entry));
            await redis.ltrim(LOG_KEY, 0, MAX_LOG - 1);
            return;
        } catch (error) {
            logger.warn("Webhook log write failed; using memory log", {
                error: (error as Error)?.message,
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
                            return JSON.parse(row) as WebhookLogEntry;
                        } catch {
                            return null;
                        }
                    }
                    return row as WebhookLogEntry;
                })
                .filter(Boolean) as WebhookLogEntry[];
        } catch (error) {
            logger.warn("Webhook log read failed; using memory log", {
                error: (error as Error)?.message,
            });
        }
    }
    return inMemoryLog.slice(0, safeLimit);
}
