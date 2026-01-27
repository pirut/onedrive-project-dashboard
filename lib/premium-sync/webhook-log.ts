import { EventEmitter } from "events";
import { getRedis } from "../planner-sync/redis";
import { logger } from "../planner-sync/logger";

export type WebhookLogEntry = {
    ts: string;
    requestId: string;
    type: "notification" | "invalid_json" | "error" | "ping";
    notificationCount?: number;
    taskIds?: string[];
    error?: string;
};

const LOG_KEY = "premium:webhook:log";
const MAX_LOG = 100;
const inMemoryLog: WebhookLogEntry[] = [];
const emitter = (globalThis as typeof globalThis & { __premiumWebhookEmitter?: EventEmitter }).__premiumWebhookEmitter || new EventEmitter();

if (!(globalThis as typeof globalThis & { __premiumWebhookEmitter?: EventEmitter }).__premiumWebhookEmitter) {
    (globalThis as typeof globalThis & { __premiumWebhookEmitter: EventEmitter }).__premiumWebhookEmitter = emitter;
}

export function getPremiumWebhookEmitter() {
    return emitter;
}

export async function appendPremiumWebhookLog(entry: WebhookLogEntry) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.lpush(LOG_KEY, JSON.stringify(entry));
            await redis.ltrim(LOG_KEY, 0, MAX_LOG - 1);
            emitter.emit("entry", entry);
            return;
        } catch (error) {
            logger.warn("Premium webhook log write failed; using memory log", {
                error: (error as Error)?.message,
            });
        }
    }
    inMemoryLog.unshift(entry);
    if (inMemoryLog.length > MAX_LOG) {
        inMemoryLog.length = MAX_LOG;
    }
    emitter.emit("entry", entry);
}

export async function listPremiumWebhookLog(limit = 50) {
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
            logger.warn("Premium webhook log read failed; using memory log", {
                error: (error as Error)?.message,
            });
        }
    }
    return inMemoryLog.slice(0, safeLimit);
}
