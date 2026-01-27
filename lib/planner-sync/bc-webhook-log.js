import { EventEmitter } from "events";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

export const LOG_KEY = "bc:webhook:log";
const MAX_LOG = 100;
const inMemoryLog = [];
const emitter = globalThis.__bcWebhookEmitter || new EventEmitter();

if (!globalThis.__bcWebhookEmitter) {
    globalThis.__bcWebhookEmitter = emitter;
}

export function getBcWebhookEmitter() {
    return emitter;
}

export async function appendBcWebhookLog(entry) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.lpush(LOG_KEY, JSON.stringify(entry));
            await redis.ltrim(LOG_KEY, 0, MAX_LOG - 1);
            emitter.emit("entry", entry);
            return;
        } catch (error) {
            logger.warn("BC webhook log write failed; using memory log", {
                error: error?.message,
            });
        }
    }
    inMemoryLog.unshift(entry);
    if (inMemoryLog.length > MAX_LOG) {
        inMemoryLog.length = MAX_LOG;
    }
    emitter.emit("entry", entry);
}

export async function listBcWebhookLog(limit = 50) {
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
            logger.warn("BC webhook log read failed; using memory log", {
                error: error?.message,
            });
        }
    }
    return inMemoryLog.slice(0, safeLimit);
}
