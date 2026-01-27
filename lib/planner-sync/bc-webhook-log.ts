import { EventEmitter } from "events";
import { getRedis } from "./redis";
import { logger } from "./logger";

export type BcWebhookLogEntry = {
    ts: string;
    requestId: string;
    type: "validation" | "notification" | "invalid_json" | "error";
    location?: "query" | "body";
    count?: number;
    enqueued?: number;
    deduped?: number;
    skipped?: number;
    secretMismatch?: number;
    missingResource?: number;
    items?: Array<{
        entitySet?: string;
        systemId?: string;
        changeType?: string;
        resource?: string;
        subscriptionId?: string;
    }>;
    error?: string;
};

const LOG_KEY = "bc:webhook:log";
const MAX_LOG = 100;
const inMemoryLog: BcWebhookLogEntry[] = [];
const emitter =
    (globalThis as typeof globalThis & { __bcWebhookEmitter?: EventEmitter }).__bcWebhookEmitter || new EventEmitter();

if (!(globalThis as typeof globalThis & { __bcWebhookEmitter?: EventEmitter }).__bcWebhookEmitter) {
    (globalThis as typeof globalThis & { __bcWebhookEmitter?: EventEmitter }).__bcWebhookEmitter = emitter;
}

export function getBcWebhookEmitter() {
    return emitter;
}

export async function appendBcWebhookLog(entry: BcWebhookLogEntry) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.lpush(LOG_KEY, JSON.stringify(entry));
            await redis.ltrim(LOG_KEY, 0, MAX_LOG - 1);
            emitter.emit("entry", entry);
            return;
        } catch (error) {
            logger.warn("BC webhook log write failed; using memory log", {
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
                            return JSON.parse(row) as BcWebhookLogEntry;
                        } catch {
                            return null;
                        }
                    }
                    return row as BcWebhookLogEntry;
                })
                .filter(Boolean) as BcWebhookLogEntry[];
        } catch (error) {
            logger.warn("BC webhook log read failed; using memory log", {
                error: (error as Error)?.message,
            });
        }
    }
    return inMemoryLog.slice(0, safeLimit);
}
