import { EventEmitter } from "events";
import { getRedis } from "../planner-sync/redis.js";
import { logger } from "../planner-sync/logger.js";

export type WebhookLogEntry = {
    ts: string;
    requestId: string;
    type: "notification" | "invalid_json" | "error" | "ping" | "unauthorized" | "skipped";
    notificationCount?: number;
    taskIds?: string[];
    reason?: string;
    error?: string;
};

const LOG_KEY = "premium:webhook:log";
const MAX_LOG = 100;
const DEFAULT_KV_TYPES = new Set([
    "notification",
    "invalid_json",
    "error",
    "ping",
    "unauthorized",
    "skipped",
]);
const inMemoryLog: WebhookLogEntry[] = [];
const emitter = (globalThis as typeof globalThis & { __premiumWebhookEmitter?: EventEmitter }).__premiumWebhookEmitter || new EventEmitter();

if (!(globalThis as typeof globalThis & { __premiumWebhookEmitter?: EventEmitter }).__premiumWebhookEmitter) {
    (globalThis as typeof globalThis & { __premiumWebhookEmitter: EventEmitter }).__premiumWebhookEmitter = emitter;
}

export function getPremiumWebhookEmitter() {
    return emitter;
}

function normalizeBoolean(value: string | undefined, fallback: boolean) {
    if (!value) return fallback;
    const normalized = value.trim().toLowerCase();
    if (["0", "false", "no", "off"].includes(normalized)) return false;
    if (["1", "true", "yes", "on"].includes(normalized)) return true;
    return fallback;
}

function resolveKvTypes() {
    const raw = (process.env.PREMIUM_WEBHOOK_LOG_TYPES || "").trim();
    if (!raw) return DEFAULT_KV_TYPES;
    const set = new Set(
        raw
            .split(",")
            .map((entry) => entry.trim())
            .filter(Boolean)
    );
    return set;
}

function shouldPersistToKv(entry: WebhookLogEntry) {
    const enabled = normalizeBoolean(process.env.PREMIUM_WEBHOOK_LOG_TO_KV, true);
    if (!enabled) return false;
    const types = resolveKvTypes();
    if (types.has("*")) return true;
    return types.has(entry.type);
}

export async function appendPremiumWebhookLog(entry: WebhookLogEntry) {
    if (shouldPersistToKv(entry)) {
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
    }
    inMemoryLog.unshift(entry);
    if (inMemoryLog.length > MAX_LOG) {
        inMemoryLog.length = MAX_LOG;
    }
    emitter.emit("entry", entry);
}

export async function listPremiumWebhookLog(limit = 50) {
    const safeLimit = Math.max(1, Math.min(limit, MAX_LOG));
    const readFromKv = normalizeBoolean(process.env.PREMIUM_WEBHOOK_LOG_TO_KV, true);
    if (readFromKv) {
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
    }
    return inMemoryLog.slice(0, safeLimit);
}
