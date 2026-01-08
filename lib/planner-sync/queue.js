import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

const QUEUE_KEY = "planner:notifications";
const inMemoryQueue = [];
let processing = false;
let pending = false;

export async function enqueueNotifications(items) {
    if (!items.length) return;
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            const payloads = items.map((item) => JSON.stringify(item));
            await redis.lpush(QUEUE_KEY, ...payloads);
            return;
        } catch (error) {
            logger.warn("KV enqueue failed; using memory queue", { error: error?.message || String(error) });
        }
    }
    inMemoryQueue.push(...items);
}

async function dequeueNotification() {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        const raw = await redis.rpop(QUEUE_KEY);
        if (!raw) return null;
        try {
            if (typeof raw === "string") return JSON.parse(raw);
            return raw;
        } catch {
            return null;
        }
    }
    return inMemoryQueue.shift() || null;
}

export async function processQueue(handler) {
    if (processing) {
        pending = true;
        return;
    }
    processing = true;
    try {
        while (true) {
            const item = await dequeueNotification();
            if (!item) break;
            try {
                await handler(item);
            } catch (error) {
                logger.error("Queue item processing failed", {
                    taskId: item.taskId,
                    error: error?.message || String(error),
                });
            }
        }
    } finally {
        processing = false;
        if (pending) {
            pending = false;
            await processQueue(handler);
        }
    }
}
