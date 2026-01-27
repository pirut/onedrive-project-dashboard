import { getRedis } from "./redis";
import { logger } from "./logger";

export type PlannerNotification = {
    taskId: string;
    subscriptionId?: string;
    receivedAt: string;
};

const QUEUE_KEY = "planner:notifications";
const inMemoryQueue: PlannerNotification[] = [];
let processing = false;
let pending = false;

export async function enqueueNotifications(items: PlannerNotification[]) {
    if (!items.length) return;
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            const payloads = items.map((item) => JSON.stringify(item));
            await redis.lpush(QUEUE_KEY, ...payloads);
            return;
        } catch (error) {
            logger.warn("KV enqueue failed; using memory queue", { error: (error as Error)?.message });
        }
    }
    inMemoryQueue.push(...items);
}

async function dequeueNotification(): Promise<PlannerNotification | null> {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        const raw = await redis.rpop(QUEUE_KEY);
        if (!raw) return null;
        try {
            if (typeof raw === "string") return JSON.parse(raw) as PlannerNotification;
            return raw as PlannerNotification;
        } catch {
            return null;
        }
    }
    return inMemoryQueue.shift() || null;
}

export async function processQueue(handler: (item: PlannerNotification) => Promise<void>) {
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
                    error: (error as Error)?.message,
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
