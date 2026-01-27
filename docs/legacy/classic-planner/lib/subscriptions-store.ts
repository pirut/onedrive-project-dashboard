import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis";
import { logger } from "./logger";

export type StoredSubscription = {
    id: string;
    planId?: string;
    resource?: string;
    expirationDateTime?: string;
    createdAt: string;
};

const FILE_PATH = process.env.PLANNER_SUBSCRIPTIONS_FILE || path.join(process.cwd(), ".planner-subscriptions.json");
const KV_KEY = "planner:subscriptions";

async function readFileStore(): Promise<StoredSubscription[]> {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        const data = JSON.parse(raw);
        if (Array.isArray(data)) return data as StoredSubscription[];
        return [];
    } catch {
        return [];
    }
}

async function writeFileStore(subscriptions: StoredSubscription[]) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(subscriptions, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write subscription file store", { error: (error as Error)?.message });
    }
}

export async function listStoredSubscriptions(): Promise<StoredSubscription[]> {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return [];
            if (typeof raw === "string") {
                const data = JSON.parse(raw);
                if (Array.isArray(data)) return data as StoredSubscription[];
            }
            if (Array.isArray(raw)) return raw as StoredSubscription[];
        } catch (error) {
            logger.warn("KV read failed for subscriptions; falling back to file", { error: (error as Error)?.message });
        }
    }
    return readFileStore();
}

export async function saveStoredSubscriptions(subscriptions: StoredSubscription[]) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(subscriptions));
            return;
        } catch (error) {
            logger.warn("KV write failed for subscriptions; falling back to file", { error: (error as Error)?.message });
        }
    }
    await writeFileStore(subscriptions);
}
