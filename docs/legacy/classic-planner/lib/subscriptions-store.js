import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

const FILE_PATH = process.env.PLANNER_SUBSCRIPTIONS_FILE || path.join(process.cwd(), ".planner-subscriptions.json");
const KV_KEY = "planner:subscriptions";

async function readFileStore() {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        const data = JSON.parse(raw);
        if (Array.isArray(data)) return data;
        return [];
    } catch {
        return [];
    }
}

async function writeFileStore(subscriptions) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(subscriptions, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write subscription file store", { error: error?.message || String(error) });
    }
}

export async function listStoredSubscriptions() {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return [];
            if (typeof raw === "string") {
                const data = JSON.parse(raw);
                if (Array.isArray(data)) return data;
            }
            if (Array.isArray(raw)) return raw;
        } catch (error) {
            logger.warn("KV read failed for subscriptions; falling back to file", { error: error?.message || String(error) });
        }
    }
    return readFileStore();
}

export async function saveStoredSubscriptions(subscriptions) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(subscriptions));
            return;
        } catch (error) {
            logger.warn("KV write failed for subscriptions; falling back to file", { error: error?.message || String(error) });
        }
    }
    await writeFileStore(subscriptions);
}
