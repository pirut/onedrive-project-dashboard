import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "../planner-sync/redis.js";
import { logger } from "../planner-sync/logger.js";

const FILE_PATH = process.env.DATAVERSE_DELTA_FILE || path.join(process.cwd(), ".dataverse-delta.json");
const KV_KEY = "dataverse:delta";

async function readFileStore() {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

async function writeFileStore(payload) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write Dataverse delta store", { error: error?.message || String(error) });
    }
}

async function readStore() {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return null;
            if (typeof raw === "string") return JSON.parse(raw);
            if (typeof raw === "object") return raw;
        } catch (error) {
            logger.warn("KV read failed for Dataverse delta store; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    return readFileStore();
}

async function writeStore(payload) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(payload));
            return;
        } catch (error) {
            logger.warn("KV write failed for Dataverse delta store; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    await writeFileStore(payload);
}

export async function getDataverseDeltaLink(entitySet) {
    const store = await readStore();
    if (!store || typeof store !== "object") return null;
    const entry = store[entitySet];
    if (!entry || !entry.deltaLink) return null;
    return entry.deltaLink;
}

export async function saveDataverseDeltaLink(entitySet, deltaLink) {
    if (!entitySet || !deltaLink) return;
    const store = (await readStore()) || {};
    store[entitySet] = { deltaLink, updatedAt: new Date().toISOString() };
    await writeStore(store);
}

export async function clearDataverseDeltaLink(entitySet) {
    if (!entitySet) return;
    const store = (await readStore()) || {};
    if (store[entitySet]) {
        delete store[entitySet];
        await writeStore(store);
    }
}
