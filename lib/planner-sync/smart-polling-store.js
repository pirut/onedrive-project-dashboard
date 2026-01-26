import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

const DEFAULT_DIR =
    process.env.NODE_ENV === "production"
        ? process.env.TMPDIR || "/tmp"
        : process.cwd();
const FILE_PATH = process.env.SYNC_SMART_POLLING_QUEUE_FILE || path.join(DEFAULT_DIR, ".smart-polling-queue.json");
const KV_KEY = "planner:smartpoll:queue";
let fileStoreWritable = true;
let fileStoreWarned = false;

function normalizeQueue(raw) {
    if (!raw || typeof raw !== "object") return null;
    const lastSeq = Number.isFinite(raw.lastSeq) ? raw.lastSeq : null;
    const projects = Array.isArray(raw.projects) ? raw.projects.filter((item) => typeof item === "string") : [];
    return { lastSeq, projects, updatedAt: typeof raw.updatedAt === "string" ? raw.updatedAt : undefined };
}

async function readFileStore() {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

async function writeFileStore(payload) {
    if (!fileStoreWritable) return;
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        const code = error?.code;
        if (code === "EROFS" || code === "EACCES" || code === "EPERM") {
            fileStoreWritable = false;
            if (!fileStoreWarned) {
                fileStoreWarned = true;
                logger.warn("Smart polling queue file store disabled", {
                    filePath: FILE_PATH,
                    error: error?.message || String(error),
                });
            }
            return;
        }
        logger.warn("Failed to write smart polling queue store", { error: error?.message || String(error) });
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
            logger.warn("KV read failed for smart polling queue; falling back to file", {
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
            logger.warn("KV write failed for smart polling queue; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    await writeFileStore(payload);
}

export async function getSmartPollingQueue() {
    const store = await readStore();
    return normalizeQueue(store);
}

export async function saveSmartPollingQueue(queue) {
    await writeStore({ ...queue, updatedAt: new Date().toISOString() });
}

export async function clearSmartPollingQueue() {
    await writeStore({ lastSeq: null, projects: [] });
}
