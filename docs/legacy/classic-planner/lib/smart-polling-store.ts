import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis";
import { logger } from "./logger";

type SmartPollingQueue = { lastSeq: number | null; projects: string[]; updatedAt?: string };

const DEFAULT_DIR =
    process.env.NODE_ENV === "production"
        ? process.env.TMPDIR || "/tmp"
        : process.cwd();
const FILE_PATH = process.env.SYNC_SMART_POLLING_QUEUE_FILE || path.join(DEFAULT_DIR, ".smart-polling-queue.json");
const KV_KEY = "planner:smartpoll:queue";
let fileStoreWritable = true;
let fileStoreWarned = false;

function normalizeQueue(raw: unknown): SmartPollingQueue | null {
    if (!raw || typeof raw !== "object") return null;
    const source = raw as { lastSeq?: unknown; projects?: unknown; updatedAt?: unknown };
    const lastSeq = Number.isFinite(source.lastSeq as number) ? (source.lastSeq as number) : null;
    const projects = Array.isArray(source.projects) ? source.projects.filter((item) => typeof item === "string") : [];
    return { lastSeq, projects, updatedAt: typeof source.updatedAt === "string" ? source.updatedAt : undefined };
}

async function readFileStore() {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        return JSON.parse(raw) as unknown;
    } catch {
        return null;
    }
}

async function writeFileStore(payload: unknown) {
    if (!fileStoreWritable) return;
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        const err = error as NodeJS.ErrnoException;
        const code = err?.code;
        if (code === "EROFS" || code === "EACCES" || code === "EPERM") {
            fileStoreWritable = false;
            if (!fileStoreWarned) {
                fileStoreWarned = true;
                logger.warn("Smart polling queue file store disabled", {
                    filePath: FILE_PATH,
                    error: err?.message,
                });
            }
            return;
        }
        logger.warn("Failed to write smart polling queue store", { error: err?.message });
    }
}

async function readStore() {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return null;
            if (typeof raw === "string") return JSON.parse(raw) as unknown;
            if (typeof raw === "object") return raw as unknown;
        } catch (error) {
            logger.warn("KV read failed for smart polling queue; falling back to file", { error: (error as Error)?.message });
        }
    }
    return readFileStore();
}

async function writeStore(payload: unknown) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(payload));
            return;
        } catch (error) {
            logger.warn("KV write failed for smart polling queue; falling back to file", { error: (error as Error)?.message });
        }
    }
    await writeFileStore(payload);
}

export async function getSmartPollingQueue(): Promise<SmartPollingQueue | null> {
    const store = await readStore();
    return normalizeQueue(store);
}

export async function saveSmartPollingQueue(queue: SmartPollingQueue) {
    await writeStore({ ...queue, updatedAt: new Date().toISOString() });
}

export async function clearSmartPollingQueue() {
    await writeStore({ lastSeq: null, projects: [] });
}
