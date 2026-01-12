import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

const FILE_PATH = process.env.PLANNER_PROJECT_SYNC_FILE || path.join(process.cwd(), ".planner-project-sync.json");
const KV_KEY = "planner:project-sync";

export function normalizeProjectNo(value) {
    return String(value || "").trim().toLowerCase();
}

export function buildDisabledProjectSet(settings) {
    const set = new Set();
    for (const setting of settings || []) {
        if (!setting || !setting.disabled) continue;
        const normalized = normalizeProjectNo(setting.projectNo);
        if (normalized) set.add(normalized);
    }
    return set;
}

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

async function writeFileStore(settings) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(settings, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write project sync store", { error: error?.message || String(error) });
    }
}

export async function listProjectSyncSettings() {
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
            logger.warn("KV read failed for project sync settings; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    return readFileStore();
}

export async function saveProjectSyncSettings(settings) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(settings));
            return;
        } catch (error) {
            logger.warn("KV write failed for project sync settings; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    await writeFileStore(settings);
}
