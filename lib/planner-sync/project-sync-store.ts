import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

export type ProjectSyncSetting = {
    projectNo: string;
    disabled?: boolean;
    updatedAt: string;
    note?: string;
};

const FILE_PATH =
    process.env.PREMIUM_PROJECT_SYNC_FILE ||
    process.env.PLANNER_PROJECT_SYNC_FILE ||
    path.join(process.cwd(), ".planner-project-sync.json");
const KV_KEY = "premium:project-sync";
const LEGACY_KV_KEY = "planner:project-sync";

export function normalizeProjectNo(value: string | undefined | null) {
    return String(value || "").trim().toLowerCase();
}

export function buildDisabledProjectSet(settings: ProjectSyncSetting[]) {
    const set = new Set<string>();
    for (const setting of settings) {
        if (!setting?.disabled) continue;
        const normalized = normalizeProjectNo(setting.projectNo);
        if (normalized) set.add(normalized);
    }
    return set;
}

async function readFileStore(): Promise<ProjectSyncSetting[]> {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        const data = JSON.parse(raw);
        if (Array.isArray(data)) return data as ProjectSyncSetting[];
        return [];
    } catch {
        return [];
    }
}

async function writeFileStore(settings: ProjectSyncSetting[]) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(settings, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write project sync store", { error: (error as Error)?.message });
    }
}

export async function listProjectSyncSettings(): Promise<ProjectSyncSetting[]> {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            let raw = await redis.get(KV_KEY);
            if (!raw) {
                raw = await redis.get(LEGACY_KV_KEY);
            }
            if (!raw) return [];
            if (typeof raw === "string") {
                const data = JSON.parse(raw);
                if (Array.isArray(data)) return data as ProjectSyncSetting[];
            }
            if (Array.isArray(raw)) return raw as ProjectSyncSetting[];
        } catch (error) {
            logger.warn("KV read failed for project sync settings; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    return readFileStore();
}

export async function saveProjectSyncSettings(settings: ProjectSyncSetting[]) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(settings));
            return;
        } catch (error) {
            logger.warn("KV write failed for project sync settings; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    await writeFileStore(settings);
}
