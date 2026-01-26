import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis";
import { logger } from "./logger";

type DeltaEntry = { deltaLink: string; updatedAt?: string };

const DEFAULT_DIR =
    process.env.NODE_ENV === "production"
        ? process.env.TMPDIR || "/tmp"
        : process.cwd();
const FILE_PATH = process.env.PLANNER_DELTA_FILE || path.join(DEFAULT_DIR, ".planner-delta.json");
const KV_KEY = "planner:delta:tasks";
let fileStoreWritable = true;
let fileStoreWarned = false;

function normalizeScopes(raw: unknown): Record<string, DeltaEntry> {
    if (!raw || typeof raw !== "object") return {};
    const source =
        (raw as { scopes?: Record<string, unknown> }).scopes && typeof (raw as { scopes?: unknown }).scopes === "object"
            ? (raw as { scopes: Record<string, unknown> }).scopes
            : (raw as Record<string, unknown>);
    const scopes: Record<string, DeltaEntry> = {};
    for (const [key, value] of Object.entries(source || {})) {
        if (value && typeof value === "object" && typeof (value as DeltaEntry).deltaLink === "string") {
            scopes[key] = {
                deltaLink: (value as DeltaEntry).deltaLink,
                updatedAt: typeof (value as DeltaEntry).updatedAt === "string" ? (value as DeltaEntry).updatedAt : undefined,
            };
        } else if (typeof value === "string") {
            scopes[key] = { deltaLink: value };
        }
    }
    return scopes;
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
                logger.warn("Planner delta file store disabled", {
                    filePath: FILE_PATH,
                    error: err?.message,
                });
            }
            return;
        }
        logger.warn("Failed to write planner delta store", { error: err?.message });
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
            logger.warn("KV read failed for planner delta; falling back to file", { error: (error as Error)?.message });
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
            logger.warn("KV write failed for planner delta; falling back to file", { error: (error as Error)?.message });
        }
    }
    await writeFileStore(payload);
}

export async function getPlannerDeltaState(scopeKey: string): Promise<DeltaEntry | null> {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    const entry = scopes[scopeKey];
    if (!entry?.deltaLink) return null;
    return entry;
}

export async function savePlannerDeltaState(scopeKey: string, deltaLink: string) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    scopes[scopeKey] = { deltaLink, updatedAt: new Date().toISOString() };
    await writeStore({ scopes });
}

export async function clearPlannerDeltaState(scopeKey: string) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    if (!scopes[scopeKey]) return;
    delete scopes[scopeKey];
    await writeStore({ scopes });
}
