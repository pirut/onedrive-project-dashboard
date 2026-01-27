import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

const DEFAULT_DIR =
    process.env.NODE_ENV === "production"
        ? process.env.TMPDIR || "/tmp"
        : process.cwd();
const FILE_PATH = process.env.PLANNER_DELTA_FILE || path.join(DEFAULT_DIR, ".planner-delta.json");
const KV_KEY = "planner:delta:tasks";
let fileStoreWritable = true;
let fileStoreWarned = false;
const DEFAULT_MAX_AGE_DAYS = 7;
const MAX_AGE_DAYS = Number(process.env.PLANNER_DELTA_MAX_AGE_DAYS || DEFAULT_MAX_AGE_DAYS);

function normalizeScopes(raw) {
    if (!raw || typeof raw !== "object") return {};
    const source = raw.scopes && typeof raw.scopes === "object" ? raw.scopes : raw;
    const scopes = {};
    for (const [key, value] of Object.entries(source || {})) {
        if (value && typeof value === "object" && typeof value.deltaLink === "string") {
            scopes[key] = {
                deltaLink: value.deltaLink,
                updatedAt: typeof value.updatedAt === "string" ? value.updatedAt : undefined,
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
                logger.warn("Planner delta file store disabled", {
                    filePath: FILE_PATH,
                    error: error?.message || String(error),
                });
            }
            return;
        }
        logger.warn("Failed to write planner delta store", { error: error?.message || String(error) });
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
            logger.warn("KV read failed for planner delta; falling back to file", {
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
            logger.warn("KV write failed for planner delta; falling back to file", {
                error: error?.message || String(error),
            });
        }
    }
    await writeFileStore(payload);
}

export async function getPlannerDeltaState(scopeKey) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    const entry = scopes[scopeKey];
    if (!entry?.deltaLink) return null;
    if (entry.updatedAt && Number.isFinite(MAX_AGE_DAYS) && MAX_AGE_DAYS > 0) {
        const updatedMs = Date.parse(entry.updatedAt);
        if (Number.isFinite(updatedMs)) {
            const ageMs = Date.now() - updatedMs;
            const maxAgeMs = MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
            if (ageMs > maxAgeMs) {
                await clearPlannerDeltaState(scopeKey);
                return null;
            }
        }
    }
    return entry;
}

export async function savePlannerDeltaState(scopeKey, deltaLink) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    scopes[scopeKey] = { deltaLink, updatedAt: new Date().toISOString() };
    await writeStore({ scopes });
}

export async function clearPlannerDeltaState(scopeKey) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    if (!scopes[scopeKey]) return;
    delete scopes[scopeKey];
    await writeStore({ scopes });
}
