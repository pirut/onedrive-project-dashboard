import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis";
import { logger } from "./logger";

type ChangeCursorEntry = { lastSeq: number; updatedAt?: string };

const FILE_PATH = process.env.BC_PROJECT_CHANGES_FILE || path.join(process.cwd(), ".bc-project-changes.json");
const KV_KEY = "bc:project-changes";

function normalizeScopes(raw: unknown): Record<string, ChangeCursorEntry> {
    if (!raw || typeof raw !== "object") return {};
    const source =
        (raw as { scopes?: Record<string, unknown> }).scopes && typeof (raw as { scopes?: unknown }).scopes === "object"
            ? (raw as { scopes: Record<string, unknown> }).scopes
            : (raw as Record<string, unknown>);
    const scopes: Record<string, ChangeCursorEntry> = {};
    for (const [key, value] of Object.entries(source || {})) {
        if (value && typeof value === "object" && typeof (value as ChangeCursorEntry).lastSeq === "number") {
            const lastSeq = (value as ChangeCursorEntry).lastSeq;
            if (Number.isFinite(lastSeq)) {
                scopes[key] = {
                    lastSeq,
                    updatedAt: typeof (value as ChangeCursorEntry).updatedAt === "string" ? (value as ChangeCursorEntry).updatedAt : undefined,
                };
            }
        } else if (typeof value === "number" && Number.isFinite(value)) {
            scopes[key] = { lastSeq: value };
        } else if (typeof value === "string" && value.trim()) {
            const parsed = Number(value);
            if (Number.isFinite(parsed)) scopes[key] = { lastSeq: parsed };
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
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write BC project change store", { error: (error as Error)?.message });
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
            logger.warn("KV read failed for BC project change store; falling back to file", {
                error: (error as Error)?.message,
            });
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
            logger.warn("KV write failed for BC project change store; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    await writeFileStore(payload);
}

export async function getBcProjectChangeCursor(scopeKey: string): Promise<number | null> {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    const entry = scopes[scopeKey];
    if (!entry || !Number.isFinite(entry.lastSeq)) return null;
    return entry.lastSeq;
}

export async function saveBcProjectChangeCursor(scopeKey: string, lastSeq: number) {
    if (!Number.isFinite(lastSeq)) {
        logger.warn("Skipping BC project change cursor save; invalid lastSeq", { scopeKey, lastSeq });
        return;
    }
    const store = await readStore();
    const scopes = normalizeScopes(store);
    scopes[scopeKey] = { lastSeq, updatedAt: new Date().toISOString() };
    await writeStore({ scopes });
}
