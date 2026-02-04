import { promises as fs } from "fs";
import path from "path";
import { getRedis } from "./redis.js";
import { logger } from "./logger.js";

export type BcAuditCursor = {
    lastProjectNo?: string | null;
    cycle?: number;
    updatedAt?: string;
};

type StoredAudit = {
    scopes?: Record<string, BcAuditCursor>;
};

const FILE_PATH = process.env.BC_AUDIT_CURSOR_FILE || path.join(process.cwd(), ".bc-audit-cursor.json");
const KV_KEY = "bc:audit:cursor";

function normalizeScopes(raw: unknown): Record<string, BcAuditCursor> {
    if (!raw || typeof raw !== "object") return {};
    const source =
        (raw as StoredAudit).scopes && typeof (raw as StoredAudit).scopes === "object"
            ? (raw as StoredAudit).scopes
            : (raw as Record<string, unknown>);
    const scopes: Record<string, BcAuditCursor> = {};
    for (const [key, value] of Object.entries(source || {})) {
        if (!value || typeof value !== "object") continue;
        const cursor = value as BcAuditCursor;
        scopes[key] = {
            lastProjectNo: typeof cursor.lastProjectNo === "string" ? cursor.lastProjectNo : cursor.lastProjectNo ?? null,
            cycle: typeof cursor.cycle === "number" && Number.isFinite(cursor.cycle) ? cursor.cycle : undefined,
            updatedAt: typeof cursor.updatedAt === "string" ? cursor.updatedAt : undefined,
        };
    }
    return scopes;
}

async function readFileStore(): Promise<StoredAudit | null> {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        return JSON.parse(raw) as StoredAudit;
    } catch {
        return null;
    }
}

async function writeFileStore(payload: StoredAudit) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(payload, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write BC audit cursor store", { error: (error as Error)?.message });
    }
}

async function readStore(): Promise<StoredAudit | null> {
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(KV_KEY);
            if (!raw) return null;
            if (typeof raw === "string") return JSON.parse(raw) as StoredAudit;
            if (typeof raw === "object") return raw as StoredAudit;
        } catch (error) {
            logger.warn("KV read failed for BC audit cursor; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    return readFileStore();
}

async function writeStore(payload: StoredAudit) {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(KV_KEY, JSON.stringify(payload));
            return;
        } catch (error) {
            logger.warn("KV write failed for BC audit cursor; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    await writeFileStore(payload);
}

export async function getBcAuditCursor(scopeKey: string): Promise<BcAuditCursor> {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    return scopes[scopeKey] || { lastProjectNo: null, cycle: 0 };
}

export async function saveBcAuditCursor(scopeKey: string, cursor: BcAuditCursor) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    scopes[scopeKey] = {
        lastProjectNo: cursor.lastProjectNo ?? null,
        cycle: typeof cursor.cycle === "number" && Number.isFinite(cursor.cycle) ? cursor.cycle : undefined,
        updatedAt: new Date().toISOString(),
    };
    await writeStore({ scopes });
}

export async function resetBcAuditCursor(scopeKey: string) {
    const store = await readStore();
    const scopes = normalizeScopes(store);
    scopes[scopeKey] = { lastProjectNo: null, cycle: (scopes[scopeKey]?.cycle || 0) + 1, updatedAt: new Date().toISOString() };
    await writeStore({ scopes });
}
