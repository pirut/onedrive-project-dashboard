import { getRedis } from "../planner-sync/redis.js";
import { logger } from "../planner-sync/logger.js";

const DEFAULT_TTL_SECONDS = Number(process.env.PREMIUM_TO_BC_WRITE_TTL_SECONDS || 120);
const KV_PREFIX = "premium:premium-write:";
const memoryCache: Map<string, number> =
    (globalThis as typeof globalThis & { __premiumToBcWriteCache?: Map<string, number> }).__premiumToBcWriteCache ||
    new Map();

if (!(globalThis as typeof globalThis & { __premiumToBcWriteCache?: Map<string, number> }).__premiumToBcWriteCache) {
    (globalThis as typeof globalThis & { __premiumToBcWriteCache?: Map<string, number> }).__premiumToBcWriteCache =
        memoryCache;
}

function normalizeId(value: unknown) {
    return String(value || "").trim();
}

function purgeMemory(now = Date.now()) {
    for (const [key, expiresAt] of memoryCache.entries()) {
        if (!expiresAt || expiresAt <= now) {
            memoryCache.delete(key);
        }
    }
}

export async function markBcTaskSystemIdsFromPremium(taskSystemIds: string[], ttlSeconds = DEFAULT_TTL_SECONDS) {
    const ids = (taskSystemIds || []).map(normalizeId).filter(Boolean);
    if (!ids.length) return { marked: 0 };

    const ttl = Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? Math.floor(ttlSeconds) : 0;
    if (!ttl) return { marked: 0 };

    const redis = getRedis({ requireWrite: true });
    if (redis) {
        let marked = 0;
        for (const id of ids) {
            try {
                await redis.set(`${KV_PREFIX}${id}`, "1", { ex: ttl });
                marked += 1;
            } catch (error) {
                logger.warn("Premium BC writeback mark failed", { id, error: (error as Error)?.message });
            }
        }
        return { marked };
    }

    const expiresAt = Date.now() + ttl * 1000;
    purgeMemory();
    for (const id of ids) {
        memoryCache.set(id, expiresAt);
    }
    return { marked: ids.length };
}

export async function wasBcTaskSystemIdUpdatedByPremium(taskSystemId: string) {
    const id = normalizeId(taskSystemId);
    if (!id) return false;

    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const value = await redis.get(`${KV_PREFIX}${id}`);
            if (value) return true;
        } catch (error) {
            logger.warn("Premium BC writeback check failed; falling back to memory", {
                id,
                error: (error as Error)?.message,
            });
        }
    }

    purgeMemory();
    return memoryCache.has(id);
}
