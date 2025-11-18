import { Redis } from "@upstash/redis";

function resolveKvCredentials() {
    const url = process.env.KV_REST_API_URL || "";
    const token = process.env.KV_REST_API_TOKEN || process.env.KV_REST_API_READ_ONLY_TOKEN || "";
    return { url, token };
}

function getRedis() {
    const { url, token } = resolveKvCredentials();
    if (!url || !token) return null;
    return new Redis({ url, token });
}

// Default bucket configuration
const DEFAULT_BUCKETS = {
    buckets: [
        { id: "todo", name: "To Do", order: 0, color: "#3b82f6" },
        { id: "in-progress", name: "In Progress", order: 1, color: "#f59e0b" },
        { id: "review", name: "Review", order: 2, color: "#8b5cf6" },
        { id: "complete", name: "Complete", order: 3, color: "#10b981" },
        { id: "archive", name: "Archive", order: 4, color: "#6b7280" },
    ],
};

// Bucket configuration operations
export async function getBuckets() {
    const redis = getRedis();
    if (!redis) return DEFAULT_BUCKETS;
    try {
        const data = await redis.get("projects:buckets");
        if (data) return data;
        // Initialize with defaults if not exists
        await redis.set("projects:buckets", DEFAULT_BUCKETS);
        return DEFAULT_BUCKETS;
    } catch (e) {
        console.warn("Failed to get buckets:", e?.message || String(e));
        return DEFAULT_BUCKETS;
    }
}

export async function setBuckets(buckets) {
    const redis = getRedis();
    if (!redis) {
        console.warn("KV not configured; cannot save buckets");
        return false;
    }
    try {
        await redis.set("projects:buckets", buckets);
        return true;
    } catch (e) {
        console.warn("Failed to save buckets:", e?.message || String(e));
        return false;
    }
}

// Project kanban state operations
export async function getProjectKanbanState(folderId) {
    const redis = getRedis();
    if (!redis) return null;
    try {
        const data = await redis.get(`projects:kanban:${folderId}`);
        return data || null;
    } catch (e) {
        console.warn(`Failed to get kanban state for ${folderId}:`, e?.message || String(e));
        return null;
    }
}

export async function setProjectKanbanState(folderId, state) {
    const redis = getRedis();
    if (!redis) {
        console.warn("KV not configured; cannot save kanban state");
        return false;
    }
    try {
        const data = {
            folderId,
            bucketId: state.bucketId,
            updatedAt: new Date().toISOString(),
        };
        await redis.set(`projects:kanban:${folderId}`, data);
        return true;
    } catch (e) {
        console.warn(`Failed to save kanban state for ${folderId}:`, e?.message || String(e));
        return false;
    }
}

export async function getAllProjectKanbanStates() {
    const redis = getRedis();
    if (!redis) return {};
    try {
        // Get all keys matching the pattern
        const keys = await redis.keys("projects:kanban:*");
        if (!keys || keys.length === 0) return {};
        const values = await redis.mget(...keys);
        const result = {};
        keys.forEach((key, index) => {
            const folderId = key.replace("projects:kanban:", "");
            if (values[index]) {
                result[folderId] = values[index];
            }
        });
        return result;
    } catch (e) {
        console.warn("Failed to get all kanban states:", e?.message || String(e));
        return {};
    }
}

// Project metadata operations
export async function getProjectMetadata(folderId) {
    const redis = getRedis();
    if (!redis) return null;
    try {
        const data = await redis.get(`projects:metadata:${folderId}`);
        return data || null;
    } catch (e) {
        console.warn(`Failed to get metadata for ${folderId}:`, e?.message || String(e));
        return null;
    }
}

export async function setProjectMetadata(folderId, metadata) {
    const redis = getRedis();
    if (!redis) {
        console.warn("KV not configured; cannot save metadata");
        return false;
    }
    try {
        await redis.set(`projects:metadata:${folderId}`, metadata);
        return true;
    } catch (e) {
        console.warn(`Failed to save metadata for ${folderId}:`, e?.message || String(e));
        return false;
    }
}

export async function setMultipleProjectMetadata(metadataMap) {
    const redis = getRedis();
    if (!redis) {
        console.warn("KV not configured; cannot save metadata");
        return false;
    }
    try {
        // Set each metadata entry individually
        for (const [folderId, metadata] of Object.entries(metadataMap)) {
            await redis.set(`projects:metadata:${folderId}`, metadata);
        }
        return true;
    } catch (e) {
        console.warn("Failed to save multiple metadata:", e?.message || String(e));
        return false;
    }
}

export async function getAllProjectMetadata() {
    const redis = getRedis();
    if (!redis) return {};
    try {
        const keys = await redis.keys("projects:metadata:*");
        if (!keys || keys.length === 0) return {};
        const values = await redis.mget(...keys);
        const result = {};
        keys.forEach((key, index) => {
            const folderId = key.replace("projects:metadata:", "");
            if (values[index]) {
                result[folderId] = values[index];
            }
        });
        return result;
    } catch (e) {
        console.warn("Failed to get all metadata:", e?.message || String(e));
        return {};
    }
}

