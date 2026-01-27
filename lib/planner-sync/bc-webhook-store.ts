import { promises as fs } from "fs";
import path from "path";
import crypto from "crypto";
import { getRedis } from "./redis";
import { logger } from "./logger";

export type BcWebhookSubscription = {
    id: string;
    entitySet: string;
    resource?: string;
    expirationDateTime?: string;
    createdAt?: string;
    notificationUrl?: string;
    clientState?: string;
};

export type BcWebhookJob = {
    entitySet: string;
    systemId: string;
    changeType?: string;
    receivedAt: string;
    subscriptionId?: string;
    resource?: string;
};

type FileStore = {
    subscriptions?: Record<string, BcWebhookSubscription>;
    jobs?: BcWebhookJob[];
    dedupe?: Record<string, number>;
    lock?: { value: string; expiresAt: number };
};

const FILE_PATH = process.env.BC_WEBHOOK_STORE_FILE || path.join(process.cwd(), ".bc-webhook-store.json");
const SUBSCRIPTION_PREFIX = "bc:subscription:";
const JOBS_KEY = "bc:jobs";
const DEDUPE_PREFIX = "bc:job_dedupe:";
const LOCK_KEY = "bc:jobs:lock";
const DEDUPE_WINDOW_SECONDS = 300;
const LOCK_TTL_SECONDS = 60;

function normalizeEntitySet(entitySet: string) {
    return (entitySet || "").trim();
}

function parseJsonString<T>(raw: unknown): T | null {
    if (!raw) return null;
    if (typeof raw === "string") {
        try {
            return JSON.parse(raw) as T;
        } catch {
            return null;
        }
    }
    if (typeof raw === "object") return raw as T;
    return null;
}

async function readFileStore(): Promise<FileStore> {
    try {
        const raw = await fs.readFile(FILE_PATH, "utf8");
        const parsed = JSON.parse(raw) as FileStore;
        if (!parsed || typeof parsed !== "object") return {};
        return parsed;
    } catch {
        return {};
    }
}

async function writeFileStore(store: FileStore) {
    try {
        await fs.writeFile(FILE_PATH, JSON.stringify(store, null, 2), "utf8");
    } catch (error) {
        logger.warn("Failed to write BC webhook store", { error: (error as Error)?.message });
    }
}

function buildDedupeKey(job: BcWebhookJob, receivedMs: number) {
    const bucket = Math.floor(receivedMs / (DEDUPE_WINDOW_SECONDS * 1000));
    const payload = `${job.entitySet}|${job.systemId}|${job.changeType || ""}|${bucket}`;
    const hash = crypto.createHash("sha1").update(payload).digest("hex");
    return `${DEDUPE_PREFIX}${hash}`;
}

function normalizeJob(job: BcWebhookJob) {
    const entitySet = normalizeEntitySet(job.entitySet);
    const systemId = (job.systemId || "").trim();
    if (!entitySet || !systemId) return null;
    return { ...job, entitySet, systemId };
}

function purgeExpiredDedupe(dedupe: Record<string, number>, now: number) {
    for (const [key, expiresAt] of Object.entries(dedupe)) {
        if (!expiresAt || expiresAt <= now) delete dedupe[key];
    }
}

export async function getBcSubscription(entitySet: string): Promise<BcWebhookSubscription | null> {
    const normalized = normalizeEntitySet(entitySet);
    if (!normalized) return null;
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const raw = await redis.get(`${SUBSCRIPTION_PREFIX}${normalized}`);
            return parseJsonString<BcWebhookSubscription>(raw);
        } catch (error) {
            logger.warn("KV read failed for BC subscription; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    const store = await readFileStore();
    return store.subscriptions?.[normalized] || null;
}

export async function saveBcSubscription(entitySet: string, subscription: BcWebhookSubscription) {
    const normalized = normalizeEntitySet(entitySet);
    if (!normalized) return;
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.set(`${SUBSCRIPTION_PREFIX}${normalized}`, JSON.stringify(subscription));
            return;
        } catch (error) {
            logger.warn("KV write failed for BC subscription; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    const store = await readFileStore();
    store.subscriptions = store.subscriptions || {};
    store.subscriptions[normalized] = subscription;
    await writeFileStore(store);
}

export async function deleteBcSubscription(entitySet: string) {
    const normalized = normalizeEntitySet(entitySet);
    if (!normalized) return;
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.del(`${SUBSCRIPTION_PREFIX}${normalized}`);
            return;
        } catch (error) {
            logger.warn("KV delete failed for BC subscription; falling back to file", {
                error: (error as Error)?.message,
            });
        }
    }
    const store = await readFileStore();
    if (store.subscriptions) {
        delete store.subscriptions[normalized];
        await writeFileStore(store);
    }
}

export async function enqueueBcJobs(jobs: BcWebhookJob[]) {
    if (!jobs.length) return { enqueued: 0, deduped: 0, skipped: 0 };
    const redis = getRedis({ requireWrite: true });
    let enqueued = 0;
    let deduped = 0;
    let skipped = 0;

    if (redis) {
        for (const raw of jobs) {
            const job = normalizeJob(raw);
            if (!job) {
                skipped += 1;
                continue;
            }
            const receivedMs = Date.parse(job.receivedAt) || Date.now();
            const dedupeKey = buildDedupeKey(job, receivedMs);
            let shouldEnqueue = true;
            try {
                const result = await redis.set(dedupeKey, "1", { nx: true, ex: DEDUPE_WINDOW_SECONDS });
                if (!result) {
                    shouldEnqueue = false;
                }
            } catch (error) {
                logger.warn("BC webhook dedupe failed; enqueueing anyway", {
                    error: (error as Error)?.message,
                });
            }
            if (!shouldEnqueue) {
                deduped += 1;
                continue;
            }
            try {
                await redis.lpush(JOBS_KEY, JSON.stringify(job));
                enqueued += 1;
            } catch (error) {
                logger.warn("BC webhook enqueue failed", { error: (error as Error)?.message });
            }
        }
        return { enqueued, deduped, skipped };
    }

    const store = await readFileStore();
    const dedupe = store.dedupe || {};
    const now = Date.now();
    purgeExpiredDedupe(dedupe, now);
    store.jobs = store.jobs || [];
    for (const raw of jobs) {
        const job = normalizeJob(raw);
        if (!job) {
            skipped += 1;
            continue;
        }
        const receivedMs = Date.parse(job.receivedAt) || Date.now();
        const dedupeKey = buildDedupeKey(job, receivedMs);
        if (dedupe[dedupeKey] && dedupe[dedupeKey] > now) {
            deduped += 1;
            continue;
        }
        dedupe[dedupeKey] = receivedMs + DEDUPE_WINDOW_SECONDS * 1000;
        store.jobs.push(job);
        enqueued += 1;
    }
    store.dedupe = dedupe;
    await writeFileStore(store);
    return { enqueued, deduped, skipped };
}

export async function popBcJobs(maxJobs = 25): Promise<BcWebhookJob[]> {
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        const jobs: BcWebhookJob[] = [];
        for (let i = 0; i < maxJobs; i += 1) {
            const raw = await redis.rpop(JOBS_KEY);
            if (!raw) break;
            const parsed = parseJsonString<BcWebhookJob>(raw);
            if (parsed) jobs.push(parsed);
        }
        return jobs;
    }

    const store = await readFileStore();
    const jobs = store.jobs || [];
    const items = jobs.splice(0, maxJobs);
    store.jobs = jobs;
    await writeFileStore(store);
    return items;
}

export async function acquireBcJobLock(ttlSeconds = LOCK_TTL_SECONDS) {
    const redis = getRedis({ requireWrite: true });
    const lockValue = Math.random().toString(36).slice(2, 12);
    if (redis) {
        try {
            const result = await redis.set(LOCK_KEY, lockValue, { nx: true, ex: ttlSeconds });
            if (!result) return null;
            return lockValue;
        } catch (error) {
            logger.warn("BC webhook lock failed", { error: (error as Error)?.message });
            return null;
        }
    }

    const store = await readFileStore();
    const now = Date.now();
    if (store.lock && store.lock.expiresAt > now) {
        return null;
    }
    store.lock = { value: lockValue, expiresAt: now + ttlSeconds * 1000 };
    await writeFileStore(store);
    return lockValue;
}

export async function releaseBcJobLock(lockValue: string | null) {
    if (!lockValue) return;
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            const current = await redis.get(LOCK_KEY);
            if (current === lockValue) {
                await redis.del(LOCK_KEY);
            }
            return;
        } catch (error) {
            logger.warn("BC webhook lock release failed", { error: (error as Error)?.message });
            return;
        }
    }

    const store = await readFileStore();
    if (store.lock?.value === lockValue) {
        delete store.lock;
        await writeFileStore(store);
    }
}
