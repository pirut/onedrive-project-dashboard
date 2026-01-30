import { EventEmitter } from "events";
import { getRedis } from "./redis.js";

type PlannerLogEntry = Record<string, unknown>;

const LOG_KEY = "planner:log";
const DEFAULT_MAX_LOG = 200;
const MAX_LOG = Number(process.env.PLANNER_LOG_MAX || DEFAULT_MAX_LOG);
const inMemoryLog: PlannerLogEntry[] = [];
const emitter = (globalThis as { __plannerLogEmitter?: EventEmitter }).__plannerLogEmitter || new EventEmitter();

if (!(globalThis as { __plannerLogEmitter?: EventEmitter }).__plannerLogEmitter) {
    (globalThis as { __plannerLogEmitter: EventEmitter }).__plannerLogEmitter = emitter;
}

function resolveMaxLog() {
    return Number.isFinite(MAX_LOG) && MAX_LOG > 0 ? MAX_LOG : DEFAULT_MAX_LOG;
}

export function getPlannerLogEmitter() {
    return emitter;
}

export async function appendPlannerLog(entry: PlannerLogEntry) {
    const maxLog = resolveMaxLog();
    const redis = getRedis({ requireWrite: true });
    if (redis) {
        try {
            await redis.lpush(LOG_KEY, JSON.stringify(entry));
            await redis.ltrim(LOG_KEY, 0, maxLog - 1);
            emitter.emit("entry", entry);
            return;
        } catch (error) {
            // Avoid recursive logging if log storage fails.
            // eslint-disable-next-line no-console
            console.warn("Planner log write failed; using memory log", { error: (error as Error)?.message });
        }
    }
    inMemoryLog.unshift(entry);
    if (inMemoryLog.length > maxLog) {
        inMemoryLog.length = maxLog;
    }
    emitter.emit("entry", entry);
}

export async function listPlannerLog(limit = 50) {
    const maxLog = resolveMaxLog();
    const safeLimit = Math.max(1, Math.min(limit, maxLog));
    const redis = getRedis({ requireWrite: false });
    if (redis) {
        try {
            const rows = await redis.lrange(LOG_KEY, 0, safeLimit - 1);
            return rows
                .map((row) => {
                    if (typeof row === "string") {
                        try {
                            return JSON.parse(row) as PlannerLogEntry;
                        } catch {
                            return null;
                        }
                    }
                    return row as PlannerLogEntry;
                })
                .filter(Boolean) as PlannerLogEntry[];
        } catch (error) {
            // eslint-disable-next-line no-console
            console.warn("Planner log read failed; using memory log", { error: (error as Error)?.message });
        }
    }
    return inMemoryLog.slice(0, safeLimit);
}
