import { appendPlannerLog } from "./planner-log.js";

const DEFAULT_MAX_LOG_STRING = 500;
const DEFAULT_MAX_LOG_ARRAY = 20;
const DEFAULT_MAX_LOG_KEYS = 40;
const DEFAULT_MAX_LOG_DEPTH = 4;
const DEFAULT_MAX_LOG_BYTES = 8000;

function readLimit(name, fallback) {
    const raw = Number(process.env[name]);
    return Number.isFinite(raw) && raw > 0 ? raw : fallback;
}

const MAX_LOG_STRING = readLimit("PLANNER_LOG_MAX_STRING", DEFAULT_MAX_LOG_STRING);
const MAX_LOG_ARRAY = readLimit("PLANNER_LOG_MAX_ARRAY", DEFAULT_MAX_LOG_ARRAY);
const MAX_LOG_KEYS = readLimit("PLANNER_LOG_MAX_KEYS", DEFAULT_MAX_LOG_KEYS);
const MAX_LOG_DEPTH = readLimit("PLANNER_LOG_MAX_DEPTH", DEFAULT_MAX_LOG_DEPTH);
const MAX_LOG_BYTES = readLimit("PLANNER_LOG_MAX_BYTES", DEFAULT_MAX_LOG_BYTES);

function truncateString(value) {
    if (value.length <= MAX_LOG_STRING) return value;
    return `${value.slice(0, MAX_LOG_STRING)}... (truncated ${value.length - MAX_LOG_STRING} chars)`;
}

function sanitizeValue(value, depth, seen) {
    if (value == null) return value;
    const valueType = typeof value;
    if (valueType === "string") return truncateString(value);
    if (valueType === "number" || valueType === "boolean") return value;
    if (valueType === "bigint") return value.toString();
    if (valueType === "symbol") return value.toString();
    if (valueType === "function") return `[Function ${value.name || "anonymous"}]`;

    if (value instanceof Date) {
        return Number.isNaN(value.getTime()) ? "Invalid Date" : value.toISOString();
    }
    if (value instanceof URL) return value.toString();
    if (value instanceof Error) {
        return {
            name: value.name,
            message: truncateString(value.message || "Error"),
            stack: value.stack ? truncateString(value.stack) : undefined,
        };
    }
    if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
        return `<Buffer ${value.length}>`;
    }
    if (ArrayBuffer.isView(value)) {
        return `<${value.constructor?.name || "TypedArray"} ${value.byteLength}>`;
    }

    if (typeof value !== "object") return String(value);
    if (seen.has(value)) return "[Circular]";
    if (depth >= MAX_LOG_DEPTH) return "[MaxDepth]";

    seen.add(value);

    if (Array.isArray(value)) {
        const items = value.slice(0, MAX_LOG_ARRAY).map((item) => sanitizeValue(item, depth + 1, seen));
        if (value.length > MAX_LOG_ARRAY) {
            items.push(`... (${value.length - MAX_LOG_ARRAY} more items)`);
        }
        return items;
    }

    if (value instanceof Map) {
        const entries = Array.from(value.entries()).slice(0, MAX_LOG_ARRAY).map(([key, val]) => [
            sanitizeValue(key, depth + 1, seen),
            sanitizeValue(val, depth + 1, seen),
        ]);
        const result = {
            type: "Map",
            size: value.size,
            entries,
        };
        if (value.size > MAX_LOG_ARRAY) {
            result.truncated = value.size - MAX_LOG_ARRAY;
        }
        return result;
    }

    if (value instanceof Set) {
        const entries = Array.from(value.values()).slice(0, MAX_LOG_ARRAY).map((entry) =>
            sanitizeValue(entry, depth + 1, seen)
        );
        const result = {
            type: "Set",
            size: value.size,
            entries,
        };
        if (value.size > MAX_LOG_ARRAY) {
            result.truncated = value.size - MAX_LOG_ARRAY;
        }
        return result;
    }

    const keys = Object.keys(value);
    const result = {};
    const limitedKeys = keys.slice(0, MAX_LOG_KEYS);
    for (const key of limitedKeys) {
        result[key] = sanitizeValue(value[key], depth + 1, seen);
    }
    if (keys.length > MAX_LOG_KEYS) {
        result.__truncatedKeys = keys.length - MAX_LOG_KEYS;
    }
    return result;
}

function normalizeMeta(meta = undefined) {
    if (!meta) return undefined;
    if (typeof meta !== "object") {
        return { meta: sanitizeValue(meta, 0, new WeakSet()) };
    }
    return sanitizeValue(meta, 0, new WeakSet());
}

export function log(level, message, meta = undefined) {
    const safeMeta = normalizeMeta(meta);
    const payload = {
        level,
        scope: "planner-sync",
        message: truncateString(message),
        timestamp: new Date().toISOString(),
        ...(safeMeta || {}),
    };
    let line = JSON.stringify(payload);
    let finalPayload = payload;
    if (line.length > MAX_LOG_BYTES) {
        finalPayload = {
            level,
            scope: "planner-sync",
            message: truncateString(message),
            timestamp: new Date().toISOString(),
            logTruncated: true,
            originalSize: line.length,
        };
        line = JSON.stringify(finalPayload);
    }
    if (level === "error") {
        // eslint-disable-next-line no-console
        console.error(line);
        appendPlannerLog(finalPayload).catch(() => {});
        return;
    }
    if (level === "warn") {
        // eslint-disable-next-line no-console
        console.warn(line);
        appendPlannerLog(finalPayload).catch(() => {});
        return;
    }
    // eslint-disable-next-line no-console
    console.log(line);
    appendPlannerLog(finalPayload).catch(() => {});
}

export const logger = {
    info(message, meta) {
        log("info", message, meta);
    },
    warn(message, meta) {
        log("warn", message, meta);
    },
    error(message, meta) {
        log("error", message, meta);
    },
};
