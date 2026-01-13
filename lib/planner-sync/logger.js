import { appendPlannerLog } from "./planner-log.js";

export function log(level, message, meta = undefined) {
    const payload = {
        level,
        scope: "planner-sync",
        message,
        timestamp: new Date().toISOString(),
        ...(meta || {}),
    };
    const line = JSON.stringify(payload);
    if (level === "error") {
        // eslint-disable-next-line no-console
        console.error(line);
        appendPlannerLog(payload).catch(() => {});
        return;
    }
    if (level === "warn") {
        // eslint-disable-next-line no-console
        console.warn(line);
        appendPlannerLog(payload).catch(() => {});
        return;
    }
    // eslint-disable-next-line no-console
    console.log(line);
    appendPlannerLog(payload).catch(() => {});
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
