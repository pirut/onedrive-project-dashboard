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
        return;
    }
    if (level === "warn") {
        // eslint-disable-next-line no-console
        console.warn(line);
        return;
    }
    // eslint-disable-next-line no-console
    console.log(line);
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
