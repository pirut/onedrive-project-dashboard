export type LogLevel = "info" | "warn" | "error";

export type LogMeta = Record<string, unknown>;

function emit(level: LogLevel, message: string, meta?: LogMeta) {
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
    info(message: string, meta?: LogMeta) {
        emit("info", message, meta);
    },
    warn(message: string, meta?: LogMeta) {
        emit("warn", message, meta);
    },
    error(message: string, meta?: LogMeta) {
        emit("error", message, meta);
    },
};
