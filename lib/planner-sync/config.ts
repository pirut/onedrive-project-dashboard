export type SyncMode = "perProjectPlan" | "singlePlan";

function readEnv(name: string, required = false) {
    const value = process.env[name];
    if (required && !value) {
        throw new Error(`Missing env ${name}`);
    }
    return value;
}

function readBoolEnv(name: string, defaultValue: boolean) {
    const value = readEnv(name);
    if (!value) return defaultValue;
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return defaultValue;
}

export function getBcConfig() {
    return {
        tenantId: readEnv("BC_TENANT_ID", true) as string,
        environment: readEnv("BC_ENVIRONMENT", true) as string,
        companyId: readEnv("BC_COMPANY_ID", true) as string,
        clientId: readEnv("BC_CLIENT_ID", true) as string,
        clientSecret: readEnv("BC_CLIENT_SECRET", true) as string,
        apiBase: readEnv("BC_API_BASE") || "https://api.businesscentral.dynamics.com/v2.0",
        publisher: readEnv("BC_API_PUBLISHER") || "cornerstone",
        group: readEnv("BC_API_GROUP") || "plannerSync",
        version: readEnv("BC_API_VERSION") || "v1.0",
    };
}

export function getGraphConfig() {
    return {
        tenantId: readEnv("GRAPH_TENANT_ID", true) as string,
        clientId: readEnv("GRAPH_CLIENT_ID", true) as string,
        clientSecret: readEnv("GRAPH_CLIENT_SECRET", true) as string,
        clientState: readEnv("GRAPH_SUBSCRIPTION_CLIENT_STATE", true) as string,
    };
}

export function getPlannerConfig() {
    return {
        groupId: readEnv("PLANNER_GROUP_ID", true) as string,
        defaultPlanId: readEnv("PLANNER_DEFAULT_PLAN_ID") || undefined,
    };
}

export function getSyncConfig() {
    const syncMode = (readEnv("SYNC_MODE") || "perProjectPlan") as SyncMode;
    if (syncMode !== "perProjectPlan" && syncMode !== "singlePlan") {
        throw new Error("SYNC_MODE must be perProjectPlan or singlePlan");
    }
    const pollMinutes = Number(readEnv("SYNC_POLL_MINUTES") || 10);
    const syncLockTimeoutMinutes = Number(readEnv("SYNC_LOCK_TIMEOUT_MINUTES") || 30);
    const preferBc = readBoolEnv("SYNC_PREFER_BC", false);
    const bcModifiedGraceMs = Number(readEnv("SYNC_BC_MODIFIED_GRACE_MS") || 2000);
    const usePlannerDelta = readBoolEnv("SYNC_USE_PLANNER_DELTA", true);
    const useSmartPolling = readBoolEnv("SYNC_USE_SMART_POLLING", false);
    return {
        syncMode,
        pollMinutes: Number.isNaN(pollMinutes) ? 10 : pollMinutes,
        timeZone: readEnv("SYNC_TIMEZONE") || "America/New_York",
        allowDefaultPlanFallback: readBoolEnv("SYNC_ALLOW_DEFAULT_PLAN_FALLBACK", true),
        syncLockTimeoutMinutes: Number.isNaN(syncLockTimeoutMinutes) ? 30 : syncLockTimeoutMinutes,
        preferBc,
        bcModifiedGraceMs: Number.isNaN(bcModifiedGraceMs) ? 2000 : bcModifiedGraceMs,
        usePlannerDelta,
        useSmartPolling,
    };
}

export function readOptionalEnv(name: string) {
    return readEnv(name) || undefined;
}
