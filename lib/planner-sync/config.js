export function readEnv(name, required = false) {
    const value = process.env[name];
    if (required && !value) {
        throw new Error(`Missing env ${name}`);
    }
    return value;
}

export function getBcConfig() {
    return {
        tenantId: readEnv("BC_TENANT_ID", true),
        environment: readEnv("BC_ENVIRONMENT", true),
        companyId: readEnv("BC_COMPANY_ID", true),
        clientId: readEnv("BC_CLIENT_ID", true),
        clientSecret: readEnv("BC_CLIENT_SECRET", true),
        apiBase: readEnv("BC_API_BASE") || "https://api.businesscentral.dynamics.com/v2.0",
        publisher: readEnv("BC_API_PUBLISHER") || "cornerstone",
        group: readEnv("BC_API_GROUP") || "plannerSync",
        version: readEnv("BC_API_VERSION") || "v1.0",
    };
}

export function getGraphConfig() {
    return {
        tenantId: readEnv("GRAPH_TENANT_ID", true),
        clientId: readEnv("GRAPH_CLIENT_ID", true),
        clientSecret: readEnv("GRAPH_CLIENT_SECRET", true),
        clientState: readEnv("GRAPH_SUBSCRIPTION_CLIENT_STATE", true),
    };
}

export function getPlannerConfig() {
    return {
        groupId: readEnv("PLANNER_GROUP_ID", true),
        defaultPlanId: readEnv("PLANNER_DEFAULT_PLAN_ID") || undefined,
    };
}

export function getSyncConfig() {
    const syncMode = readEnv("SYNC_MODE") || "perProjectPlan";
    if (syncMode !== "perProjectPlan" && syncMode !== "singlePlan") {
        throw new Error("SYNC_MODE must be perProjectPlan or singlePlan");
    }
    const pollMinutes = Number(readEnv("SYNC_POLL_MINUTES") || 10);
    return {
        syncMode,
        pollMinutes: Number.isNaN(pollMinutes) ? 10 : pollMinutes,
        timeZone: readEnv("SYNC_TIMEZONE") || "America/New_York",
    };
}
