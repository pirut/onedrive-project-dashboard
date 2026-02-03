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
        tenantId: readEnv("TENANT_ID", true) as string,
        environment: readEnv("BC_ENVIRONMENT", true) as string,
        companyId: readEnv("BC_COMPANY_ID", true) as string,
        clientId: readEnv("BC_CLIENT_ID", true) as string,
        clientSecret: readEnv("MICROSOFT_CLIENT_SECRET", true) as string,
        apiBase: readEnv("BC_API_BASE") || "https://api.businesscentral.dynamics.com/v2.0",
        publisher: readEnv("BC_API_PUBLISHER") || "cornerstone",
        group: readEnv("BC_API_GROUP") || "plannerSync",
        version: readEnv("BC_API_VERSION") || "v1.0",
        projectChangesEntitySet: readEnv("BC_PROJECT_CHANGES_ENTITY_SET") || undefined,
    };
}

export function getSyncConfig() {
    const syncLockTimeoutMinutes = Number(readEnv("SYNC_LOCK_TIMEOUT_MINUTES") || 30);
    const preferBc = readBoolEnv("SYNC_PREFER_BC", false);
    const bcModifiedGraceMs = Number(readEnv("SYNC_BC_MODIFIED_GRACE_MS") || 2000);
    const maxProjectsPerRunRaw = readEnv("SYNC_MAX_PROJECTS_PER_RUN") || (process.env.VERCEL ? "50" : "0");
    const maxProjectsPerRun = Number(maxProjectsPerRunRaw);
    return {
        syncLockTimeoutMinutes: Number.isNaN(syncLockTimeoutMinutes) ? 30 : syncLockTimeoutMinutes,
        preferBc,
        bcModifiedGraceMs: Number.isNaN(bcModifiedGraceMs) ? 2000 : bcModifiedGraceMs,
        maxProjectsPerRun: Number.isNaN(maxProjectsPerRun) ? 0 : Math.max(0, Math.floor(maxProjectsPerRun)),
    };
}

export function readOptionalEnv(name: string) {
    return readEnv(name) || undefined;
}
