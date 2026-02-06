export type PremiumDeleteBehavior = "clearLink" | "ignore";

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

function readNumberEnv(name: string, fallback: number) {
    const raw = readEnv(name);
    if (!raw) return fallback;
    const num = Number(raw);
    return Number.isFinite(num) ? num : fallback;
}

function readListEnv(name: string) {
    const raw = readEnv(name);
    if (!raw) return [];
    return raw
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
}

function readNumberListEnv(name: string, fallback: number[]) {
    const values = readListEnv(name);
    if (!values.length) return fallback;
    const parsed = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
    return parsed.length ? parsed : fallback;
}

const DEFAULT_SYNC_TASK_NUMBERS: number[] = [];

export function getPremiumSyncConfig() {
    return {
        preferBc: readBoolEnv("SYNC_PREFER_BC", true),
        bcModifiedGraceMs: readNumberEnv("SYNC_BC_MODIFIED_GRACE_MS", 2000),
        premiumModifiedGraceMs: readNumberEnv("SYNC_PREMIUM_MODIFIED_GRACE_MS", 2000),
        syncLockTimeoutMinutes: readNumberEnv("SYNC_LOCK_TIMEOUT_MINUTES", 30),
        taskConcurrency: Math.max(1, Math.floor(readNumberEnv("SYNC_TASK_CONCURRENCY", 6))),
        projectConcurrency: Math.max(1, Math.floor(readNumberEnv("SYNC_PROJECT_CONCURRENCY", 2))),
        deleteBehavior: (readEnv("PREMIUM_DELETE_BEHAVIOR") || "clearLink") as PremiumDeleteBehavior,
        maxProjectsPerRun: Math.max(0, Math.floor(readNumberEnv("SYNC_MAX_PROJECTS_PER_RUN", 0))),
        pollPageSize: Math.max(1, Math.floor(readNumberEnv("PREMIUM_POLL_PAGE_SIZE", 200))),
        pollMaxPages: Math.max(1, Math.floor(readNumberEnv("PREMIUM_POLL_MAX_PAGES", 10))),
        previewPageSize: Math.max(1, Math.floor(readNumberEnv("PREMIUM_PREVIEW_PAGE_SIZE", 50))),
        previewMaxPages: Math.max(1, Math.floor(readNumberEnv("PREMIUM_PREVIEW_MAX_PAGES", 1))),
        useScheduleApi: readBoolEnv("DATAVERSE_USE_SCHEDULE_API", true),
        requireScheduleApi: readBoolEnv("DATAVERSE_REQUIRE_SCHEDULE_API", true),
        plannerGroupId: (readEnv("PLANNER_GROUP_ID") || "").trim(),
        plannerGroupResourceIds: readListEnv("PLANNER_GROUP_RESOURCE_IDS"),
        allowedTaskNumbers: readNumberListEnv("SYNC_TASK_NO_ALLOWLIST", DEFAULT_SYNC_TASK_NUMBERS),
    };
}

export function getDataverseMappingConfig() {
    const taskProjectLookupField = readEnv("DATAVERSE_TASK_PROJECT_LOOKUP_FIELD") || "msdyn_project";
    const taskProjectIdField =
        readEnv("DATAVERSE_TASK_PROJECT_ID_FIELD") || `_${taskProjectLookupField}_value`;
    return {
        projectEntitySet: readEnv("DATAVERSE_PROJECT_ENTITY_SET") || "msdyn_projects",
        taskEntitySet: readEnv("DATAVERSE_TASK_ENTITY_SET") || "msdyn_projecttasks",
        projectIdField: readEnv("DATAVERSE_PROJECT_ID_FIELD") || "msdyn_projectid",
        taskIdField: readEnv("DATAVERSE_TASK_ID_FIELD") || "msdyn_projecttaskid",
        projectTitleField: readEnv("DATAVERSE_PROJECT_TITLE_FIELD") || "msdyn_subject",
        taskTitleField: readEnv("DATAVERSE_TASK_TITLE_FIELD") || "msdyn_subject",
        taskProjectLookupField,
        taskProjectIdField,
        projectBcNoField: readEnv("DATAVERSE_BC_PROJECT_NO_FIELD") || "",
        taskBcNoField: readEnv("DATAVERSE_BC_TASK_NO_FIELD") || "",
        taskStartField: readEnv("DATAVERSE_TASK_START_FIELD") || "msdyn_start",
        taskFinishField: readEnv("DATAVERSE_TASK_FINISH_FIELD") || "msdyn_finish",
        taskPercentField: readEnv("DATAVERSE_TASK_PERCENT_FIELD") || "msdyn_percentcomplete",
        taskDescriptionField: readEnv("DATAVERSE_TASK_DESCRIPTION_FIELD") || "",
        percentScale: readNumberEnv("DATAVERSE_PERCENT_SCALE", 1),
        percentMin: readNumberEnv("DATAVERSE_PERCENT_MIN", 0),
        percentMax: readNumberEnv("DATAVERSE_PERCENT_MAX", 100),
        allowTaskCreate: readBoolEnv("DATAVERSE_ALLOW_TASK_CREATE", true),
        allowTaskDelete: readBoolEnv("DATAVERSE_ALLOW_TASK_DELETE", false),
        taskModifiedField: readEnv("DATAVERSE_TASK_MODIFIED_FIELD") || "modifiedon",
    };
}
