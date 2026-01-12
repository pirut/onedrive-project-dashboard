import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import { logger } from "../../lib/planner-sync/logger.js";

const BC_MODIFIED_FIELDS = [
    "systemModifiedAt",
    "lastModifiedDateTime",
    "lastModifiedAt",
    "modifiedAt",
    "modifiedOn",
    "lastModifiedOn",
    "systemModifiedOn",
];

function parseNumber(value, fallback) {
    const num = Number(value);
    if (!Number.isFinite(num)) return fallback;
    return num;
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const limit = Math.max(1, Math.min(200, parseNumber(url.searchParams.get("limit"), 25)));
    const includeAll = url.searchParams.get("all") === "1";

    const bcClient = new BusinessCentralClient();
    try {
        const tasks = await bcClient.listProjectTasks(includeAll ? undefined : "plannerTaskId ne ''");
        const sample = tasks.slice(0, limit);

        const foundFields = BC_MODIFIED_FIELDS.filter((field) => sample.some((task) => task?.[field] != null));
        const sampleItems = sample.map((task) => {
            const modifiedValues = {};
            for (const field of BC_MODIFIED_FIELDS) {
                if (task?.[field] != null) {
                    modifiedValues[field] = task[field];
                }
            }
            return {
                projectNo: task.projectNo,
                taskNo: task.taskNo,
                systemId: task.systemId,
                lastSyncAt: task.lastSyncAt,
                modified: modifiedValues,
            };
        });

        res.status(200).json({
            ok: true,
            filter: includeAll ? "all tasks" : "plannerTaskId ne ''",
            total: tasks.length,
            sampleCount: sample.length,
            foundFields,
            sample: sampleItems,
        });
    } catch (error) {
        logger.error("BC timestamp debug failed", { error: error?.message || String(error) });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
