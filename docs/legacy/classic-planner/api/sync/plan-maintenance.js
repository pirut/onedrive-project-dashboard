import { syncPlannerPlanTitlesAndDedupe } from "../../lib/planner-sync/index.js";
import { logger } from "../../lib/planner-sync/logger.js";

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

export default async function handler(req, res) {
    if (req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const body = await readJsonBody(req);
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const dryRun = Boolean(body?.dryRun);

    try {
        const result = await syncPlannerPlanTitlesAndDedupe({
            projectNo: projectNo || undefined,
            dryRun,
        });
        res.status(200).json({ ok: true, result });
    } catch (error) {
        logger.error("Planner plan maintenance failed", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
