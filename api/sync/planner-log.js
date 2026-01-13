import { listPlannerLog } from "../../lib/planner-sync/planner-log.js";

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key");
    if (req.method === "OPTIONS") {
        res.status(204).end();
        return;
    }
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const limit = Number(req.query?.limit || 200);
    const items = await listPlannerLog(Number.isNaN(limit) ? 200 : limit);
    res.status(200).json({ ok: true, items });
}
