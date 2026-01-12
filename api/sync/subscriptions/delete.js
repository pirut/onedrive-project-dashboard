import { GraphClient } from "../../../../lib/planner-sync/graph-client.js";
import { logger } from "../../../../lib/planner-sync/logger.js";

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
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        const body = await readJsonBody(req);
        const ids = Array.isArray(body?.ids) ? body.ids.map((id) => String(id).trim()).filter(Boolean) : [];
        const deleteAll = Boolean(body?.all);

        const graphClient = new GraphClient();
        const allSubs = await graphClient.listSubscriptions();
        const plannerSubs = allSubs.filter((sub) => (sub.resource || "").includes("/planner/"));
        const targetSubs = deleteAll ? plannerSubs : plannerSubs.filter((sub) => ids.includes(sub.id));

        const deleted = [];
        const failed = [];
        for (const sub of targetSubs) {
            try {
                await graphClient.deleteSubscription(sub.id);
                deleted.push(sub.id);
            } catch (error) {
                failed.push({ id: sub.id, error: error?.message || String(error) });
            }
        }

        res.status(failed.length ? 207 : 200).json({
            ok: failed.length === 0,
            deleted,
            failed,
            totalPlanner: plannerSubs.length,
        });
    } catch (error) {
        logger.error("POST /api/sync/subscriptions/delete - Failed", {
            error: error?.message || String(error),
        });
        res.status(500).json({ ok: false, error: error?.message || String(error) });
    }
}
