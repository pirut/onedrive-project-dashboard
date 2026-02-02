import "../../lib/planner-sync/bootstrap.js";
import { syncPremiumChanges } from "../../lib/premium-sync/index.js";
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
    if (req.method !== "POST" && req.method !== "GET") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const body = req.method === "POST" ? await readJsonBody(req) : null;
    const requestId = body?.requestId ? String(body.requestId) : undefined;

    try {
        const result = await syncPremiumChanges({ requestId });
        res.status(200).json({ ok: true, result });
    } catch (error) {
        logger.error("Premium to BC sync failed", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
