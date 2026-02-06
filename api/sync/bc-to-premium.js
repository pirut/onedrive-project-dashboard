import "../../lib/planner-sync/bootstrap.js";
import { syncBcToPremium, syncPremiumChanges } from "../../lib/premium-sync/index.js";
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
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const projectNos = Array.isArray(body?.projectNos) ? body.projectNos.map((value) => String(value).trim()).filter(Boolean) : [];
    const includePremiumChanges = body?.includePremiumChanges === true;

    try {
        const bcResult = await syncBcToPremium(projectNo || undefined, {
            requestId: body?.requestId ? String(body.requestId) : undefined,
            projectNos: projectNos.length ? projectNos : undefined,
            preferPlanner: false,
        });
        let premiumResult = null;
        if (includePremiumChanges) {
            premiumResult = await syncPremiumChanges({ requestId: body?.requestId ? String(body.requestId) : undefined });
        }
        res.status(200).json({ ok: true, result: { bcToPremium: bcResult, premiumToBc: premiumResult } });
    } catch (error) {
        logger.error("BC to Premium sync failed", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
