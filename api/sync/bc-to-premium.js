import { syncBcToPremium, syncPremiumChanges } from "../../lib/premium-sync/index.js";
import { logger } from "../../lib/planner-sync/logger.js";
import { getCronSecret, isCronAuthorized } from "../../lib/planner-sync/cron-auth.js";

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

    const expected = getCronSecret();
    const provided = req.headers["x-cron-secret"] || req.query?.cronSecret || req.query?.cronsecret || "";
    if (expected && req.method === "GET" && !isCronAuthorized(String(provided || ""))) {
        res.status(401).json({ ok: false, error: "Unauthorized" });
        return;
    }
    const body = req.method === "POST" ? await readJsonBody(req) : null;
    const projectNo = body?.projectNo ? String(body.projectNo).trim() : "";
    const projectNos = Array.isArray(body?.projectNos) ? body.projectNos.map((value) => String(value).trim()).filter(Boolean) : [];
    const includePremiumChanges = body?.includePremiumChanges !== false;

    try {
        const bcResult = await syncBcToPremium(projectNo || undefined, {
            requestId: body?.requestId ? String(body.requestId) : undefined,
            projectNos: projectNos.length ? projectNos : undefined,
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
