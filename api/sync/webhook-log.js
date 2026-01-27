import { listPremiumWebhookLog } from "../../lib/premium-sync/index.js";

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

    const limit = Number(req.query?.limit || 50);
    const items = await listPremiumWebhookLog(Number.isNaN(limit) ? 50 : limit);
    res.status(200).json({ ok: true, items });
}
