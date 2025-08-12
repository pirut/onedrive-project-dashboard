import { logSubmission } from "../lib/kv.js";

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    if (req.method === "OPTIONS") return res.status(204).end();
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    try {
        const body = req.body && typeof req.body === "object" ? req.body : await readJsonBody(req);
        if (body == null) {
            await logSubmission({ type: "json", status: "invalid", reason: "Body is not valid JSON" });
            return res.status(400).json({ error: "Body must be valid JSON" });
        }

    const logged = await logSubmission({ type: "json", status: "ok", payload: body });
    return res.status(200).json({ ok: true, logged, received: body });
    } catch (e) {
        await logSubmission({ type: "json", status: "error", error: e?.message || String(e) });
        return res.status(500).json({ error: e?.message || String(e) });
    }
}
