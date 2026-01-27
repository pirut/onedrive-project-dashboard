import { getBcWebhookEmitter, listBcWebhookLog } from "../../lib/planner-sync/bc-webhook-log.js";

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

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders?.();

    const emitter = getBcWebhookEmitter();
    const include = String(req.query?.include || "").trim() === "1";
    if (include) {
        const items = await listBcWebhookLog(20);
        for (const entry of items.reverse()) {
            res.write(`data: ${JSON.stringify(entry)}\n\n`);
        }
    }

    const send = (entry) => {
        res.write(`data: ${JSON.stringify(entry)}\n\n`);
    };
    emitter.on("entry", send);

    const keepAlive = setInterval(() => {
        res.write(": ping\n\n");
    }, 25000);

    req.on("close", () => {
        clearInterval(keepAlive);
        emitter.off("entry", send);
    });
}
