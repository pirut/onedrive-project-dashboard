import { Redis } from "@upstash/redis";
import { getKvInfo } from "../lib/kv.js";

function createRedis() {
    const url = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || process.env.KV_URL || process.env.REDIS_URL;
    const token = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_READ_ONLY_TOKEN;
    if (!url || !token) return null;
    return new Redis({ url, token });
}

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

    const info = getKvInfo();
    const redis = createRedis();
    if (!redis) return res.status(200).json({ ok: false, info, error: "KV not configured in this environment" });

    const body = req.body && typeof req.body === "object" ? req.body : await readJsonBody(req);
    const payload = body && typeof body === "object" ? body : { note: "no body provided" };

    try {
        const record = { type: "debug", ts: new Date().toISOString(), payload };
        const push = await redis.lpush("submissions", JSON.stringify(record));
        const len = await redis.llen("submissions");
        const last3 = await redis.lrange("submissions", 0, 2);
        return res.status(200).json({
            ok: true,
            info,
            push,
            len,
            last3: last3.map((s) => {
                try {
                    return JSON.parse(s);
                } catch {
                    return s;
                }
            }),
        });
    } catch (e) {
        return res.status(200).json({ ok: false, info, error: e?.message || String(e) });
    }
}
