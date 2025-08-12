export const config = { runtime: 'nodejs18.x' };

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") {
        res.status(204).end();
        return;
    }
    if (req.method !== "GET") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }
    res.status(200).json({ ok: true });
}
