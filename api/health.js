export const config = { runtime: "nodejs18.x" };

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  // Optional API key enforcement
  const configuredKeys = (process.env.API_KEYS || process.env.API_KEY || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (configuredKeys.length > 0) {
    const headerKey = req.headers["x-api-key"]; // preferred
    const auth = req.headers["authorization"]; // optional: Bearer <key>
    const provided = headerKey || (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ") ? auth.slice(7) : null);
    if (!provided || !configuredKeys.includes(String(provided))) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
  }
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
