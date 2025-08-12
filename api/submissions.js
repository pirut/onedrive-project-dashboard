import { listSubmissions } from "../lib/kv.js";

export default async function handler(req, res) {
  const origin = process.env.CORS_ORIGIN || "*";
  res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });

  const limit = parseInt(req.query.limit || "100", 10);
  const items = await listSubmissions(Math.max(1, Math.min(500, limit)));
  res.status(200).json({ ok: true, items });
}


