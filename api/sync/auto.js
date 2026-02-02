import "../../lib/planner-sync/bootstrap.js";
import { runPremiumSyncDecision } from "../../lib/premium-sync/index.js";
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

function parseBool(value) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

function parseNumber(value) {
    if (value == null || value === "") return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
}

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const body = req.method === "POST" ? await readJsonBody(req) : null;

    const dryRunParam = parseBool(body?.dryRun ?? url.searchParams.get("dryRun"));
    const executeParam = parseBool(body?.execute ?? url.searchParams.get("execute"));
    const dryRun = req.method === "GET" || dryRunParam === true || executeParam === false;

    const preferBc = parseBool(body?.preferBc ?? url.searchParams.get("preferBc"));
    const graceMs = parseNumber(body?.graceMs ?? url.searchParams.get("graceMs"));
    const requestId = body?.requestId ? String(body.requestId) : url.searchParams.get("requestId") || undefined;

    try {
        const { decision, result } = await runPremiumSyncDecision({
            requestId,
            dryRun,
            preferBc: preferBc == null ? undefined : preferBc,
            graceMs: graceMs == null ? undefined : graceMs,
        });
        res.status(200).json({ ok: true, dryRun, decision, result });
    } catch (error) {
        logger.error("Auto sync decision failed", { error: error?.message || String(error) });
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
