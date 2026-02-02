import { runPremiumSyncDecision } from "../../../../../lib/premium-sync";
import { logger } from "../../../../../lib/planner-sync/logger";

async function readJsonBody(request: Request) {
    try {
        return await request.json();
    } catch {
        return null;
    }
}

function parseBool(value: unknown) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

function parseNumber(value: unknown) {
    if (value == null || value === "") return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
}

async function handle(request: Request) {
    const url = new URL(request.url);
    const body = request.method === "POST" ? await readJsonBody(request) : null;
    const dryRunParam = parseBool(body?.dryRun ?? url.searchParams.get("dryRun"));
    const executeParam = parseBool(body?.execute ?? url.searchParams.get("execute"));
    const dryRun = request.method === "GET" || dryRunParam === true || executeParam === false;
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
        return new Response(JSON.stringify({ ok: true, dryRun, decision, result }, null, 2), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        });
    } catch (error) {
        logger.error("Auto sync decision failed", { error: (error as Error)?.message || String(error) });
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}

export async function GET(request: Request) {
    return handle(request);
}

export async function POST(request: Request) {
    return handle(request);
}
