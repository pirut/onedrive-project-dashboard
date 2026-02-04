import "../../lib/planner-sync/bootstrap.js";
import { BusinessCentralClient } from "../../lib/planner-sync/bc-client.js";
import { getBcAuditCursor, saveBcAuditCursor } from "../../lib/planner-sync/bc-audit-store.js";
import { syncBcToPremium } from "../../lib/premium-sync/index.js";
import { logger } from "../../lib/planner-sync/logger.js";

const AUDIT_SCOPE = "premium";
const DEFAULT_BATCH_SIZE = Math.max(1, Math.floor(Number(process.env.BC_AUDIT_BATCH_SIZE || 100)));

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

function escapeODataString(value) {
    return String(value || "").replace(/'/g, "''");
}

async function listAuditBatch(bcClient, lastProjectNo, batchSize) {
    const filter = lastProjectNo ? `projectNo gt '${escapeODataString(lastProjectNo)}'` : undefined;
    const projects = await bcClient.listProjectsPaged({
        filter,
        orderBy: "projectNo asc",
        top: batchSize,
        select: ["projectNo"],
    });
    return projects;
}

export default async function handler(req, res) {
    if (req.method !== "GET" && req.method !== "POST") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host || "localhost"}`;
    const url = new URL(req.url || "", origin);
    const body = req.method === "POST" ? await readJsonBody(req) : null;

    const batchSize = Math.max(
        1,
        Math.floor(parseNumber(body?.batchSize ?? url.searchParams.get("batchSize")) ?? DEFAULT_BATCH_SIZE)
    );
    const reset = parseBool(body?.reset ?? url.searchParams.get("reset")) === true;
    const dryRun = parseBool(body?.dryRun ?? url.searchParams.get("dryRun")) === true || req.method === "GET";
    const requestId = body?.requestId ? String(body.requestId) : url.searchParams.get("requestId") || undefined;

    const bcClient = new BusinessCentralClient();
    let cursor = await getBcAuditCursor(AUDIT_SCOPE);
    const cursorBefore = { ...cursor };
    let cycle = cursor.cycle || 0;
    if (reset) {
        cycle += 1;
        cursor = { ...cursor, lastProjectNo: null };
    }

    let cycleReset = false;
    let projects = await listAuditBatch(bcClient, cursor.lastProjectNo || null, batchSize);
    if (!projects.length && cursor.lastProjectNo) {
        cycle += 1;
        cycleReset = true;
        cursor = { ...cursor, lastProjectNo: null };
        projects = await listAuditBatch(bcClient, null, batchSize);
    }

    const projectNos = projects
        .map((project) => (project.projectNo || "").trim())
        .filter(Boolean);
    const lastProjectNo = projectNos.length ? projectNos[projectNos.length - 1] : null;

    let syncResult = null;
    if (!dryRun && projectNos.length) {
        try {
            syncResult = await syncBcToPremium(undefined, {
                requestId,
                projectNos,
                preferPlanner: false,
            });
        } catch (error) {
            logger.error("BC audit sync failed", { requestId, error: error?.message || String(error) });
            res.status(500).json({ ok: false, error: error?.message || String(error) });
            return;
        }
    }

    await saveBcAuditCursor(AUDIT_SCOPE, { lastProjectNo, cycle });

    res.status(200).json({
        ok: true,
        dryRun,
        batchSize,
        cycleReset,
        cursorBefore,
        cursorAfter: { lastProjectNo, cycle },
        projects: projectNos.length,
        projectNos,
        result: syncResult,
    });
}
