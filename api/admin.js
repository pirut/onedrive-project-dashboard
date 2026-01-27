import "isomorphic-fetch";
import crypto from "crypto";
import { kvDiagnostics, listSubmissions } from "../lib/kv.js";
import { renderDashboardPage, renderLoginPage } from "../lib/admin-dashboard/render.js";

const ADMIN_USERNAME = process.env.ADMIN_USERNAME || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "";
const ADMIN_SESSION_SECRET = process.env.ADMIN_SESSION_SECRET || "";

function signSession(data) {
    if (!ADMIN_SESSION_SECRET) return null;
    const h = crypto.createHmac("sha256", ADMIN_SESSION_SECRET);
    h.update(data);
    return h.digest("hex");
}

function safeEqual(a = "", b = "") {
    const ab = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    if (ab.length !== bb.length) return false;
    return crypto.timingSafeEqual(ab, bb);
}

function parseCookies(req) {
    const hdr = req.headers["cookie"];
    const out = {};
    if (!hdr) return out;
    String(hdr)
        .split(";")
        .map((s) => s.trim())
        .forEach((pair) => {
            const eq = pair.indexOf("=");
            if (eq === -1) return;
            const k = decodeURIComponent(pair.slice(0, eq).trim());
            const v = decodeURIComponent(pair.slice(eq + 1).trim());
            out[k] = v;
        });
    return out;
}

async function readFormBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    const params = new URLSearchParams(text);
    const obj = {};
    for (const [k, v] of params) obj[k] = v;
    return obj;
}

function sessionCookie(username) {
    const ts = Date.now().toString();
    const raw = `${username}|${ts}`;
    const sig = signSession(raw);
    if (!sig) return null;
    const value = Buffer.from(`${raw}|${sig}`).toString("base64url");
    const maxAge = 7 * 24 * 60 * 60;
    const secure = process.env.NODE_ENV === "production";
    return [
        `admin_session=${value}`,
        "Path=/",
        "HttpOnly",
        "SameSite=Lax",
        `Max-Age=${maxAge}`,
        secure ? "Secure" : null,
    ]
        .filter(Boolean)
        .join("; ");
}

function verifySession(cookieVal) {
    try {
        if (!cookieVal) return false;
        const decoded = Buffer.from(cookieVal, "base64url").toString("utf8");
        const parts = decoded.split("|");
        if (parts.length !== 3) return false;
        const [u, ts, sig] = parts;
        const raw = `${u}|${ts}`;
        const expect = signSession(raw);
        if (!expect) return false;
        if (!safeEqual(sig, expect)) return false;
        const ageMs = Date.now() - Number(ts);
        if (!Number.isFinite(ageMs) || ageMs > 7 * 24 * 60 * 60 * 1000) return false;
        return { username: u };
    } catch {
        return false;
    }
}

async function ping(url, opts = {}) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), opts.timeoutMs || 1500);
    try {
        const res = await fetch(url, { signal: controller.signal, headers: { "cache-control": "no-cache" } });
        clearTimeout(t);
        return { ok: res.ok, status: res.status };
    } catch (e) {
        clearTimeout(t);
        return { ok: false, error: e?.message || String(e) };
    }
}

function envPresence(name) {
    return Boolean(process.env[name]);
}

async function buildDashboardData(req) {
    const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
    const notificationUrlDefault =
        process.env.GRAPH_NOTIFICATION_URL || process.env.PLANNER_NOTIFICATION_URL || `${origin}/api/webhooks/graph/planner`;
    const bcNotificationUrlDefault = process.env.BC_WEBHOOK_NOTIFICATION_URL || `${origin}/api/webhooks/bc`;

    const endpoints = [
        { name: "/api/health", url: `${origin}/api/health`, desc: "Base health" },
        { name: "/api/submissions", url: `${origin}/api/submissions?limit=5`, desc: "Recent logs (KV)" },
        { name: "/api/kv-diag", url: `${origin}/api/kv-diag`, desc: "KV diagnostics" },
    ];
    const checks = await Promise.all(endpoints.map(async (e) => ({ ...e, result: await ping(e.url) })));

    const graphEnvOk = ["TENANT_ID", "MSAL_CLIENT_ID", "MSAL_CLIENT_SECRET", "DEFAULT_SITE_URL", "DEFAULT_LIBRARY"].every(envPresence);
    const plannerEnvRequired = [
        "BC_TENANT_ID",
        "BC_ENVIRONMENT",
        "BC_COMPANY_ID",
        "BC_CLIENT_ID",
        "BC_CLIENT_SECRET",
        "GRAPH_TENANT_ID",
        "GRAPH_CLIENT_ID",
        "GRAPH_CLIENT_SECRET",
        "GRAPH_SUBSCRIPTION_CLIENT_STATE",
        "PLANNER_GROUP_ID",
        "SYNC_MODE",
    ];
    const plannerEnvMissing = plannerEnvRequired.filter((name) => !envPresence(name));
    const plannerEnvOk = plannerEnvMissing.length === 0;
    const kvDiag = await kvDiagnostics();
    const submissions = await listSubmissions(500);

    return {
        origin,
        notificationUrlDefault,
        bcNotificationUrlDefault,
        graphEnvOk,
        plannerEnvOk,
        plannerEnvMissing,
        kvDiag,
        submissions,
        checks,
        adminConfigured: Boolean(ADMIN_PASSWORD && ADMIN_SESSION_SECRET),
        adminUsername: ADMIN_USERNAME,
    };
}

export default async function handler(req, res) {
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");

    const cookies = parseCookies(req);
    const sess = verifySession(cookies["admin_session"]);
    const adminConfigured = Boolean(ADMIN_PASSWORD && ADMIN_SESSION_SECRET);

    if (req.method === "GET") {
        if (sess) {
            const data = await buildDashboardData(req);
            res.status(200).send(renderDashboardPage(data));
            return;
        }
        res.status(200).send(renderLoginPage({ adminUsername: ADMIN_USERNAME, adminConfigured, message: null }));
        return;
    }

    if (req.method === "POST") {
        const form = await readFormBody(req);
        const u = String(form.username || "").trim();
        const p = String(form.password || "").trim();
        const userOk = u === ADMIN_USERNAME;
        const passOk = safeEqual(p, ADMIN_PASSWORD);

        if (!userOk || !passOk || !ADMIN_SESSION_SECRET) {
            res.status(200).send(
                renderLoginPage({
                    adminUsername: ADMIN_USERNAME,
                    adminConfigured,
                    message: "Invalid credentials or admin secret not configured",
                })
            );
            return;
        }

        const cookie = sessionCookie(u);
        if (!cookie) {
            res.status(200).send(
                renderLoginPage({
                    adminUsername: ADMIN_USERNAME,
                    adminConfigured,
                    message: "Admin session secret not configured",
                })
            );
            return;
        }

        res.setHeader("Set-Cookie", cookie);
        const data = await buildDashboardData(req);
        res.status(200).send(renderDashboardPage(data));
        return;
    }

    res.status(405).send(renderLoginPage({ adminUsername: ADMIN_USERNAME, adminConfigured, message: "Method not allowed" }));
}
