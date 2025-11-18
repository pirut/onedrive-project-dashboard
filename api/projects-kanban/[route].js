import "isomorphic-fetch";
import crypto from "crypto";
import { ConfidentialClientApplication } from "@azure/msal-node";
import {
    getBuckets,
    setBuckets,
    getProjectKanbanState,
    setProjectKanbanState,
    getAllProjectKanbanStates,
    setProjectMetadata,
    setMultipleProjectMetadata,
    getAllProjectMetadata,
} from "../../lib/projects-kv.js";

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

// Admin auth configuration
const ADMIN_SESSION_SECRET = process.env.ADMIN_SESSION_SECRET || "";

// SharePoint configuration
const TENANT_ID = readEnv("TENANT_ID", true);
const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
const MSAL_CLIENT_SECRET = readEnv("MSAL_CLIENT_SECRET", true);
const MS_GRAPH_SCOPE = readEnv("MS_GRAPH_SCOPE") || "https://graph.microsoft.com/.default";
const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);
const DEFAULT_LIBRARY = readEnv("DEFAULT_LIBRARY", true);

const msalApp = new ConfidentialClientApplication({
    auth: {
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientId: MSAL_CLIENT_ID,
        clientSecret: MSAL_CLIENT_SECRET,
    },
});

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

async function readJsonBody(req) {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const text = Buffer.concat(chunks).toString("utf8");
    try {
        return JSON.parse(text);
    } catch {
        return {};
    }
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

function requireAuth(req) {
    const cookies = parseCookies(req);
    const session = verifySession(cookies.admin_session);
    if (!session) {
        return { error: "Unauthorized", status: 401 };
    }
    return { session };
}

async function getAppToken() {
    const result = await msalApp.acquireTokenByClientCredential({ scopes: [MS_GRAPH_SCOPE] });
    return result.accessToken;
}

async function graphFetch(path, accessToken, options = {}) {
    const res = await fetch(`https://graph.microsoft.com/v1.0${path}`, {
        method: options.method || "GET",
        headers: { Authorization: `Bearer ${accessToken}`, ...(options.headers || {}) },
        body: options.body,
    });
    if (!res.ok) {
        const text = await res.text();
        throw new Error(`Graph ${options.method || "GET"} ${path} -> ${res.status}: ${text}`);
    }
    const ct = res.headers.get("content-type") || "";
    if (ct.includes("application/json")) return res.json();
    return res.arrayBuffer();
}

async function graphFetchAllPages(path, accessToken) {
    const allItems = [];
    let nextLink = null;
    let currentPath = path;

    do {
        const res = await fetch(`https://graph.microsoft.com/v1.0${currentPath}`, {
            headers: { Authorization: `Bearer ${accessToken}` },
        });

        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Graph GET ${currentPath} -> ${res.status}: ${text}`);
        }

        const data = await res.json();
        if (data.value && Array.isArray(data.value)) {
            allItems.push(...data.value);
        }
        nextLink = data["@odata.nextLink"];
        if (nextLink) {
            currentPath = nextLink.replace("https://graph.microsoft.com/v1.0", "");
        }
    } while (nextLink);

    return { value: allItems };
}

function encodeDrivePath(path) {
    const trimmed = String(path || "").replace(/^\/+|\/+$/g, "");
    if (!trimmed) return "";
    return trimmed
        .split("/")
        .map((seg) => encodeURIComponent(seg))
        .join("/");
}

async function fetchAllFolders(accessToken, siteUrl, libraryPath) {
    const url = new URL(siteUrl);
    const host = url.host;
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
    const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
    const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
    const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
    const [driveName, ...subPathParts] = libPathTrimmed.split("/");
    const drive = (drives.value || []).find((d) => d.name === driveName);
    if (!drive) throw new Error(`Library not found: ${driveName}`);

    let resItems;
    if (subPathParts.length === 0) {
        resItems = await graphFetchAllPages(`/drives/${drive.id}/root/children`, accessToken);
    } else {
        const subPath = encodeDrivePath(subPathParts.join("/"));
        resItems = await graphFetchAllPages(`/drives/${drive.id}/root:/${subPath}:/children`, accessToken);
    }

    const foldersOnly = (resItems.value || [])
        .filter((it) => it.folder)
        .map((it) => {
            const name = (it.name || "").trim().toLowerCase();
            const parentPath = it.parentReference?.path || "";
            const isArchived =
                name.includes("(archive)") ||
                name.includes("archive") ||
                parentPath.toLowerCase().includes("(archive)") ||
                parentPath.toLowerCase().includes("/archive");

            return {
                id: it.id,
                name: it.name,
                webUrl: it.webUrl || null,
                createdDateTime: it.createdDateTime || null,
                lastModifiedDateTime: it.lastModifiedDateTime || null,
                size: it.size || 0,
                driveId: it.parentReference?.driveId || null,
                parentPath: it.parentReference?.path || null,
                isArchived,
            };
        });

    return foldersOnly;
}

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (req.method === "OPTIONS") return res.status(204).end();

    const auth = requireAuth(req);
    if (auth.error) {
        return res.status(auth.status).json({ error: auth.error });
    }

    // Extract route from URL path
    // For /api/projects-kanban/data, the route should be "data"
    const path = req.url.split("?")[0];
    const pathParts = path.split("/").filter(Boolean);
    const kanbanIndex = pathParts.indexOf("projects-kanban");
    const route = kanbanIndex >= 0 && kanbanIndex < pathParts.length - 1 ? pathParts[kanbanIndex + 1] : "";

    // GET /api/projects-kanban/data
    if (req.method === "GET" && route === "data") {
        try {
            const buckets = await getBuckets();
            const kanbanStates = await getAllProjectKanbanStates();
            const metadata = await getAllProjectMetadata();

            const projects = Object.values(metadata).map((meta) => {
                const state = kanbanStates[meta.id] || {};
                const defaultBucket = buckets.buckets.find((b) => b.id === "todo") || buckets.buckets[0];
                return {
                    id: meta.id,
                    name: meta.name,
                    webUrl: meta.webUrl,
                    createdDateTime: meta.createdDateTime,
                    lastModifiedDateTime: meta.lastModifiedDateTime,
                    isArchived: meta.isArchived || false,
                    bucketId: state.bucketId || defaultBucket?.id,
                };
            });

            return res.status(200).json({ projects, buckets: buckets.buckets });
        } catch (e) {
            return res.status(500).json({ error: e.message || String(e) });
        }
    }

    // POST /api/projects-kanban/move
    if (req.method === "POST" && route === "move") {
        try {
            const body = await readJsonBody(req);
            const { projectId, bucketId } = body;
            if (!projectId || !bucketId) {
                return res.status(400).json({ error: "projectId and bucketId required" });
            }

            const success = await setProjectKanbanState(projectId, { bucketId });
            if (!success) {
                return res.status(500).json({ error: "Failed to save state" });
            }

            const buckets = await getBuckets();
            const archiveBucket = buckets.buckets.find((b) => b.id === "archive");
            if (archiveBucket && (bucketId === "archive" || bucketId === archiveBucket.id)) {
                const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
                fetch(`${origin}/api/projects-kanban/sync`, {
                    method: "POST",
                }).catch(() => {});
            }

            return res.status(200).json({ ok: true });
        } catch (e) {
            return res.status(500).json({ error: e.message || String(e) });
        }
    }

    // POST /api/projects-kanban/archive
    if (req.method === "POST" && route === "archive") {
        try {
            const body = await readJsonBody(req);
            const { projectId } = body;
            if (!projectId) {
                return res.status(400).json({ error: "projectId required" });
            }

            const buckets = await getBuckets();
            const archiveBucket = buckets.buckets.find((b) => b.id === "archive");
            if (!archiveBucket) {
                return res.status(400).json({ error: "Archive bucket not found" });
            }

            const success = await setProjectKanbanState(projectId, { bucketId: archiveBucket.id });
            if (!success) {
                return res.status(500).json({ error: "Failed to archive project" });
            }

            const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
            fetch(`${origin}/api/projects-kanban/sync`, {
                method: "POST",
            }).catch(() => {});

            return res.status(200).json({ ok: true });
        } catch (e) {
            return res.status(500).json({ error: e.message || String(e) });
        }
    }

    // POST /api/projects-kanban/unarchive
    if (req.method === "POST" && route === "unarchive") {
        try {
            const body = await readJsonBody(req);
            const { projectId } = body;
            if (!projectId) {
                return res.status(400).json({ error: "projectId required" });
            }

            const buckets = await getBuckets();
            const defaultBucket = buckets.buckets.find((b) => b.id === "todo") || buckets.buckets[0];
            if (!defaultBucket) {
                return res.status(400).json({ error: "Default bucket not found" });
            }

            const success = await setProjectKanbanState(projectId, { bucketId: defaultBucket.id });
            if (!success) {
                return res.status(500).json({ error: "Failed to unarchive project" });
            }

            const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
            fetch(`${origin}/api/projects-kanban/sync`, {
                method: "POST",
            }).catch(() => {});

            return res.status(200).json({ ok: true });
        } catch (e) {
            return res.status(500).json({ error: e.message || String(e) });
        }
    }

    // GET/POST /api/projects-kanban/buckets
    if (route === "buckets") {
        if (req.method === "GET") {
            try {
                const buckets = await getBuckets();
                return res.status(200).json(buckets);
            } catch (e) {
                return res.status(500).json({ error: e.message || String(e) });
            }
        } else if (req.method === "POST") {
            try {
                const body = await readJsonBody(req);
                const success = await setBuckets(body);
                if (!success) {
                    return res.status(500).json({ error: "Failed to save buckets" });
                }
                return res.status(200).json({ ok: true });
            } catch (e) {
                return res.status(500).json({ error: e.message || String(e) });
            }
        }
    }

    // POST /api/projects-kanban/sync
    if (req.method === "POST" && route === "sync") {
        try {
            const accessToken = await getAppToken();
            const siteUrl = req.query.siteUrl || DEFAULT_SITE_URL;
            const libraryPath = req.query.libraryPath || DEFAULT_LIBRARY;

            const folders = await fetchAllFolders(accessToken, siteUrl, libraryPath);

            const metadataMap = {};
            folders.forEach((folder) => {
                metadataMap[folder.id] = {
                    id: folder.id,
                    name: folder.name,
                    webUrl: folder.webUrl,
                    createdDateTime: folder.createdDateTime,
                    lastModifiedDateTime: folder.lastModifiedDateTime,
                    size: folder.size,
                    driveId: folder.driveId,
                    parentPath: folder.parentPath,
                    isArchived: folder.isArchived,
                };
            });

            await setMultipleProjectMetadata(metadataMap);

            const buckets = await getBuckets();
            const defaultBucket = buckets.buckets.find((b) => b.id === "todo") || buckets.buckets[0];
            const existingStates = await getAllProjectKanbanStates();

            for (const folderId of Object.keys(metadataMap)) {
                if (!existingStates[folderId] && defaultBucket) {
                    await setProjectKanbanState(folderId, { bucketId: defaultBucket.id });
                }
            }

            return res.status(200).json({ ok: true, synced: folders.length });
        } catch (e) {
            return res.status(500).json({ error: e.message || String(e) });
        }
    }

    return res.status(404).json({ error: "Not found" });
}

