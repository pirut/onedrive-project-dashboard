import "isomorphic-fetch";
import crypto from "crypto";
import { ConfidentialClientApplication } from "@azure/msal-node";
import {
    getBuckets,
    setBuckets,
    getProjectKanbanState,
    setProjectKanbanState,
    getAllProjectKanbanStates,
    getProjectMetadata,
    setProjectMetadata,
    setMultipleProjectMetadata,
    getAllProjectMetadata,
    getLastUpdateTimestamp,
    updateLastUpdateTimestamp,
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

async function findFolderByName(accessToken, driveId, parentItemId, folderName) {
    const childrenPath = parentItemId === "root" 
        ? `/drives/${driveId}/root/children`
        : `/drives/${driveId}/items/${parentItemId}/children`;
    const children = await graphFetchAllPages(childrenPath, accessToken);
    const folder = (children.value || []).find(
        (it) => it.folder && (it.name || "").trim().toLowerCase() === String(folderName).trim().toLowerCase()
    );
    return folder || null;
}

async function ensureArchiveFolder(accessToken, driveId, libraryPath) {
    const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
    const [driveName, ...subPathParts] = libPathTrimmed.split("/");
    
    // Find the root or sub-path parent (where projects are stored)
    let parentId = "root";
    if (subPathParts.length > 0) {
        const subPath = encodeDrivePath(subPathParts.join("/"));
        const parentItem = await graphFetch(`/drives/${driveId}/root:/${subPath}`, accessToken);
        parentId = parentItem.id;
    }
    
    // Check if (ARCHIVE) folder exists at this level
    let archiveFolder = await findFolderByName(accessToken, driveId, parentId, "(ARCHIVE)");
    
    if (!archiveFolder) {
        // Create (ARCHIVE) folder if it doesn't exist
        const payload = {
            name: "(ARCHIVE)",
            folder: {},
            "@microsoft.graph.conflictBehavior": "fail",
        };
        const createPath = parentId === "root" 
            ? `/drives/${driveId}/root/children`
            : `/drives/${driveId}/items/${parentId}/children`;
        try {
            archiveFolder = await graphFetch(createPath, accessToken, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
        } catch (createError) {
            // If creation fails, try to find it again (might have been created concurrently)
            archiveFolder = await findFolderByName(accessToken, driveId, parentId, "(ARCHIVE)");
            if (!archiveFolder) {
                throw createError;
            }
        }
    }
    
    return archiveFolder.id;
}

async function moveFolderInSharePoint(accessToken, folderId, driveId, targetParentId, folderName) {
    const moveBody = {
        parentReference: { id: targetParentId },
        name: folderName,
    };
    
    const moved = await graphFetch(
        `/drives/${driveId}/items/${folderId}`,
        accessToken,
        {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(moveBody),
        }
    );
    
    return moved;
}

async function fetchAllFolders(accessToken, siteUrl, libraryPath) {
    try {
        console.log(`[fetchAllFolders] Starting fetch for ${siteUrl}/${libraryPath}`);
        const url = new URL(siteUrl);
        const host = url.host;
        const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
        const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
        
        console.log(`[fetchAllFolders] Resolving site: ${host}:/sites/${sitePath}`);
        const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
        const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
        const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
        const [driveName, ...subPathParts] = libPathTrimmed.split("/");
        const drive = (drives.value || []).find((d) => d.name === driveName);
        if (!drive) {
            const availableDrives = (drives.value || []).map(d => d.name).join(", ");
            throw new Error(`Library "${driveName}" not found. Available drives: ${availableDrives}`);
        }
        console.log(`[fetchAllFolders] Found drive: ${drive.name} (${drive.id})`);

        // Find the parent folder (root or sub-path)
        let parentId = "root";
        if (subPathParts.length > 0) {
            const subPath = encodeDrivePath(subPathParts.join("/"));
            console.log(`[fetchAllFolders] Resolving sub-path: ${subPath}`);
            const parentItem = await graphFetch(`/drives/${drive.id}/root:/${subPath}`, accessToken);
            parentId = parentItem.id;
            console.log(`[fetchAllFolders] Parent folder ID: ${parentId}`);
        }

        // Fetch folders from main location
        const mainChildrenPath = parentId === "root" 
            ? `/drives/${drive.id}/root/children`
            : `/drives/${drive.id}/items/${parentId}/children`;
        console.log(`[fetchAllFolders] Fetching main folders from: ${mainChildrenPath}`);
        const resItems = await graphFetchAllPages(mainChildrenPath, accessToken);
        console.log(`[fetchAllFolders] Found ${resItems.value?.length || 0} items in main location`);

        const allFolders = [];

        // Process main folders (exclude the (ARCHIVE) folder itself)
        const mainFolders = (resItems.value || [])
            .filter((it) => it.folder)
            .filter((it) => {
                const name = (it.name || "").trim().toLowerCase();
                // Exclude the (ARCHIVE) folder itself - we'll fetch its children separately
                return name !== "(archive)";
            })
            .map((it) => {
                return {
                    id: it.id,
                    name: it.name,
                    webUrl: it.webUrl || null,
                    createdDateTime: it.createdDateTime || null,
                    lastModifiedDateTime: it.lastModifiedDateTime || null,
                    size: it.size || 0,
                    driveId: it.parentReference?.driveId || null,
                    parentPath: it.parentReference?.path || null,
                    isArchived: false,
                };
            });

        console.log(`[fetchAllFolders] Found ${mainFolders.length} main folders (excluding archive)`);
        allFolders.push(...mainFolders);

        // Find and fetch folders inside (ARCHIVE) folder
        console.log(`[fetchAllFolders] Looking for (ARCHIVE) folder`);
        const archiveFolder = await findFolderByName(accessToken, drive.id, parentId, "(ARCHIVE)");
        if (archiveFolder) {
            console.log(`[fetchAllFolders] Found (ARCHIVE) folder: ${archiveFolder.id}`);
            const archiveChildrenPath = `/drives/${drive.id}/items/${archiveFolder.id}/children`;
            console.log(`[fetchAllFolders] Fetching folders inside archive: ${archiveChildrenPath}`);
            const archiveItems = await graphFetchAllPages(archiveChildrenPath, accessToken);
            console.log(`[fetchAllFolders] Found ${archiveItems.value?.length || 0} items in archive`);
            
            const archivedFolders = (archiveItems.value || [])
                .filter((it) => it.folder)
                .map((it) => {
                    return {
                        id: it.id,
                        name: it.name,
                        webUrl: it.webUrl || null,
                        createdDateTime: it.createdDateTime || null,
                        lastModifiedDateTime: it.lastModifiedDateTime || null,
                        size: it.size || 0,
                        driveId: it.parentReference?.driveId || null,
                        parentPath: it.parentReference?.path || null,
                        isArchived: true, // All folders inside (ARCHIVE) are archived
                    };
                });

            console.log(`[fetchAllFolders] Found ${archivedFolders.length} archived folders`);
            allFolders.push(...archivedFolders);
        } else {
            console.log(`[fetchAllFolders] No (ARCHIVE) folder found`);
        }

        console.log(`[fetchAllFolders] Total folders: ${allFolders.length}`);
        return allFolders;
    } catch (e) {
        console.error(`[fetchAllFolders] Error:`, e);
        throw new Error(`Failed to fetch folders: ${e.message}`);
    }
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
            const lastUpdate = await getLastUpdateTimestamp();

            console.log(`[Data] Loading data: ${Object.keys(metadata).length} projects, ${Object.keys(kanbanStates).length} states`);

            const archiveBucket = buckets.buckets.find((b) => b.id === "archive");
            const defaultBucket = buckets.buckets.find((b) => b.id === "todo") || buckets.buckets[0];
            
            const projects = Object.values(metadata).map((meta) => {
                const state = kanbanStates[meta.id] || {};
                // If folder is archived, always use archive bucket
                const bucketId = meta.isArchived 
                    ? (archiveBucket?.id || "archive")
                    : (state.bucketId || defaultBucket?.id);
                return {
                    id: meta.id,
                    name: meta.name,
                    webUrl: meta.webUrl,
                    createdDateTime: meta.createdDateTime,
                    lastModifiedDateTime: meta.lastModifiedDateTime,
                    isArchived: meta.isArchived || false,
                    bucketId,
                };
            });

            return res.status(200).json({ projects, buckets: buckets.buckets, lastUpdate });
        } catch (e) {
            console.error("[Data] Error:", e);
            return res.status(500).json({ error: e.message || String(e), stack: e.stack });
        }
    }

    // GET /api/projects-kanban/export
    if (req.method === "GET" && route === "export") {
        try {
            const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
            const format = (url.searchParams.get("format") || "csv").toLowerCase();

            const buckets = await getBuckets();
            const kanbanStates = await getAllProjectKanbanStates();
            const metadata = await getAllProjectMetadata();

            const bucketById = {};
            for (const b of buckets.buckets || []) {
                bucketById[b.id] = b;
            }

            // Active = not archived
            const activeProjects = Object.values(metadata)
                .filter((meta) => !meta.isArchived)
                .map((meta) => {
                    const state = kanbanStates[meta.id] || {};
                    const bucket = bucketById[state.bucketId] || null;
                    return {
                        id: meta.id,
                        name: meta.name,
                        bucketId: state.bucketId || null,
                        bucketName: bucket?.name || null,
                        webUrl: meta.webUrl || null,
                        createdDateTime: meta.createdDateTime || null,
                        lastModifiedDateTime: meta.lastModifiedDateTime || null,
                    };
                });

            if (format === "json") {
                res.setHeader("Content-Type", "application/json; charset=utf-8");
                res.setHeader("Content-Disposition", 'attachment; filename="active-projects.json"');
                return res.status(200).json({ projects: activeProjects });
            }

            // Default to CSV
            const headers = [
                "id",
                "name",
                "bucketId",
                "bucketName",
                "webUrl",
                "createdDateTime",
                "lastModifiedDateTime",
            ];

            const escapeCsv = (value) => {
                if (value === null || value === undefined) return "";
                const str = String(value);
                if (/[",\n]/.test(str)) {
                    return `"${str.replace(/"/g, '""')}"`;
                }
                return str;
            };

            const lines = [
                headers.join(","),
                ...activeProjects.map((p) =>
                    headers.map((h) => escapeCsv(p[h])).join(",")
                ),
            ];

            const csv = lines.join("\r\n");
            res.setHeader("Content-Type", "text/csv; charset=utf-8");
            res.setHeader("Content-Disposition", 'attachment; filename="active-projects.csv"');
            return res.status(200).send(csv);
        } catch (e) {
            console.error("[Export] Error:", e);
            return res.status(500).json({ error: e.message || String(e) });
        }
    }

    // GET /api/projects-kanban/timestamp - for real-time polling
    if (req.method === "GET" && route === "timestamp") {
        try {
            const timestamp = await getLastUpdateTimestamp();
            return res.status(200).json({ timestamp });
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

            const buckets = await getBuckets();
            const archiveBucket = buckets.buckets.find((b) => b.id === "archive");
            const metadata = await getProjectMetadata(projectId);
            
            if (!metadata) {
                return res.status(404).json({ error: "Project not found" });
            }

            // Check if project is archived - archived folders must stay in archive bucket
            if (metadata.isArchived) {
                if (bucketId !== archiveBucket?.id && bucketId !== "archive") {
                    return res.status(400).json({ error: "Archived folders must remain in the Archive bucket" });
                }
            }

            // Move folder in SharePoint if moving to/from archive bucket
            const isMovingToArchive = bucketId === archiveBucket?.id || bucketId === "archive";
            const wasArchived = metadata.isArchived;

            if (isMovingToArchive && !wasArchived) {
                // Moving TO archive - move folder to (ARCHIVE) folder in SharePoint
                try {
                    const accessToken = await getAppToken();
                    const siteUrl = req.query.siteUrl || DEFAULT_SITE_URL;
                    const libraryPath = req.query.libraryPath || DEFAULT_LIBRARY;
                    
                    const url = new URL(siteUrl);
                    const host = url.host;
                    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
                    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
                    const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
                    const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
                    const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
                    const [driveName] = libPathTrimmed.split("/");
                    const drive = (drives.value || []).find((d) => d.name === driveName);
                    if (!drive) throw new Error(`Library not found: ${driveName}`);

                    // Ensure archive folder exists and get its ID
                    const archiveFolderId = await ensureArchiveFolder(accessToken, drive.id, libraryPath);
                    
                    // Move the folder
                    await moveFolderInSharePoint(accessToken, projectId, metadata.driveId || drive.id, archiveFolderId, metadata.name);
                } catch (moveError) {
                    console.error("Failed to move folder to archive:", moveError);
                    return res.status(500).json({ error: `Failed to move folder to archive: ${moveError.message}` });
                }
            } else if (!isMovingToArchive && wasArchived) {
                // Moving FROM archive - move folder back to main folder
                try {
                    const accessToken = await getAppToken();
                    const siteUrl = req.query.siteUrl || DEFAULT_SITE_URL;
                    const libraryPath = req.query.libraryPath || DEFAULT_LIBRARY;
                    
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

                    // Find the main parent folder (root or sub-path)
                    let mainParentId = "root";
                    if (subPathParts.length > 0) {
                        const subPath = encodeDrivePath(subPathParts.join("/"));
                        const parentItem = await graphFetch(`/drives/${drive.id}/root:/${subPath}`, accessToken);
                        mainParentId = parentItem.id;
                    }
                    
                    // Move the folder back to main location
                    await moveFolderInSharePoint(accessToken, projectId, metadata.driveId || drive.id, mainParentId, metadata.name);
                } catch (moveError) {
                    console.error("Failed to move folder from archive:", moveError);
                    return res.status(500).json({ error: `Failed to move folder from archive: ${moveError.message}` });
                }
            }

            // Update kanban state
            const success = await setProjectKanbanState(projectId, { bucketId });
            if (!success) {
                return res.status(500).json({ error: "Failed to save state" });
            }

            // Update timestamp for real-time updates
            await updateLastUpdateTimestamp();

            // Trigger sync to refresh metadata
            const origin = `${req.headers["x-forwarded-proto"] || "https"}://${req.headers.host}`;
            fetch(`${origin}/api/projects-kanban/sync`, {
                method: "POST",
            }).catch(() => {});

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

            // Move folder to archive in SharePoint
            try {
                const accessToken = await getAppToken();
                const siteUrl = req.query.siteUrl || DEFAULT_SITE_URL;
                const libraryPath = req.query.libraryPath || DEFAULT_LIBRARY;
                
                const url = new URL(siteUrl);
                const host = url.host;
                const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
                const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
                const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
                const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
                const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
                const [driveName] = libPathTrimmed.split("/");
                const drive = (drives.value || []).find((d) => d.name === driveName);
                if (!drive) throw new Error(`Library not found: ${driveName}`);

                const metadata = await getProjectMetadata(projectId);
                if (!metadata) {
                    return res.status(404).json({ error: "Project not found" });
                }

                const archiveFolderId = await ensureArchiveFolder(accessToken, drive.id, libraryPath);
                await moveFolderInSharePoint(accessToken, projectId, metadata.driveId || drive.id, archiveFolderId, metadata.name);
            } catch (moveError) {
                console.error("Failed to move folder to archive:", moveError);
                return res.status(500).json({ error: `Failed to move folder to archive: ${moveError.message}` });
            }

            const success = await setProjectKanbanState(projectId, { bucketId: archiveBucket.id });
            if (!success) {
                return res.status(500).json({ error: "Failed to archive project" });
            }

            await updateLastUpdateTimestamp();

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

            // Move folder from archive back to main folder in SharePoint
            try {
                const accessToken = await getAppToken();
                const siteUrl = req.query.siteUrl || DEFAULT_SITE_URL;
                const libraryPath = req.query.libraryPath || DEFAULT_LIBRARY;
                
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

                const metadata = await getProjectMetadata(projectId);
                if (!metadata) {
                    return res.status(404).json({ error: "Project not found" });
                }

                // Find the main parent folder (root or sub-path)
                let mainParentId = "root";
                if (subPathParts.length > 0) {
                    const subPath = encodeDrivePath(subPathParts.join("/"));
                    const parentItem = await graphFetch(`/drives/${drive.id}/root:/${subPath}`, accessToken);
                    mainParentId = parentItem.id;
                }
                
                await moveFolderInSharePoint(accessToken, projectId, metadata.driveId || drive.id, mainParentId, metadata.name);
            } catch (moveError) {
                console.error("Failed to move folder from archive:", moveError);
                return res.status(500).json({ error: `Failed to move folder from archive: ${moveError.message}` });
            }

            const success = await setProjectKanbanState(projectId, { bucketId: defaultBucket.id });
            if (!success) {
                return res.status(500).json({ error: "Failed to unarchive project" });
            }

            await updateLastUpdateTimestamp();

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
                await updateLastUpdateTimestamp();
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

            console.log(`[Sync] Starting sync for ${siteUrl}/${libraryPath}`);
            const folders = await fetchAllFolders(accessToken, siteUrl, libraryPath);
            console.log(`[Sync] Fetched ${folders.length} folders`);

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
            console.log(`[Sync] Saved ${Object.keys(metadataMap).length} metadata entries`);

            const buckets = await getBuckets();
            const archiveBucket = buckets.buckets.find((b) => b.id === "archive");
            const defaultBucket = buckets.buckets.find((b) => b.id === "todo") || buckets.buckets[0];
            const existingStates = await getAllProjectKanbanStates();

            let newStates = 0;
            let updatedStates = 0;
            for (const folderId of Object.keys(metadataMap)) {
                const metadata = metadataMap[folderId];
                // If folder is archived, assign to archive bucket
                // Otherwise, use existing state or default to "todo"
                if (!existingStates[folderId]) {
                    const targetBucketId = metadata.isArchived 
                        ? (archiveBucket?.id || "archive")
                        : (defaultBucket?.id);
                    if (targetBucketId) {
                        await setProjectKanbanState(folderId, { bucketId: targetBucketId });
                        newStates++;
                    }
                } else if (metadata.isArchived && existingStates[folderId].bucketId !== archiveBucket?.id) {
                    // Update existing archived folders to archive bucket
                    await setProjectKanbanState(folderId, { bucketId: archiveBucket?.id || "archive" });
                    updatedStates++;
                }
            }
            console.log(`[Sync] Created ${newStates} new states, updated ${updatedStates} states`);

            // Update timestamp for real-time updates
            await updateLastUpdateTimestamp();

            return res.status(200).json({ ok: true, synced: folders.length, newStates, updatedStates });
        } catch (e) {
            console.error("[Sync] Error:", e);
            return res.status(500).json({ error: e.message || String(e), stack: e.stack });
        }
    }

    return res.status(404).json({ error: "Not found" });
}

