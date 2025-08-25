import "isomorphic-fetch";
import { ConfidentialClientApplication } from "@azure/msal-node";
import { logSubmission } from "../lib/kv.js";

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

const TENANT_ID = readEnv("TENANT_ID", true);
const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
const MSAL_CLIENT_SECRET = readEnv("MSAL_CLIENT_SECRET", true);
const MS_GRAPH_SCOPE = readEnv("MS_GRAPH_SCOPE") || "https://graph.microsoft.com/.default";
const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);
const DEFAULT_LIBRARY = readEnv("DEFAULT_LIBRARY", true);

// FastField configuration
const FASTFIELD_API_URL = readEnv("FASTFIELD_API_URL", true);
const FASTFIELD_AUTH_HEADER = readEnv("FASTFIELD_AUTH_HEADER", true); // Basic Auth header
const FASTFIELD_TABLE_ID = readEnv("FASTFIELD_TABLE_ID", true); // We need the table ID, not name
const FASTFIELD_TABLE_NAME = readEnv("FASTFIELD_TABLE_NAME") || "Cornerstone Active Projects";

const msalApp = new ConfidentialClientApplication({
    auth: {
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientId: MSAL_CLIENT_ID,
        clientSecret: MSAL_CLIENT_SECRET,
    },
});

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

function encodeDrivePath(path) {
    const trimmed = String(path || "").replace(/^\/+|\/+$/g, "");
    if (!trimmed) return "";
    return trimmed
        .split("/")
        .map((seg) => encodeURIComponent(seg))
        .join("/");
}

async function fetchAllFolders(accessToken, siteUrl, libraryPath) {
    console.log(`Resolving site: ${siteUrl}`);
    console.log(`Library path: ${libraryPath}`);

    // Use direct site access (which we know works)
    const url = new URL(siteUrl);
    const host = url.host;
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;

    console.log(`Host: ${host}`);
    console.log(`Site path: ${sitePath}`);

    // Access the site directly using the working method
    const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
    console.log(`Site ID: ${site.id}`);
    console.log(`Site name: ${site.displayName || site.name}`);

    const drives = await graphFetch(`/sites/${site.id}/drives`, accessToken);
    console.log(`Found ${drives.value?.length || 0} drives`);

    const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
    const [driveName, ...subPathParts] = libPathTrimmed.split("/");
    console.log(`Looking for drive: ${driveName}`);
    console.log(`Sub path parts:`, subPathParts);

    const drive = (drives.value || []).find((d) => d.name === driveName);
    if (!drive) {
        console.log(`Available drives:`, drives.value?.map((d) => d.name) || []);
        throw new Error(`Library not found: ${driveName}`);
    }
    console.log(`Found drive: ${drive.name} (ID: ${drive.id})`);

    // List children (subPath or root)
    let resItems;
    if (subPathParts.length === 0) {
        console.log(`Fetching root children from drive ${drive.id}`);
        resItems = await graphFetch(`/drives/${drive.id}/root/children`, accessToken);
    } else {
        const subPath = encodeDrivePath(subPathParts.join("/"));
        console.log(`Fetching children from path: ${subPath}`);
        resItems = await graphFetch(`/drives/${drive.id}/root:/${subPath}:/children`, accessToken);
    }

    // Keep only folders, exclude archive folders
    const foldersOnly = (resItems.value || [])
        .filter((it) => it.folder)
        .filter((it) => {
            const name = (it.name || "").trim().toLowerCase();
            return !name.includes("(archive)") && !name.includes("archive");
        })
        .map((it) => ({
            id: it.id,
            name: it.name,
            webUrl: it.webUrl || null,
            createdDateTime: it.createdDateTime || null,
            lastModifiedDateTime: it.lastModifiedDateTime || null,
            size: it.size || 0,
            driveId: it.parentReference?.driveId || null,
            parentPath: it.parentReference?.path || null,
        }));

    return foldersOnly;
}

async function syncToFastField(folders) {
    console.log(`Syncing ${folders.length} folders to FastField table: ${FASTFIELD_TABLE_NAME}`);

    // First, authenticate to get a session token
    console.log("Authenticating with FastField...");
    const authResponse = await fetch("https://api.fastfieldforms.com/services/v3/authenticate", {
        method: "POST",
        headers: {
            Authorization: FASTFIELD_AUTH_HEADER,
            "Cache-Control": "no-cache",
            "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
        },
    });

    if (!authResponse.ok) {
        const errorText = await authResponse.text();
        throw new Error(`FastField authentication failed: ${authResponse.status} - ${errorText}`);
    }

    const authResult = await authResponse.json();
    const sessionToken = authResult.sessionToken;
    console.log(`✅ Authentication successful, session expires: ${authResult.sessionExpiration}`);

    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    // Process folders in batches to avoid overwhelming the API
    const batchSize = 10;
    for (let i = 0; i < folders.length; i += batchSize) {
        const batch = folders.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(folders.length / batchSize)} (${batch.length} folders)`);

        // Process each folder individually
        for (const folder of batch) {
            try {
                const payload = {
                    data: {
                        // FastField expects specific field names based on their datatable structure
                        "Project Name": folder.name,
                        "Sharepoint Link": folder.webUrl || "",
                    },
                };

                const response = await fetch(`${FASTFIELD_API_URL}/${FASTFIELD_TABLE_ID}/items`, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "Cache-Control": "no-cache",
                        "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
                        "X-Gatekeeper-SessionToken": sessionToken,
                    },
                    body: JSON.stringify(payload),
                });

                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`HTTP ${response.status}: ${errorText}`);
                }

                const result = await response.json();
                console.log(`✅ Synced folder: ${folder.name}`);
                successCount++;

                // Add a small delay between requests to be respectful to the API
                await new Promise((resolve) => setTimeout(resolve, 100));
            } catch (error) {
                console.error(`❌ Failed to sync folder ${folder.name}:`, error.message);
                errors.push({ folder: folder.name, error: error.message });
                errorCount++;
            }
        }

        // Add a longer delay between batches
        if (i + batchSize < folders.length) {
            console.log("Waiting 2 seconds before next batch...");
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
    }

    return {
        successCount,
        errorCount,
        errors,
        totalProcessed: folders.length,
    };
}

export default async function handler(req, res) {
    // This function can be called manually via HTTP or by Vercel Cron
    const isCronRequest = req.headers["x-vercel-cron"] === "1";

    try {
        console.log(`[${new Date().toISOString()}] Starting folder sync to FastField...`);

        const accessToken = await getAppToken();
        const siteUrl = req.query?.siteUrl || DEFAULT_SITE_URL;
        const libraryPath = req.query?.libraryPath || DEFAULT_LIBRARY;

        console.log(`Fetching folders from ${siteUrl}/${libraryPath}...`);
        const folders = await fetchAllFolders(accessToken, siteUrl, libraryPath);

        console.log(`Found ${folders.length} folders, syncing to FastField...`);
        const fastFieldResult = await syncToFastField(folders);

        const result = {
            success: true,
            syncedAt: new Date().toISOString(),
            foldersCount: folders.length,
            siteUrl,
            libraryPath,
            fastFieldResult,
        };

        // Log the successful sync
        await logSubmission({
            type: "folder-sync-cron",
            status: "ok",
            foldersCount: folders.length,
            siteUrl,
            libraryPath,
            fastFieldResult,
        });

        console.log(`[${new Date().toISOString()}] Folder sync completed successfully. Synced ${folders.length} folders.`);

        // Return appropriate response based on how it was called
        if (isCronRequest) {
            // For cron jobs, return minimal response
            return res.status(200).json({ ok: true, synced: folders.length });
        } else {
            // For manual calls, return detailed response
            return res.status(200).json(result);
        }
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Folder sync failed:`, error);

        // Log the error
        await logSubmission({
            type: "folder-sync-cron",
            status: "error",
            error: error?.message || String(error),
        });

        if (isCronRequest) {
            // For cron jobs, return error status
            return res.status(500).json({ error: error.message || String(error) });
        } else {
            // For manual calls, return detailed error
            return res.status(500).json({
                success: false,
                error: error.message || String(error),
                timestamp: new Date().toISOString(),
            });
        }
    }
}
