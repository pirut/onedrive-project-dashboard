import "isomorphic-fetch";
import { ConfidentialClientApplication } from "@azure/msal-node";
import { logSubmission } from "../lib/kv.js";
import { processStagingJobWalks } from "../lib/process-staging.js";

// Force dynamic rendering to prevent caching issues with cron jobs
export const dynamic = "force-dynamic";

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

const TENANT_ID = readEnv("TENANT_ID", true);
const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
const MICROSOFT_CLIENT_SECRET = readEnv("MICROSOFT_CLIENT_SECRET", true);
const MS_GRAPH_SCOPE = readEnv("MS_GRAPH_SCOPE") || "https://graph.microsoft.com/.default";
const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);
const DEFAULT_LIBRARY = readEnv("DEFAULT_LIBRARY", true);

// FastField configuration
const FASTFIELD_API_URL = readEnv("FASTFIELD_API_URL", true);
const FASTFIELD_AUTH_HEADER = readEnv("FASTFIELD_AUTH_HEADER", true); // Basic Auth header
const FASTFIELD_TABLE_ID = readEnv("FASTFIELD_TABLE_ID", true); // We need the table ID, not name
const FASTFIELD_TABLE_NAME = readEnv("FASTFIELD_TABLE_NAME") || "Cornerstone Active Projects";

function getCronSecretCandidates(req) {
    return [
        req.query?.cronSecret,
        req.headers["x-cron-secret"],
        req.headers["x-vercel-cron-secret"],
        (req.headers["authorization"] || "").replace(/^Bearer\s+/i, ""),
    ]
        .map((value) => String(value || "").trim())
        .filter(Boolean);
}

function isCronRequest(req) {
    return Boolean(req.headers["x-vercel-cron"] || getCronSecretCandidates(req).length > 0);
}

function isAuthorized(req) {
    const expected = (process.env.CRON_SECRET || "").trim();
    if (!expected) return true;
    return getCronSecretCandidates(req).includes(expected);
}

const msalApp = new ConfidentialClientApplication({
    auth: {
        authority: `https://login.microsoftonline.com/${TENANT_ID}`,
        clientId: MSAL_CLIENT_ID,
        clientSecret: MICROSOFT_CLIENT_SECRET,
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

async function graphFetchAllPages(path, accessToken, options = {}) {
    const allItems = [];
    let nextLink = null;
    let pageCount = 0;

    // Start with the initial path
    let currentPath = path;

    do {
        pageCount++;
        console.log(`Fetching page ${pageCount}${nextLink ? " (next page)" : ""}...`);
        
        const res = await fetch(`https://graph.microsoft.com/v1.0${currentPath}`, {
            method: options.method || "GET",
            headers: { Authorization: `Bearer ${accessToken}`, ...(options.headers || {}) },
            body: options.body,
        });

        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Graph ${options.method || "GET"} ${currentPath} -> ${res.status}: ${text}`);
        }

        const data = await res.json();
        
        // Collect items from this page
        if (data.value && Array.isArray(data.value)) {
            allItems.push(...data.value);
            console.log(`Page ${pageCount}: Found ${data.value.length} items (total so far: ${allItems.length})`);
        }

        // Check for next page link
        nextLink = data["@odata.nextLink"];
        if (nextLink) {
            // Extract the path from the full URL (remove the base URL)
            currentPath = nextLink.replace("https://graph.microsoft.com/v1.0", "");
        }
    } while (nextLink);

    console.log(`Fetched all pages: ${pageCount} page(s), ${allItems.length} total items`);
    
    // Return in the same format as a single page response
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

    // List children (subPath or root) - fetch all pages to handle pagination
    let resItems;
    if (subPathParts.length === 0) {
        console.log(`Fetching root children from drive ${drive.id} (all pages)...`);
        resItems = await graphFetchAllPages(`/drives/${drive.id}/root/children`, accessToken);
    } else {
        const subPath = encodeDrivePath(subPathParts.join("/"));
        console.log(`Fetching children from path: ${subPath} (all pages)...`);
        resItems = await graphFetchAllPages(`/drives/${drive.id}/root:/${subPath}:/children`, accessToken);
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

async function getExistingItems(sessionToken) {
    console.log("📋 Checking for existing items in FastField...");
    try {
        const allItems = [];
        let pageNumber = 1;
        let totalPages = 1;

        // Fetch all pages
        do {
            const response = await fetch(`${FASTFIELD_API_URL}/${FASTFIELD_TABLE_ID}/items?pageNumber=${pageNumber}`, {
                method: "GET",
                headers: {
                    "Cache-Control": "no-cache",
                    "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
                    "X-Gatekeeper-SessionToken": sessionToken,
                },
            });

            if (!response.ok) {
                console.log(`⚠️  Could not fetch page ${pageNumber}: ${response.status}`);
                break;
            }

            const responseData = await response.json();

            // Handle paginated response structure
            const data = responseData.data?.records || responseData;

            if (Array.isArray(data)) {
                allItems.push(...data);
            }

            // Update pagination info
            if (responseData.data) {
                totalPages = responseData.data.totalPages || 1;
                console.log(`📄 Fetched page ${pageNumber}/${totalPages} (${data.length} items)`);
            }

            pageNumber++;
        } while (pageNumber <= totalPages);

        const existingItems = new Set();
        const duplicates = [];
        const nameToItems = new Map();

        // Process all items
        allItems.forEach((item) => {
            if (item.data && item.data["79075b7641bb4bfebd0af28fbc851904"]) {
                const name = item.data["79075b7641bb4bfebd0af28fbc851904"];
                existingItems.add(name);

                if (!nameToItems.has(name)) {
                    nameToItems.set(name, []);
                }
                nameToItems.get(name).push(item);
            }
        });

        // Find duplicates
        nameToItems.forEach((items, name) => {
            if (items.length > 1) {
                duplicates.push({ name, items });
            }
        });

        console.log(`📋 Found ${existingItems.size} unique items in FastField (from ${allItems.length} total items)`);
        if (duplicates.length > 0) {
            console.log(`⚠️  Found ${duplicates.length} items with duplicates`);
        }
        return { items: existingItems, duplicates, allItems };
    } catch (error) {
        console.log(`⚠️  Error fetching existing items: ${error.message}`);
        return { items: new Set(), duplicates: [], allItems: [] };
    }
}

async function cleanupDuplicates(sessionToken, duplicates) {
    if (duplicates.length === 0) {
        console.log("✅ No duplicates found to clean up");
        return { deleted: 0 };
    }

    console.log(`🧹 Cleaning up ${duplicates.length} duplicate items...`);
    let deletedCount = 0;
    const errors = [];

    for (const { name, items } of duplicates) {
        // Keep the first item, delete the rest
        const itemsToDelete = items.slice(1);
        console.log(`🗑️  Deleting ${itemsToDelete.length} duplicates for "${name}"`);

        for (const item of itemsToDelete) {
            try {
                const response = await fetch(`${FASTFIELD_API_URL}/${FASTFIELD_TABLE_ID}/items/${item.id}`, {
                    method: "DELETE",
                    headers: {
                        "Cache-Control": "no-cache",
                        "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
                        "X-Gatekeeper-SessionToken": sessionToken,
                    },
                });

                if (response.ok) {
                    console.log(`✅ Deleted duplicate: ${name} (ID: ${item.id})`);
                    deletedCount++;
                } else {
                    const errorText = await response.text();
                    console.error(`❌ Failed to delete duplicate ${name} (ID: ${item.id}): ${response.status} - ${errorText}`);
                    errors.push({ name, itemId: item.id, error: `${response.status}: ${errorText}` });
                }

                // Small delay between deletions
                await new Promise((resolve) => setTimeout(resolve, 200));
            } catch (error) {
                console.error(`❌ Error deleting duplicate ${name} (ID: ${item.id}):`, error.message);
                errors.push({ name, itemId: item.id, error: error.message });
            }
        }
    }

    console.log(`🧹 Cleanup completed: ${deletedCount} duplicates deleted`);
    if (errors.length > 0) {
        console.log(`⚠️  ${errors.length} deletion errors occurred`);
    }

    return { deleted: deletedCount, errors };
}

async function removeOrphanedEntries(sessionToken, currentFolders, allFastFieldItems) {
    console.log("🔍 Checking for orphaned entries (folders removed from SharePoint)...");

    // Create a set of current folder names for fast lookup
    const currentFolderNames = new Set(currentFolders.map((folder) => folder.name));

    // Find items in FastField that are no longer in SharePoint
    const orphanedItems = [];
    allFastFieldItems.forEach((item) => {
        if (item.data && item.data["79075b7641bb4bfebd0af28fbc851904"]) {
            const projectName = item.data["79075b7641bb4bfebd0af28fbc851904"];
            if (!currentFolderNames.has(projectName)) {
                orphanedItems.push({
                    id: item.id,
                    name: projectName,
                    item: item,
                });
            }
        }
    });

    if (orphanedItems.length === 0) {
        console.log("✅ No orphaned entries found");
        return { deleted: 0, errors: [] };
    }

    console.log(`🗑️  Found ${orphanedItems.length} orphaned entries to remove from FastField`);

    let deletedCount = 0;
    const errors = [];

    // Process orphaned items in batches to avoid overwhelming the API
    const batchSize = 3;
    for (let i = 0; i < orphanedItems.length; i += batchSize) {
        const batch = orphanedItems.slice(i, i + batchSize);
        console.log(`Removing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(orphanedItems.length / batchSize)} (${batch.length} items)`);

        for (const orphanedItem of batch) {
            let retries = 0;
            const maxRetries = 3;

            while (retries < maxRetries) {
                try {
                    const response = await fetch(`${FASTFIELD_API_URL}/${FASTFIELD_TABLE_ID}/items/${orphanedItem.id}`, {
                        method: "DELETE",
                        headers: {
                            "Cache-Control": "no-cache",
                            "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
                            "X-Gatekeeper-SessionToken": sessionToken,
                        },
                    });

                    if (response.status === 429) {
                        // Rate limited - wait and retry
                        const errorText = await response.text();
                        const match = errorText.match(/Try again in (\d+) seconds/);
                        const waitTime = match ? parseInt(match[1]) * 1000 : 5000;

                        console.log(`⏳ Rate limited for ${orphanedItem.name}, waiting ${waitTime / 1000}s before retry ${retries + 1}/${maxRetries}...`);
                        await new Promise((resolve) => setTimeout(resolve, waitTime));
                        retries++;
                        continue;
                    }

                    if (response.ok) {
                        console.log(`✅ Removed orphaned entry: ${orphanedItem.name} (ID: ${orphanedItem.id})`);
                        deletedCount++;
                        break; // Success, exit retry loop
                    } else {
                        const errorText = await response.text();
                        throw new Error(`HTTP ${response.status}: ${errorText}`);
                    }
                } catch (error) {
                    retries++;
                    if (retries >= maxRetries) {
                        console.error(`❌ Failed to remove orphaned entry ${orphanedItem.name} after ${maxRetries} retries:`, error.message);
                        errors.push({ name: orphanedItem.name, itemId: orphanedItem.id, error: error.message });
                    } else {
                        console.log(`⚠️  Retry ${retries}/${maxRetries} for ${orphanedItem.name}: ${error.message}`);
                        await new Promise((resolve) => setTimeout(resolve, 2000 * retries)); // Exponential backoff
                    }
                }
            }

            // Wait between deletions to avoid rate limiting
            await new Promise((resolve) => setTimeout(resolve, 500));
        }

        // Wait between batches
        if (i + batchSize < orphanedItems.length) {
            console.log("Waiting 2 seconds before next batch...");
            await new Promise((resolve) => setTimeout(resolve, 2000));
        }
    }

    console.log(`🗑️  Orphaned entry removal completed: ${deletedCount} entries removed`);
    if (errors.length > 0) {
        console.log(`⚠️  ${errors.length} removal errors occurred`);
    }

    return { deleted: deletedCount, errors };
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

    // Get existing items and check for duplicates
    const { items: existingItems, duplicates, allItems } = await getExistingItems(sessionToken);

    // Clean up duplicates first
    const cleanupResult = await cleanupDuplicates(sessionToken, duplicates);

    // Remove orphaned entries (folders that no longer exist in SharePoint)
    const orphanedRemovalResult = await removeOrphanedEntries(sessionToken, folders, allItems);

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    const errors = [];

    // Filter out existing items
    const newFolders = folders.filter((folder) => !existingItems.has(folder.name));
    console.log(`📊 Processing ${newFolders.length} new folders (skipping ${folders.length - newFolders.length} existing)`);

    if (newFolders.length === 0) {
        console.log("✅ All folders already exist in FastField!");
        return {
            successCount: 0,
            errorCount: 0,
            skippedCount: folders.length,
            cleanupResult,
            orphanedRemovalResult,
            errors: [],
            totalProcessed: folders.length,
        };
    }

    // Process folders in batches to avoid overwhelming the API
    const batchSize = 5; // Reduced batch size for better rate limiting
    for (let i = 0; i < newFolders.length; i += batchSize) {
        const batch = newFolders.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(newFolders.length / batchSize)} (${batch.length} folders)`);

        // Process each folder individually with retry logic
        for (const folder of batch) {
            let retries = 0;
            const maxRetries = 3;

            while (retries < maxRetries) {
                try {
                    const payload = {
                        data: {
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

                    if (response.status === 429) {
                        // Rate limited - wait and retry
                        const errorText = await response.text();
                        const match = errorText.match(/Try again in (\d+) seconds/);
                        const waitTime = match ? parseInt(match[1]) * 1000 : 5000;

                        console.log(`⏳ Rate limited for ${folder.name}, waiting ${waitTime / 1000}s before retry ${retries + 1}/${maxRetries}...`);
                        await new Promise((resolve) => setTimeout(resolve, waitTime));
                        retries++;
                        continue;
                    }

                    if (!response.ok) {
                        const errorText = await response.text();
                        throw new Error(`HTTP ${response.status}: ${errorText}`);
                    }

                    const result = await response.json();
                    console.log(`✅ Synced folder: ${folder.name}`);
                    successCount++;

                    // Wait between requests to avoid rate limiting
                    await new Promise((resolve) => setTimeout(resolve, 500));
                    break; // Success, exit retry loop
                } catch (error) {
                    retries++;
                    if (retries >= maxRetries) {
                        console.error(`❌ Failed to sync folder ${folder.name} after ${maxRetries} retries:`, error.message);
                        errors.push({ folder: folder.name, error: error.message });
                        errorCount++;
                    } else {
                        console.log(`⚠️  Retry ${retries}/${maxRetries} for ${folder.name}: ${error.message}`);
                        await new Promise((resolve) => setTimeout(resolve, 2000 * retries)); // Exponential backoff
                    }
                }
            }
        }

        // Wait between batches
        if (i + batchSize < newFolders.length) {
            console.log("Waiting 3 seconds before next batch...");
            await new Promise((resolve) => setTimeout(resolve, 3000));
        }
    }

    return { successCount, errorCount, skippedCount, cleanupResult, orphanedRemovalResult, errors, totalProcessed: folders.length };
}

export default async function handler(req, res) {
    const requestId = Math.random().toString(36).slice(2, 12);
    const cronRequest = isCronRequest(req);

    if (!isAuthorized(req)) {
        return res.status(401).json({
            success: false,
            error: "Unauthorized",
            requestId,
            timestamp: new Date().toISOString(),
        });
    }

    try {
        console.log(`[${new Date().toISOString()}] Starting folder sync to FastField...`);

        const accessToken = await getAppToken();
        const siteUrl = req.query?.siteUrl || DEFAULT_SITE_URL;
        const libraryPath = req.query?.libraryPath || DEFAULT_LIBRARY;

        console.log(`Fetching folders from ${siteUrl}/${libraryPath}...`);
        const folders = await fetchAllFolders(accessToken, siteUrl, libraryPath);

        console.log(`Found ${folders.length} folders, syncing to FastField...`);
        const fastFieldResult = await syncToFastField(folders);

        console.log("Processing staging Job Walk PDFs...");
        const stagingResult = await processStagingJobWalks({
            onResult: async (entry) => {
                const base = {
                    type: "pdf_ingest",
                    traceId: entry.traceId,
                    steps: entry.steps || [],
                    source: "fastfield_move_cron",
                };
                if (entry.status === "ok") {
                    await logSubmission({
                        ...base,
                        status: "ok",
                        filename: entry.filename,
                        folderName: entry.folderName,
                    });
                } else {
                    await logSubmission({
                        ...base,
                        status: "error",
                        filename: entry.filename,
                        error: entry.error || "",
                        phase: entry.phase || "",
                    });
                }
            },
        });

        // Log detailed results
        console.log(`📊 Sync Results:`);
        console.log(`   ✅ Successfully synced: ${fastFieldResult.successCount} folders`);
        console.log(`   ⏭️  Skipped (already exist): ${fastFieldResult.skippedCount} folders`);
        console.log(`   ❌ Failed to sync: ${fastFieldResult.errorCount} folders`);
        console.log(`   🧹 Duplicates cleaned up: ${fastFieldResult.cleanupResult.deleted} items`);
        console.log(`   🗑️  Orphaned entries removed: ${fastFieldResult.orphanedRemovalResult.deleted} items`);
        console.log(`   📈 Total processed: ${fastFieldResult.totalProcessed} folders`);
        console.log(`   📦 Staging PDFs processed: ${stagingResult.processed.length}`);
        if (stagingResult.errors.length) {
            console.log(`   ⚠️  Staging errors: ${stagingResult.errors.length}`);
        }

        const result = {
            success: true,
            syncedAt: new Date().toISOString(),
            foldersCount: folders.length,
            siteUrl,
            libraryPath,
            fastFieldResult,
            stagingResult,
        };

        // Log the successful sync
        await logSubmission({
            type: "folder-sync-cron",
            status: "ok",
            foldersCount: folders.length,
            siteUrl,
            libraryPath,
            fastFieldResult,
            stagingResult,
        });

        console.log(`[${new Date().toISOString()}] Folder sync completed successfully. Synced ${folders.length} folders.`);

        // Return appropriate response based on how it was called
        if (cronRequest) {
            // For cron jobs, return minimal response
            return res.status(200).json({ ok: true, synced: folders.length, stagingProcessed: stagingResult.processed.length });
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

        if (cronRequest) {
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
