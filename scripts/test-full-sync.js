#!/usr/bin/env node

import "isomorphic-fetch";
import dotenv from "dotenv";
import { ConfidentialClientApplication } from "@azure/msal-node";

// Load environment variables
dotenv.config();

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

// Environment variables
const TENANT_ID = readEnv("TENANT_ID", true);
const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
const MICROSOFT_CLIENT_SECRET = readEnv("MICROSOFT_CLIENT_SECRET", true);
const MS_GRAPH_SCOPE = readEnv("MS_GRAPH_SCOPE") || "https://graph.microsoft.com/.default";
const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);
const DEFAULT_LIBRARY = readEnv("DEFAULT_LIBRARY", true);

// FastField configuration
const FASTFIELD_API_URL = readEnv("FASTFIELD_API_URL");
const FASTFIELD_AUTH_HEADER = readEnv("FASTFIELD_AUTH_HEADER"); // Basic Auth header
const FASTFIELD_TABLE_ID = readEnv("FASTFIELD_TABLE_ID"); // We need the table ID, not name
const FASTFIELD_TABLE_NAME = readEnv("FASTFIELD_TABLE_NAME") || "Cornerstone Active Projects";

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
    console.log(`Fetching folders from ${siteUrl}/${libraryPath}...`);

    // Resolve drive
    const url = new URL(siteUrl);
    const host = url.host;
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;

    console.log(`Host: ${host}`);
    console.log(`Site path: ${sitePath}`);

    const site = await graphFetch(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, accessToken);
    console.log(`Site ID: ${site.id}`);

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
        return { items: existingItems, duplicates };
    } catch (error) {
        console.log(`⚠️  Error fetching existing items: ${error.message}`);
        return { items: new Set(), duplicates: [] };
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
    const { items: existingItems, duplicates } = await getExistingItems(sessionToken);

    // Clean up duplicates first
    const cleanupResult = await cleanupDuplicates(sessionToken, duplicates);

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

    return { successCount, errorCount, skippedCount, cleanupResult, errors, totalProcessed: folders.length };
}

async function main() {
    try {
        console.log("🚀 Starting full folder sync to FastField...\n");

        // Test Microsoft Graph authentication
        console.log("1. Testing Microsoft Graph authentication...");
        const accessToken = await getAppToken();
        console.log("✅ Authentication successful\n");

        // Test folder fetching
        console.log("2. Fetching all folders from SharePoint...");
        const folders = await fetchAllFolders(accessToken, DEFAULT_SITE_URL, DEFAULT_LIBRARY);
        console.log(`✅ Found ${folders.length} folders\n`);

        // Sync to FastField
        if (FASTFIELD_API_URL && FASTFIELD_AUTH_HEADER && FASTFIELD_TABLE_ID) {
            console.log("3. Syncing all folders to FastField...");
            const result = await syncToFastField(folders);

            console.log("\n🎉 Sync completed!");
            console.log(`📊 Results:`);
            console.log(`   ✅ Successfully synced: ${result.successCount} folders`);
            console.log(`   ⏭️  Skipped (already exist): ${result.skippedCount} folders`);
            console.log(`   ❌ Failed to sync: ${result.errorCount} folders`);
            console.log(`   🧹 Duplicates cleaned up: ${result.cleanupResult.deleted} items`);
            console.log(`   📈 Total processed: ${result.totalProcessed} folders`);

            if (result.errors.length > 0) {
                console.log("\n❌ Errors encountered:");
                result.errors.slice(0, 5).forEach((error, index) => {
                    console.log(`   ${index + 1}. ${error.folder}: ${error.error}`);
                });
                if (result.errors.length > 5) {
                    console.log(`   ... and ${result.errors.length - 5} more errors`);
                }
            }
        } else {
            console.log("3. ⚠️  FastField sync skipped (not configured)");
        }

        console.log("\n🎉 Full sync process completed!");
    } catch (error) {
        console.error("❌ Sync failed:", error.message);
        process.exit(1);
    }
}

main();
