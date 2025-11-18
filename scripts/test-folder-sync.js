#!/usr/bin/env node

import "isomorphic-fetch";
import { ConfidentialClientApplication } from "@azure/msal-node";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

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
const FASTFIELD_API_URL = readEnv("FASTFIELD_API_URL");
const FASTFIELD_AUTH_HEADER = readEnv("FASTFIELD_AUTH_HEADER"); // Basic Auth header
const FASTFIELD_TABLE_ID = readEnv("FASTFIELD_TABLE_ID"); // We need the table ID, not name
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

async function testFastFieldSync(folders) {
    if (!FASTFIELD_API_URL || !FASTFIELD_AUTH_HEADER) {
        console.log("⚠️  FastField API not configured - skipping sync test");
        console.log("   Set FASTFIELD_API_URL and FASTFIELD_AUTH_HEADER to test full sync");
        return null;
    }

    console.log(`Testing FastField sync with ${folders.length} folders...`);
    console.log(`API URL: ${FASTFIELD_API_URL}`);
    console.log(`Table: ${FASTFIELD_TABLE_NAME}`);

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

    // Test with just the first folder to avoid overwhelming the API during testing
    const testFolder = folders[0];
    if (!testFolder) {
        console.log("No folders to test with");
        return null;
    }

    const payload = {
        data: {
            // FastField expects specific field names based on their datatable structure
            "Project Name": testFolder.name,
            "Sharepoint Link": testFolder.webUrl || "",
        },
    };

    console.log("Testing with payload:", JSON.stringify(payload, null, 2));

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
        throw new Error(`FastField API error: ${response.status} - ${errorText}`);
    }

    const result = await response.json();
    console.log("✅ Test sync successful!");
    return result;
}

async function main() {
    try {
        console.log("🧪 Testing folder sync functionality...\n");

        // Test Microsoft Graph authentication
        console.log("1. Testing Microsoft Graph authentication...");
        const accessToken = await getAppToken();
        console.log("✅ Authentication successful\n");

        // Test folder fetching
        console.log("2. Testing folder fetching...");
        const folders = await fetchAllFolders(accessToken, DEFAULT_SITE_URL, DEFAULT_LIBRARY);
        console.log(`✅ Found ${folders.length} folders\n`);

        // Display sample folders
        console.log("3. Sample folders found:");
        folders.slice(0, 5).forEach((folder, index) => {
            console.log(`   ${index + 1}. ${folder.name} (ID: ${folder.id})`);
        });
        if (folders.length > 5) {
            console.log(`   ... and ${folders.length - 5} more folders`);
        }
        console.log();

        // Test FastField sync (if configured)
        if (FASTFIELD_API_URL && FASTFIELD_AUTH_HEADER) {
            console.log("4. Testing FastField sync...");
            const result = await testFastFieldSync(folders);
            console.log("✅ FastField sync successful");
            console.log(`   Result:`, JSON.stringify(result, null, 2));
        } else {
            console.log("4. ⚠️  FastField sync test skipped (not configured)");
        }

        console.log("\n🎉 All tests completed successfully!");
        console.log("\nNext steps:");
        console.log("1. Configure FastField API credentials in your environment");
        console.log("2. Deploy to Vercel with: vercel --prod");
        console.log("3. The cron job will run automatically every hour");
    } catch (error) {
        console.error("❌ Test failed:", error.message);
        process.exit(1);
    }
}

main();
