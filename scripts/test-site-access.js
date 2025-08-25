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

async function main() {
    console.log("🔍 Testing different SharePoint site access methods...\n");

    const TENANT_ID = readEnv("TENANT_ID", true);
    const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
    const MSAL_CLIENT_SECRET = readEnv("MSAL_CLIENT_SECRET", true);
    const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);

    const msalApp = new ConfidentialClientApplication({
        auth: {
            authority: `https://login.microsoftonline.com/${TENANT_ID}`,
            clientId: MSAL_CLIENT_ID,
            clientSecret: MSAL_CLIENT_SECRET,
        },
    });

    const result = await msalApp.acquireTokenByClientCredential({
        scopes: ["https://graph.microsoft.com/.default"],
    });

    const accessToken = result.accessToken;
    const url = new URL(DEFAULT_SITE_URL);
    const host = url.host;
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
    const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;

    console.log(`Testing access to: ${DEFAULT_SITE_URL}`);
    console.log(`Host: ${host}`);
    console.log(`Site path: ${sitePath}`);
    console.log();

    // Method 1: Try with site ID
    console.log("1. Testing with site ID method...");
    try {
        const siteResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, {
            headers: { Authorization: `Bearer ${accessToken}` },
        });

        if (siteResponse.ok) {
            const site = await siteResponse.json();
            console.log("   ✅ Success!");
            console.log(`   🆔 Site ID: ${site.id}`);
            console.log(`   📝 Site name: ${site.displayName || site.name}`);
            console.log(`   🔗 Web URL: ${site.webUrl}`);
        } else {
            const errorText = await siteResponse.text();
            console.log(`   ❌ Failed: ${siteResponse.status}`);
            console.log(`   📝 Error: ${errorText}`);
        }
    } catch (error) {
        console.log(`   ❌ Exception: ${error.message}`);
    }
    console.log();

    // Method 2: Try with root site
    console.log("2. Testing root site access...");
    try {
        const rootResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${host}:/`, {
            headers: { Authorization: `Bearer ${accessToken}` },
        });

        if (rootResponse.ok) {
            const root = await rootResponse.json();
            console.log("   ✅ Root site access successful!");
            console.log(`   🆔 Root Site ID: ${root.id}`);
            console.log(`   📝 Root Site name: ${root.displayName || root.name}`);
        } else {
            const errorText = await rootResponse.text();
            console.log(`   ❌ Root site failed: ${rootResponse.status}`);
            console.log(`   📝 Error: ${errorText}`);
        }
    } catch (error) {
        console.log(`   ❌ Root site exception: ${error.message}`);
    }
    console.log();

    // Method 3: Try listing all sites
    console.log("3. Testing list all sites...");
    try {
        const sitesResponse = await fetch("https://graph.microsoft.com/v1.0/sites", {
            headers: { Authorization: `Bearer ${accessToken}` },
        });

        if (sitesResponse.ok) {
            const sites = await sitesResponse.json();
            console.log("   ✅ Sites list successful!");
            console.log(`   📊 Found ${sites.value?.length || 0} sites`);

            // Look for our target site
            const targetSite = sites.value?.find((site) => site.webUrl?.includes(sitePath) || site.displayName?.toLowerCase().includes(sitePath.toLowerCase()));

            if (targetSite) {
                console.log("   🎯 Found target site in list:");
                console.log(`      🆔 ID: ${targetSite.id}`);
                console.log(`      📝 Name: ${targetSite.displayName || targetSite.name}`);
                console.log(`      🔗 URL: ${targetSite.webUrl}`);
            } else {
                console.log("   ⚠️  Target site not found in list");
                console.log("   📋 Available sites:");
                sites.value?.slice(0, 5).forEach((site, i) => {
                    console.log(`      ${i + 1}. ${site.displayName || site.name} (${site.webUrl})`);
                });
                if (sites.value?.length > 5) {
                    console.log(`      ... and ${sites.value.length - 5} more`);
                }
            }
        } else {
            const errorText = await sitesResponse.text();
            console.log(`   ❌ Sites list failed: ${sitesResponse.status}`);
            console.log(`   📝 Error: ${errorText}`);
        }
    } catch (error) {
        console.log(`   ❌ Sites list exception: ${error.message}`);
    }
    console.log();

    // Method 4: Try with different site path formats
    console.log("4. Testing different site path formats...");
    const pathVariations = [sitePath, `sites/${sitePath}`, sitePath.toLowerCase(), sitePath.toUpperCase()];

    for (const path of pathVariations) {
        try {
            const testResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${host}:/sites/${encodeURIComponent(path)}`, {
                headers: { Authorization: `Bearer ${accessToken}` },
            });

            if (testResponse.ok) {
                const site = await testResponse.json();
                console.log(`   ✅ Success with path: "${path}"`);
                console.log(`   🆔 Site ID: ${site.id}`);
                break;
            } else {
                console.log(`   ❌ Failed with path: "${path}" (${testResponse.status})`);
            }
        } catch (error) {
            console.log(`   ❌ Exception with path: "${path}": ${error.message}`);
        }
    }

    console.log("\n🔧 Troubleshooting recommendations:");
    console.log("1. Check if the app has 'Sites.ReadWrite.All' permission");
    console.log("2. Verify admin consent has been granted");
    console.log("3. Check if the site name is correct (case-sensitive)");
    console.log("4. Try using the site ID instead of the site name");
    console.log("5. Ensure the app has access to the specific SharePoint site");
}

main().catch(console.error);
