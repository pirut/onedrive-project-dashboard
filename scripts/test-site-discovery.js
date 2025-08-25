#!/usr/bin/env node

import "isomorphic-fetch";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

async function main() {
    console.log("🔍 Testing different site discovery methods...\n");

    const TENANT_ID = readEnv("TENANT_ID", true);
    const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
    const MSAL_CLIENT_SECRET = readEnv("MSAL_CLIENT_SECRET", true);
    const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);

    try {
        // Get token
        const tokenResponse = await fetch(`https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`, {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body: new URLSearchParams({
                grant_type: "client_credentials",
                client_id: MSAL_CLIENT_ID,
                client_secret: MSAL_CLIENT_SECRET,
                scope: "https://graph.microsoft.com/.default",
            }),
        });

        if (!tokenResponse.ok) {
            const errorText = await tokenResponse.text();
            console.log(`❌ Token request failed: ${tokenResponse.status}`);
            console.log(`📝 Error: ${errorText}`);
            return;
        }

        const tokenData = await tokenResponse.json();
        console.log("✅ Token acquired successfully");

        // Method 1: Try to access the specific site directly
        console.log("\n1. Testing direct site access...");
        const url = new URL(DEFAULT_SITE_URL);
        const host = url.host;
        const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
        const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;

        console.log(`   Host: ${host}`);
        console.log(`   Site path: ${sitePath}`);

        try {
            const directSiteResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, {
                headers: {
                    Authorization: `Bearer ${tokenData.access_token}`,
                },
            });

            if (directSiteResponse.ok) {
                const site = await directSiteResponse.json();
                console.log("   ✅ Direct site access successful!");
                console.log(`   🆔 Site ID: ${site.id}`);
                console.log(`   📝 Site name: ${site.displayName || site.name}`);
                console.log(`   🔗 Web URL: ${site.webUrl}`);

                // Test getting drives for this site
                console.log("\n   💾 Testing drives access...");
                const drivesResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${site.id}/drives`, {
                    headers: {
                        Authorization: `Bearer ${tokenData.access_token}`,
                    },
                });

                if (drivesResponse.ok) {
                    const drives = await drivesResponse.json();
                    console.log(`   ✅ Found ${drives.value?.length || 0} drives`);
                    drives.value?.forEach((drive, index) => {
                        console.log(`      ${index + 1}. ${drive.name} (${drive.id})`);
                    });
                } else {
                    console.log(`   ❌ Drives access failed: ${drivesResponse.status}`);
                }
            } else {
                const errorText = await directSiteResponse.text();
                console.log(`   ❌ Direct site access failed: ${directSiteResponse.status}`);
                console.log(`   📝 Error: ${errorText}`);
            }
        } catch (error) {
            console.log(`   ❌ Direct site access exception: ${error.message}`);
        }

        // Method 2: Try to access root site
        console.log("\n2. Testing root site access...");
        try {
            const rootResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${host}:/`, {
                headers: {
                    Authorization: `Bearer ${tokenData.access_token}`,
                },
            });

            if (rootResponse.ok) {
                const root = await rootResponse.json();
                console.log("   ✅ Root site access successful!");
                console.log(`   🆔 Root Site ID: ${root.id}`);
                console.log(`   📝 Root Site name: ${root.displayName || root.name}`);
                console.log(`   🔗 Root Web URL: ${root.webUrl}`);
            } else {
                const errorText = await rootResponse.text();
                console.log(`   ❌ Root site access failed: ${rootResponse.status}`);
                console.log(`   📝 Error: ${errorText}`);
            }
        } catch (error) {
            console.log(`   ❌ Root site access exception: ${error.message}`);
        }

        // Method 3: Try to access by site ID (if we have it)
        console.log("\n3. Testing site access by ID...");
        try {
            // Try some common site IDs
            const commonSiteIds = [
                "root", // Root site
                "00000000-0000-0000-0000-000000000000", // Sometimes used for root
            ];

            for (const siteId of commonSiteIds) {
                const siteByIdResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${siteId}`, {
                    headers: {
                        Authorization: `Bearer ${tokenData.access_token}`,
                    },
                });

                if (siteByIdResponse.ok) {
                    const site = await siteByIdResponse.json();
                    console.log(`   ✅ Site access by ID '${siteId}' successful!`);
                    console.log(`   🆔 Site ID: ${site.id}`);
                    console.log(`   📝 Site name: ${site.displayName || site.name}`);
                    console.log(`   🔗 Web URL: ${site.webUrl}`);
                    break;
                } else {
                    console.log(`   ❌ Site access by ID '${siteId}' failed: ${siteByIdResponse.status}`);
                }
            }
        } catch (error) {
            console.log(`   ❌ Site access by ID exception: ${error.message}`);
        }

        // Method 4: Try to list all sites with different parameters
        console.log("\n4. Testing sites list with different parameters...");
        const sitesParams = ["", "?$top=100", "?$filter=siteCollection/root ne null", "?$select=id,displayName,webUrl"];

        for (const params of sitesParams) {
            try {
                const sitesResponse = await fetch(`https://graph.microsoft.com/v1.0/sites${params}`, {
                    headers: {
                        Authorization: `Bearer ${tokenData.access_token}`,
                    },
                });

                if (sitesResponse.ok) {
                    const sites = await sitesResponse.json();
                    console.log(`   ✅ Sites list with params '${params}' successful: ${sites.value?.length || 0} sites`);
                    if (sites.value?.length > 0) {
                        sites.value.slice(0, 3).forEach((site, index) => {
                            console.log(`      ${index + 1}. ${site.displayName || site.name} (${site.webUrl})`);
                        });
                        if (sites.value.length > 3) {
                            console.log(`      ... and ${sites.value.length - 3} more`);
                        }
                        break; // Found sites, no need to try other params
                    }
                } else {
                    console.log(`   ❌ Sites list with params '${params}' failed: ${sitesResponse.status}`);
                }
            } catch (error) {
                console.log(`   ❌ Sites list with params '${params}' exception: ${error.message}`);
            }
        }
    } catch (error) {
        console.error("❌ Authentication failed:", error.message);
    }

    console.log("\n🔧 Next steps:");
    console.log("1. If direct site access works, we can use that approach");
    console.log("2. If root site access works, we can navigate from there");
    console.log("3. If no sites are found, the app might need specific site permissions");
}

main().catch(console.error);
