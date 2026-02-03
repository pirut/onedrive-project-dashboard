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
    console.log("🔍 Testing simple authentication approach...\n");

    const TENANT_ID = readEnv("TENANT_ID", true);
    const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
    const MICROSOFT_CLIENT_SECRET = readEnv("MICROSOFT_CLIENT_SECRET", true);

    console.log("Testing with application permissions (read-only)...");
    console.log("This should work if the app has Sites.Read.All permission\n");

    try {
        // Use client credentials flow with read-only scope
        const tokenResponse = await fetch(`https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`, {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body: new URLSearchParams({
                grant_type: "client_credentials",
                client_id: MSAL_CLIENT_ID,
                client_secret: MICROSOFT_CLIENT_SECRET,
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
        console.log(`📝 Token expires in: ${tokenData.expires_in} seconds`);

        // Test basic Graph API access
        console.log("\n🧪 Testing Graph API access...");
        const graphResponse = await fetch("https://graph.microsoft.com/v1.0/", {
            headers: {
                Authorization: `Bearer ${tokenData.access_token}`,
            },
        });

        if (graphResponse.ok) {
            const graphInfo = await graphResponse.json();
            console.log("✅ Graph API access successful");
            console.log(`📊 Graph version: ${graphInfo.graphVersion || "N/A"}`);
        } else {
            console.log(`❌ Graph API access failed: ${graphResponse.status}`);
        }

        // Test sites access
        console.log("\n🏢 Testing sites access...");
        const sitesResponse = await fetch("https://graph.microsoft.com/v1.0/sites", {
            headers: {
                Authorization: `Bearer ${tokenData.access_token}`,
            },
        });

        if (sitesResponse.ok) {
            const sites = await sitesResponse.json();
            console.log(`✅ Sites access successful! Found ${sites.value?.length || 0} sites`);

            // Look for the target site
            const targetSite = sites.value?.find((site) => site.webUrl?.includes("work") || site.displayName?.toLowerCase().includes("work"));

            if (targetSite) {
                console.log("🎯 Found target site:");
                console.log(`   🆔 ID: ${targetSite.id}`);
                console.log(`   📝 Name: ${targetSite.displayName || targetSite.name}`);
                console.log(`   🔗 URL: ${targetSite.webUrl}`);

                // Test accessing the specific site
                console.log("\n📁 Testing specific site access...");
                const siteResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${targetSite.id}`, {
                    headers: {
                        Authorization: `Bearer ${tokenData.access_token}`,
                    },
                });

                if (siteResponse.ok) {
                    const site = await siteResponse.json();
                    console.log("✅ Site access successful!");
                    console.log(`   📊 Site ID: ${site.id}`);
                    console.log(`   📝 Site name: ${site.displayName || site.name}`);

                    // Test getting drives
                    console.log("\n💾 Testing drives access...");
                    const drivesResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${targetSite.id}/drives`, {
                        headers: {
                            Authorization: `Bearer ${tokenData.access_token}`,
                        },
                    });

                    if (drivesResponse.ok) {
                        const drives = await drivesResponse.json();
                        console.log(`✅ Drives access successful! Found ${drives.value?.length || 0} drives`);
                        drives.value?.forEach((drive, index) => {
                            console.log(`   ${index + 1}. ${drive.name} (${drive.id})`);
                        });
                    } else {
                        console.log(`❌ Drives access failed: ${drivesResponse.status}`);
                    }
                } else {
                    const errorText = await siteResponse.text();
                    console.log(`❌ Site access failed: ${siteResponse.status}`);
                    console.log(`📝 Error: ${errorText}`);
                }
            } else {
                console.log("⚠️  Target site not found in list");
                console.log("📋 Available sites:");
                sites.value?.slice(0, 5).forEach((site, i) => {
                    console.log(`   ${i + 1}. ${site.displayName || site.name} (${site.webUrl})`);
                });
            }
        } else {
            const errorText = await sitesResponse.text();
            console.log(`❌ Sites access failed: ${sitesResponse.status}`);
            console.log(`📝 Error: ${errorText}`);
        }
    } catch (error) {
        console.error("❌ Authentication failed:", error.message);
    }

    console.log("\n🔧 If this still fails, you need to:");
    console.log("1. Add Sites.Read.All permission to your Azure AD app");
    console.log("2. Grant admin consent for the permission");
    console.log("3. Wait a few minutes for changes to propagate");
}

main().catch(console.error);
