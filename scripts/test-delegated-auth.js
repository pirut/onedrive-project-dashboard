#!/usr/bin/env node

import "isomorphic-fetch";
import { PublicClientApplication } from "@azure/msal-node";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

async function main() {
    console.log("🔍 Testing delegated authentication...\n");
    console.log("This will open a browser window for you to sign in.\n");

    const TENANT_ID = readEnv("TENANT_ID", true);
    const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);

    const msalApp = new PublicClientApplication({
        auth: {
            authority: `https://login.microsoftonline.com/${TENANT_ID}`,
            clientId: MSAL_CLIENT_ID,
        },
    });

    try {
        // Get the device code for authentication
        const deviceCodeResponse = await msalApp.acquireTokenByDeviceCode({
            scopes: ["https://graph.microsoft.com/Sites.Read.All"],
            deviceCodeCallback: (response) => {
                console.log(`📱 Please visit: ${response.verificationUri}`);
                console.log(`🔑 Enter code: ${response.userCode}`);
                console.log(`⏰ This code expires in ${response.expiresIn} seconds`);
            },
        });

        console.log("✅ Authentication successful!");
        console.log(`👤 User: ${deviceCodeResponse.account?.name || deviceCodeResponse.account?.username}`);
        console.log(`📝 Token expires: ${new Date(deviceCodeResponse.expiresOn).toISOString()}`);

        // Test sites access
        console.log("\n🏢 Testing sites access...");
        const sitesResponse = await fetch("https://graph.microsoft.com/v1.0/sites", {
            headers: { Authorization: `Bearer ${deviceCodeResponse.accessToken}` },
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
                    headers: { Authorization: `Bearer ${deviceCodeResponse.accessToken}` },
                });

                if (siteResponse.ok) {
                    const site = await siteResponse.json();
                    console.log("✅ Site access successful!");
                    console.log(`   📊 Site ID: ${site.id}`);
                    console.log(`   📝 Site name: ${site.displayName || site.name}`);
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
        console.log("\n🔧 This might indicate:");
        console.log("1. The app doesn't have delegated permissions configured");
        console.log("2. The user doesn't have access to the SharePoint sites");
        console.log("3. There are conditional access policies blocking access");
    }
}

main().catch(console.error);
