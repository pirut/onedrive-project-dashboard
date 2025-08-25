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
    console.log("🔍 Diagnosing authentication and configuration...\n");

    // Check environment variables
    console.log("1. Checking environment variables:");
    const envVars = {
        TENANT_ID: readEnv("TENANT_ID"),
        MSAL_CLIENT_ID: readEnv("MSAL_CLIENT_ID"),
        MSAL_CLIENT_SECRET: readEnv("MSAL_CLIENT_SECRET"),
        DEFAULT_SITE_URL: readEnv("DEFAULT_SITE_URL"),
        DEFAULT_LIBRARY: readEnv("DEFAULT_LIBRARY"),
    };

    for (const [key, value] of Object.entries(envVars)) {
        if (value) {
            console.log(`   ✅ ${key}: ${key.includes("SECRET") ? "***" + value.slice(-4) : value}`);
        } else {
            console.log(`   ❌ ${key}: MISSING`);
        }
    }
    console.log();

    // Test MSAL configuration
    console.log("2. Testing MSAL configuration...");
    try {
        const msalApp = new ConfidentialClientApplication({
            auth: {
                authority: `https://login.microsoftonline.com/${envVars.TENANT_ID}`,
                clientId: envVars.MSAL_CLIENT_ID,
                clientSecret: envVars.MSAL_CLIENT_SECRET,
            },
        });

        console.log("   ✅ MSAL app created successfully");

        // Test token acquisition
        console.log("   🔄 Acquiring token...");
        const result = await msalApp.acquireTokenByClientCredential({
            scopes: ["https://graph.microsoft.com/.default"],
        });

        console.log("   ✅ Token acquired successfully");
        console.log(`   📝 Token expires: ${new Date(result.expiresOn).toISOString()}`);
        console.log();

        // Test basic Graph API call
        console.log("3. Testing Microsoft Graph API access...");
        const testResponse = await fetch("https://graph.microsoft.com/v1.0/me", {
            headers: { Authorization: `Bearer ${result.accessToken}` },
        });

        if (testResponse.ok) {
            const me = await testResponse.json();
            console.log("   ✅ Graph API access successful");
            console.log(`   👤 User: ${me.displayName || me.userPrincipalName}`);
        } else {
            console.log("   ⚠️  Graph API access failed (this might be expected for app-only auth)");
            console.log(`   📊 Status: ${testResponse.status}`);
        }
        console.log();

        // Test site access
        console.log("4. Testing site access...");
        if (envVars.DEFAULT_SITE_URL) {
            const url = new URL(envVars.DEFAULT_SITE_URL);
            const host = url.host;
            const pathname = url.pathname.replace(/^\/+|\/+$/g, "");
            const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;

            console.log(`   🌐 Host: ${host}`);
            console.log(`   📁 Site path: ${sitePath}`);

            try {
                const siteResponse = await fetch(`https://graph.microsoft.com/v1.0/sites/${host}:/sites/${encodeURIComponent(sitePath)}`, {
                    headers: { Authorization: `Bearer ${result.accessToken}` },
                });

                if (siteResponse.ok) {
                    const site = await siteResponse.json();
                    console.log("   ✅ Site access successful");
                    console.log(`   🆔 Site ID: ${site.id}`);
                    console.log(`   📝 Site name: ${site.displayName || site.name}`);
                } else {
                    const errorText = await siteResponse.text();
                    console.log("   ❌ Site access failed");
                    console.log(`   📊 Status: ${siteResponse.status}`);
                    console.log(`   📝 Error: ${errorText}`);
                }
            } catch (error) {
                console.log("   ❌ Site access failed with exception");
                console.log(`   📝 Error: ${error.message}`);
            }
        } else {
            console.log("   ⚠️  DEFAULT_SITE_URL not configured");
        }
    } catch (error) {
        console.error("   ❌ Authentication failed:", error.message);
        console.log();
        console.log("🔧 Troubleshooting tips:");
        console.log("   1. Check your TENANT_ID, MSAL_CLIENT_ID, and MSAL_CLIENT_SECRET");
        console.log("   2. Verify the app has the correct Microsoft Graph permissions");
        console.log("   3. Ensure admin consent has been granted for the app");
        console.log("   4. Check if the app is configured for app-only authentication");
    }

    console.log("\n📋 Summary:");
    console.log("   - If you see ✅ marks, your authentication is working");
    console.log("   - If you see ❌ marks, check the troubleshooting tips above");
    console.log("   - The site access test is the most important for your use case");
}

main().catch(console.error);
