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

// Simple JWT decode function (no signature verification needed)
function decodeJwt(token) {
    try {
        const parts = token.split(".");
        if (parts.length !== 3) return null;

        const payload = parts[1];
        const decoded = Buffer.from(payload, "base64").toString("utf8");
        return JSON.parse(decoded);
    } catch (error) {
        return null;
    }
}

async function main() {
    console.log("🔍 Verifying app permissions from token claims...\n");

    const TENANT_ID = readEnv("TENANT_ID", true);
    const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
    const MSAL_CLIENT_SECRET = readEnv("MSAL_CLIENT_SECRET", true);

    const msalApp = new ConfidentialClientApplication({
        auth: {
            authority: `https://login.microsoftonline.com/${TENANT_ID}`,
            clientId: MSAL_CLIENT_ID,
            clientSecret: MSAL_CLIENT_SECRET,
        },
    });

    try {
        const result = await msalApp.acquireTokenByClientCredential({
            scopes: ["https://graph.microsoft.com/.default"],
        });

        console.log("✅ Token acquired successfully");
        console.log(`📝 Token expires: ${new Date(result.expiresOn).toISOString()}`);

        // Decode the token to see the claims
        const tokenPayload = decodeJwt(result.accessToken);
        if (tokenPayload) {
            console.log("\n📋 Token Claims:");
            console.log(`   🆔 App ID: ${tokenPayload.appid || "N/A"}`);
            console.log(`   🏢 Tenant ID: ${tokenPayload.tid || "N/A"}`);
            console.log(`   👤 User ID: ${tokenPayload.oid || "N/A"}`);
            console.log(`   📅 Issued at: ${new Date(tokenPayload.iat * 1000).toISOString()}`);
            console.log(`   📅 Expires at: ${new Date(tokenPayload.exp * 1000).toISOString()}`);

            // Check for roles (application permissions)
            if (tokenPayload.roles && tokenPayload.roles.length > 0) {
                console.log("\n🔐 Application Roles (Permissions):");
                tokenPayload.roles.forEach((role, index) => {
                    console.log(`   ${index + 1}. ${role}`);
                });
            } else {
                console.log("\n❌ No application roles found in token");
                console.log("   This means the app doesn't have any application permissions granted");
            }

            // Check for scp (delegated permissions)
            if (tokenPayload.scp) {
                console.log("\n🔑 Delegated Scopes:");
                console.log(`   ${tokenPayload.scp}`);
            }

            // Check for wids (Windows identity claims)
            if (tokenPayload.wids && tokenPayload.wids.length > 0) {
                console.log("\n🪟 Windows Identity Claims:");
                tokenPayload.wids.forEach((wid, index) => {
                    console.log(`   ${index + 1}. ${wid}`);
                });
            }
        } else {
            console.log("❌ Could not decode token");
        }

        // Test a simple Graph API call that should work with basic permissions
        console.log("\n🧪 Testing basic Graph API access...");
        const testResponse = await fetch("https://graph.microsoft.com/v1.0/", {
            headers: { Authorization: `Bearer ${result.accessToken}` },
        });

        if (testResponse.ok) {
            const graphInfo = await testResponse.json();
            console.log("✅ Basic Graph API access works");
            console.log(`   📊 Graph version: ${graphInfo.graphVersion || "N/A"}`);
        } else {
            console.log(`❌ Basic Graph API access failed: ${testResponse.status}`);
        }
    } catch (error) {
        console.error("❌ Failed to acquire token:", error.message);
    }

    console.log("\n🔧 Next steps:");
    console.log("1. If no roles are listed, the app needs application permissions");
    console.log("2. If roles are listed but sites access fails, check conditional access");
    console.log("3. Try the delegated authentication test: npm run test-delegated");
}

main().catch(console.error);
