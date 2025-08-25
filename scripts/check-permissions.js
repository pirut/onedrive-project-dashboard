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
    console.log("🔍 Checking app permissions and testing different scopes...\n");

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

    // Test different scopes
    const scopesToTest = [
        "https://graph.microsoft.com/.default",
        "https://graph.microsoft.com/Sites.Read.All",
        "https://graph.microsoft.com/Sites.Selected",
        "https://graph.microsoft.com/Files.Read.All",
    ];

    for (const scope of scopesToTest) {
        console.log(`Testing scope: ${scope}`);
        try {
            const result = await msalApp.acquireTokenByClientCredential({
                scopes: [scope],
            });

            console.log(`   ✅ Token acquired successfully`);
            console.log(`   📝 Token expires: ${new Date(result.expiresOn).toISOString()}`);
            console.log(`   🔑 Token type: ${result.tokenType}`);
            console.log(`   📊 Scopes granted: ${result.scopes?.join(", ") || "none"}`);

            // Test a simple Graph API call
            const testResponse = await fetch("https://graph.microsoft.com/v1.0/me", {
                headers: { Authorization: `Bearer ${result.accessToken}` },
            });

            if (testResponse.ok) {
                const me = await testResponse.json();
                console.log(`   👤 User info: ${me.displayName || me.userPrincipalName}`);
            } else {
                console.log(`   ⚠️  /me endpoint failed: ${testResponse.status}`);
            }

            // Test sites endpoint
            const sitesResponse = await fetch("https://graph.microsoft.com/v1.0/sites", {
                headers: { Authorization: `Bearer ${result.accessToken}` },
            });

            if (sitesResponse.ok) {
                const sites = await sitesResponse.json();
                console.log(`   🏢 Sites access: ✅ (${sites.value?.length || 0} sites found)`);
            } else {
                const errorText = await sitesResponse.text();
                console.log(`   🏢 Sites access: ❌ (${sitesResponse.status})`);
                console.log(`   📝 Error: ${errorText}`);
            }
        } catch (error) {
            console.log(`   ❌ Failed: ${error.message}`);
        }
        console.log();
    }

    // Test with tenant admin consent endpoint
    console.log("Testing tenant admin consent status...");
    try {
        const consentResponse = await fetch(`https://graph.microsoft.com/v1.0/oauth2PermissionGrants?$filter=clientId eq '${MSAL_CLIENT_ID}'`, {
            headers: {
                Authorization: `Bearer ${(await msalApp.acquireTokenByClientCredential({ scopes: ["https://graph.microsoft.com/.default"] })).accessToken}`,
            },
        });

        if (consentResponse.ok) {
            const consent = await consentResponse.json();
            console.log(`   📊 Found ${consent.value?.length || 0} permission grants`);
            consent.value?.forEach((grant, i) => {
                console.log(`   ${i + 1}. Resource: ${grant.resourceId}, Scope: ${grant.scope}`);
            });
        } else {
            console.log(`   ❌ Could not check consent: ${consentResponse.status}`);
        }
    } catch (error) {
        console.log(`   ❌ Consent check failed: ${error.message}`);
    }

    console.log("\n🔧 Additional troubleshooting steps:");
    console.log("1. Check if the app is configured for 'Accounts in this organizational directory only'");
    console.log("2. Verify the app has a client secret (not just a certificate)");
    console.log("3. Check if there are any conditional access policies blocking the app");
    console.log("4. Try creating a new app registration with different permissions");
    console.log("5. Check the Azure AD audit logs for permission grant failures");
}

main().catch(console.error);
