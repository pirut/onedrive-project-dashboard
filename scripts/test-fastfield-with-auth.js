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
    console.log("🔍 Testing FastField with provided Basic Auth header...\n");

    const FASTFIELD_AUTH_HEADER = readEnv("FASTFIELD_AUTH_HEADER", true);
    const FASTFIELD_TABLE_NAME = readEnv("FASTFIELD_TABLE_NAME") || "Cornerstone Active Projects";

    try {
        // Step 1: Authenticate
        console.log("1. Authenticating with FastField...");
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
            console.log(`❌ Authentication failed: ${authResponse.status}`);
            console.log(`📝 Error: ${errorText}`);
            return;
        }

        const authResult = await authResponse.json();
        console.log("✅ Authentication successful!");
        console.log(`📝 Session Token: ${authResult.sessionToken}`);
        console.log(`⏰ Expires: ${authResult.sessionExpiration}`);

        // Use the actual session token from the response
        const sessionToken = authResult.sessionToken;

        // Step 2: Get datatables list
        console.log("\n2. Getting datatables list...");
        const tablesResponse = await fetch("https://api.fastfieldforms.com/services/v3/datatables/list", {
            method: "GET",
            headers: {
                "Cache-Control": "no-cache",
                "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
                "X-Gatekeeper-SessionToken": sessionToken,
            },
        });

        if (!tablesResponse.ok) {
            const errorText = await tablesResponse.text();
            console.log(`❌ Failed to get datatables: ${tablesResponse.status}`);
            console.log(`📝 Error: ${errorText}`);
            return;
        }

        const tablesResponseData = await tablesResponse.json();
        console.log(`✅ Found ${tablesResponseData.length || 0} datatables`);
        console.log(`📝 Raw response:`, JSON.stringify(tablesResponseData, null, 2));

        // Handle different response formats
        const tables = Array.isArray(tablesResponseData)
            ? tablesResponseData
            : tablesResponseData.value
            ? tablesResponseData.value
            : tablesResponseData.data
            ? tablesResponseData.data
            : [];

        console.log(`📊 Processed tables array length: ${tables.length}`);

        // Look for our target table
        const targetTable = tables.find(
            (table) =>
                table.name === FASTFIELD_TABLE_NAME ||
                table.displayName === FASTFIELD_TABLE_NAME ||
                table.name?.toLowerCase().includes("cornerstone") ||
                table.displayName?.toLowerCase().includes("cornerstone")
        );

        if (targetTable) {
            console.log("\n🎯 Found target table:");
            console.log(`   🆔 Table ID: ${targetTable.listId}`);
            console.log(`   📝 Table Name: ${targetTable.name}`);
            console.log(`   📋 Last Sync: ${targetTable.lastSyncAt}`);
            console.log(`   📊 Fields: ${targetTable.config?.fields?.length || 0}`);

            // Show available fields
            if (targetTable.config?.fields) {
                console.log("   📋 Available fields:");
                targetTable.config.fields.forEach((field) => {
                    console.log(`      - ${field.displayName} (${field.type})${field.isRequired ? " *required" : ""}`);
                });
            }

            // Step 3: Test creating an item
            console.log("\n3. Testing item creation...");
            const testPayload = {
                data: {
                    "Project Name": "Test Project - " + new Date().toISOString(),
                    "Sharepoint Link": "https://example.com/test-project",
                },
            };

            const createResponse = await fetch(`https://api.fastfieldforms.com/services/v3/datatables/${targetTable.listId}/items`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                    "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
                    "X-Gatekeeper-SessionToken": sessionToken,
                },
                body: JSON.stringify(testPayload),
            });

            if (createResponse.ok) {
                const result = await createResponse.json();
                console.log("✅ Item creation successful!");
                console.log(`📝 Response:`, JSON.stringify(result, null, 2));

                console.log("\n🎉 SUCCESS! FastField API is working correctly.");
                console.log(`📋 Use these environment variables:`);
                console.log(`   FASTFIELD_AUTH_HEADER=${FASTFIELD_AUTH_HEADER}`);
                console.log(`   FASTFIELD_TABLE_ID=${targetTable.listId}`);
            } else {
                const errorText = await createResponse.text();
                console.log(`❌ Item creation failed: ${createResponse.status}`);
                console.log(`📝 Error: ${errorText}`);
            }
        } else {
            console.log("❌ Target table not found");
            console.log("📋 Available tables:");
            tables.forEach((table, index) => {
                console.log(`   ${index + 1}. ${table.name || table.displayName} (ID: ${table.id})`);
            });
        }
    } catch (error) {
        console.error("❌ Failed:", error.message);
    }

    console.log("\n🔧 Next steps:");
    console.log("1. Add the table ID to your environment variables");
    console.log("2. Test the full sync process");
    console.log("3. Deploy to Vercel");
}

main().catch(console.error);
