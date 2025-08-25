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

// Environment variables
const FASTFIELD_API_URL = readEnv("FASTFIELD_API_URL");
const FASTFIELD_AUTH_HEADER = readEnv("FASTFIELD_AUTH_HEADER");
const FASTFIELD_TABLE_ID = readEnv("FASTFIELD_TABLE_ID");

async function checkExistingItems() {
    console.log("🔍 Checking existing items in FastField...");

    // First, authenticate to get a session token
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
        throw new Error(`FastField authentication failed: ${authResponse.status} - ${errorText}`);
    }

    const authResult = await authResponse.json();
    const sessionToken = authResult.sessionToken;
    console.log(`✅ Authentication successful, session expires: ${authResult.sessionExpiration}`);

    // Get existing items
    console.log("2. Fetching existing items...");
    const response = await fetch(`${FASTFIELD_API_URL}/${FASTFIELD_TABLE_ID}/items`, {
        method: "GET",
        headers: {
            "Cache-Control": "no-cache",
            "FastField-API-Key": "08c75cee57ac40afbad2909ce48c68c4",
            "X-Gatekeeper-SessionToken": sessionToken,
        },
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to fetch items: ${response.status} - ${errorText}`);
    }

    const responseData = await response.json();
    console.log(`📊 Raw response:`, JSON.stringify(responseData, null, 2));

    // Handle paginated response structure
    const data = responseData.data?.records || responseData;
    console.log(`📊 Found ${Array.isArray(data) ? data.length : 0} items`);

    if (Array.isArray(data) && data.length > 0) {
        console.log("\n📋 Sample items structure:");
        data.slice(0, 3).forEach((item, index) => {
            console.log(`\nItem ${index + 1}:`);
            console.log(`  ID: ${item.id}`);
            console.log(`  Data:`, JSON.stringify(item.data, null, 2));

            // Check for Project Name field
            if (item.data && item.data["79075b7641bb4bfebd0af28fbc851904"]) {
                console.log(`  ✅ Project Name found: "${item.data["79075b7641bb4bfebd0af28fbc851904"]}"`);
            } else {
                console.log(`  ❌ Project Name field not found`);
                console.log(`  Available keys:`, Object.keys(item.data || {}));
            }
        });

        // Check for duplicates
        const nameCounts = {};
        data.forEach((item) => {
            if (item.data && item.data["79075b7641bb4bfebd0af28fbc851904"]) {
                const name = item.data["79075b7641bb4bfebd0af28fbc851904"];
                nameCounts[name] = (nameCounts[name] || 0) + 1;
            }
        });

        const duplicates = Object.entries(nameCounts).filter(([name, count]) => count > 1);
        if (duplicates.length > 0) {
            console.log(`\n⚠️  Found ${duplicates.length} items with duplicates:`);
            duplicates.forEach(([name, count]) => {
                console.log(`  - "${name}": ${count} instances`);
            });
        } else {
            console.log(`\n✅ No duplicates found`);
        }
    } else {
        console.log("📭 No items found in the datatable");
    }
}

checkExistingItems().catch(console.error);
