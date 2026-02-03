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
    console.log("🔍 Testing SharePoint REST API access...\n");

    const TENANT_ID = readEnv("TENANT_ID", true);
    const MSAL_CLIENT_ID = readEnv("MSAL_CLIENT_ID", true);
    const MICROSOFT_CLIENT_SECRET = readEnv("MICROSOFT_CLIENT_SECRET", true);
    const DEFAULT_SITE_URL = readEnv("DEFAULT_SITE_URL", true);

    console.log(`Testing access to: ${DEFAULT_SITE_URL}\n`);

    try {
        // Get token for SharePoint
        const tokenResponse = await fetch(`https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`, {
            method: "POST",
            headers: {
                "Content-Type": "application/x-www-form-urlencoded",
            },
            body: new URLSearchParams({
                grant_type: "client_credentials",
                client_id: MSAL_CLIENT_ID,
                client_secret: MICROSOFT_CLIENT_SECRET,
                scope: "https://cornerstonecompaniesflc.sharepoint.com/.default",
            }),
        });

        if (!tokenResponse.ok) {
            const errorText = await tokenResponse.text();
            console.log(`❌ Token request failed: ${tokenResponse.status}`);
            console.log(`📝 Error: ${errorText}`);
            return;
        }

        const tokenData = await tokenResponse.json();
        console.log("✅ SharePoint token acquired successfully");
        console.log(`📝 Token expires in: ${tokenData.expires_in} seconds`);

        // Test SharePoint REST API
        console.log("\n🏢 Testing SharePoint REST API...");

        // Test site info
        const siteResponse = await fetch(`${DEFAULT_SITE_URL}/_api/web`, {
            headers: {
                Authorization: `Bearer ${tokenData.access_token}`,
                Accept: "application/json;odata=verbose",
            },
        });

        if (siteResponse.ok) {
            const siteData = await siteResponse.json();
            console.log("✅ Site access successful!");
            console.log(`📝 Site title: ${siteData.d?.Title || "N/A"}`);
            console.log(`🆔 Site ID: ${siteData.d?.Id || "N/A"}`);
            console.log(`🔗 Site URL: ${siteData.d?.Url || "N/A"}`);

            // Test getting lists/libraries
            console.log("\n📚 Testing document libraries...");
            const listsResponse = await fetch(`${DEFAULT_SITE_URL}/_api/web/lists?$filter=BaseTemplate eq 101`, {
                headers: {
                    Authorization: `Bearer ${tokenData.access_token}`,
                    Accept: "application/json;odata=verbose",
                },
            });

            if (listsResponse.ok) {
                const listsData = await listsResponse.json();
                console.log(`✅ Document libraries found: ${listsData.d?.results?.length || 0}`);

                listsData.d?.results?.forEach((list, index) => {
                    console.log(`   ${index + 1}. ${list.Title} (${list.EntityTypeName})`);
                });

                // Test getting folders from the first document library
                if (listsData.d?.results?.length > 0) {
                    const firstList = listsData.d.results[0];
                    console.log(`\n📁 Testing folders in: ${firstList.Title}`);

                    const foldersResponse = await fetch(
                        `${DEFAULT_SITE_URL}/_api/web/lists('${firstList.Id}')/items?$filter=FSObjType eq 1&$select=Title,Id,FileLeafRef`,
                        {
                            headers: {
                                Authorization: `Bearer ${tokenData.access_token}`,
                                Accept: "application/json;odata=verbose",
                            },
                        }
                    );

                    if (foldersResponse.ok) {
                        const foldersData = await foldersResponse.json();
                        console.log(`✅ Found ${foldersData.d?.results?.length || 0} folders`);

                        foldersData.d?.results?.slice(0, 5).forEach((folder, index) => {
                            console.log(`   ${index + 1}. ${folder.Title || folder.FileLeafRef}`);
                        });

                        if (foldersData.d?.results?.length > 5) {
                            console.log(`   ... and ${foldersData.d.results.length - 5} more folders`);
                        }
                    } else {
                        console.log(`❌ Failed to get folders: ${foldersResponse.status}`);
                    }
                }
            } else {
                console.log(`❌ Failed to get document libraries: ${listsResponse.status}`);
            }
        } else {
            const errorText = await siteResponse.text();
            console.log(`❌ Site access failed: ${siteResponse.status}`);
            console.log(`📝 Error: ${errorText}`);
        }
    } catch (error) {
        console.error("❌ Authentication failed:", error.message);
    }

    console.log("\n🔧 If this works, we can use SharePoint REST API instead of Microsoft Graph");
    console.log("This might work even if Graph permissions haven't propagated yet");
}

main().catch(console.error);
