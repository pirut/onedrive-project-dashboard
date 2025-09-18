#!/usr/bin/env node
import "dotenv/config";
import { moveFileFromStaging, getAppToken, resolveDrive, getParentItemId, graphFetch } from "../api/ingest-pdf.js";

async function main() {
    const stagingSite = process.env.FASTFIELD_STAGING_SITE_URL || process.env.DEFAULT_SITE_URL;
    const stagingLibrary = process.env.FASTFIELD_STAGING_LIBRARY_PATH;
    const destSite = process.env.DEFAULT_SITE_URL;
    const destLibrary = process.env.DEFAULT_LIBRARY;

    if (!stagingLibrary) {
        console.error("FASTFIELD_STAGING_LIBRARY_PATH is required");
        process.exit(1);
    }
    if (!destSite || !destLibrary) {
        console.error("DEFAULT_SITE_URL and DEFAULT_LIBRARY must be configured");
        process.exit(1);
    }

    const token = await getAppToken();
    const { drive, subPathParts } = await resolveDrive(token, stagingSite, stagingLibrary);
    let folderId = "root";
    if (subPathParts.length) {
        folderId = await getParentItemId(token, drive.id, subPathParts);
    }
    const listPath = folderId === "root"
        ? `/drives/${drive.id}/root/children`
        : `/drives/${drive.id}/items/${folderId}/children`;
    const listing = await graphFetch(listPath, token);
    const files = (listing.value || []).filter((item) => item.file && /\.pdf$/i.test(item.name || ""));

    if (!files.length) {
        console.log("No PDF files found in staging folder.");
        return;
    }

    console.log(`Found ${files.length} PDF(s) in staging. Processing...`);

    for (const file of files) {
        const filename = file.name;
        console.log(`\n▶ Processing ${filename}`);
        try {
            const result = await moveFileFromStaging({
                stagingSiteUrl: stagingSite,
                stagingLibraryPath: stagingLibrary,
                stagingFilename: filename,
                destinationSiteUrl: destSite,
                destinationLibraryPath: destLibrary,
                push: (msg, meta = {}) => {
                    if (meta && Object.keys(meta).length) {
                        console.log(`  - ${msg}`, meta);
                    } else {
                        console.log(`  - ${msg}`);
                    }
                },
                setPhase: () => {},
            });
            console.log(`✅ Moved to ${result.folderName}/${result.filename}`);
        } catch (err) {
            console.error(`❌ Failed to move ${filename}:`, err?.message || err);
        }
    }
}

main().catch((err) => {
    console.error("Fatal error:", err?.message || err);
    process.exit(1);
});
