#!/usr/bin/env node
import "dotenv/config";
import { processStagingJobWalks } from "../lib/process-staging.js";

async function main() {
    const result = await processStagingJobWalks({
        onResult: (entry) => {
            if (entry.status === "ok") {
                console.log(`✅ ${entry.filename} -> ${entry.folderName}`);
            } else {
                console.error(`❌ ${entry.filename}: ${entry.error || "unknown error"}`);
            }
            if (entry.steps) {
                entry.steps.forEach((step) => {
                    console.log(`  - ${step.ts} ${step.msg}`, step.msg ? { ...step, ts: undefined, msg: undefined } : step);
                });
            }
        },
    });

    console.log(`\nProcessed ${result.processed.length} file(s); ${result.errors.length} error(s); scanned ${result.filesScanned}.`);
    if (result.errors.length) process.exitCode = 1;
}

main().catch((err) => {
    console.error("Fatal error:", err?.message || err);
    process.exit(1);
});
