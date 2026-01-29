import { DataverseClient } from "../lib/dataverse-client.js";
import { getDataverseMappingConfig } from "../lib/premium-sync/config.js";

function parseArgs(argv) {
    const out = {
        dryRun: true,
        projects: [],
        limit: 0,
    };
    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        if (arg === "--confirm") {
            out.dryRun = false;
        } else if (arg === "--dry-run") {
            out.dryRun = true;
        } else if (arg === "--project" || arg === "--projectNo") {
            const value = argv[i + 1];
            if (value) {
                out.projects.push(String(value));
                i += 1;
            }
        } else if (arg === "--projects" || arg === "--projectNos") {
            const value = argv[i + 1];
            if (value) {
                out.projects.push(...String(value).split(",").map((v) => v.trim()).filter(Boolean));
                i += 1;
            }
        } else if (arg === "--limit") {
            const value = Number(argv[i + 1]);
            if (Number.isFinite(value) && value > 0) {
                out.limit = Math.floor(value);
                i += 1;
            }
        }
    }
    return out;
}

function escapeODataString(value) {
    return String(value).replace(/'/g, "''");
}

async function listProjects(dataverse, mapping, projects, limit) {
    const projectField = mapping.projectBcNoField;
    if (!projectField) {
        throw new Error("Missing DATAVERSE_BC_PROJECT_NO_FIELD.");
    }
    const select = [mapping.projectIdField, mapping.projectTitleField, projectField].filter(Boolean);
    let filter = `${projectField} ne null and ${projectField} ne ''`;
    if (projects.length) {
        const clauses = projects.map((value) => `${projectField} eq '${escapeODataString(value)}'`);
        filter = clauses.join(" or ");
    }

    const items = [];
    let nextLink = null;
    let first = true;
    do {
        let res;
        if (first) {
            res = await dataverse.list(mapping.projectEntitySet, {
                select,
                filter,
                top: 200,
            });
            items.push(...res.value);
            nextLink = res.nextLink || null;
            first = false;
        } else if (nextLink) {
            const resp = await dataverse.request(nextLink);
            const data = await resp.json();
            if (Array.isArray(data?.value)) items.push(...data.value);
            nextLink = data?."@odata.nextLink" || null;
        }
        if (limit && items.length >= limit) {
            return items.slice(0, limit);
        }
    } while (nextLink);

    return items;
}

async function run() {
    const args = parseArgs(process.argv.slice(2));
    const dataverse = new DataverseClient();
    const mapping = getDataverseMappingConfig();
    const projects = await listProjects(dataverse, mapping, args.projects, args.limit);

    if (!projects.length) {
        console.log("No BC-linked Dataverse projects found.");
        return;
    }

    console.log(`Found ${projects.length} BC-linked Dataverse projects.`);
    for (const project of projects) {
        const id = project[mapping.projectIdField];
        const title = project[mapping.projectTitleField];
        const bcNo = project[mapping.projectBcNoField];
        console.log(`- ${String(bcNo || "").padEnd(12)} | ${String(title || "(no title)")}`);
    }

    if (args.dryRun) {
        console.log("\nDry run only. Re-run with --confirm to delete these projects.");
        return;
    }

    console.log("\nDeleting projects...");
    let deleted = 0;
    let failed = 0;
    for (const project of projects) {
        const id = project[mapping.projectIdField];
        if (!id) {
            failed += 1;
            console.warn("Missing project id, skipping.");
            continue;
        }
        try {
            await dataverse.delete(mapping.projectEntitySet, String(id));
            deleted += 1;
        } catch (error) {
            failed += 1;
            console.warn(`Failed to delete ${id}: ${error?.message || String(error)}`);
        }
    }

    console.log(`Done. Deleted ${deleted}. Failed ${failed}.`);
}

run().catch((error) => {
    console.error(error?.message || String(error));
    process.exit(1);
});
