import { DataverseClient } from "../lib/dataverse-client.js";
import { BusinessCentralClient } from "../lib/planner-sync/bc-client.js";

function envPresence(name) {
    return Boolean(process.env[name]);
}

async function runDataverseCheck() {
    try {
        const client = new DataverseClient();
        const who = await client.whoAmI();
        return { ok: true, whoAmI: who };
    } catch (error) {
        return { ok: false, error: error?.message || String(error) };
    }
}

async function runBcCheck() {
    try {
        const bc = new BusinessCentralClient();
        const projects = await bc.listProjects();
        return { ok: true, projects: projects.length };
    } catch (error) {
        return { ok: false, error: error?.message || String(error) };
    }
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const graphEnvRequired = ["TENANT_ID", "MSAL_CLIENT_ID", "MSAL_CLIENT_SECRET", "DEFAULT_SITE_URL", "DEFAULT_LIBRARY"];
    const bcEnvRequired = ["BC_TENANT_ID", "BC_ENVIRONMENT", "BC_COMPANY_ID", "BC_CLIENT_ID", "BC_CLIENT_SECRET"];
    const dataverseEnvRequired = [
        "DATAVERSE_BASE_URL",
        "DATAVERSE_TENANT_ID",
        "DATAVERSE_CLIENT_ID",
        "DATAVERSE_CLIENT_SECRET",
    ];

    const graphMissing = graphEnvRequired.filter((name) => !envPresence(name));
    const bcMissing = bcEnvRequired.filter((name) => !envPresence(name));
    const dataverseMissing = dataverseEnvRequired.filter((name) => !envPresence(name));

    const checks = {
        dataverse: dataverseMissing.length ? { ok: false, error: "Missing Dataverse env" } : await runDataverseCheck(),
        businessCentral: bcMissing.length ? { ok: false, error: "Missing BC env" } : await runBcCheck(),
    };

    const routes = {
        "POST /api/sync/bc-to-premium": "BC → Planner Premium sync",
        "POST /api/sync/premium-to-bc": "Premium → BC sync (Dataverse delta)",
        "POST /api/sync/auto": "Auto sync (choose most recent changes)",
        "GET /api/sync/premium-project-link": "Resolve Premium plan link for a BC project",
        "GET /api/sync/debug-bookable-resource": "Debug Dataverse bookable resource lookup",
        "GET /api/sync/list-bookable-resources": "List Dataverse bookable resources",
        "GET /api/sync/debug-project-team": "List Dataverse project team members",
        "GET /api/sync/debug-operation-sets": "List Dataverse operation sets (schedule API)",
        "POST /api/sync/clear-operation-sets": "Delete Dataverse operation sets (schedule API)",
        "POST /api/sync/premium-change/poll": "Poll Premium changes via Dataverse delta (legacy)",
        "POST /api/webhooks/dataverse": "Dataverse webhook receiver",
        "POST /api/webhooks/bc": "Business Central webhook receiver",
        "GET /api/sync/projects": "Premium project list + sync state",
        "POST /api/sync/projects": "Toggle per-project sync or clear links",
        "POST /api/sync/bc-jobs/process": "Process BC webhook job queue",
        "POST /api/sync/clear-bc-sync-lock": "Clear BC syncLock for a task",
        "POST /api/sync/bc-subscriptions/create": "Create BC webhook subscription",
        "POST /api/sync/bc-subscriptions/renew": "Renew BC webhook subscription",
        "POST /api/sync/bc-subscriptions/delete": "Delete BC webhook subscription",
    };

    res.status(200).json({
        ok: true,
        timestamp: new Date().toISOString(),
        env: {
            graph: { ok: graphMissing.length === 0, missing: graphMissing },
            businessCentral: { ok: bcMissing.length === 0, missing: bcMissing },
            dataverse: { ok: dataverseMissing.length === 0, missing: dataverseMissing },
        },
        checks,
        routes,
    });
}
