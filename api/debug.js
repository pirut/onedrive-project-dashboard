function envPresence(name) {
    return Boolean(process.env[name]);
}

function truncateText(text, maxLength = 1000) {
    if (!text) return "";
    if (text.length <= maxLength) return text;
    return `${text.slice(0, maxLength)}...`;
}

function parseEntitySets(metadataText, limit = 200) {
    if (!metadataText) return [];
    const entitySets = [];
    const regex = /<EntitySet\s+Name="([^"]+)"\s+EntityType="([^"]+)"\s*\/?>/g;
    let match;
    while ((match = regex.exec(metadataText)) && entitySets.length < limit) {
        entitySets.push({ name: match[1], entityType: match[2] });
    }
    return entitySets;
}

async function runBcDiagnostics() {
    const required = [
        "BC_TENANT_ID",
        "BC_ENVIRONMENT",
        "BC_COMPANY_ID",
        "BC_CLIENT_ID",
        "BC_CLIENT_SECRET",
    ];
    const missing = required.filter((name) => !envPresence(name));
    const apiBase = process.env.BC_API_BASE || "https://api.businesscentral.dynamics.com/v2.0";
    const publisher = process.env.BC_API_PUBLISHER || "cornerstone";
    const group = process.env.BC_API_GROUP || "plannerSync";
    const version = process.env.BC_API_VERSION || "v1.0";
    const tenantId = process.env.BC_TENANT_ID;
    const environment = process.env.BC_ENVIRONMENT;
    const companyId = process.env.BC_COMPANY_ID;

    const diagnostics = {
        ok: false,
        env: {
            ok: missing.length === 0,
            missing,
            config: {
                apiBase,
                tenantId,
                environment,
                companyId,
                publisher,
                group,
                version,
            },
        },
        checks: {},
    };

    if (missing.length > 0) {
        return diagnostics;
    }

    let token;
    try {
        const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
        const body = new URLSearchParams({
            grant_type: "client_credentials",
            client_id: process.env.BC_CLIENT_ID,
            client_secret: process.env.BC_CLIENT_SECRET,
            scope: "https://api.businesscentral.dynamics.com/.default",
        });
        const tokenRes = await fetch(tokenUrl, {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body,
        });
        if (!tokenRes.ok) {
            const text = await tokenRes.text();
            diagnostics.checks.token = {
                ok: false,
                status: tokenRes.status,
                error: truncateText(text),
                url: tokenUrl,
            };
            return diagnostics;
        }
        const tokenData = await tokenRes.json().catch(() => null);
        token = tokenData?.access_token;
        diagnostics.checks.token = {
            ok: Boolean(token),
            status: tokenRes.status,
            url: tokenUrl,
        };
        if (!token) {
            return diagnostics;
        }
    } catch (error) {
        diagnostics.checks.token = {
            ok: false,
            status: 0,
            error: error && error.message ? error.message : String(error),
        };
        return diagnostics;
    }

    const headers = {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
    };
    const baseUrl = `${apiBase}/${tenantId}/${environment}/api/${publisher}/${group}/${version}`;

    try {
        const metadataUrl = `${baseUrl}/$metadata`;
        const metadataRes = await fetch(metadataUrl, { headers });
        const metadataText = await metadataRes.text();
        diagnostics.checks.customApiMetadata = {
            ok: metadataRes.ok,
            status: metadataRes.status,
            url: metadataUrl,
            entitySets: metadataRes.ok ? parseEntitySets(metadataText) : undefined,
            error: metadataRes.ok ? undefined : truncateText(metadataText),
        };
    } catch (error) {
        diagnostics.checks.customApiMetadata = {
            ok: false,
            status: 0,
            error: error && error.message ? error.message : String(error),
        };
    }

    try {
        const companiesUrl = `${apiBase}/${tenantId}/${environment}/api/v2.0/companies`;
        const companiesRes = await fetch(companiesUrl, { headers });
        let companiesCount;
        let companiesError;
        if (companiesRes.ok) {
            const companiesData = await companiesRes.json().catch(() => null);
            if (Array.isArray(companiesData?.value)) {
                companiesCount = companiesData.value.length;
            }
        } else {
            companiesError = truncateText(await companiesRes.text());
        }
        diagnostics.checks.standardCompanies = {
            ok: companiesRes.ok,
            status: companiesRes.status,
            url: companiesUrl,
            count: companiesCount,
            error: companiesError || undefined,
        };
    } catch (error) {
        diagnostics.checks.standardCompanies = {
            ok: false,
            status: 0,
            error: error && error.message ? error.message : String(error),
        };
    }

    try {
        const tasksUrl = `${baseUrl}/projectTasks?$top=1`;
        const tasksRes = await fetch(tasksUrl, { headers });
        let tasksCount;
        let tasksError;
        if (tasksRes.ok) {
            const tasksData = await tasksRes.json().catch(() => null);
            if (Array.isArray(tasksData?.value)) {
                tasksCount = tasksData.value.length;
            }
        } else {
            tasksError = truncateText(await tasksRes.text());
        }
        diagnostics.checks.projectTasksTop1 = {
            ok: tasksRes.ok,
            status: tasksRes.status,
            url: tasksUrl,
            count: tasksCount,
            error: tasksError || undefined,
        };
    } catch (error) {
        diagnostics.checks.projectTasksTop1 = {
            ok: false,
            status: 0,
            error: error && error.message ? error.message : String(error),
        };
    }

    diagnostics.ok =
        diagnostics.env.ok &&
        diagnostics.checks.token?.ok &&
        diagnostics.checks.customApiMetadata?.ok &&
        diagnostics.checks.standardCompanies?.ok;

    return diagnostics;
}

export default async function handler(req, res) {
    const origin = process.env.CORS_ORIGIN || "*";
    res.setHeader("Access-Control-Allow-Origin", origin === "*" ? "*" : origin);
    res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key");
    if (req.method === "OPTIONS") {
        res.status(204).end();
        return;
    }
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    const plannerEnvRequired = [
        "BC_TENANT_ID",
        "BC_ENVIRONMENT",
        "BC_COMPANY_ID",
        "BC_CLIENT_ID",
        "BC_CLIENT_SECRET",
        "GRAPH_TENANT_ID",
        "GRAPH_CLIENT_ID",
        "GRAPH_CLIENT_SECRET",
        "GRAPH_SUBSCRIPTION_CLIENT_STATE",
        "PLANNER_GROUP_ID",
        "SYNC_MODE",
    ];
    const plannerEnvMissing = plannerEnvRequired.filter((name) => !envPresence(name));

    const routes = {
        "GET /api/health": "Base health check",
        "GET /api/debug": "Admin debug info",
        "POST /api/sync/run-bc-to-planner": "BC → Planner sync",
        "POST /api/sync/run-poll": "Planner → BC polling",
        "POST /api/sync/subscriptions/create": "Create Graph subscriptions",
        "POST /api/sync/subscriptions/renew": "Renew Graph subscriptions",
        "POST /api/webhooks/graph/planner?validationToken=debug": "Webhook validation",
    };

    let bcDiagnostics;
    try {
        bcDiagnostics = await runBcDiagnostics();
    } catch (error) {
        bcDiagnostics = {
            ok: false,
            error: error && error.message ? error.message : String(error),
        };
    }

    res.status(200).json({
        ok: true,
        now: new Date().toISOString(),
        plannerEnv: {
            ok: plannerEnvMissing.length === 0,
            missing: plannerEnvMissing,
        },
        bc: bcDiagnostics,
        routes,
    });
}
