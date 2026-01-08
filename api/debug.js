function envPresence(name) {
    return Boolean(process.env[name]);
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

    res.status(200).json({
        ok: true,
        now: new Date().toISOString(),
        plannerEnv: {
            ok: plannerEnvMissing.length === 0,
            missing: plannerEnvMissing,
        },
        routes,
    });
}
