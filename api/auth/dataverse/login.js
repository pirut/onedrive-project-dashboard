import { buildDataverseAuthorizeUrl, createDataverseAuthState, getDataverseAuthStateSecret, getDataverseOAuthConfig, warnIfMissingAuthSecret } from "../../../lib/dataverse-oauth.js";

function getOrigin(req) {
    const proto = req.headers["x-forwarded-proto"] || "https";
    const host = req.headers["x-forwarded-host"] || req.headers.host;
    return host ? `${proto}://${host}` : "";
}

export default async function handler(req, res) {
    if (req.method !== "GET") {
        res.status(405).json({ ok: false, error: "Method not allowed" });
        return;
    }

    try {
        warnIfMissingAuthSecret();
        const origin = getOrigin(req);
        const config = getDataverseOAuthConfig(origin);
        const secret = getDataverseAuthStateSecret();
        const state = createDataverseAuthState(secret);
        const prompt = req.query?.prompt ? String(req.query.prompt) : undefined;
        const url = buildDataverseAuthorizeUrl(config, state, prompt);
        res.writeHead(302, { Location: url });
        res.end();
    } catch (error) {
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
