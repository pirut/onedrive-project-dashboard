import { exchangeDataverseCode, getDataverseAuthStateSecret, getDataverseOAuthConfig, verifyDataverseAuthState } from "../../../lib/dataverse-oauth.js";
import { saveDataverseRefreshToken } from "../../../lib/dataverse-auth-store.js";

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
        const { code, state, error, error_description: errorDescription } = req.query || {};
        if (error) {
            res.status(400).json({ ok: false, error: String(error), description: errorDescription ? String(errorDescription) : undefined });
            return;
        }
        if (!code || !state) {
            res.status(400).json({ ok: false, error: "Missing code or state" });
            return;
        }
        const origin = getOrigin(req);
        const config = getDataverseOAuthConfig(origin);
        const secret = getDataverseAuthStateSecret();
        if (!verifyDataverseAuthState(String(state), secret)) {
            res.status(400).json({ ok: false, error: "Invalid OAuth state" });
            return;
        }
        const token = await exchangeDataverseCode(config, String(code));
        if (token.refresh_token) {
            await saveDataverseRefreshToken(token.refresh_token);
        }
        const redirect = (process.env.DATAVERSE_AUTH_SUCCESS_REDIRECT || "").trim();
        if (redirect) {
            res.writeHead(302, { Location: redirect });
            res.end();
            return;
        }
        res.status(200).json({ ok: true, hasRefreshToken: Boolean(token.refresh_token), scope: token.scope });
    } catch (error) {
        res.status(400).json({ ok: false, error: error?.message || String(error) });
    }
}
