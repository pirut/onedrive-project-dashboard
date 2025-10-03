import "isomorphic-fetch";

function readEnv(name, required = false) {
    const val = process.env[name];
    if (required && !val) throw new Error(`Missing env ${name}`);
    return val;
}

const USPS_CLIENT_ID = readEnv("USPS_CLIENT_ID", true);
const USPS_CLIENT_SECRET = readEnv("USPS_CLIENT_SECRET", true);
const USPS_SCOPE = readEnv("USPS_SCOPE") || null;
const USPS_AUDIENCE = readEnv("USPS_AUDIENCE") || null;
const USPS_API_BASE = readEnv("USPS_API_BASE") || "https://api.usps.com";
const USPS_TOKEN_URL = readEnv("USPS_TOKEN_URL") || `${USPS_API_BASE}/oauth2/v3/token`;

async function fetchToken() {
    const params = new URLSearchParams({ grant_type: "client_credentials" });
    params.set("client_id", USPS_CLIENT_ID);
    params.set("client_secret", USPS_CLIENT_SECRET);
    if (USPS_SCOPE) params.set("scope", USPS_SCOPE);
    if (USPS_AUDIENCE) params.set("audience", USPS_AUDIENCE);

    const basic = Buffer.from(`${USPS_CLIENT_ID}:${USPS_CLIENT_SECRET}`).toString("base64");

    const res = await fetch(USPS_TOKEN_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/x-www-form-urlencoded",
            Accept: "application/json",
            Authorization: `Basic ${basic}`,
            "x-ibm-client-id": USPS_CLIENT_ID,
            "x-ibm-client-secret": USPS_CLIENT_SECRET,
        },
        body: params,
    });

    const text = await res.text();
    let data = null;
    if (text) {
        try {
            data = JSON.parse(text);
        } catch (err) {
            throw new Error(`Token response parse error: ${err.message || err}`);
        }
    }
    if (!res.ok) {
        const detail = data?.error_description || data?.message || text || `HTTP ${res.status}`;
        const err = new Error(`USPS token request failed: ${detail}`);
        err.status = res.status;
        err.payload = data || text;
        throw err;
    }
    return data || {};
}

export default async function handler(_req, res) {
    try {
        const tokenData = await fetchToken();
        const expiresIn = Number(tokenData.expires_in || tokenData.expiresIn || 0) || null;
        const issuedToken = tokenData.access_token || tokenData.accessToken || "";
        const preview = issuedToken ? `${issuedToken.slice(0, 4)}…${issuedToken.slice(-4)}` : null;
        res.status(200).json({ ok: true, expiresIn, tokenPreview: preview, scope: tokenData.scope || null });
    } catch (err) {
        res.status(err.status || 500).json({ ok: false, error: err.message || String(err), detail: err.payload || null });
    }
}
