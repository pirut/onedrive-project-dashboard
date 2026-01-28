import { headers } from "next/headers";
import { exchangeDataverseCode, getDataverseAuthStateSecret, getDataverseOAuthConfig, verifyDataverseAuthState } from "../../../../../lib/dataverse-oauth";
import { saveDataverseRefreshToken } from "../../../../../lib/dataverse-auth-store";

function getOrigin() {
    const hdrs = headers();
    const proto = hdrs.get("x-forwarded-proto") || "https";
    const host = hdrs.get("x-forwarded-host") || hdrs.get("host");
    return host ? `${proto}://${host}` : "";
}

export async function GET(request: Request) {
    try {
        const url = new URL(request.url);
        const error = url.searchParams.get("error");
        const errorDescription = url.searchParams.get("error_description");
        if (error) {
            return new Response(
                JSON.stringify({ ok: false, error, description: errorDescription || undefined }, null, 2),
                { status: 400, headers: { "Content-Type": "application/json" } }
            );
        }
        const code = url.searchParams.get("code");
        const state = url.searchParams.get("state");
        if (!code || !state) {
            return new Response(JSON.stringify({ ok: false, error: "Missing code or state" }, null, 2), {
                status: 400,
                headers: { "Content-Type": "application/json" },
            });
        }
        const origin = getOrigin();
        const config = getDataverseOAuthConfig(origin);
        const secret = getDataverseAuthStateSecret();
        if (!verifyDataverseAuthState(state, secret)) {
            return new Response(JSON.stringify({ ok: false, error: "Invalid OAuth state" }, null, 2), {
                status: 400,
                headers: { "Content-Type": "application/json" },
            });
        }
        const token = await exchangeDataverseCode(config, code);
        if (token.refresh_token) {
            await saveDataverseRefreshToken(token.refresh_token);
        }
        const redirect = (process.env.DATAVERSE_AUTH_SUCCESS_REDIRECT || "").trim();
        if (redirect) {
            return Response.redirect(redirect, 302);
        }
        return new Response(
            JSON.stringify({ ok: true, hasRefreshToken: Boolean(token.refresh_token), scope: token.scope }, null, 2),
            { status: 200, headers: { "Content-Type": "application/json" } }
        );
    } catch (error) {
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
