import { headers } from "next/headers";
import { buildDataverseAuthorizeUrl, createDataverseAuthState, getDataverseAuthStateSecret, getDataverseOAuthConfig, warnIfMissingAuthSecret } from "../../../../../lib/dataverse-oauth";

function getOrigin() {
    const hdrs = headers();
    const proto = hdrs.get("x-forwarded-proto") || "https";
    const host = hdrs.get("x-forwarded-host") || hdrs.get("host");
    return host ? `${proto}://${host}` : "";
}

export async function GET(request: Request) {
    try {
        warnIfMissingAuthSecret();
        const origin = getOrigin();
        const config = getDataverseOAuthConfig(origin);
        const secret = getDataverseAuthStateSecret();
        const state = createDataverseAuthState(secret);
        const prompt = new URL(request.url).searchParams.get("prompt") || undefined;
        const url = buildDataverseAuthorizeUrl(config, state, prompt || undefined);
        return Response.redirect(url, 302);
    } catch (error) {
        return new Response(JSON.stringify({ ok: false, error: (error as Error)?.message || String(error) }, null, 2), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }
}
