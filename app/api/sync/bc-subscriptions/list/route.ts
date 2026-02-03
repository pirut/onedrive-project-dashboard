import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { getBcConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["projectTasks"];

function normalizeValue(value: string | undefined | null) {
    return (value || "").trim().replace(/^\/+/, "").toLowerCase();
}

function matchesResource(resource: string | undefined | null, entitySet: string) {
    const normalized = normalizeValue(resource);
    if (!normalized) return false;
    const { publisher, group, version, companyId } = getBcConfig();
    const expected = normalizeValue(`api/${publisher}/${group}/${version}/companies(${companyId})/${entitySet}`);
    if (normalized === expected) return true;
    const entityLower = normalizeValue(entitySet);
    const companyToken = normalizeValue(`companies(${companyId})/${entityLower}`);
    if (companyToken && normalized.includes(companyToken)) return true;
    if (entityLower && normalized.endsWith(`/${entityLower}`)) return true;
    return false;
}

function resolveNotificationUrl(request: Request, url: URL) {
    const configured = (process.env.BC_WEBHOOK_NOTIFICATION_URL || "").trim();
    const fromQuery = (url.searchParams.get("notificationUrl") || "").trim();
    if (fromQuery) return fromQuery;
    if (configured) return configured;
    const proto = request.headers.get("x-forwarded-proto") || "https";
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || "localhost";
    return `${proto}://${host}/api/webhooks/bc`;
}

function resolveEntitySets(url: URL) {
    const multi = url.searchParams.getAll("entitySet");
    if (multi.length) return multi.filter(Boolean);
    const csv = (url.searchParams.get("entitySets") || "").trim();
    if (csv) return csv.split(",").map((item) => item.trim()).filter(Boolean);
    return DEFAULT_ENTITY_SETS;
}

export async function GET(request: Request) {
    const url = new URL(request.url);
    const entitySets = resolveEntitySets(url);
    const notificationUrl = resolveNotificationUrl(request, url);
    const normalizedNotificationUrl = normalizeValue(notificationUrl);

    try {
        const bcClient = new BusinessCentralClient();
        const list = await bcClient.listWebhookSubscriptions();
        const stored = [];
        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const entry = await getBcSubscription(normalized);
            stored.push({
                entitySet: normalized,
                storedId: entry?.id || null,
                storedResource: entry?.resource || null,
                storedExpiration: entry?.expirationDateTime || null,
            });
        }

        const items = list.map((item) => {
            const matches = entitySets.filter((entitySet) => matchesResource(item?.resource, entitySet));
            const notificationMatch = normalizedNotificationUrl
                ? normalizeValue(item?.notificationUrl) === normalizedNotificationUrl
                : null;
            return {
                id: (item as { id?: string })?.id || null,
                resource: (item as { resource?: string })?.resource || null,
                notificationUrl: (item as { notificationUrl?: string })?.notificationUrl || null,
                clientState: (item as { clientState?: string })?.clientState || null,
                expirationDateTime: (item as { expirationDateTime?: string })?.expirationDateTime || null,
                matches,
                notificationMatch,
            };
        });

        return new Response(
            JSON.stringify({
                ok: true,
                entitySets,
                notificationUrl,
                count: items.length,
                stored,
                items,
            }),
            {
                status: 200,
                headers: { "Content-Type": "application/json" },
            },
        );
    } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("GET /api/sync/bc-subscriptions/list - Failed", { error: errorMessage });
        return new Response(JSON.stringify({ ok: false, error: errorMessage }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
        });
    }
}
