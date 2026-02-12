import { BusinessCentralClient } from "../../../../../lib/planner-sync/bc-client";
import { getBcSubscription, saveBcSubscription } from "../../../../../lib/planner-sync/bc-webhook-store";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

const DEFAULT_ENTITY_SETS = ["premiumSyncQueue"];
const SUBSCRIPTION_TTL_HOURS = 48;
const RENEWAL_BUFFER_HOURS = 6;
const EXPIRY_BUFFER_MS = 60 * 1000;

function resolveBaseUrl(request: Request) {
    const url = new URL(request.url);
    const proto = request.headers.get("x-forwarded-proto") || url.protocol.replace(":", "");
    const host = request.headers.get("x-forwarded-host") || request.headers.get("host") || url.host;
    return `${proto}://${host}`;
}

function buildExpirationDate() {
    return new Date(Date.now() + SUBSCRIPTION_TTL_HOURS * 60 * 60 * 1000).toISOString();
}

function isExpiringSoon(expirationDateTime: string | undefined | null, bufferMs = EXPIRY_BUFFER_MS) {
    if (!expirationDateTime) return true;
    const expMs = Date.parse(String(expirationDateTime));
    if (!Number.isFinite(expMs)) return true;
    return expMs <= Date.now() + bufferMs;
}

function parseBool(value: unknown) {
    if (value == null) return null;
    if (typeof value === "boolean") return value;
    const normalized = String(value).trim().toLowerCase();
    if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
    return null;
}

async function handle(request: Request) {
    const startTime = Date.now();
    const url = new URL(request.url);
    const requestId = crypto.randomUUID();

    try {
        let body: { entitySets?: string[]; forceRecreate?: unknown } | null = null;
        try {
            const bodyText = await request.text();
            if (bodyText) body = JSON.parse(bodyText);
        } catch (error) {
            logger.warn("Failed to parse request body", {
                requestId,
                error: error instanceof Error ? error.message : String(error),
            });
        }

        const entitySets = Array.isArray(body?.entitySets) && body?.entitySets.length ? body.entitySets : DEFAULT_ENTITY_SETS;
        const forceRecreate = parseBool(body?.forceRecreate ?? url.searchParams.get("forceRecreate")) === true;
        const expirationDateTime = buildExpirationDate();
        const bcClient = new BusinessCentralClient();
        const renewed: string[] = [];
        const created: string[] = [];
        const skipped: string[] = [];
        const failed: Array<{ entitySet: string; error: string }> = [];

        for (const entitySet of entitySets) {
            const normalized = (entitySet || "").trim();
            if (!normalized) continue;
            const stored = await getBcSubscription(normalized);
            const notificationUrl =
                stored?.notificationUrl ||
                process.env.BC_WEBHOOK_NOTIFICATION_URL ||
                `${resolveBaseUrl(request)}/api/webhooks/bc`;
            const clientState = stored?.clientState || (process.env.BC_WEBHOOK_SHARED_SECRET || "").trim() || undefined;
            if (!forceRecreate && stored?.expirationDateTime) {
                const expMs = Date.parse(stored.expirationDateTime);
                const bufferMs = RENEWAL_BUFFER_HOURS * 60 * 60 * 1000;
                if (Number.isFinite(expMs) && expMs > Date.now() + bufferMs) {
                    skipped.push(normalized);
                    continue;
                }
            }

            try {
                if (!forceRecreate && stored?.id) {
                    const updated = await bcClient.renewWebhookSubscription(stored.id, expirationDateTime);
                    await saveBcSubscription(normalized, {
                        ...stored,
                        id: updated?.id || stored.id,
                        expirationDateTime: updated?.expirationDateTime || expirationDateTime,
                    });
                    renewed.push(normalized);
                    continue;
                }
            } catch (error) {
                logger.warn("BC subscription renewal failed", {
                    requestId,
                    entitySet: normalized,
                    error: error instanceof Error ? error.message : String(error),
                });
            }

            try {
                if (stored?.id) {
                    try {
                        await bcClient.deleteWebhookSubscription(stored.id);
                    } catch (error) {
                        logger.warn("Failed to delete old BC subscription before recreate", {
                            requestId,
                            entitySet: normalized,
                            subscriptionId: stored.id,
                            error: error instanceof Error ? error.message : String(error),
                        });
                    }
                }
                const subscription = await bcClient.createWebhookSubscription({
                    entitySet: normalized,
                    notificationUrl,
                    clientState,
                });
                await saveBcSubscription(normalized, {
                    id: subscription?.id || "",
                    entitySet: normalized,
                    resource: subscription?.resource,
                    expirationDateTime: subscription?.expirationDateTime || expirationDateTime,
                    createdAt: new Date().toISOString(),
                    notificationUrl,
                    clientState,
                });
                if (stored?.id && stored.id !== subscription?.id) {
                    try {
                        await bcClient.deleteWebhookSubscription(stored.id);
                    } catch (error) {
                        logger.warn("Failed to delete previous BC subscription after recreate", {
                            requestId,
                            entitySet: normalized,
                            subscriptionId: stored.id,
                            error: error instanceof Error ? error.message : String(error),
                        });
                    }
                }
                created.push(normalized);
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                if (/subscription already exist/i.test(errorMessage)) {
                    const list = await bcClient.listWebhookSubscriptions();
                    const existing = list.find((item) =>
                        String(item?.resource || "").toLowerCase().includes(normalized.toLowerCase())
                    );
                    if (forceRecreate && (existing as { id?: string })?.id) {
                        const existingId = String((existing as { id?: string })?.id || "");
                        if (existingId) {
                            try {
                                await bcClient.deleteWebhookSubscription(existingId);
                            } catch (deleteError) {
                                logger.warn("Failed to delete matching BC subscription before forced recreate", {
                                    requestId,
                                    entitySet: normalized,
                                    subscriptionId: existingId,
                                    error: deleteError instanceof Error ? deleteError.message : String(deleteError),
                                });
                            }
                        }
                        const recreated = await bcClient.createWebhookSubscription({
                            entitySet: normalized,
                            notificationUrl,
                            clientState,
                        });
                        await saveBcSubscription(normalized, {
                            id: recreated?.id || existingId,
                            entitySet: normalized,
                            resource: recreated?.resource || (existing as { resource?: string })?.resource,
                            expirationDateTime: recreated?.expirationDateTime || expirationDateTime,
                            createdAt: new Date().toISOString(),
                            notificationUrl,
                            clientState,
                        });
                        created.push(normalized);
                        continue;
                    }
                    if ((existing as { id?: string })?.id && !isExpiringSoon((existing as { expirationDateTime?: string })?.expirationDateTime)) {
                        await saveBcSubscription(normalized, {
                            id: String((existing as { id?: string }).id || ""),
                            entitySet: normalized,
                            resource: (existing as { resource?: string })?.resource,
                            expirationDateTime: (existing as { expirationDateTime?: string })?.expirationDateTime,
                            createdAt: new Date().toISOString(),
                            notificationUrl,
                            clientState,
                        });
                        renewed.push(normalized);
                        continue;
                    }
                }
                failed.push({ entitySet: normalized, error: errorMessage });
            }
        }

        const duration = Date.now() - startTime;
        return new Response(
            JSON.stringify({ ok: true, forceRecreate, renewed, created, skipped, failed, requestId, duration }),
            {
                status: 200,
                headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
            }
        );
    } catch (error) {
        const duration = Date.now() - startTime;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error("POST /api/sync/bc-subscriptions/renew - Failed", { requestId, duration, error: errorMessage });
        return new Response(JSON.stringify({ ok: false, error: errorMessage, requestId, duration }), {
            status: 500,
            headers: { "Content-Type": "application/json", "X-Request-ID": requestId },
        });
    }
}

export async function POST(request: Request) {
    return handle(request);
}

export async function GET(request: Request) {
    return handle(request);
}
