import { enqueueAndProcessNotifications } from "../../../../../lib/planner-sync";
import { getGraphConfig } from "../../../../../lib/planner-sync/config";
import { logger } from "../../../../../lib/planner-sync/logger";

export const dynamic = "force-dynamic";

type GraphNotification = {
    subscriptionId?: string;
    clientState?: string;
    resource?: string;
    resourceData?: {
        id?: string;
    };
};

export async function POST(request: Request) {
    const url = new URL(request.url);
    const validationToken = url.searchParams.get("validationToken");
    if (validationToken) {
        return new Response(validationToken, {
            status: 200,
            headers: { "Content-Type": "text/plain" },
        });
    }

    let payload: { value?: GraphNotification[] } | null = null;
    try {
        payload = await request.json();
    } catch {
        return new Response(JSON.stringify({ ok: false, error: "Invalid JSON" }), {
            status: 400,
            headers: { "Content-Type": "application/json" },
        });
    }

    const notifications = payload?.value || [];
    if (!notifications.length) {
        return new Response(JSON.stringify({ ok: true, received: 0 }), {
            status: 202,
            headers: { "Content-Type": "application/json" },
        });
    }

    const { clientState } = getGraphConfig();
    const items = notifications
        .map((notification) => {
            if (notification.clientState !== clientState) {
                logger.warn("Graph notification clientState mismatch", {
                    subscriptionId: notification.subscriptionId,
                });
                return null;
            }
            const taskId = notification.resourceData?.id || notification.resource?.split("/").pop();
            if (!taskId) return null;
            return {
                taskId,
                subscriptionId: notification.subscriptionId,
                receivedAt: new Date().toISOString(),
            };
        })
        .filter(Boolean) as { taskId: string; subscriptionId?: string; receivedAt: string }[];

    if (items.length) {
        enqueueAndProcessNotifications(items).catch((error) => {
            logger.error("Failed to enqueue planner notifications", { error: (error as Error)?.message });
        });
    }

    return new Response(JSON.stringify({ ok: true, received: items.length }), {
        status: 202,
        headers: { "Content-Type": "application/json" },
    });
}
