import { getBcWebhookEmitter, listBcWebhookLog } from "../../../../lib/planner-sync/bc-webhook-log";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
    const url = new URL(request.url);
    const include = url.searchParams.get("include") === "1";
    const encoder = new TextEncoder();
    const stream = new TransformStream();
    const writer = stream.writable.getWriter();
    const emitter = getBcWebhookEmitter();
    let closed = false;

    const write = async (line: string) => {
        if (closed) return;
        await writer.write(encoder.encode(line));
    };

    const close = () => {
        if (closed) return;
        closed = true;
        emitter.off("entry", send);
        clearInterval(keepAlive);
        writer.close().catch(() => undefined);
    };

    const send = (entry: unknown) => {
        write(`data: ${JSON.stringify(entry)}\n\n`).catch(() => undefined);
    };

    if (include) {
        const items = await listBcWebhookLog(20);
        for (const entry of items.reverse()) {
            await write(`data: ${JSON.stringify(entry)}\n\n`);
        }
    }

    emitter.on("entry", send);
    const keepAlive = setInterval(() => {
        write(": ping\n\n").catch(() => undefined);
    }, 25000);

    request.signal.addEventListener("abort", close);
    if (request.signal.aborted) close();

    return new Response(stream.readable, {
        status: 200,
        headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache, no-transform",
            Connection: "keep-alive",
        },
    });
}
