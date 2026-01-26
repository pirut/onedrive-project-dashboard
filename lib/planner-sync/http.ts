import { logger } from "./logger";

export type FetchRetryOptions = {
    retries?: number;
    baseDelayMs?: number;
    retryStatusCodes?: number[];
    maxDelayMs?: number;
    maxTotalDelayMs?: number;
};

const DEFAULT_RETRY_STATUS = [429, 503, 504];

function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfter(headerValue: string | null) {
    if (!headerValue) return null;
    const seconds = Number(headerValue);
    if (!Number.isNaN(seconds) && seconds > 0) return seconds * 1000;
    const date = Date.parse(headerValue);
    if (!Number.isNaN(date)) {
        const delay = date - Date.now();
        return delay > 0 ? delay : null;
    }
    return null;
}

function readOptionalNumber(name: string) {
    const value = process.env[name];
    if (!value) return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
}

export async function fetchWithRetry(
    url: string,
    options: RequestInit,
    retryOptions: FetchRetryOptions = {}
): Promise<Response> {
    const retries = retryOptions.retries ?? 4;
    const baseDelayMs = retryOptions.baseDelayMs ?? 500;
    const retryStatusCodes = retryOptions.retryStatusCodes ?? DEFAULT_RETRY_STATUS;
    const envMaxDelayMs = readOptionalNumber("FETCH_RETRY_MAX_DELAY_MS");
    const envMaxTotalDelayMs = readOptionalNumber("FETCH_RETRY_MAX_TOTAL_MS");
    const vercelMaxDelayMs = process.env.VERCEL ? 5000 : null;
    const vercelMaxTotalDelayMs = process.env.VERCEL ? 15000 : null;
    const maxDelayMs = retryOptions.maxDelayMs ?? envMaxDelayMs ?? vercelMaxDelayMs;
    const maxTotalDelayMs = retryOptions.maxTotalDelayMs ?? envMaxTotalDelayMs ?? vercelMaxTotalDelayMs;
    const start = Date.now();
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
        try {
            const res = await fetch(url, options);
            if (!retryStatusCodes.includes(res.status) || attempt === retries) {
                return res;
            }
            const retryAfter = parseRetryAfter(res.headers.get("retry-after"));
            let delay = retryAfter ?? baseDelayMs * Math.pow(2, attempt);
            if (maxDelayMs != null) delay = Math.min(delay, maxDelayMs);
            const elapsed = Date.now() - start;
            if (maxTotalDelayMs != null && elapsed + delay > maxTotalDelayMs) {
                logger.warn("Fetch retry budget exceeded; returning last response", {
                    url,
                    status: res.status,
                    attempt,
                    delayMs: delay,
                    elapsedMs: elapsed,
                    maxTotalDelayMs,
                });
                return res;
            }
            logger.info("Fetch retry scheduled", { url, status: res.status, attempt, delayMs: delay });
            await sleep(delay);
        } catch (error) {
            lastError = error instanceof Error ? error : new Error(String(error));
            if (attempt === retries) {
                break;
            }
            let delay = baseDelayMs * Math.pow(2, attempt);
            if (maxDelayMs != null) delay = Math.min(delay, maxDelayMs);
            const elapsed = Date.now() - start;
            if (maxTotalDelayMs != null && elapsed + delay > maxTotalDelayMs) {
                logger.warn("Fetch retry budget exceeded after error", {
                    url,
                    attempt,
                    delayMs: delay,
                    elapsedMs: elapsed,
                    maxTotalDelayMs,
                    error: lastError.message,
                });
                break;
            }
            logger.info("Fetch retry after error", { url, attempt, delayMs: delay, error: lastError.message });
            await sleep(delay);
        }
    }

    throw lastError ?? new Error(`Fetch failed for ${url}`);
}

export async function readResponseText(res: Response) {
    try {
        return await res.text();
    } catch {
        return "";
    }
}

export async function readResponseJson<T>(res: Response): Promise<T | null> {
    const contentType = res.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) return null;
    try {
        return (await res.json()) as T;
    } catch {
        return null;
    }
}
