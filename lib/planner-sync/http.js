import { logger } from "./logger.js";

const DEFAULT_RETRY_STATUS = [429, 503, 504];

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfter(headerValue) {
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

export async function fetchWithRetry(url, options, retryOptions = {}) {
    const retries = retryOptions.retries ?? 4;
    const baseDelayMs = retryOptions.baseDelayMs ?? 500;
    const retryStatusCodes = retryOptions.retryStatusCodes ?? DEFAULT_RETRY_STATUS;
    let lastError = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
        try {
            const res = await fetch(url, options);
            if (!retryStatusCodes.includes(res.status) || attempt === retries) {
                return res;
            }
            const retryAfter = parseRetryAfter(res.headers.get("retry-after"));
            const delay = retryAfter ?? baseDelayMs * Math.pow(2, attempt);
            logger.warn("Fetch retry scheduled", { url, status: res.status, attempt, delayMs: delay });
            await sleep(delay);
        } catch (error) {
            lastError = error instanceof Error ? error : new Error(String(error));
            if (attempt === retries) {
                break;
            }
            const delay = baseDelayMs * Math.pow(2, attempt);
            logger.warn("Fetch retry after error", { url, attempt, delayMs: delay, error: lastError.message });
            await sleep(delay);
        }
    }

    throw lastError ?? new Error(`Fetch failed for ${url}`);
}

export async function readResponseText(res) {
    try {
        return await res.text();
    } catch {
        return "";
    }
}

export async function readResponseJson(res) {
    const contentType = res.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) return null;
    try {
        return await res.json();
    } catch {
        return null;
    }
}
