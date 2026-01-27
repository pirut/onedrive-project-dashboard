export function getCronSecret() {
    return (process.env.CRON_SECRET || "").trim();
}

export function isCronAuthorized(provided: string | null | undefined) {
    const expected = getCronSecret();
    if (!expected) return false;
    return provided === expected;
}
