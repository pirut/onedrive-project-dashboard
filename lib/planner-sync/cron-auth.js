export function getCronSecret() {
    return (process.env.CRON_SECRET || "").trim();
}

export function isCronAuthorized(provided) {
    const expected = getCronSecret();
    if (!expected) return false;
    return provided === expected;
}
