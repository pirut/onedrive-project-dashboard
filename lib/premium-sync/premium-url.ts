export type PremiumProjectUrlContext = {
    tenantId?: string;
    orgId?: string;
};

export function getTenantIdForUrl() {
    return process.env.TENANT_ID || "";
}

export function getPremiumProjectUrlTemplate(context: PremiumProjectUrlContext = {}) {
    const tenantId = context.tenantId || getTenantIdForUrl();
    const orgId = context.orgId || "";
    const rawTemplate = (process.env.PREMIUM_PROJECT_URL_TEMPLATE || "").trim();
    if (rawTemplate) {
        return rawTemplate
            .replace("{tenantId}", tenantId || "")
            .replace("{orgId}", orgId || "");
    }
    const base = (process.env.PREMIUM_PROJECT_WEB_BASE || "").trim();
    if (base) {
        const clean = base.replace(/\/+$/, "");
        if (clean.includes("{projectId}")) {
            return clean
                .replace("{tenantId}", tenantId || "")
                .replace("{orgId}", orgId || "");
        }
        const orgSegment = orgId ? `/org/${orgId}` : "";
        const tidParam = tenantId ? `?tid=${tenantId}` : "";
        return `${clean}/{projectId}${orgSegment}${tidParam}`;
    }
    const orgSegment = orgId ? `/org/${orgId}` : "";
    const tidParam = tenantId ? `?tid=${tenantId}` : "";
    return `https://planner.cloud.microsoft/webui/premiumplan/{projectId}${orgSegment}${tidParam}`;
}

export function buildPremiumProjectUrl(template: string, projectId: string) {
    if (!template || !projectId) return "";
    return template.replace("{projectId}", projectId);
}
