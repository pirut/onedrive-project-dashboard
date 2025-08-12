import React, { useEffect, useMemo, useState } from "react";
import { PublicClientApplication, InteractionRequiredAuthError } from "@azure/msal-browser";
import { Client } from "@microsoft/microsoft-graph-client";
import "isomorphic-fetch";

const CLIENT_ID = import.meta.env.VITE_AZURE_AD_CLIENT_ID;
const TENANT_ID = import.meta.env.VITE_AZURE_AD_TENANT_ID || "common";
const SCOPES = ["User.Read", "Files.Read.All"];

function useMsal() {
    const msal = useMemo(
        () =>
            new PublicClientApplication({
                auth: {
                    clientId: CLIENT_ID,
                    authority: `https://login.microsoftonline.com/${TENANT_ID}`,
                    redirectUri: window.location.origin,
                },
                cache: { cacheLocation: "localStorage" },
            }),
        []
    );
    return msal;
}

async function getToken(msal) {
    const account = msal.getAllAccounts()[0];
    if (!account) {
        await msal.loginPopup({ scopes: SCOPES });
    }
    const active = msal.getAllAccounts()[0];
    try {
        const res = await msal.acquireTokenSilent({ scopes: SCOPES, account: active });
        return res.accessToken;
    } catch (e) {
        if (e instanceof InteractionRequiredAuthError) {
            const res = await msal.acquireTokenPopup({ scopes: SCOPES, account: active });
            return res.accessToken;
        }
        throw e;
    }
}

async function makeGraphClient(msal) {
    const token = await getToken(msal);
    return Client.init({ authProvider: (done) => done(null, token) });
}

function csvEscape(val) {
    if (val == null) return "";
    const s = String(val);
    if (/[",\n]/.test(s)) return '"' + s.replaceAll('"', '""') + '"';
    return s;
}

function downloadCsv(rows, filename = "projects.csv") {
    const header = ["Name", "Link"];
    const lines = [header, ...rows].map((r) => r.map(csvEscape).join(",")).join("\n");
    const blob = new Blob([lines], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

export default function OneDriveProjectDashboard() {
    const msal = useMsal();
    const [msalReady, setMsalReady] = useState(false);
    const [signedIn, setSignedIn] = useState(false);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    // OneDrive (me) path input
    const [folderPath, setFolderPath] = useState("/Projects");
    // Source switch
    const [source, setSource] = useState("sharepoint"); // 'onedrive' | 'sharepoint'
    // SharePoint inputs
    const [siteUrl, setSiteUrl] = useState("https://cornerstonecompaniesflc.sharepoint.com/sites/work");
    const [libraryPath, setLibraryPath] = useState("Documents/Cornerstone Jobs");
    const [items, setItems] = useState([]);

    // MSAL v3 requires initialize() before any other API calls
    useEffect(() => {
        let isMounted = true;
        (async () => {
            try {
                await msal.initialize();
                if (!isMounted) return;
                setMsalReady(true);
                setSignedIn(msal.getAllAccounts().length > 0);
            } catch (e) {
                if (!isMounted) return;
                setError(e?.message || String(e));
            }
        })();
        return () => {
            isMounted = false;
        };
    }, [msal]);

    const handleSignIn = async () => {
        if (!msalReady) return;
        setError(null);
        try {
            await msal.loginPopup({ scopes: SCOPES });
            setSignedIn(true);
        } catch (e) {
            setError(e.message || String(e));
        }
    };

    const handleSignOut = async () => {
        if (!msalReady) return;
        const account = msal.getAllAccounts()[0];
        if (account) await msal.logoutPopup({ account });
        setSignedIn(false);
        setItems([]);
    };

    function encodeDrivePath(path) {
        const trimmed = String(path || "").replace(/^\/+|\/+$/g, "");
        if (!trimmed) return "";
        return trimmed
            .split("/")
            .map((seg) => encodeURIComponent(seg))
            .join("/");
    }

    const loadProjects = async () => {
        setError(null);
        setLoading(true);
        try {
            if (!msalReady) throw new Error("Authentication initializing. Try again in a moment.");
            const client = await makeGraphClient(msal);
            let res;
            if (source === "onedrive") {
                let path = folderPath.trim();
                if (!path.startsWith("/")) path = "/" + path;
                if (path !== "/" && path.endsWith("/")) path = path.slice(0, -1);
                res = await client.api(`/me/drive/root:${path}:/children`).select(["name", "webUrl", "folder", "file", "parentReference"]).top(999).get();
            } else {
                // SharePoint: resolve site and library drive
                const url = new URL(siteUrl);
                const host = url.host; // e.g., YOURTENANT.sharepoint.com
                const pathname = url.pathname.replace(/^\/+|\/+$/g, ""); // e.g., sites/work or just work
                const sitePath = pathname.startsWith("sites/") ? pathname.slice("sites/".length) : pathname;
                const site = await client.api(`/sites/${host}:/sites/${encodeURIComponent(sitePath)}`).get();
                const drives = await client.api(`/sites/${site.id}/drives`).get();
                const libPathTrimmed = String(libraryPath || "").replace(/^\/+|\/+$/g, "");
                const [driveName, ...subPathParts] = libPathTrimmed.split("/");
                const drive = (drives.value || []).find((d) => d.name === driveName);
                if (!drive) throw new Error(`Library not found: ${driveName}`);
                if (subPathParts.length === 0) {
                    res = await client.api(`/drives/${drive.id}/root/children`).select(["name", "webUrl", "folder", "file", "parentReference"]).top(999).get();
                } else {
                    const subPath = encodeDrivePath(subPathParts.join("/"));
                    res = await client
                        .api(`/drives/${drive.id}/root:/${subPath}:/children`)
                        .select(["name", "webUrl", "folder", "file", "parentReference"])
                        .top(999)
                        .get();
                }
            }

            // Keep only folders and exclude any named "(ARCHIVE)" (case-insensitive)
            const foldersOnly = (res.value || []).filter((it) => it.folder).filter((it) => (it.name || "").trim().toLowerCase() !== "(archive)");
            setItems(foldersOnly);
        } catch (e) {
            setError(e.message || String(e));
        } finally {
            setLoading(false);
        }
    };

    const handleExport = () => {
        const rows = items.map((it) => [it.name, it.webUrl || ""]);
        downloadCsv(rows);
    };

    // Sync to FastField Data Table: POST to a webhook if provided; else download JSON payload
    const [syncing, setSyncing] = useState(false);
    const handleSyncToFastField = async () => {
        const webhookUrl = import.meta.env.VITE_FASTFIELD_SYNC_WEBHOOK_URL;
        const payload = {
            tableName: import.meta.env.VITE_FASTFIELD_TABLE_NAME || "CornerstoneProjects",
            generatedAt: new Date().toISOString(),
            source,
            siteUrl: source === "sharepoint" ? siteUrl : null,
            libraryPath: source === "sharepoint" ? libraryPath : null,
            rows: items.map((it) => ({
                id: it.id,
                name: it.name,
                link: it.webUrl || null,
                driveId: it.parentReference?.driveId || null,
                parentPath: it.parentReference?.path || null,
            })),
        };
        if (!webhookUrl) {
            // Download as JSON for manual upload
            const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = "fastfield-data-table-payload.json";
            a.click();
            URL.revokeObjectURL(url);
            return;
        }
        try {
            setError(null);
            setSyncing(true);
            const res = await fetch(webhookUrl, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            if (!res.ok) throw new Error(`Sync failed (${res.status})`);
        } catch (e) {
            setError(e?.message || String(e));
        } finally {
            setSyncing(false);
        }
    };

    return (
        <div style={{ minHeight: "100vh", background: "#f9fafb", color: "#111827", padding: "24px" }}>
            <div style={{ maxWidth: 900, margin: "0 auto" }}>
                <header style={{ marginBottom: 24, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <h1 style={{ fontSize: 22, fontWeight: 600 }}>OneDrive Project Dashboard</h1>
                    <div>
                        {!signedIn ? (
                            <button
                                onClick={handleSignIn}
                                disabled={!msalReady}
                                style={{ padding: "8px 16px", borderRadius: 999, background: "black", color: "white", opacity: msalReady ? 1 : 0.5 }}
                            >
                                Sign in
                            </button>
                        ) : (
                            <button
                                onClick={handleSignOut}
                                style={{ padding: "8px 16px", borderRadius: 999, background: "white", border: "1px solid #e5e7eb" }}
                            >
                                Sign out
                            </button>
                        )}
                    </div>
                </header>

                <div style={{ background: "white", borderRadius: 16, padding: 16, marginBottom: 24, boxShadow: "0 1px 2px rgba(0,0,0,0.06)" }}>
                    <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12 }}>
                        <label style={{ fontSize: 14, fontWeight: 600 }}>Source</label>
                        <select
                            value={source}
                            onChange={(e) => setSource(e.target.value)}
                            style={{ borderRadius: 12, border: "1px solid #e5e7eb", padding: "8px 12px" }}
                        >
                            <option value="sharepoint">SharePoint site</option>
                            <option value="onedrive">OneDrive (me)</option>
                        </select>
                    </div>

                    {source === "sharepoint" ? (
                        <>
                            <label style={{ display: "block", fontSize: 14, fontWeight: 600, marginBottom: 6 }}>SharePoint site URL</label>
                            <input
                                value={siteUrl}
                                onChange={(e) => setSiteUrl(e.target.value)}
                                placeholder="https://YOURTENANT.sharepoint.com/sites/work"
                                style={{ width: "100%", borderRadius: 12, border: "1px solid #e5e7eb", padding: "8px 12px", marginBottom: 12 }}
                            />
                            <label style={{ display: "block", fontSize: 14, fontWeight: 600, marginBottom: 6 }}>Library path</label>
                            <input
                                value={libraryPath}
                                onChange={(e) => setLibraryPath(e.target.value)}
                                placeholder="Documents/Cornerstone Jobs"
                                style={{ width: "100%", borderRadius: 12, border: "1px solid #e5e7eb", padding: "8px 12px", marginBottom: 12 }}
                            />
                        </>
                    ) : (
                        <>
                            <label style={{ display: "block", fontSize: 14, fontWeight: 600, marginBottom: 6 }}>OneDrive folder path</label>
                            <input
                                value={folderPath}
                                onChange={(e) => setFolderPath(e.target.value)}
                                placeholder="/Projects"
                                style={{ width: "100%", borderRadius: 12, border: "1px solid #e5e7eb", padding: "8px 12px", marginBottom: 12 }}
                            />
                        </>
                    )}

                    <div style={{ display: "flex", gap: 8 }}>
                        <button
                            onClick={loadProjects}
                            disabled={!signedIn || loading}
                            style={{ padding: "8px 16px", borderRadius: 12, background: "#2563eb", color: "white", opacity: !signedIn || loading ? 0.5 : 1 }}
                        >
                            {loading ? "Loading…" : "Load projects"}
                        </button>
                        <button
                            onClick={handleSyncToFastField}
                            disabled={items.length === 0 || syncing}
                            style={{
                                padding: "8px 16px",
                                borderRadius: 12,
                                background: "#7c3aed",
                                color: "white",
                                opacity: items.length === 0 || syncing ? 0.5 : 1,
                            }}
                        >
                            {syncing ? "Syncing…" : "Sync to FastField (Data Table)"}
                        </button>
                        <button
                            onClick={handleExport}
                            disabled={items.length === 0}
                            style={{ padding: "8px 16px", borderRadius: 12, background: "#059669", color: "white", opacity: items.length === 0 ? 0.5 : 1 }}
                        >
                            Export CSV
                        </button>
                    </div>
                </div>

                {error && (
                    <div style={{ background: "#fef2f2", border: "1px solid #fecaca", color: "#991b1b", padding: 12, borderRadius: 12, marginBottom: 16 }}>
                        {error}
                    </div>
                )}

                <div style={{ background: "white", borderRadius: 16, boxShadow: "0 1px 2px rgba(0,0,0,0.06)" }}>
                    <div
                        style={{ padding: "12px 16px", fontSize: 14, color: "#4b5563", display: "flex", alignItems: "center", justifyContent: "space-between" }}
                    >
                        <span>{items.length === 0 ? "No folders loaded yet." : `${items.length} folder${items.length !== 1 ? "s" : ""}`}</span>
                    </div>
                    <ul style={{ borderTop: "1px solid #f3f4f6" }}>
                        {items.map((it) => (
                            <li
                                key={it.id}
                                style={{
                                    padding: "12px 16px",
                                    display: "flex",
                                    alignItems: "center",
                                    justifyContent: "space-between",
                                    borderTop: "1px solid #f3f4f6",
                                }}
                            >
                                <div>
                                    <div style={{ fontWeight: 600 }}>{it.name}</div>
                                    <a
                                        href={it.webUrl}
                                        target="_blank"
                                        rel="noreferrer"
                                        style={{ fontSize: 14, color: "#2563eb", textDecoration: "underline" }}
                                    >
                                        Open in OneDrive
                                    </a>
                                </div>
                                <div style={{ fontSize: 12, color: "#6b7280" }}>
                                    {it.folder && typeof it.folder.childCount === "number" ? it.folder.childCount : 0} items
                                </div>
                            </li>
                        ))}
                    </ul>
                </div>

                <footer style={{ marginTop: 24, fontSize: 12, color: "#6b7280" }}>
                    Powered by Microsoft Graph. Scopes required: Files.Read.All, User.Read.
                </footer>
            </div>
        </div>
    );
}
