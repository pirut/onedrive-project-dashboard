(function () {
    if (window.__shareRemindersDashboardInit) return;
    window.__shareRemindersDashboardInit = true;

    var refreshBtn = document.getElementById("share-reminders-refresh");
    var filterInput = document.getElementById("share-reminders-filter");
    var selectAllBtn = document.getElementById("share-reminders-select-all");
    var clearBtn = document.getElementById("share-reminders-clear");
    var ensureSelectedBtn = document.getElementById("share-reminders-ensure-selected");
    var ensureFilteredBtn = document.getElementById("share-reminders-ensure-filtered");
    var countEl = document.getElementById("share-reminders-count");
    var statusEl = document.getElementById("share-reminders-status");
    var outputEl = document.getElementById("share-reminders-output");
    var tbody = document.getElementById("share-reminders-tbody");

    var projects = [];
    var filtered = [];
    var selected = new Set();

    function htmlEscape(s) {
        return String(s)
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
    }

    function parseDateMs(value) {
        if (!value) return NaN;
        var ms = Date.parse(String(value));
        return Number.isFinite(ms) ? ms : NaN;
    }

    function formatRelativeTime(ms) {
        if (!Number.isFinite(ms)) return "—";
        var delta = Date.now() - ms;
        if (delta < 0) return "just now";
        var sec = Math.floor(delta / 1000);
        if (sec < 60) return "just now";
        var min = Math.floor(sec / 60);
        if (min < 60) return min + "m ago";
        var hrs = Math.floor(min / 60);
        if (hrs < 24) return hrs + "h ago";
        var days = Math.floor(hrs / 24);
        return days + "d ago";
    }

    function setStatus(text, tone) {
        if (!statusEl) return;
        var toneClass = tone === "ok" ? "ok" : tone === "bad" ? "bad" : tone === "warn" ? "warn" : "muted";
        statusEl.textContent = text;
        statusEl.className = "small " + toneClass;
    }

    function renderOutput(payload) {
        if (!outputEl) return;
        if (payload == null) {
            outputEl.style.display = "none";
            outputEl.textContent = "";
            return;
        }
        var text = "";
        try {
            text = typeof payload === "string" ? payload : JSON.stringify(payload, null, 2);
        } catch (e) {
            text = String(payload);
        }
        outputEl.textContent = text;
        outputEl.style.display = "block";
    }

    function updateCount() {
        if (!countEl) return;
        countEl.textContent = selected.size + " selected";
        var none = selected.size === 0;
        if (ensureSelectedBtn) ensureSelectedBtn.disabled = none;
    }

    function renderTable() {
        if (!tbody) return;
        var q = filterInput && filterInput.value ? filterInput.value.trim().toLowerCase() : "";
        filtered = !q
            ? projects
            : projects.filter(function (item) {
                  var hay = [item.projectNo, item.description, item.status, item.premiumProjectId].join(" ").toLowerCase();
                  return hay.indexOf(q) !== -1;
              });
        var rows = filtered
            .map(function (item) {
                var projectNo = item.projectNo || "";
                var premiumId = item.premiumProjectId || "";
                var selectedRow = selected.has(projectNo);
                var lastSyncMs = parseDateMs(item.lastSyncAt);
                var lastSyncLabel = formatRelativeTime(lastSyncMs);
                var lastSyncTitle = item.lastSyncAt ? htmlEscape(item.lastSyncAt) : "";
                var syncBadge = item.syncDisabled
                    ? '<span class="pill warn">Disabled</span>'
                    : '<span class="pill ok">Enabled</span>';
                var actionBtn = premiumId
                    ? '<button type="button" data-action="ensure-one" data-project-no="' +
                      htmlEscape(projectNo) +
                      '" data-premium-project-id="' +
                      htmlEscape(premiumId) +
                      '" style="background:#60a5fa;color:#07101f;border:0;border-radius:8px;padding:6px 10px;cursor:pointer">Ensure</button>'
                    : '<span class="muted small">No Premium ID</span>';
                return (
                    "<tr>" +
                    '<td><input type="checkbox" data-project-no="' +
                    htmlEscape(projectNo) +
                    '"' +
                    (selectedRow ? " checked" : "") +
                    " /></td>" +
                    '<td class="mono small">' +
                    htmlEscape(projectNo) +
                    "</td>" +
                    "<td>" +
                    htmlEscape(item.description || "") +
                    (item.status ? '<div class="small muted">' + htmlEscape(item.status) + "</div>" : "") +
                    "</td>" +
                    "<td>" +
                    (premiumId ? '<span class="mono small">' + htmlEscape(premiumId) + "</span>" : '<span class="muted">—</span>') +
                    "</td>" +
                    "<td>" +
                    syncBadge +
                    "</td>" +
                    "<td>" +
                    (lastSyncTitle
                        ? '<span class="mono small" title="' + lastSyncTitle + '">' + htmlEscape(lastSyncLabel) + "</span>"
                        : '<span class="muted">—</span>') +
                    "</td>" +
                    "<td>" +
                    actionBtn +
                    "</td>" +
                    "</tr>"
                );
            })
            .join("");
        tbody.innerHTML = rows || '<tr><td colspan="7" class="muted">No projects found.</td></tr>';
        updateCount();
    }

    async function fetchProjects() {
        setStatus("Loading projects…", "muted");
        try {
            var res = await fetch("/api/sync/projects", { headers: { "cache-control": "no-cache" }, credentials: "same-origin" });
            var payload = await res.json();
            if (!res.ok || (payload && payload.ok === false)) {
                throw new Error(payload && payload.error ? payload.error : "HTTP " + res.status);
            }
            projects = Array.isArray(payload.projects) ? payload.projects : [];
            var allowed = new Set(projects.map(function (item) { return item.projectNo; }));
            selected.forEach(function (projectNo) {
                if (!allowed.has(projectNo)) selected.delete(projectNo);
            });
            renderTable();
            setStatus("Loaded " + projects.length + " projects.", "ok");
            renderOutput(null);
        } catch (err) {
            projects = [];
            filtered = [];
            selected.clear();
            renderTable();
            renderOutput(err && err.message ? err.message : String(err));
            setStatus("Failed to load projects: " + (err && err.message ? err.message : "error"), "bad");
        }
    }

    async function ensureOne(projectNo, premiumProjectId) {
        if (!projectNo || !premiumProjectId) return { ok: false, skipped: true, reason: "missing_project_or_premium_id" };
        var res = await fetch("/api/sync/projects", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "same-origin",
            body: JSON.stringify({ action: "share-access", projectNo: projectNo, premiumProjectId: premiumProjectId }),
        });
        var payload = await res.json();
        if (!res.ok || (payload && payload.ok === false)) {
            throw new Error(payload && payload.error ? payload.error : "HTTP " + res.status);
        }
        return payload;
    }

    async function ensureMany(items, label) {
        if (!items.length) {
            setStatus("No projects selected.", "warn");
            return;
        }
        setStatus(label + " (" + items.length + ")…", "muted");
        var results = [];
        var failed = [];
        var skipped = [];
        for (var i = 0; i < items.length; i += 1) {
            var item = items[i];
            if (!item.premiumProjectId) {
                skipped.push(item.projectNo);
                continue;
            }
            try {
                var out = await ensureOne(item.projectNo, item.premiumProjectId);
                results.push({
                    projectNo: item.projectNo,
                    ok: true,
                    shareReminderTask: out && out.access ? out.access.shareReminderTask || null : null,
                });
            } catch (err) {
                failed.push({
                    projectNo: item.projectNo,
                    error: err && err.message ? err.message : "error",
                });
            }
        }
        var summary = {
            total: items.length,
            success: results.length,
            failed: failed.length,
            skipped: skipped.length,
            results: results,
            failures: failed,
            skippedProjectNos: skipped,
        };
        renderOutput(summary);
        if (failed.length) {
            setStatus("Completed with failures: " + failed.length + " failed, " + skipped.length + " skipped.", "warn");
        } else if (skipped.length) {
            setStatus("Completed with skips: " + skipped.length + " had no Premium ID.", "warn");
        } else {
            setStatus("Completed successfully for " + results.length + " projects.", "ok");
        }
    }

    if (refreshBtn) {
        refreshBtn.addEventListener("click", function () {
            fetchProjects();
        });
    }
    if (filterInput) {
        filterInput.addEventListener("input", function () {
            renderTable();
        });
    }
    if (selectAllBtn) {
        selectAllBtn.addEventListener("click", function () {
            filtered.forEach(function (item) {
                if (item.projectNo) selected.add(item.projectNo);
            });
            renderTable();
        });
    }
    if (clearBtn) {
        clearBtn.addEventListener("click", function () {
            selected.clear();
            renderTable();
        });
    }
    if (ensureSelectedBtn) {
        ensureSelectedBtn.addEventListener("click", function () {
            var lookup = new Map(projects.map(function (item) { return [item.projectNo, item]; }));
            var items = Array.from(selected)
                .map(function (projectNo) { return lookup.get(projectNo); })
                .filter(Boolean);
            ensureMany(items, "Ensuring Share Project tasks for selected projects");
        });
    }
    if (ensureFilteredBtn) {
        ensureFilteredBtn.addEventListener("click", function () {
            ensureMany(filtered.slice(), "Ensuring Share Project tasks for filtered projects");
        });
    }
    if (tbody) {
        tbody.addEventListener("change", function (ev) {
            var target = ev.target;
            if (!target || target.tagName !== "INPUT" || target.type !== "checkbox") return;
            var projectNo = target.getAttribute("data-project-no") || "";
            if (!projectNo) return;
            if (target.checked) selected.add(projectNo);
            else selected.delete(projectNo);
            updateCount();
        });
        tbody.addEventListener("click", function (ev) {
            var target = ev.target && ev.target.closest ? ev.target.closest("button") : null;
            if (!target) return;
            var action = target.getAttribute("data-action") || "";
            if (action !== "ensure-one") return;
            var projectNo = target.getAttribute("data-project-no") || "";
            var premiumProjectId = target.getAttribute("data-premium-project-id") || "";
            setStatus("Ensuring Share Project task for " + projectNo + "…", "muted");
            ensureOne(projectNo, premiumProjectId)
                .then(function (payload) {
                    renderOutput(payload);
                    setStatus("Share Project task ensured for " + projectNo + ".", "ok");
                })
                .catch(function (err) {
                    renderOutput({ projectNo: projectNo, error: err && err.message ? err.message : "error" });
                    setStatus("Failed for " + projectNo + ": " + (err && err.message ? err.message : "error"), "bad");
                });
        });
    }

    fetchProjects();
})();
