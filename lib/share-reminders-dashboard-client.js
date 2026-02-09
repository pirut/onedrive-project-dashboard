(function () {
    if (window.__shareRemindersDashboardInit) return;
    window.__shareRemindersDashboardInit = true;

    var refreshBtn = document.getElementById("share-reminders-refresh");
    var filterInput = document.getElementById("share-reminders-filter");
    var selectAllBtn = document.getElementById("share-reminders-select-all");
    var clearBtn = document.getElementById("share-reminders-clear");
    var ensureSelectedBtn = document.getElementById("share-reminders-ensure-selected");
    var ensureFilteredBtn = document.getElementById("share-reminders-ensure-filtered");
    var ownerTeamIdInput = document.getElementById("share-owner-team-id");
    var ownerTeamAadGroupIdInput = document.getElementById("share-owner-team-aad-group-id");
    var ownerTeamSearchInput = document.getElementById("share-owner-team-search");
    var loadTeamsBtn = document.getElementById("share-owner-team-load");
    var enableTaskCheckbox = document.getElementById("share-enable-task");
    var primaryResourceInput = document.getElementById("share-primary-resource-name");
    var taskTitleInput = document.getElementById("share-task-title");
    var countEl = document.getElementById("share-reminders-count");
    var statusEl = document.getElementById("share-reminders-status");
    var outputEl = document.getElementById("share-reminders-output");
    var tbody = document.getElementById("share-reminders-tbody");

    var projects = [];
    var filtered = [];
    var selected = new Set();
    var storageKey = "projectSharingManager:v1";

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

    function loadStoredConfig() {
        try {
            var raw = window.localStorage.getItem(storageKey);
            if (!raw) return;
            var parsed = JSON.parse(raw);
            if (ownerTeamIdInput && typeof parsed.ownerTeamId === "string" && parsed.ownerTeamId.trim()) {
                ownerTeamIdInput.value = parsed.ownerTeamId;
            }
            if (ownerTeamAadGroupIdInput && typeof parsed.ownerTeamAadGroupId === "string") {
                if (parsed.ownerTeamAadGroupId.trim()) ownerTeamAadGroupIdInput.value = parsed.ownerTeamAadGroupId;
            }
            if (enableTaskCheckbox) enableTaskCheckbox.checked = !!parsed.enableTask;
            if (primaryResourceInput && typeof parsed.primaryResourceName === "string") {
                if (parsed.primaryResourceName.trim()) primaryResourceInput.value = parsed.primaryResourceName;
            }
            if (taskTitleInput && typeof parsed.taskTitle === "string" && parsed.taskTitle.trim()) {
                taskTitleInput.value = parsed.taskTitle;
            }
        } catch (err) {
            // ignore local storage parse errors
        }
    }

    function saveStoredConfig() {
        try {
            window.localStorage.setItem(
                storageKey,
                JSON.stringify({
                    ownerTeamId: ownerTeamIdInput ? ownerTeamIdInput.value.trim() : "",
                    ownerTeamAadGroupId: ownerTeamAadGroupIdInput ? ownerTeamAadGroupIdInput.value.trim() : "",
                    enableTask: !!(enableTaskCheckbox && enableTaskCheckbox.checked),
                    primaryResourceName: primaryResourceInput ? primaryResourceInput.value.trim() : "",
                    taskTitle: taskTitleInput ? taskTitleInput.value.trim() : "",
                })
            );
        } catch (err) {
            // ignore local storage write errors
        }
    }

    function getShareConfig() {
        return {
            plannerOwnerTeamId: ownerTeamIdInput ? ownerTeamIdInput.value.trim() : "",
            plannerOwnerTeamAadGroupId: ownerTeamAadGroupIdInput ? ownerTeamAadGroupIdInput.value.trim() : "",
            plannerShareReminderTaskEnabled: !!(enableTaskCheckbox && enableTaskCheckbox.checked),
            plannerPrimaryResourceName: primaryResourceInput ? primaryResourceInput.value.trim() : "",
            plannerShareReminderTaskTitle: taskTitleInput && taskTitleInput.value.trim() ? taskTitleInput.value.trim() : "Share Project",
        };
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
                      '" style="background:#60a5fa;color:#07101f;border:0;border-radius:8px;padding:6px 10px;cursor:pointer">Apply</button>'
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

    async function listOwnerTeams() {
        var q = ownerTeamSearchInput && ownerTeamSearchInput.value ? ownerTeamSearchInput.value.trim() : "";
        var query = q ? "?q=" + encodeURIComponent(q) : "";
        setStatus("Loading Dataverse teams…", "muted");
        try {
            var res = await fetch("/api/sync/list-owner-teams" + query, {
                headers: { "cache-control": "no-cache" },
                credentials: "same-origin",
            });
            var payload = await res.json();
            if (!res.ok || (payload && payload.ok === false)) {
                throw new Error(payload && payload.error ? payload.error : "HTTP " + res.status);
            }
            renderOutput(payload);
            setStatus(
                "Loaded " + (Array.isArray(payload.teams) ? payload.teams.length : 0) + " Dataverse team candidates.",
                "ok"
            );
        } catch (err) {
            renderOutput(err && err.message ? err.message : String(err));
            setStatus("Failed to load teams: " + (err && err.message ? err.message : "error"), "bad");
        }
    }

    async function ensureOne(projectNo, premiumProjectId, shareConfig) {
        if (!projectNo || !premiumProjectId) return { ok: false, skipped: true, reason: "missing_project_or_premium_id" };
        var requestPayload = {
            action: "share-access",
            projectNo: projectNo,
            premiumProjectId: premiumProjectId,
            plannerOwnerTeamId: shareConfig && shareConfig.plannerOwnerTeamId ? shareConfig.plannerOwnerTeamId : "",
            plannerOwnerTeamAadGroupId:
                shareConfig && shareConfig.plannerOwnerTeamAadGroupId ? shareConfig.plannerOwnerTeamAadGroupId : "",
            plannerShareReminderTaskEnabled: !!(shareConfig && shareConfig.plannerShareReminderTaskEnabled),
            plannerPrimaryResourceName:
                shareConfig && shareConfig.plannerPrimaryResourceName ? shareConfig.plannerPrimaryResourceName : "",
            plannerShareReminderTaskTitle:
                shareConfig && shareConfig.plannerShareReminderTaskTitle ? shareConfig.plannerShareReminderTaskTitle : "Share Project",
        };
        var res = await fetch("/api/sync/projects", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "same-origin",
            body: JSON.stringify(requestPayload),
        });
        var responsePayload = await res.json();
        if (!res.ok || (responsePayload && responsePayload.ok === false)) {
            throw new Error(responsePayload && responsePayload.error ? responsePayload.error : "HTTP " + res.status);
        }
        return responsePayload;
    }

    async function ensureMany(items, label) {
        if (!items.length) {
            setStatus("No projects selected.", "warn");
            return;
        }
        var shareConfig = getShareConfig();
        if (
            !shareConfig.plannerOwnerTeamId &&
            !shareConfig.plannerOwnerTeamAadGroupId &&
            !shareConfig.plannerShareReminderTaskEnabled
        ) {
            setStatus("Set Owner Team ID / AAD Group ID, or enable Share Project reminder task.", "warn");
            return;
        }
        saveStoredConfig();
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
                var out = await ensureOne(item.projectNo, item.premiumProjectId, shareConfig);
                var access = out && out.access ? out.access : null;
                results.push({
                    projectNo: item.projectNo,
                    ok: true,
                    ownerTeam: access ? access.ownerTeam || null : null,
                    shareReminderTask: access ? access.shareReminderTask || null : null,
                    errors: access && Array.isArray(access.errors) ? access.errors : [],
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
            var ownerChanges = results.filter(function (row) { return row.ownerTeam && row.ownerTeam.changed; }).length;
            setStatus(
                "Completed successfully for " +
                    results.length +
                    " projects. Owner changed on " +
                    ownerChanges +
                    ".",
                "ok"
            );
        }
    }

    if (refreshBtn) {
        refreshBtn.addEventListener("click", function () {
            fetchProjects();
        });
    }
    if (loadTeamsBtn) {
        loadTeamsBtn.addEventListener("click", function () {
            listOwnerTeams();
        });
    }
    if (ownerTeamSearchInput) {
        ownerTeamSearchInput.addEventListener("keydown", function (ev) {
            if (ev.key === "Enter") {
                ev.preventDefault();
                listOwnerTeams();
            }
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
            ensureMany(items, "Applying sharing for selected projects");
        });
    }
    if (ensureFilteredBtn) {
        ensureFilteredBtn.addEventListener("click", function () {
            ensureMany(filtered.slice(), "Applying sharing for filtered projects");
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
            var shareConfig = getShareConfig();
            if (
                !shareConfig.plannerOwnerTeamId &&
                !shareConfig.plannerOwnerTeamAadGroupId &&
                !shareConfig.plannerShareReminderTaskEnabled
            ) {
                setStatus("Set Owner Team ID / AAD Group ID, or enable Share Project reminder task.", "warn");
                return;
            }
            saveStoredConfig();
            setStatus("Applying sharing for " + projectNo + "…", "muted");
            ensureOne(projectNo, premiumProjectId, shareConfig)
                .then(function (payload) {
                    renderOutput(payload);
                    var ownerTeam = payload && payload.access ? payload.access.ownerTeam || null : null;
                    if (ownerTeam && ownerTeam.changed) {
                        setStatus("Owner team applied for " + projectNo + ".", "ok");
                    } else if (ownerTeam && ownerTeam.alreadyOwned) {
                        setStatus("Project " + projectNo + " already owned by configured team.", "ok");
                    } else {
                        setStatus("Sharing check completed for " + projectNo + ".", "ok");
                    }
                })
                .catch(function (err) {
                    renderOutput({ projectNo: projectNo, error: err && err.message ? err.message : "error" });
                    setStatus("Failed for " + projectNo + ": " + (err && err.message ? err.message : "error"), "bad");
                });
        });
    }

    [ownerTeamIdInput, ownerTeamAadGroupIdInput, primaryResourceInput, taskTitleInput].forEach(function (el) {
        if (!el) return;
        el.addEventListener("change", saveStoredConfig);
        el.addEventListener("blur", saveStoredConfig);
    });
    if (enableTaskCheckbox) {
        enableTaskCheckbox.addEventListener("change", saveStoredConfig);
    }

    loadStoredConfig();
    fetchProjects();
})();
