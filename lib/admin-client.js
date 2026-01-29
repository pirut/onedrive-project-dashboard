
    (function(){
      if(window.__adminClientInit) return;
      window.__adminClientInit = true;
      var tbody = document.getElementById('subs-tbody');
      var input = document.getElementById('filter-input');
      var last = document.getElementById('last-updated');
      var refreshBtn = document.getElementById('refresh-btn');
      var uspsForm = document.getElementById('usps-form');
      var uspsFileInput = document.getElementById('usps-file');
      var uspsStatusEl = document.getElementById('usps-status');
      var uspsSummaryEl = document.getElementById('usps-summary');
      var uspsDownloadWrap = document.getElementById('usps-download-wrap');
      var uspsDownloadEl = document.getElementById('usps-download');
      var uspsPendingDownloadWrap = document.getElementById('usps-pending-download-wrap');
      var uspsPendingDownloadEl = document.getElementById('usps-pending-download');
      var uspsPreviewEl = document.getElementById('usps-preview');
      var uspsResetBtn = document.getElementById('usps-reset');
      var uspsVerifyBtn = document.getElementById('usps-verify-btn');
      var exportCsvBtn = document.getElementById('export-active-csv');
      var exportJsonBtn = document.getElementById('export-active-json');
      var plannerProjectInput = document.getElementById('planner-project-no');
      var plannerDebugTaskInput = document.getElementById('planner-debug-task-no');
      var plannerNotifyInput = document.getElementById('planner-notify-url');
      var plannerRunBcBtn = document.getElementById('planner-run-bc');
      var plannerRunBcPrBtn = document.getElementById('planner-run-bc-pr00001');
      var plannerPollPremiumBtn = document.getElementById('planner-poll-premium');
      var plannerDebugTaskBtn = document.getElementById('planner-debug-task');
      var plannerDebugBcTimestampsBtn = document.getElementById('planner-debug-bc-timestamps');
      var plannerDebugDecisionBtn = document.getElementById('planner-debug-decision');
      var plannerWebhookLogBtn = document.getElementById('planner-webhook-log');
      var plannerWebhookStreamBtn = document.getElementById('planner-webhook-stream');
      var plannerWebhookStopBtn = document.getElementById('planner-webhook-stop');
      var plannerWebhookClearBtn = document.getElementById('planner-webhook-clear');
      var plannerTestWebhookBtn = document.getElementById('planner-test-webhook');
      var plannerStatusEl = document.getElementById('planner-status');
      var plannerOutputEl = document.getElementById('planner-output');
      var plannerWebhookOutputEl = document.getElementById('planner-webhook-output');
      var plannerWebhookFeedEl = document.getElementById('planner-webhook-feed');
      var bcNotifyInput = document.getElementById('bc-notify-url');
      var bcEntitySetsInput = document.getElementById('bc-entity-sets');
      var bcCronSecretInput = document.getElementById('bc-cron-secret');
      var bcResourceInput = document.getElementById('bc-resource');
      var bcSystemIdInput = document.getElementById('bc-system-id');
      var bcChangeTypeInput = document.getElementById('bc-change-type');
      var bcSubsListBtn = document.getElementById('bc-subs-list');
      var bcSubsCreateBtn = document.getElementById('bc-subs-create');
      var bcSubsRenewBtn = document.getElementById('bc-subs-renew');
      var bcSubsDeleteBtn = document.getElementById('bc-subs-delete');
      var bcWebhookValidateBtn = document.getElementById('bc-webhook-validate');
      var bcWebhookTestBtn = document.getElementById('bc-webhook-test');
      var bcJobsProcessBtn = document.getElementById('bc-jobs-process');
      var bcWebhookLogBtn = document.getElementById('bc-webhook-log');
      var bcWebhookStreamBtn = document.getElementById('bc-webhook-stream');
      var bcWebhookStopBtn = document.getElementById('bc-webhook-stop');
      var bcWebhookClearBtn = document.getElementById('bc-webhook-clear');
      var bcWebhookStatusEl = document.getElementById('bc-webhook-status');
      var bcWebhookOutputEl = document.getElementById('bc-webhook-output');
      var bcWebhookFeedEl = document.getElementById('bc-webhook-feed');
      var plannerLogEl = document.getElementById('planner-log');
      var plannerSyncLogLoadBtn = document.getElementById('planner-sync-log-load');
      var plannerSyncLogClearBtn = document.getElementById('planner-sync-log-clear');
      var plannerSyncLogOutputEl = document.getElementById('planner-sync-log-output');
      var plannerSyncLogStatusEl = document.getElementById('planner-sync-log-status');
      var plannerProjectsTbody = document.getElementById('planner-projects-tbody');
      var plannerProjectsStatusEl = document.getElementById('planner-projects-status');
      var plannerProjectsFilterInput = document.getElementById('planner-projects-filter');
      var plannerProjectsRefreshBtn = document.getElementById('planner-projects-refresh');
      var plannerProjectsSelectAllBtn = document.getElementById('planner-projects-select-all');
      var plannerProjectsClearBtn = document.getElementById('planner-projects-clear');
      var plannerProjectsBulkEnableBtn = document.getElementById('planner-projects-bulk-enable');
      var plannerProjectsBulkDisableBtn = document.getElementById('planner-projects-bulk-disable');
      var plannerProjectsBulkSyncBtn = document.getElementById('planner-projects-bulk-sync');
      var plannerProjectsCountEl = document.getElementById('planner-projects-count');
      var plannerAssetsRefreshBtn = document.getElementById('planner-assets-refresh');
      var plannerAssetsFilterInput = document.getElementById('planner-assets-filter');
      var plannerAssetsStatusEl = document.getElementById('planner-assets-status');
      var plannerAssetsOutputEl = document.getElementById('planner-assets-output');
      var plannerAssetsGraphTbody = document.getElementById('planner-assets-graph-tbody');
      var plannerAssetsDvTbody = document.getElementById('planner-assets-dv-tbody');
      var plannerAssetsGraphSelectAllBtn = document.getElementById('planner-assets-graph-select-all');
      var plannerAssetsGraphClearBtn = document.getElementById('planner-assets-graph-clear');
      var plannerAssetsDvSelectAllBtn = document.getElementById('planner-assets-dv-select-all');
      var plannerAssetsDvClearBtn = document.getElementById('planner-assets-dv-clear');
      var plannerAssetsConvertBtn = document.getElementById('planner-assets-convert');
      var plannerAssetsDeleteGraphBtn = document.getElementById('planner-assets-delete-graph');
      var plannerAssetsDeleteDvBtn = document.getElementById('planner-assets-delete-dv');
      var plannerAssetsGraphCountEl = document.getElementById('planner-assets-graph-count');
      var plannerAssetsDvCountEl = document.getElementById('planner-assets-dv-count');
      var uspsLoading = false;
      var uspsDownloadUrl = null;
      var uspsPendingDownloadUrl = null;
      var cache = [];
      var refreshing = false;
      var plannerProjectsCache = [];
      var plannerOrphanCache = [];
      var plannerProjectsSelected = new Set();
      var plannerProjectsFiltered = [];
      var plannerAssetsGraphCache = [];
      var plannerAssetsDvCache = [];
      var plannerAssetsGraphSelected = new Set();
      var plannerAssetsDvSelected = new Set();
      var plannerAssetsGraphFiltered = [];
      var plannerAssetsDvFiltered = [];

      function htmlEscape(s){
        return String(s)
          .replaceAll('&','&amp;')
          .replaceAll('<','&lt;')
          .replaceAll('>','&gt;')
          .replaceAll('"','&quot;')
          .replaceAll("'","&#39;");
      }
      function statusCls(s){
        s = String(s||'').toLowerCase();
        if(s==='ok' || s==='success' || s==='validated') return 'ok';
        if(s==='error' || s==='failed' || s==='fail') return 'bad';
        if(s==='warn' || s==='warning') return 'warn';
        return s ? 'warn' : 'muted';
      }
      function formatRelativeTime(ms){
        if(!Number.isFinite(ms)) return '—';
        var delta = Date.now() - ms;
        if(delta < 0) return 'just now';
        var sec = Math.floor(delta / 1000);
        if(sec < 60) return 'just now';
        var min = Math.floor(sec / 60);
        if(min < 60) return min + 'm ago';
        var hrs = Math.floor(min / 60);
        if(hrs < 24) return hrs + 'h ago';
        var days = Math.floor(hrs / 24);
        if(days < 7) return days + 'd ago';
        var weeks = Math.floor(days / 7);
        if(weeks < 5) return weeks + 'w ago';
        var months = Math.floor(days / 30);
        if(months < 12) return months + 'mo ago';
        var years = Math.floor(days / 365);
        return years + 'y ago';
      }
      function setUspsStatus(text, tone){
        if(!uspsStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        uspsStatusEl.textContent = text;
        uspsStatusEl.className = 'small ' + toneClass;
      }
      function setPlannerStatus(text, tone){
        if(!plannerStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        plannerStatusEl.textContent = text;
        plannerStatusEl.className = 'small ' + toneClass;
      }
      function setBcStatus(text, tone){
        if(!bcWebhookStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        bcWebhookStatusEl.textContent = text;
        bcWebhookStatusEl.className = 'small ' + toneClass;
      }
      function setPlannerSyncLogStatus(text, tone){
        if(!plannerSyncLogStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        plannerSyncLogStatusEl.textContent = text;
        plannerSyncLogStatusEl.className = 'small ' + toneClass;
      }
      function setPlannerProjectsStatus(text, tone){
        if(!plannerProjectsStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        plannerProjectsStatusEl.textContent = text;
        plannerProjectsStatusEl.className = 'small ' + toneClass;
      }
      function updatePlannerProjectsCount(){
        if(!plannerProjectsCountEl) return;
        var count = plannerProjectsSelected.size;
        plannerProjectsCountEl.textContent = count ? (count + ' selected') : 'No projects selected';
        var disabled = count === 0;
        if(plannerProjectsBulkEnableBtn) plannerProjectsBulkEnableBtn.disabled = disabled;
        if(plannerProjectsBulkDisableBtn) plannerProjectsBulkDisableBtn.disabled = disabled;
        if(plannerProjectsBulkSyncBtn) plannerProjectsBulkSyncBtn.disabled = disabled;
      }
      function renderPlannerProjects(){
        if(!plannerProjectsTbody) return;
        var q = (plannerProjectsFilterInput && plannerProjectsFilterInput.value ? plannerProjectsFilterInput.value.trim().toLowerCase() : '');
        var filtered = !q ? plannerProjectsCache : plannerProjectsCache.filter(function(item){
          var hay = [
            item.projectNo,
            item.description,
            item.status,
            item.premiumProjectId
          ].join(' ').toLowerCase();
          return hay.indexOf(q) !== -1;
        });
        plannerProjectsFiltered = filtered;
        var rows = filtered.map(function(item){
          var premiumId = item.premiumProjectId || '—';
          var premiumCell = premiumId && premiumId !== '—'
            ? ('<div class="mono small">' + htmlEscape(premiumId) + '</div>')
            : '<span class="muted">—</span>';
          var syncBadge = item.syncDisabled ? '<span class="badge warn">Disabled</span>' : '<span class="badge ok">Enabled</span>';
          var lastSyncMs = item.lastSyncAt ? Date.parse(item.lastSyncAt) : NaN;
          var lastSyncLabel = formatRelativeTime(lastSyncMs);
          var lastSyncTitle = item.lastSyncAt ? htmlEscape(item.lastSyncAt) : '';
          var lastSyncCell = lastSyncTitle
            ? '<span class="mono small" title="' + lastSyncTitle + '">' + htmlEscape(lastSyncLabel) + '</span>'
            : '<span class="muted">—</span>';
          var toggleLabel = item.syncDisabled ? 'Enable' : 'Disable';
          var toggleStyle = item.syncDisabled ? 'background:#2b61d1;color:#fff' : 'background:#1f2a44;color:#e6ecff';
          var toggleBtn = '<button type="button" data-action="toggle-sync" data-project-no="' + htmlEscape(item.projectNo) + '" data-disabled="' + (item.syncDisabled ? '1' : '0') + '" style="' + toggleStyle + ';border:0;border-radius:6px;padding:4px 8px;cursor:pointer;font-size:12px">' + toggleLabel + '</button>';
          var syncBtn = '<button type="button" data-action="sync-project" data-project-no="' + htmlEscape(item.projectNo) + '" style="background:#0f8b4c;color:#fff;border:0;border-radius:6px;padding:4px 8px;cursor:pointer;font-size:12px">Sync</button>';
          var clearBtn = item.premiumProjectId ? '<button type="button" data-action="clear-links" data-project-no="' + htmlEscape(item.projectNo) + '" style="background:#ef4444;color:#fff;border:0;border-radius:6px;padding:4px 8px;cursor:pointer;font-size:12px">Clear links</button>' : '';
          var isSelected = plannerProjectsSelected.has(item.projectNo);
          var selectBox = '<input type="checkbox" data-project-no="' + htmlEscape(item.projectNo) + '"' + (isSelected ? ' checked' : '') + ' />';
          return '<tr>'
            + '<td>' + selectBox + '</td>'
            + '<td class="mono small">' + htmlEscape(item.projectNo || '') + '</td>'
            + '<td>' + htmlEscape(item.description || '') + (item.status ? ('<div class="small muted">' + htmlEscape(item.status) + '</div>') : '') + '</td>'
            + '<td>' + premiumCell + '</td>'
            + '<td>' + lastSyncCell + '</td>'
            + '<td>' + syncBadge + '</td>'
            + '<td style="display:flex;gap:6px;flex-wrap:wrap">' + syncBtn + toggleBtn + clearBtn + '</td>'
            + '</tr>';
        }).join('');
        plannerProjectsTbody.innerHTML = rows || '<tr><td colspan="7" class="muted">No projects found.</td></tr>';
        updatePlannerProjectsCount();
      }
      function setPlannerAssetsStatus(text, tone){
        if(!plannerAssetsStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        plannerAssetsStatusEl.textContent = text;
        plannerAssetsStatusEl.className = 'small ' + toneClass;
      }
      function renderPlannerAssetsOutput(payload){
        if(!plannerAssetsOutputEl) return;
        if(payload == null){
          plannerAssetsOutputEl.style.display = 'none';
          plannerAssetsOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        plannerAssetsOutputEl.textContent = text;
        plannerAssetsOutputEl.style.display = 'block';
      }
      function updatePlannerAssetsCounts(){
        if(plannerAssetsGraphCountEl){
          var count = plannerAssetsGraphSelected.size;
          plannerAssetsGraphCountEl.textContent = count ? (count + ' selected') : 'No plans selected';
        }
        if(plannerAssetsDvCountEl){
          var countDv = plannerAssetsDvSelected.size;
          plannerAssetsDvCountEl.textContent = countDv ? (countDv + ' selected') : 'No projects selected';
        }
        if(plannerAssetsConvertBtn) plannerAssetsConvertBtn.disabled = plannerAssetsGraphSelected.size === 0;
        if(plannerAssetsDeleteGraphBtn) plannerAssetsDeleteGraphBtn.disabled = plannerAssetsGraphSelected.size === 0;
        if(plannerAssetsDeleteDvBtn) plannerAssetsDeleteDvBtn.disabled = plannerAssetsDvSelected.size === 0;
      }
      function renderPlannerAssets(){
        var q = (plannerAssetsFilterInput && plannerAssetsFilterInput.value ? plannerAssetsFilterInput.value.trim().toLowerCase() : '');
        var graphFiltered = !q ? plannerAssetsGraphCache : plannerAssetsGraphCache.filter(function(item){
          var hay = [item.title, item.id, item.owner].join(' ').toLowerCase();
          return hay.indexOf(q) !== -1;
        });
        var dvFiltered = !q ? plannerAssetsDvCache : plannerAssetsDvCache.filter(function(item){
          var hay = [item.title, item.id, item.bcNo].join(' ').toLowerCase();
          return hay.indexOf(q) !== -1;
        });
        plannerAssetsGraphFiltered = graphFiltered;
        plannerAssetsDvFiltered = dvFiltered;
        if(plannerAssetsGraphTbody){
          var graphRows = graphFiltered.map(function(item){
            var isSelected = plannerAssetsGraphSelected.has(item.id);
            var selectBox = '<input type="checkbox" data-graph-id="' + htmlEscape(item.id) + '"' + (isSelected ? ' checked' : '') + ' />';
            var created = item.createdDateTime ? ('<span class="mono small" title="' + htmlEscape(item.createdDateTime) + '">' + htmlEscape(item.createdDateTime.split('T')[0]) + '</span>') : '<span class="muted">—</span>';
            return '<tr>'
              + '<td>' + selectBox + '</td>'
              + '<td>' + htmlEscape(item.title || '') + '</td>'
              + '<td class="mono small">' + htmlEscape(item.id || '') + '</td>'
              + '<td>' + created + '</td>'
              + '</tr>';
          }).join('');
          plannerAssetsGraphTbody.innerHTML = graphRows || '<tr><td colspan="4" class="muted">No Graph plans found.</td></tr>';
        }
        if(plannerAssetsDvTbody){
          var dvRows = dvFiltered.map(function(item){
            var isSelected = plannerAssetsDvSelected.has(item.id);
            var selectBox = '<input type="checkbox" data-dv-id="' + htmlEscape(item.id) + '"' + (isSelected ? ' checked' : '') + ' />';
            return '<tr>'
              + '<td>' + selectBox + '</td>'
              + '<td>' + htmlEscape(item.title || '') + '</td>'
              + '<td class="mono small">' + htmlEscape(item.bcNo || '—') + '</td>'
              + '<td class="mono small">' + htmlEscape(item.id || '') + '</td>'
              + '</tr>';
          }).join('');
          plannerAssetsDvTbody.innerHTML = dvRows || '<tr><td colspan="4" class="muted">No Premium projects found.</td></tr>';
        }
        updatePlannerAssetsCounts();
      }
      async function fetchPlannerAssets(){
        setPlannerAssetsStatus('Loading Graph + Premium projects…', 'muted');
        try{
          var res = await fetch('/api/admin-planner-assets', { headers: { 'cache-control': 'no-cache' }, credentials: 'same-origin' });
          var payload;
          try{
            payload = await res.json();
          }catch(parseErr){
            var text = await res.text();
            throw new Error(text || (parseErr && parseErr.message ? parseErr.message : 'Invalid JSON'));
          }
          if(!res.ok || (payload && payload.ok === false)){
            var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
            throw new Error(msg);
          }
          plannerAssetsGraphCache = Array.isArray(payload.graphPlans) ? payload.graphPlans : [];
          plannerAssetsDvCache = Array.isArray(payload.dataverseProjects) ? payload.dataverseProjects.map(function(item){
            return {
              id: item[payload.mapping.projectIdField],
              title: item[payload.mapping.projectTitleField],
              bcNo: payload.mapping.projectBcNoField ? item[payload.mapping.projectBcNoField] : ''
            };
          }) : [];
          plannerAssetsGraphSelected.clear();
          plannerAssetsDvSelected.clear();
          renderPlannerAssets();
          renderPlannerAssetsOutput(null);
          setPlannerAssetsStatus('Loaded ' + plannerAssetsGraphCache.length + ' Graph plans and ' + plannerAssetsDvCache.length + ' Premium projects.', 'ok');
        }catch(err){
          plannerAssetsGraphCache = [];
          plannerAssetsDvCache = [];
          plannerAssetsGraphSelected.clear();
          plannerAssetsDvSelected.clear();
          renderPlannerAssets();
          renderPlannerAssetsOutput(err && err.message ? err.message : err);
          setPlannerAssetsStatus('Failed to load planner assets: ' + (err && err.message ? err.message : 'error'), 'bad');
        }
      }
      async function runPlannerAssetsAction(payload){
        var res = await fetch('/api/admin-planner-assets', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
          credentials: 'same-origin'
        });
        var data;
        try{
          data = await res.json();
        }catch(parseErr){
          var text = await res.text();
          throw new Error(text || (parseErr && parseErr.message ? parseErr.message : 'Invalid JSON'));
        }
        if(!res.ok || (data && data.ok === false)){
          var msg = data && data.error ? data.error : ('HTTP ' + res.status);
          throw new Error(msg);
        }
        return data;
      }
      function renderPlannerOutput(payload){
        if(!plannerOutputEl) return;
        if(payload == null){
          plannerOutputEl.style.display = 'none';
          plannerOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        plannerOutputEl.textContent = text;
        plannerOutputEl.style.display = 'block';
      }
      function renderWebhookOutput(payload){
        if(!plannerWebhookOutputEl) return;
        if(payload == null){
          plannerWebhookOutputEl.style.display = 'none';
          plannerWebhookOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        plannerWebhookOutputEl.textContent = text;
        plannerWebhookOutputEl.style.display = 'block';
      }
      function renderBcOutput(payload){
        if(!bcWebhookOutputEl) return;
        if(payload == null){
          bcWebhookOutputEl.style.display = 'none';
          bcWebhookOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        bcWebhookOutputEl.textContent = text;
        bcWebhookOutputEl.style.display = 'block';
      }
      function renderPlannerSyncLog(items){
        if(!plannerSyncLogOutputEl) return;
        if(items == null){
          plannerSyncLogOutputEl.style.display = 'none';
          plannerSyncLogOutputEl.textContent = '';
          return;
        }
        var text = '';
        if(Array.isArray(items)){
          text = items.map(function(entry){
            try { return JSON.stringify(entry); } catch(e){ return String(entry); }
          }).join('\n');
        } else {
          try { text = typeof items === 'string' ? items : JSON.stringify(items, null, 2); }
          catch(e){ text = String(items); }
        }
        plannerSyncLogOutputEl.textContent = text;
        plannerSyncLogOutputEl.style.display = 'block';
      }
      function appendWebhookLine(entry){
        if(!plannerWebhookFeedEl) return;
        var line = '';
        try { line = JSON.stringify(entry); } catch(e){ line = String(entry); }
        var existing = plannerWebhookFeedEl.textContent || '';
        plannerWebhookFeedEl.textContent = (existing ? existing + '\n' : '') + line;
        plannerWebhookFeedEl.style.display = 'block';
      }
      function appendBcWebhookLine(entry){
        if(!bcWebhookFeedEl) return;
        var line = '';
        try { line = JSON.stringify(entry); } catch(e){ line = String(entry); }
        var existing = bcWebhookFeedEl.textContent || '';
        bcWebhookFeedEl.textContent = (existing ? existing + '\n' : '') + line;
        bcWebhookFeedEl.style.display = 'block';
      }
      function logPlannerEvent(message, tone){
        if(!plannerLogEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        var ts = new Date().toLocaleTimeString();
        var item = '<li><span class="mono small">' + htmlEscape(ts) + '</span> '
          + '<span class="badge ' + toneClass + '">' + htmlEscape(tone || 'info') + '</span> '
          + htmlEscape(message) + '</li>';
        plannerLogEl.insertAdjacentHTML('afterbegin', item);
        var items = plannerLogEl.querySelectorAll('li');
        if(items.length > 12){
          for(var i = items.length - 1; i >= 12; i--){ items[i].remove(); }
        }
      }
      function setPlannerBusy(isBusy){
        var buttons = [
          plannerRunBcBtn,
          plannerRunBcPrBtn,
          plannerDebugTaskBtn,
          plannerWebhookLogBtn,
          plannerWebhookStreamBtn,
          plannerWebhookStopBtn,
          plannerWebhookClearBtn,
          plannerTestWebhookBtn
        ];
        buttons.forEach(function(btn){ if(btn) btn.disabled = isBusy; });
      }
      function renderUspsPreview(rows){
        if(!uspsPreviewEl) return;
        if(!Array.isArray(rows) || !rows.length){
          uspsPreviewEl.className = 'small muted';
          uspsPreviewEl.textContent = 'Preview will show first rows after processing.';
          return;
        }
        var total = rows.length;
        var sample = rows.slice(0, Math.min(total, 5));
        var items = sample.map(function(row){
          var status = row && row.status ? String(row.status) : (row && row.error ? 'error' : (row && (row.address1 || row.zip5) ? 'success' : 'pending'));
          var cls = statusCls(status);
          var addressParts = [];
          if(row && row.address1) addressParts.push(row.address1);
          if(row && row.address2) addressParts.push(row.address2);
          var cityState = [];
          if(row && row.city) cityState.push(row.city);
          if(row && row.state) cityState.push(row.state);
          if(cityState.length) addressParts.push(cityState.join(', '));
          var zip = '';
          if(row){
            var zip5 = (row.zip5 || '').toString().trim();
            var zip4 = (row.zip4 || '').toString().trim();
            if(zip5) zip = zip5 + (zip4 ? '-' + zip4 : '');
          }
          if(zip) addressParts.push(zip);
          if(row && row.country) addressParts.push(row.country);
          var displayAddress = addressParts.join(', ');
          if(!displayAddress) displayAddress = '(no standardized match)';
          var inputParts = [];
          if(row && row.input_address1) inputParts.push(row.input_address1);
          if(row && row.input_address2) inputParts.push(row.input_address2);
          var inputCityState = [];
          if(row && row.input_city) inputCityState.push(row.input_city);
          if(row && row.input_state) inputCityState.push(row.input_state);
          if(inputCityState.length) inputParts.push(inputCityState.join(', '));
          if(row){
            var inputZip = '';
            if(row.input_zip5) inputZip = String(row.input_zip5);
            if(row.input_zip4) inputZip = inputZip ? inputZip + '-' + row.input_zip4 : String(row.input_zip4);
            if(inputZip) inputParts.push(inputZip);
          }
          if(row && row.input_country) inputParts.push(row.input_country);
          var metaParts = [];
          if(row && row.row != null) metaParts.push('row ' + row.row);
          if(inputParts.length) metaParts.push('input: ' + inputParts.join(', '));
          if(row && row.dpvConfirmation) metaParts.push('dpv: ' + row.dpvConfirmation);
          var meta = metaParts.map(function(part){ return htmlEscape(String(part)); }).join(' · ');
          var errorBlock = row && row.error ? '<div class="bad small">' + htmlEscape(String(row.error)) + '</div>' : '';
          var notesBlock = row && row.footnotes ? '<div class="muted small">' + htmlEscape('Notes: ' + row.footnotes) + '</div>' : '';
          return '<li>'
            + '<span class="badge ' + cls + '">' + htmlEscape(String(status)) + '</span> '
            + htmlEscape(displayAddress)
            + (meta ? '<span class="meta">' + meta + '</span>' : '')
            + errorBlock
            + notesBlock
            + '</li>';
        }).join('');
        uspsPreviewEl.className = 'small';
        uspsPreviewEl.innerHTML = '<div class="muted small">Preview (' + sample.length + ' of ' + total + ')</div><ul class="step-list">' + items + '</ul>';
      }
      function resetUspsState(options){
        if(uspsDownloadUrl){
          URL.revokeObjectURL(uspsDownloadUrl);
          uspsDownloadUrl = null;
        }
        if(uspsPendingDownloadUrl){
          URL.revokeObjectURL(uspsPendingDownloadUrl);
          uspsPendingDownloadUrl = null;
        }
        if(uspsSummaryEl){
          uspsSummaryEl.style.display = 'none';
          uspsSummaryEl.textContent = '';
        }
        if(uspsDownloadWrap){
          uspsDownloadWrap.style.display = 'none';
        }
        if(uspsPendingDownloadWrap){
          uspsPendingDownloadWrap.style.display = 'none';
        }
        renderUspsPreview([]);
        if(options && options.clearFile && uspsFileInput){
          uspsFileInput.value = '';
        }
        if(!options || options.keepStatus !== true){
          setUspsStatus('Choose a CSV to start.', 'muted');
        }
      }
      resetUspsState();
      function formatStep(step){
        if(!step) return '';
        var clone = Object.assign({}, step);
        var ts = clone.ts ? '<span class="mono small">' + htmlEscape(clone.ts) + '</span>' : '';
        var msg = clone.msg ? htmlEscape(clone.msg) : '';
        delete clone.ts;
        delete clone.msg;
        var metaEntries = Object.keys(clone).filter(function(k){ return clone[k] !== undefined && clone[k] !== null && clone[k] !== ''; });
        var meta = '';
        if(metaEntries.length){
          var metaStr = metaEntries.map(function(k){
            var v = clone[k];
            if(typeof v === 'object'){ try { v = JSON.stringify(v); } catch(e){ v = String(v); } }
            v = String(v);
            if(v.length > 240) v = v.slice(0,240) + '…';
            return k + '=' + v;
          }).join(' · ');
          meta = '<span class="meta">' + htmlEscape(metaStr) + '</span>';
        }
        return '<li>' + (ts ? ts + (msg ? ' ' : '') : '') + msg + meta + '</li>';
      }
      function buildDetails(it){
        var parts = [];
        if(it.folderName) parts.push('folder: <span class="mono">' + htmlEscape(it.folderName) + '</span>');
        if(it.phase) parts.push('phase: <span class="mono">' + htmlEscape(it.phase) + '</span>');
        if(it.traceId) parts.push('trace: <span class="mono">' + htmlEscape(it.traceId) + '</span>');
        if(it.errorStatus != null) parts.push('status: <span class="mono">' + htmlEscape(String(it.errorStatus)) + '</span>');
        if(it.errorContentRange) parts.push('range: <span class="mono">' + htmlEscape(it.errorContentRange) + '</span>');
        if(it.reason) parts.push('reason: ' + htmlEscape(it.reason));
        if(it.error) parts.push('error: ' + htmlEscape(it.error));
        var main = parts.join(' · ');
        var extras = [];
        if(Array.isArray(it.steps) && it.steps.length){
          extras.push('<details class="log"><summary>Logs (' + it.steps.length + ')</summary><ul class="step-list">' + it.steps.map(formatStep).join('') + '</ul></details>');
        }
        if(typeof it.errorResponse === 'string' && it.errorResponse){
          var resp = it.errorResponse.length > 2000 ? it.errorResponse.slice(0,2000) + '…' : it.errorResponse;
          extras.push('<details class="log"><summary>Error response</summary><pre class="log-block">' + htmlEscape(resp) + '</pre></details>');
        }
        if(typeof it.errorStack === 'string' && it.errorStack){
          var stack = it.errorStack.length > 4000 ? it.errorStack.slice(0,4000) + '…' : it.errorStack;
          extras.push('<details class="log"><summary>Error stack</summary><pre class="log-block">' + htmlEscape(stack) + '</pre></details>');
        }
        if(!main && !extras.length) return '<span class="muted">—</span>';
        return main + (extras.length ? '<div class="log-extras">' + extras.join('') + '</div>' : '');
      }
      function render(items){
        var q = (input.value||'').trim().toLowerCase();
        var filtered = !q ? items : items.filter(function(it){
          var hay = [
            it.loggedAt,
            it.type,
            it.status,
            it.folderName,
            it.phase,
            it.reason,
            it.error,
            it.traceId,
            it.errorStatus,
            it.errorContentRange,
            it.errorResponse,
            (it.uploaded==null?'':String(it.uploaded))
          ].join(' ').toLowerCase();
          return hay.indexOf(q) !== -1;
        });
        var rows = filtered.map(function(it){
          var files = Array.isArray(it.files) ? it.files : [];
          var filesCount = files.length ? (files.length + ' file' + (files.length===1?'':'s')) : '';
          var links = files.map(function(f){
            return '<a href="' + htmlEscape(f.url||'') + '" target="_blank" rel="noreferrer">' + htmlEscape(f.filename||f.url||'file') + '</a>';
          }).join(', ');
          var details = buildDetails(it);
          return '<tr>'
            + '<td class="mono small">' + htmlEscape(it.loggedAt||'') + '</td>'
            + '<td>' + htmlEscape(it.type||'') + '</td>'
            + '<td><span class="badge ' + statusCls(it.status) + '">' + (htmlEscape(it.status||'')||'n/a') + '</span></td>'
            + '<td>' + details + '</td>'
            + '<td style="text-align:center">' + htmlEscape(it.uploaded==null?'':String(it.uploaded)) + '</td>'
            + '<td>' + filesCount + (links?'<div class="small">' + links + '</div>':'') + '</td>'
            + '</tr>';
        }).join('');
        tbody.innerHTML = rows || '<tr><td colspan="6" class="muted">No submissions yet.</td></tr>';
        last.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
      }
      async function fetchAndRender(){
        try{
          var res = await fetch('/api/submissions?limit=500', { headers: { 'cache-control': 'no-cache' } });
          if(!res.ok) return;
          var data = await res.json();
          cache = Array.isArray(data.items)? data.items : [];
          render(cache);
          if(last) last.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
        }catch(e){
          if(last) last.textContent = 'Last updated: failed (' + (e && e.message ? e.message : 'error') + ')';
        }
      }
      if(uspsResetBtn){
        uspsResetBtn.addEventListener('click', function(){
          resetUspsState({ clearFile: true });
        });
      }
      if(uspsVerifyBtn){
        uspsVerifyBtn.addEventListener('click', async function(){
          if(uspsLoading) return;
          uspsLoading = true;
          var originalText = uspsVerifyBtn.textContent;
          uspsVerifyBtn.disabled = true;
          uspsVerifyBtn.textContent = 'Verifying…';
          setUspsStatus('Verifying USPS credentials…', 'muted');
          try{
            var res = await fetch('/api/usps-verify');
            var payload;
            var ct = res.headers.get('content-type') || '';
            if(ct.indexOf('application/json') !== -1){
              payload = await res.json();
            } else {
              var raw = await res.text();
              try{ payload = JSON.parse(raw); }
              catch(e){ throw new Error(raw || ('HTTP ' + res.status)); }
            }
            if(!res.ok || (payload && payload.ok === false)){
              var message = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(message);
            }
            var expires = payload && payload.expiresIn ? ('Token expires in ' + Math.round(payload.expiresIn) + 's.') : 'Token verified.';
            var preview = payload && payload.tokenPreview ? ' Preview: ' + payload.tokenPreview : '';
            setUspsStatus('USPS credentials verified. ' + expires + preview, 'ok');
          }catch(err){
            setUspsStatus('Verification failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          }finally{
            uspsLoading = false;
            uspsVerifyBtn.disabled = false;
            uspsVerifyBtn.textContent = originalText;
          }
        });
      }
      if(uspsFileInput){
        uspsFileInput.addEventListener('change', function(){
          if(uspsFileInput.files && uspsFileInput.files.length){
            resetUspsState({ keepStatus: true });
            setUspsStatus('Ready to format ' + uspsFileInput.files[0].name + '.', 'muted');
          } else {
            resetUspsState();
          }
        });
      }
      if(uspsForm){
        uspsForm.addEventListener('submit', async function(ev){
          ev.preventDefault();
          if(uspsLoading) return;
          if(!uspsFileInput || !uspsFileInput.files || !uspsFileInput.files[0]){
            setUspsStatus('Choose a CSV file first.', 'warn');
            return;
          }
          uspsLoading = true;
          resetUspsState({ keepStatus: true });
          setUspsStatus('Formatting addresses…', 'muted');
          try{
            var fd = new FormData();
            var file = uspsFileInput.files[0];
            fd.append('file', file, file.name);
            var res = await fetch('/api/usps-format', { method: 'POST', body: fd });
            var payload;
            var ct = res.headers.get('content-type') || '';
            if(ct.indexOf('application/json') !== -1){
              payload = await res.json();
            } else {
              var rawText = await res.text();
              try { payload = JSON.parse(rawText); }
              catch(parseErr){ throw new Error(rawText || ('HTTP ' + res.status)); }
            }
            if(!res.ok || (payload && payload.ok === false) || (payload && payload.error)){
              var errMessage = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(errMessage);
            }
            if(uspsSummaryEl && payload && payload.summary){
              var summary = payload.summary || {};
              var processed = summary.processed != null ? summary.processed : (summary.success != null ? summary.success : 0);
              var total = summary.total != null ? summary.total : 0;
              var pendingCount = summary.pending != null ? summary.pending : 0;
              var errorCount = summary.errors != null ? summary.errors : 0;
              var requestsUsed = summary.requestsUsed;
              var maxRequests = summary.maxRequests;
              var uniqueRequests = summary.uniqueRequests;
              var summaryParts = [];
              summaryParts.push('Processed ' + processed + ' of ' + total + ' rows.');
              if(pendingCount){ summaryParts.push(pendingCount + ' pending (quota limit).'); }
              if(errorCount){ summaryParts.push(errorCount + ' errors.'); }
              if(requestsUsed != null && maxRequests != null){ summaryParts.push('USPS requests used: ' + requestsUsed + '/' + maxRequests + '.'); }
              if(uniqueRequests != null){ summaryParts.push('Unique addresses this run: ' + uniqueRequests + '.'); }
              uspsSummaryEl.style.display = 'block';
              uspsSummaryEl.className = 'small';
              uspsSummaryEl.textContent = summaryParts.join(' ');
            }
            if(uspsDownloadWrap && uspsDownloadEl){
              if(payload && payload.csv){
                if(uspsDownloadUrl){ URL.revokeObjectURL(uspsDownloadUrl); }
                uspsDownloadUrl = URL.createObjectURL(new Blob([payload.csv], { type: 'text/csv;charset=utf-8;' }));
                uspsDownloadEl.href = uspsDownloadUrl;
                uspsDownloadEl.download = (payload && payload.filename) || 'addresses-standardized.csv';
                uspsDownloadWrap.style.display = 'block';
              } else {
                if(uspsDownloadUrl){ URL.revokeObjectURL(uspsDownloadUrl); uspsDownloadUrl = null; }
                uspsDownloadWrap.style.display = 'none';
              }
            }
            if(uspsPendingDownloadWrap && uspsPendingDownloadEl){
              if(payload && payload.pendingCsv){
                if(uspsPendingDownloadUrl){ URL.revokeObjectURL(uspsPendingDownloadUrl); }
                uspsPendingDownloadUrl = URL.createObjectURL(new Blob([payload.pendingCsv], { type: 'text/csv;charset=utf-8;' }));
                uspsPendingDownloadEl.href = uspsPendingDownloadUrl;
                uspsPendingDownloadEl.download = (payload && payload.pendingFilename) || 'addresses-pending.csv';
                uspsPendingDownloadWrap.style.display = 'block';
              } else {
                if(uspsPendingDownloadUrl){ URL.revokeObjectURL(uspsPendingDownloadUrl); uspsPendingDownloadUrl = null; }
                uspsPendingDownloadWrap.style.display = 'none';
              }
            }
            if(payload && Array.isArray(payload.rows)){
              renderUspsPreview(payload.rows);
            } else {
              renderUspsPreview([]);
            }
            var sum = payload && payload.summary ? payload.summary : {};
            var sumProcessed = sum.processed != null ? sum.processed : (sum.success != null ? sum.success : 0);
            var sumPending = sum.pending != null ? sum.pending : 0;
            var sumErrors = sum.errors != null ? sum.errors : 0;
            var tone = 'ok';
            var statusMsg = 'Formatting complete.';
            if(sumPending){
              tone = 'warn';
              statusMsg = 'Processed ' + sumProcessed + ' rows; ' + sumPending + ' pending due to quota. Download the pending list to resume later.';
            } else if(sumErrors){
              tone = 'warn';
              statusMsg = 'Processed ' + sumProcessed + ' rows with ' + sumErrors + ' errors. Review the preview for details.';
            } else if(payload && (!payload.csv || sumProcessed === 0)){
              statusMsg = 'No rows processed.';
            } else {
              statusMsg += ' Download is ready.';
            }
            setUspsStatus(statusMsg, tone);
          }catch(err){
            resetUspsState({ keepStatus: true });
            setUspsStatus('Error: ' + (err && err.message ? err.message : 'Unable to format addresses.'), 'bad');
          }finally{
            uspsLoading = false;
          }
        });
      }
      input.addEventListener('input', function(){ render(cache); });
      async function runPlannerRequest(options){
        var url = options.url;
        var body = options.body;
        var method = options.method || 'POST';
        setPlannerStatus(options.label + '…', 'muted');
        renderPlannerOutput(null);
        setPlannerBusy(true);
        logPlannerEvent(options.label + ' started', 'info');
        logPlannerEvent('Request: ' + method + ' ' + url, 'info');
        if(typeof fetch !== 'function'){
          setPlannerStatus('Fetch is not available in this browser.', 'bad');
          logPlannerEvent('Fetch is not available in this browser.', 'bad');
          setPlannerBusy(false);
          return;
        }
        if(body){
          try {
            logPlannerEvent('Body: ' + JSON.stringify(body), 'info');
          } catch(e) {
            logPlannerEvent('Body: [unserializable]', 'warn');
          }
        }
        try{
          var startTime = Date.now();
          var res = await fetch(url, {
            method: method,
            headers: body ? { 'Content-Type': 'application/json' } : undefined,
            body: body ? JSON.stringify(body) : undefined
          });
          var ct = res.headers.get('content-type') || '';
          logPlannerEvent('Response: ' + res.status + ' ' + (res.statusText || ''), res.ok ? 'ok' : 'bad');
          logPlannerEvent('Content-Type: ' + (ct || 'n/a'), 'info');
          var payload;
          if(ct.indexOf('application/json') !== -1){
            payload = await res.json();
          } else {
            payload = await res.text();
          }
          if(!res.ok || (payload && payload.ok === false)){
            var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
            setPlannerStatus(options.label + ' failed: ' + msg, 'bad');
            renderPlannerOutput(payload || msg);
            logPlannerEvent(options.label + ' failed: ' + msg, 'bad');
            return;
          }
          var elapsed = Date.now() - startTime;
          setPlannerStatus(options.label + ' complete in ' + elapsed + 'ms.', 'ok');
          renderPlannerOutput(payload);
          logPlannerEvent(options.label + ' complete', 'ok');
          return;
        }catch(err){
          setPlannerStatus(options.label + ' failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          renderPlannerOutput(err && err.message ? err.message : err);
          logPlannerEvent(options.label + ' failed: ' + (err && err.message ? err.message : 'error'), 'bad');
        } finally {
          setPlannerBusy(false);
        }
      }
      function parseEntitySets(input){
        if(!input) return [];
        return String(input).split(',').map(function(item){ return item.trim(); }).filter(Boolean);
      }
      function getCronSecret(){
        return bcCronSecretInput && bcCronSecretInput.value ? bcCronSecretInput.value.trim() : '';
      }
      async function runBcRequest(options){
        var url = options.url;
        var body = options.body;
        var method = options.method || (body ? 'POST' : 'GET');
        var headers = options.headers || {};
        setBcStatus((options.label || 'Request') + '…', 'muted');
        renderBcOutput(null);
        if(typeof fetch !== 'function'){
          setBcStatus('Fetch is not available in this browser.', 'bad');
          return;
        }
        if(body && !headers['Content-Type']){
          headers['Content-Type'] = 'application/json';
        }
        try{
          var res = await fetch(url, {
            method: method,
            headers: Object.keys(headers).length ? headers : undefined,
            body: body ? JSON.stringify(body) : undefined
          });
          var ct = res.headers.get('content-type') || '';
          var payload = ct.indexOf('application/json') !== -1 ? await res.json() : await res.text();
          if(!res.ok || (payload && payload.ok === false)){
            var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
            setBcStatus((options.label || 'Request') + ' failed: ' + msg, 'bad');
            renderBcOutput(payload || msg);
            return payload;
          }
          setBcStatus((options.label || 'Request') + ' complete', 'ok');
          renderBcOutput(payload);
          return payload;
        }catch(err){
          setBcStatus((options.label || 'Request') + ' failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          renderBcOutput(err && err.message ? err.message : err);
        }
      }
      async function fetchPlannerProjects(){
        if(!plannerProjectsTbody) return;
        setPlannerProjectsStatus('Loading premium projects…', 'muted');
        try{
          var res = await fetch('/api/sync/projects', { headers: { 'cache-control': 'no-cache' } });
          var payload = await res.json();
          if(!res.ok || (payload && payload.ok === false)){
            var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
            throw new Error(msg);
          }
          plannerProjectsCache = Array.isArray(payload.projects) ? payload.projects : [];
          plannerOrphanCache = [];
          if(plannerProjectsSelected.size){
            var available = new Set(plannerProjectsCache.map(function(item){ return item.projectNo; }));
            plannerProjectsSelected.forEach(function(projectNo){
              if(!available.has(projectNo)) plannerProjectsSelected.delete(projectNo);
            });
          }
          renderPlannerProjects();
          setPlannerProjectsStatus('Loaded ' + plannerProjectsCache.length + ' projects.', 'ok');
        }catch(err){
          plannerProjectsCache = [];
          plannerOrphanCache = [];
          plannerProjectsSelected.clear();
          renderPlannerProjects();
          setPlannerProjectsStatus('Failed to load premium projects: ' + (err && err.message ? err.message : 'error'), 'bad');
        }
      }
      async function updateProjectSync(projectNo, disabled){
        if(!projectNo) return;
        setPlannerProjectsStatus((disabled ? 'Disabling' : 'Enabling') + ' sync for ' + projectNo + '…', 'muted');
        try{
          await applyProjectSync(projectNo, disabled);
          await fetchPlannerProjects();
        }catch(err){
          setPlannerProjectsStatus('Failed to update sync: ' + (err && err.message ? err.message : 'error'), 'bad');
        }
      }
      async function applyProjectSync(projectNo, disabled){
        var res = await fetch('/api/sync/projects', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ projectNo: projectNo, disabled: !!disabled })
        });
        var payload = await res.json();
        if(!res.ok || (payload && payload.ok === false)){
          var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
          throw new Error(msg);
        }
        return payload;
      }
      async function clearProjectLinks(projectNo){
        if(!projectNo) return;
        var confirmed = window.confirm('Clear Premium links for ' + projectNo + '? This disables sync and removes Premium IDs from BC.');
        if(!confirmed) return;
        setPlannerProjectsStatus('Clearing Premium links for ' + projectNo + '…', 'muted');
        try{
          var res = await fetch('/api/sync/projects', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'clear-links', projectNo: projectNo })
          });
          var payload = await res.json();
          if(!res.ok || (payload && payload.ok === false)){
            var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
            throw new Error(msg);
          }
          renderPlannerOutput(payload);
          await fetchPlannerProjects();
          setPlannerProjectsStatus('Links cleared for ' + projectNo + '.', 'ok');
          logPlannerEvent('Links cleared for ' + projectNo, 'ok');
        }catch(err){
          var msg = err && err.message ? err.message : 'error';
          renderPlannerOutput({ error: msg });
          setPlannerProjectsStatus('Clear links failed for ' + projectNo + ': ' + msg, 'bad');
          logPlannerEvent('Clear links failed for ' + projectNo + ': ' + msg, 'bad');
        }
      }
      async function bulkToggleSync(disabled){
        if(!plannerProjectsSelected.size){
          setPlannerProjectsStatus('Select at least one project to update.', 'warn');
          return;
        }
        var list = Array.from(plannerProjectsSelected);
        setPlannerProjectsStatus((disabled ? 'Disabling' : 'Enabling') + ' sync for ' + list.length + ' projects…', 'muted');
        var failed = [];
        for(var i = 0; i < list.length; i++){
          var projectNo = list[i];
          try{
            await applyProjectSync(projectNo, disabled);
          }catch(err){
            failed.push(projectNo + ': ' + (err && err.message ? err.message : 'error'));
          }
        }
        await fetchPlannerProjects();
        if(failed.length){
          setPlannerProjectsStatus('Bulk update completed with ' + failed.length + ' failures.', 'warn');
          logPlannerEvent('Bulk sync toggle errors: ' + failed.join(' | '), 'warn');
        } else {
          setPlannerProjectsStatus('Bulk update complete for ' + list.length + ' projects.', 'ok');
        }
      }
      async function runBcSync(projectNo){
        var res = await fetch('/api/sync/bc-to-premium', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ projectNo: projectNo })
        });
        var ct = res.headers.get('content-type') || '';
        var payload;
        if(ct.indexOf('application/json') !== -1){
          payload = await res.json();
        } else {
          payload = await res.text();
        }
        if(!res.ok || (payload && payload.ok === false)){
          var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
          throw new Error(msg);
        }
        return payload;
      }
      async function bulkSyncProjects(){
        if(!plannerProjectsSelected.size){
          setPlannerProjectsStatus('Select at least one project to sync.', 'warn');
          return;
        }
        var list = Array.from(plannerProjectsSelected);
        setPlannerProjectsStatus('Running sync for ' + list.length + ' projects…', 'muted');
        var failed = [];
        for(var i = 0; i < list.length; i++){
          var projectNo = list[i];
          try{
            await runBcSync(projectNo);
            logPlannerEvent('Sync (' + projectNo + ') complete', 'ok');
          }catch(err){
            var msg = err && err.message ? err.message : 'error';
            failed.push(projectNo + ': ' + msg);
            logPlannerEvent('Sync (' + projectNo + ') failed: ' + msg, 'bad');
          }
        }
        await fetchPlannerProjects();
        if(failed.length){
          setPlannerProjectsStatus('Bulk sync completed with ' + failed.length + ' failures.', 'warn');
        } else {
          setPlannerProjectsStatus('Bulk sync complete for ' + list.length + ' projects.', 'ok');
        }
      }
      if(plannerRunBcBtn){
        plannerRunBcBtn.addEventListener('click', function(){
          var projectNo = plannerProjectInput && plannerProjectInput.value ? plannerProjectInput.value.trim() : '';
          var body = projectNo ? { projectNo: projectNo } : undefined;
          runPlannerRequest({ label: 'Sync', url: '/api/sync/bc-to-premium', body: body });
        });
      }
      if(plannerRunBcPrBtn){
        plannerRunBcPrBtn.addEventListener('click', function(){
          runPlannerRequest({ label: 'Sync (PR00001)', url: '/api/sync/bc-to-premium', body: { projectNo: 'PR00001' } });
        });
      }
      if(plannerPollPremiumBtn){
        plannerPollPremiumBtn.addEventListener('click', function(){
          var cronSecret = getCronSecret();
          var url = cronSecret ? ('/api/sync/premium-change/poll?cronSecret=' + encodeURIComponent(cronSecret)) : '/api/sync/premium-change/poll';
          runPlannerRequest({ label: 'Poll Premium Changes', url: url, method: 'POST' });
        });
      }
      if(plannerDebugTaskBtn){
        plannerDebugTaskBtn.addEventListener('click', function(){
          var projectNo = plannerProjectInput && plannerProjectInput.value ? plannerProjectInput.value.trim() : '';
          var taskNo = plannerDebugTaskInput && plannerDebugTaskInput.value ? plannerDebugTaskInput.value.trim() : '';
          if(!projectNo || !taskNo){
            setPlannerStatus('Enter both Project No and Task No to inspect.', 'warn');
            logPlannerEvent('Missing Project No or Task No for debug.', 'warn');
            return;
          }
          runPlannerRequest({
            label: 'Inspect BC task',
            url: '/api/sync/debug-bc-task',
            body: { projectNo: projectNo, taskNo: taskNo }
          });
        });
      }
      if(plannerDebugDecisionBtn){
        plannerDebugDecisionBtn.addEventListener('click', function(){
          var projectNo = plannerProjectInput && plannerProjectInput.value ? plannerProjectInput.value.trim() : '';
          var taskNo = plannerDebugTaskInput && plannerDebugTaskInput.value ? plannerDebugTaskInput.value.trim() : '';
          if(!projectNo || !taskNo){
            setPlannerStatus('Enter both Project No and Task No to inspect.', 'warn');
            logPlannerEvent('Missing Project No or Task No for decision debug.', 'warn');
            return;
          }
          runPlannerRequest({
            label: 'Inspect sync decision',
            url: '/api/sync/debug-bc-task?includePremium=1',
            body: { projectNo: projectNo, taskNo: taskNo }
          });
        });
      }
      if(plannerDebugBcTimestampsBtn){
        plannerDebugBcTimestampsBtn.addEventListener('click', function(){
          runPlannerRequest({
            label: 'Inspect BC timestamps',
            url: '/api/sync/debug-bc-timestamps?limit=25',
            method: 'GET'
          });
        });
      }
      if(plannerSyncLogLoadBtn){
        plannerSyncLogLoadBtn.addEventListener('click', async function(){
          setPlannerSyncLogStatus('Loading premium logs...', 'muted');
          renderPlannerSyncLog(null);
          try{
            var res = await fetch('/api/sync/premium-log?limit=200', {
              headers: { 'cache-control': 'no-cache' }
            });
            var payload = await res.json();
            if(!res.ok || (payload && payload.ok === false)){
              var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(msg);
            }
            renderPlannerSyncLog(payload && payload.items ? payload.items : payload);
            var count = payload && payload.items ? payload.items.length : 0;
            setPlannerSyncLogStatus('Logs loaded (' + count + ').', 'ok');
          }catch(err){
            setPlannerSyncLogStatus('Log load failed: ' + (err && err.message ? err.message : 'error'), 'bad');
            renderPlannerSyncLog({ error: err && err.message ? err.message : String(err) });
          }
        });
      }
      if(plannerSyncLogClearBtn){
        plannerSyncLogClearBtn.addEventListener('click', function(){
          renderPlannerSyncLog(null);
          setPlannerSyncLogStatus('Cleared.', 'muted');
        });
      }
      if(plannerWebhookLogBtn){
        plannerWebhookLogBtn.addEventListener('click', async function(){
          setPlannerStatus('Loading webhook log…', 'muted');
          renderWebhookOutput(null);
          try{
            var res = await fetch('/api/sync/webhook-log?limit=50', {
              headers: { 'cache-control': 'no-cache' }
            });
            var payload = await res.json();
            if(!res.ok || (payload && payload.ok === false)){
              var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(msg);
            }
            renderWebhookOutput(payload);
            setPlannerStatus('Webhook log loaded.', 'ok');
            logPlannerEvent('Webhook log loaded', 'ok');
          }catch(err){
            setPlannerStatus('Webhook log failed: ' + (err && err.message ? err.message : 'error'), 'bad');
            renderWebhookOutput({ error: err && err.message ? err.message : String(err) });
            logPlannerEvent('Webhook log failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          }
        });
      }
      var webhookEventSource = null;
      function stopWebhookStream(){
        if(webhookEventSource){
          webhookEventSource.close();
          webhookEventSource = null;
          setPlannerStatus('Webhook feed stopped.', 'muted');
          logPlannerEvent('Webhook feed stopped', 'info');
        }
      }
      if(plannerWebhookStreamBtn){
        plannerWebhookStreamBtn.addEventListener('click', function(){
          if(typeof EventSource === 'undefined'){
            setPlannerStatus('EventSource not supported in this browser.', 'bad');
            logPlannerEvent('EventSource not supported.', 'bad');
            return;
          }
          if(webhookEventSource){
            setPlannerStatus('Webhook feed already running.', 'warn');
            return;
          }
          if(plannerWebhookFeedEl){
            plannerWebhookFeedEl.textContent = '';
            plannerWebhookFeedEl.style.display = 'block';
          }
          webhookEventSource = new EventSource('/api/sync/webhook-log-stream?include=1');
          webhookEventSource.onmessage = function(ev){
            if(!ev || !ev.data) return;
            try{
              appendWebhookLine(JSON.parse(ev.data));
            }catch(e){
              appendWebhookLine(ev.data);
            }
          };
          webhookEventSource.onerror = function(){
            setPlannerStatus('Webhook feed error. Reconnect to resume.', 'warn');
            logPlannerEvent('Webhook feed error', 'warn');
            stopWebhookStream();
          };
          setPlannerStatus('Webhook feed running…', 'ok');
          logPlannerEvent('Webhook feed started', 'ok');
        });
      }
      if(plannerWebhookStopBtn){
        plannerWebhookStopBtn.addEventListener('click', function(){
          stopWebhookStream();
        });
      }
      if(plannerWebhookClearBtn){
        plannerWebhookClearBtn.addEventListener('click', function(){
          if(plannerWebhookFeedEl){
            plannerWebhookFeedEl.textContent = '';
            plannerWebhookFeedEl.style.display = 'none';
          }
          setPlannerStatus('Webhook feed cleared.', 'muted');
          logPlannerEvent('Webhook feed cleared', 'info');
        });
      }
      if(plannerTestWebhookBtn){
        plannerTestWebhookBtn.addEventListener('click', function(){
          runPlannerRequest({
            label: 'Webhook ping',
            url: '/api/webhooks/dataverse',
            method: 'GET'
          });
        });
      }
      if(bcSubsListBtn){
        bcSubsListBtn.addEventListener('click', function(){
          var entitySets = parseEntitySets(bcEntitySetsInput && bcEntitySetsInput.value ? bcEntitySetsInput.value : '');
          var qs = entitySets.length ? ('?entitySets=' + encodeURIComponent(entitySets.join(','))) : '';
          runBcRequest({ label: 'List BC subscriptions', url: '/api/sync/bc-subscriptions/list' + qs, method: 'GET' });
        });
      }
      if(bcSubsCreateBtn){
        bcSubsCreateBtn.addEventListener('click', function(){
          var entitySets = parseEntitySets(bcEntitySetsInput && bcEntitySetsInput.value ? bcEntitySetsInput.value : '');
          var notificationUrl = bcNotifyInput && bcNotifyInput.value ? bcNotifyInput.value.trim() : '';
          var body = {};
          if(entitySets.length) body.entitySets = entitySets;
          if(notificationUrl) body.notificationUrl = notificationUrl;
          runBcRequest({ label: 'Create BC subscription', url: '/api/sync/bc-subscriptions/create', body: Object.keys(body).length ? body : undefined });
        });
      }
      if(bcSubsRenewBtn){
        bcSubsRenewBtn.addEventListener('click', function(){
          var entitySets = parseEntitySets(bcEntitySetsInput && bcEntitySetsInput.value ? bcEntitySetsInput.value : '');
          var body = {};
          if(entitySets.length) body.entitySets = entitySets;
          var headers = {};
          var secret = getCronSecret();
          if(secret) headers['x-cron-secret'] = secret;
          runBcRequest({ label: 'Renew BC subscriptions', url: '/api/sync/bc-subscriptions/renew', body: Object.keys(body).length ? body : undefined, headers: headers });
        });
      }
      if(bcSubsDeleteBtn){
        bcSubsDeleteBtn.addEventListener('click', function(){
          var entitySets = parseEntitySets(bcEntitySetsInput && bcEntitySetsInput.value ? bcEntitySetsInput.value : '');
          var body = {};
          if(entitySets.length) body.entitySets = entitySets;
          runBcRequest({ label: 'Delete BC subscription', url: '/api/sync/bc-subscriptions/delete', body: Object.keys(body).length ? body : undefined });
        });
      }
      if(bcWebhookValidateBtn){
        bcWebhookValidateBtn.addEventListener('click', function(){
          var token = 'bc_' + Math.random().toString(36).slice(2, 10);
          runBcRequest({ label: 'BC webhook validation', url: '/api/webhooks/bc?validationToken=' + encodeURIComponent(token), method: 'POST' });
        });
      }
      if(bcWebhookTestBtn){
        bcWebhookTestBtn.addEventListener('click', function(){
          var resource = bcResourceInput && bcResourceInput.value ? bcResourceInput.value.trim() : '';
          var systemId = bcSystemIdInput && bcSystemIdInput.value ? bcSystemIdInput.value.trim() : '';
          var entitySets = parseEntitySets(bcEntitySetsInput && bcEntitySetsInput.value ? bcEntitySetsInput.value : '');
          if(!resource && systemId && entitySets.length){
            resource = entitySets[0] + '(' + systemId + ')';
          }
          if(!resource){
            setBcStatus('Provide a resource or systemId to send a test webhook.', 'warn');
            return;
          }
          var changeType = bcChangeTypeInput && bcChangeTypeInput.value ? bcChangeTypeInput.value.trim() : '';
          if(!changeType) changeType = 'updated';
          runBcRequest({
            label: 'BC webhook test',
            url: '/api/webhooks/bc',
            body: { value: [{ resource: resource, changeType: changeType }] }
          });
        });
      }
      if(bcJobsProcessBtn){
        bcJobsProcessBtn.addEventListener('click', function(){
          var headers = {};
          var secret = getCronSecret();
          if(secret) headers['x-cron-secret'] = secret;
          runBcRequest({ label: 'Process BC jobs', url: '/api/sync/bc-jobs/process', method: 'POST', headers: headers });
        });
      }
      if(bcWebhookLogBtn){
        bcWebhookLogBtn.addEventListener('click', async function(){
          setBcStatus('Loading webhook log…', 'muted');
          renderBcOutput(null);
          try{
            var res = await fetch('/api/sync/bc-webhook-log?limit=50', {
              headers: { 'cache-control': 'no-cache' }
            });
            var payload = await res.json();
            if(!res.ok || (payload && payload.ok === false)){
              var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(msg);
            }
            renderBcOutput(payload);
            setBcStatus('Webhook log loaded.', 'ok');
          }catch(err){
            setBcStatus('Webhook log failed: ' + (err && err.message ? err.message : 'error'), 'bad');
            renderBcOutput({ error: err && err.message ? err.message : String(err) });
          }
        });
      }
      var bcWebhookEventSource = null;
      function stopBcWebhookStream(){
        if(bcWebhookEventSource){
          bcWebhookEventSource.close();
          bcWebhookEventSource = null;
          setBcStatus('Webhook feed stopped.', 'muted');
        }
      }
      if(bcWebhookStreamBtn){
        bcWebhookStreamBtn.addEventListener('click', function(){
          if(typeof EventSource === 'undefined'){
            setBcStatus('EventSource not supported in this browser.', 'bad');
            return;
          }
          if(bcWebhookEventSource){
            setBcStatus('Webhook feed already running.', 'warn');
            return;
          }
          if(bcWebhookFeedEl){
            bcWebhookFeedEl.textContent = '';
            bcWebhookFeedEl.style.display = 'block';
          }
          bcWebhookEventSource = new EventSource('/api/sync/bc-webhook-log-stream?include=1');
          bcWebhookEventSource.onmessage = function(ev){
            if(!ev || !ev.data) return;
            try{
              appendBcWebhookLine(JSON.parse(ev.data));
            }catch(e){
              appendBcWebhookLine(ev.data);
            }
          };
          bcWebhookEventSource.onerror = function(){
            setBcStatus('Webhook feed error. Reconnect to resume.', 'warn');
            stopBcWebhookStream();
          };
          setBcStatus('Webhook feed running…', 'ok');
        });
      }
      if(bcWebhookStopBtn){
        bcWebhookStopBtn.addEventListener('click', function(){
          stopBcWebhookStream();
        });
      }
      if(bcWebhookClearBtn){
        bcWebhookClearBtn.addEventListener('click', function(){
          if(bcWebhookFeedEl){
            bcWebhookFeedEl.textContent = '';
            bcWebhookFeedEl.style.display = 'none';
          }
          setBcStatus('Webhook feed cleared.', 'muted');
        });
      }
      window.__plannerAction = function(action){
        if(action === 'bc' && plannerRunBcBtn){ plannerRunBcBtn.click(); return; }
        if(action === 'bc-pr00001' && plannerRunBcPrBtn){ plannerRunBcPrBtn.click(); return; }
        if(action === 'test-webhook' && plannerTestWebhookBtn){ plannerTestWebhookBtn.click(); return; }
        setPlannerStatus('Planner action not available: ' + action, 'warn');
        logPlannerEvent('Planner action not available: ' + action, 'warn');
      };

      if(plannerStatusEl){
        logPlannerEvent('Planner UI ready', 'ok');
      }

      if(typeof window !== 'undefined'){
        window.addEventListener('error', function(ev){
          var msg = ev && ev.message ? ev.message : 'Unknown script error';
          setPlannerStatus('Script error: ' + msg, 'bad');
          logPlannerEvent('Script error: ' + msg, 'bad');
        });
        window.addEventListener('unhandledrejection', function(ev){
          var reason = ev && ev.reason ? (ev.reason.message || ev.reason) : 'Unhandled rejection';
          setPlannerStatus('Unhandled rejection: ' + reason, 'bad');
          logPlannerEvent('Unhandled rejection: ' + reason, 'bad');
        });
      }

      async function downloadActiveProjects(format){
        var target = format === 'json' ? exportJsonBtn : exportCsvBtn;
        if(!target) return;
        try {
          console.log('[Export] Starting download for format:', format);
        } catch(_) {}
        var originalText = target.textContent;
        target.disabled = true;
        target.textContent = (format === 'json' ? 'Exporting JSON…' : 'Exporting CSV…');
        try{
          var res = await fetch('/api/projects-kanban/export?format=' + encodeURIComponent(format || 'csv'), {
            headers: { 'cache-control': 'no-cache' }
          });
          try {
            console.log('[Export] Response status:', res.status, 'ok:', res.ok);
          } catch(_) {}
          if(!res.ok){
            var errPayload = null;
            try { errPayload = await res.json(); } catch(parseErr){
              try { console.log('[Export] Failed to parse error JSON:', parseErr); } catch(__){}
            }
            var msg = (errPayload && errPayload.error) ? errPayload.error : ('HTTP ' + res.status);
            try {
              console.log('[Export] Error payload:', errPayload);
            } catch(_) {}
            throw new Error(msg);
          }
          var ct = res.headers.get('content-type') || '';
          try {
            console.log('[Export] Content-Type:', ct);
          } catch(_) {}
          var filename = (format === 'json') ? 'active-projects.json' : 'active-projects.csv';
          var blob;
          if(ct.indexOf('application/json') !== -1 || format === 'json'){
            var text = await res.text();
            try {
              console.log('[Export] JSON text length:', text && text.length);
            } catch(_) {}
            blob = new Blob([text], { type: 'application/json;charset=utf-8;' });
          } else {
            var textCsv = await res.text();
            try {
              console.log('[Export] CSV text length:', textCsv && textCsv.length);
            } catch(_) {}
            blob = new Blob([textCsv], { type: 'text/csv;charset=utf-8;' });
          }
          var url = URL.createObjectURL(blob);
          var a = document.createElement('a');
          a.href = url;
          a.download = filename;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
        } catch(e){
          try {
            console.error('[Export] Export failed:', e);
          } catch(_) {}
          alert('Export failed: ' + (e && e.message ? e.message : 'error'));
        } finally {
          target.disabled = false;
          target.textContent = originalText;
        }
      }

      if(exportCsvBtn){
        exportCsvBtn.addEventListener('click', function(){
          downloadActiveProjects('csv');
        });
      }
      if(exportJsonBtn){
        exportJsonBtn.addEventListener('click', function(){
          downloadActiveProjects('json');
        });
      }
      async function handleRefresh(){
        if(refreshing) return;
        refreshing = true;
        if(refreshBtn){ refreshBtn.disabled = true; refreshBtn.textContent = 'Refreshing…'; }
        try{
          await fetchAndRender();
        } finally {
          refreshing = false;
          if(refreshBtn){ refreshBtn.disabled = false; refreshBtn.textContent = 'Refresh'; }
        }
      }
      if(refreshBtn){ refreshBtn.addEventListener('click', handleRefresh); }

      if(plannerProjectsFilterInput){
        plannerProjectsFilterInput.addEventListener('input', function(){
          renderPlannerProjects();
        });
      }
      if(plannerProjectsRefreshBtn){
        plannerProjectsRefreshBtn.addEventListener('click', function(){
          fetchPlannerProjects();
        });
      }
      if(plannerProjectsSelectAllBtn){
        plannerProjectsSelectAllBtn.addEventListener('click', function(){
          plannerProjectsFiltered.forEach(function(item){
            plannerProjectsSelected.add(item.projectNo);
          });
          renderPlannerProjects();
        });
      }
      if(plannerProjectsClearBtn){
        plannerProjectsClearBtn.addEventListener('click', function(){
          plannerProjectsSelected.clear();
          renderPlannerProjects();
        });
      }
      if(plannerProjectsBulkEnableBtn){
        plannerProjectsBulkEnableBtn.addEventListener('click', function(){
          bulkToggleSync(false);
        });
      }
      if(plannerProjectsBulkDisableBtn){
        plannerProjectsBulkDisableBtn.addEventListener('click', function(){
          bulkToggleSync(true);
        });
      }
      if(plannerProjectsBulkSyncBtn){
        plannerProjectsBulkSyncBtn.addEventListener('click', function(){
          bulkSyncProjects();
        });
      }
      if(plannerProjectsTbody){
        plannerProjectsTbody.addEventListener('click', async function(ev){
          var target = ev.target;
          if(!target) return;
          var button = target.closest('button');
          if(!button) return;
          var action = button.getAttribute('data-action');
          var projectNo = button.getAttribute('data-project-no') || '';
          if(action === 'toggle-sync'){
            var isDisabled = button.getAttribute('data-disabled') === '1';
            await updateProjectSync(projectNo, !isDisabled);
            return;
          }
          if(action === 'sync-project'){
            await runPlannerRequest({
              label: 'Sync (' + projectNo + ')',
              url: '/api/sync/bc-to-premium',
              body: { projectNo: projectNo }
            });
            await fetchPlannerProjects();
            return;
          }
          if(action === 'clear-links'){
            await clearProjectLinks(projectNo);
            return;
          }
        });
        plannerProjectsTbody.addEventListener('change', function(ev){
          var target = ev.target;
          if(!target || target.tagName !== 'INPUT') return;
          if(target.type !== 'checkbox') return;
          var projectNo = target.getAttribute('data-project-no') || '';
          if(!projectNo) return;
          if(target.checked){
            plannerProjectsSelected.add(projectNo);
          } else {
            plannerProjectsSelected.delete(projectNo);
          }
          updatePlannerProjectsCount();
        });
      }

      if(plannerAssetsFilterInput){
        plannerAssetsFilterInput.addEventListener('input', function(){
          renderPlannerAssets();
        });
      }
      if(plannerAssetsRefreshBtn){
        plannerAssetsRefreshBtn.addEventListener('click', function(){
          fetchPlannerAssets();
        });
      }
      if(plannerAssetsGraphSelectAllBtn){
        plannerAssetsGraphSelectAllBtn.addEventListener('click', function(){
          plannerAssetsGraphFiltered.forEach(function(item){
            plannerAssetsGraphSelected.add(item.id);
          });
          renderPlannerAssets();
        });
      }
      if(plannerAssetsGraphClearBtn){
        plannerAssetsGraphClearBtn.addEventListener('click', function(){
          plannerAssetsGraphSelected.clear();
          renderPlannerAssets();
        });
      }
      if(plannerAssetsDvSelectAllBtn){
        plannerAssetsDvSelectAllBtn.addEventListener('click', function(){
          plannerAssetsDvFiltered.forEach(function(item){
            plannerAssetsDvSelected.add(item.id);
          });
          renderPlannerAssets();
        });
      }
      if(plannerAssetsDvClearBtn){
        plannerAssetsDvClearBtn.addEventListener('click', function(){
          plannerAssetsDvSelected.clear();
          renderPlannerAssets();
        });
      }
      if(plannerAssetsGraphTbody){
        plannerAssetsGraphTbody.addEventListener('change', function(ev){
          var target = ev.target;
          if(!target || target.tagName !== 'INPUT') return;
          if(target.type !== 'checkbox') return;
          var id = target.getAttribute('data-graph-id') || '';
          if(!id) return;
          if(target.checked){
            plannerAssetsGraphSelected.add(id);
          } else {
            plannerAssetsGraphSelected.delete(id);
          }
          updatePlannerAssetsCounts();
        });
        plannerAssetsGraphTbody.addEventListener('click', function(ev){
          var target = ev.target && ev.target.closest ? ev.target.closest('input[type=\"checkbox\"]') : null;
          if(!target) return;
          var id = target.getAttribute('data-graph-id') || '';
          if(!id) return;
          if(target.checked){
            plannerAssetsGraphSelected.add(id);
          } else {
            plannerAssetsGraphSelected.delete(id);
          }
          updatePlannerAssetsCounts();
        });
      }
      if(plannerAssetsDvTbody){
        plannerAssetsDvTbody.addEventListener('change', function(ev){
          var target = ev.target;
          if(!target || target.tagName !== 'INPUT') return;
          if(target.type !== 'checkbox') return;
          var id = target.getAttribute('data-dv-id') || '';
          if(!id) return;
          if(target.checked){
            plannerAssetsDvSelected.add(id);
          } else {
            plannerAssetsDvSelected.delete(id);
          }
          updatePlannerAssetsCounts();
        });
        plannerAssetsDvTbody.addEventListener('click', function(ev){
          var target = ev.target && ev.target.closest ? ev.target.closest('input[type=\"checkbox\"]') : null;
          if(!target) return;
          var id = target.getAttribute('data-dv-id') || '';
          if(!id) return;
          if(target.checked){
            plannerAssetsDvSelected.add(id);
          } else {
            plannerAssetsDvSelected.delete(id);
          }
          updatePlannerAssetsCounts();
        });
      }
      if(plannerAssetsConvertBtn){
        plannerAssetsConvertBtn.addEventListener('click', async function(){
          if(!plannerAssetsGraphSelected.size){
            setPlannerAssetsStatus('Select at least one Graph plan to convert.', 'warn');
            return;
          }
          var ok = window.confirm('Convert ' + plannerAssetsGraphSelected.size + ' Graph plans into Premium projects?');
          if(!ok) return;
          setPlannerAssetsStatus('Converting selected Graph plans…', 'muted');
          try{
            var plans = plannerAssetsGraphCache.filter(function(item){
              return plannerAssetsGraphSelected.has(item.id);
            }).map(function(item){
              return { id: item.id, title: item.title };
            });
            var data = await runPlannerAssetsAction({ action: 'convert-graph', plans: plans });
            await fetchPlannerAssets();
            var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
            renderPlannerAssetsOutput(data);
            if(failures.length){
              setPlannerAssetsStatus('Conversion completed with ' + failures.length + ' failures.', 'warn');
            } else {
              setPlannerAssetsStatus('Conversion complete.', 'ok');
            }
          }catch(err){
            renderPlannerAssetsOutput(err && err.message ? err.message : err);
            setPlannerAssetsStatus('Convert failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          }
        });
      }
      if(plannerAssetsDeleteGraphBtn){
        plannerAssetsDeleteGraphBtn.addEventListener('click', async function(){
          if(!plannerAssetsGraphSelected.size){
            setPlannerAssetsStatus('Select at least one Graph plan to delete.', 'warn');
            return;
          }
          var ok = window.confirm('Delete ' + plannerAssetsGraphSelected.size + ' Graph plans? This cannot be undone.');
          if(!ok) return;
          setPlannerAssetsStatus('Deleting Graph plans…', 'muted');
          try{
            var ids = Array.from(plannerAssetsGraphSelected);
            var data = await runPlannerAssetsAction({ action: 'delete-graph', ids: ids });
            await fetchPlannerAssets();
            var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
            renderPlannerAssetsOutput(data);
            if(failures.length){
              setPlannerAssetsStatus('Delete completed with ' + failures.length + ' failures.', 'warn');
            } else {
              setPlannerAssetsStatus('Graph plans deleted.', 'ok');
            }
          }catch(err){
            renderPlannerAssetsOutput(err && err.message ? err.message : err);
            setPlannerAssetsStatus('Delete failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          }
        });
      }
      if(plannerAssetsDeleteDvBtn){
        plannerAssetsDeleteDvBtn.addEventListener('click', async function(){
          if(!plannerAssetsDvSelected.size){
            setPlannerAssetsStatus('Select at least one Premium project to delete.', 'warn');
            return;
          }
          var ok = window.confirm('Delete ' + plannerAssetsDvSelected.size + ' Premium projects? This cannot be undone.');
          if(!ok) return;
          setPlannerAssetsStatus('Deleting Premium projects…', 'muted');
          try{
            var ids = Array.from(plannerAssetsDvSelected);
            var data = await runPlannerAssetsAction({ action: 'delete-dataverse', ids: ids });
            await fetchPlannerAssets();
            var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
            renderPlannerAssetsOutput(data);
            if(failures.length){
              setPlannerAssetsStatus('Delete completed with ' + failures.length + ' failures.', 'warn');
            } else {
              setPlannerAssetsStatus('Premium projects deleted.', 'ok');
            }
          }catch(err){
            renderPlannerAssetsOutput(err && err.message ? err.message : err);
            setPlannerAssetsStatus('Delete failed: ' + (err && err.message ? err.message : 'error'), 'bad');
          }
        });
      }
      if(plannerAssetsGraphTbody || plannerAssetsDvTbody){
        fetchPlannerAssets();
      }

      // Debug panel handlers
      var debugTestBtn = document.getElementById('debug-test-btn');
      var debugClearBtn = document.getElementById('debug-clear-btn');
      var debugRefreshBtn = document.getElementById('debug-refresh-btn');
      var debugStatusEl = document.getElementById('debug-status');
      var debugOutputEl = document.getElementById('debug-output');
      var debugRoutesEl = document.getElementById('debug-routes');
      var debugRoutesTbody = document.getElementById('debug-routes-tbody');
      
      function setDebugStatus(text, tone){
        if(!debugStatusEl) return;
        var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
        debugStatusEl.textContent = text;
        debugStatusEl.className = 'small ' + toneClass;
      }
      
      function renderDebugOutput(payload){
        if(!debugOutputEl) return;
        if(payload == null){
          debugOutputEl.style.display = 'none';
          debugOutputEl.textContent = '';
          return;
        }
        var text = '';
        try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
        catch(e){ text = String(payload); }
        debugOutputEl.textContent = text;
        debugOutputEl.style.display = 'block';
      }
      
      function renderDebugRoutes(routes){
        if(!debugRoutesTbody || !debugRoutesEl) return;
        if(!routes || typeof routes !== 'object'){
          debugRoutesEl.style.display = 'none';
          return;
        }
        
        var rows = Object.keys(routes).map(function(route){
          var desc = routes[route];
          var parts = route.split(' ');
          var method = parts[0] || 'GET';
          var path = parts.slice(1).join(' ') || route;
          var testBtn = '<button type="button" onclick="testRoute(\'' + htmlEscape(route) + '\')" style="background:#2b61d1;color:#fff;border:0;border-radius:4px;padding:4px 8px;cursor:pointer;font-size:11px">Test</button>';
          return '<tr>' +
            '<td><span class="mono small">' + htmlEscape(method) + '</span></td>' +
            '<td><span class="mono small">' + htmlEscape(path) + '</span></td>' +
            '<td>' + htmlEscape(desc) + '</td>' +
            '<td>' + testBtn + '</td>' +
            '</tr>';
        }).join('');
        
        debugRoutesTbody.innerHTML = rows || '<tr><td colspan="4" class="muted">No routes available</td></tr>';
        debugRoutesEl.style.display = 'block';
      }
      
      window.testRoute = function(route){
        var parts = route.split(' ');
        var method = parts[0] || 'GET';
        var path = parts.slice(1).join(' ') || route;
        setDebugStatus('Testing ' + route + '…', 'muted');
        renderDebugOutput(null);
        fetch(path, {
          method: method,
          headers: method === 'POST' ? { 'Content-Type': 'application/json' } : {}
        })
        .then(function(res){
          return res.text().then(function(text){
            var payload;
            try { payload = JSON.parse(text); }
            catch(e){ payload = text; }
            return { ok: res.ok, status: res.status, payload: payload };
          });
        })
        .then(function(result){
          if(result.ok){
            setDebugStatus('Route test successful (HTTP ' + result.status + ')', 'ok');
          } else {
            setDebugStatus('Route test failed (HTTP ' + result.status + ')', 'bad');
          }
          renderDebugOutput(result);
        })
        .catch(function(err){
          setDebugStatus('Route test error: ' + (err && err.message ? err.message : 'error'), 'bad');
          renderDebugOutput({ error: err && err.message ? err.message : String(err) });
        });
      };
      
      if(debugTestBtn){
        debugTestBtn.addEventListener('click', async function(){
          setDebugStatus('Fetching debug information…', 'muted');
          renderDebugOutput(null);
          try{
            var res = await fetch('/api/debug', {
              method: 'GET',
              headers: { 'cache-control': 'no-cache' }
            });
            var payload;
            var ct = res.headers.get('content-type') || '';
            if(ct.indexOf('application/json') !== -1){
              payload = await res.json();
            } else {
              var raw = await res.text();
              try{ payload = JSON.parse(raw); }
              catch(e){ throw new Error(raw || ('HTTP ' + res.status)); }
            }
            if(!res.ok || (payload && payload.ok === false)){
              var message = payload && payload.error ? payload.error : ('HTTP ' + res.status);
              throw new Error(message);
            }
            setDebugStatus('Debug information retrieved successfully', 'ok');
            renderDebugOutput(payload);
            if(payload && payload.routes){
              renderDebugRoutes(payload.routes);
            }
          }catch(err){
            setDebugStatus('Failed to fetch debug info: ' + (err && err.message ? err.message : 'error'), 'bad');
            renderDebugOutput({ error: err && err.message ? err.message : String(err) });
          }
        });
      }
      
      if(debugClearBtn){
        debugClearBtn.addEventListener('click', function(){
          setDebugStatus('Click "Test Debug Endpoint" to fetch debug information.', 'muted');
          renderDebugOutput(null);
          if(debugRoutesEl) debugRoutesEl.style.display = 'none';
        });
      }
      
      if(debugRefreshBtn){
        debugRefreshBtn.addEventListener('click', function(){
          if(debugTestBtn) debugTestBtn.click();
        });
      }
      window.__debugAction = function(action){
        if(action === 'test' && debugTestBtn){ debugTestBtn.click(); return; }
        if(action === 'clear' && debugClearBtn){ debugClearBtn.click(); return; }
        if(action === 'refresh' && debugRefreshBtn){ debugRefreshBtn.click(); return; }
        setDebugStatus('Debug action not available: ' + action, 'warn');
      };

      fetchAndRender();
      fetchPlannerProjects();
    })();
  
