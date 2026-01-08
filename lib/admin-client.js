
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
      var plannerPlanIdsInput = document.getElementById('planner-plan-ids');
      var plannerRunBcBtn = document.getElementById('planner-run-bc');
      var plannerRunBcPrBtn = document.getElementById('planner-run-bc-pr00001');
      var plannerRunPollBtn = document.getElementById('planner-run-poll');
      var plannerDebugTaskBtn = document.getElementById('planner-debug-task');
      var plannerWebhookLogBtn = document.getElementById('planner-webhook-log');
      var plannerWebhookStreamBtn = document.getElementById('planner-webhook-stream');
      var plannerWebhookStopBtn = document.getElementById('planner-webhook-stop');
      var plannerCreateSubsBtn = document.getElementById('planner-create-subs');
      var plannerRenewSubsBtn = document.getElementById('planner-renew-subs');
      var plannerTestWebhookBtn = document.getElementById('planner-test-webhook');
      var plannerStatusEl = document.getElementById('planner-status');
      var plannerOutputEl = document.getElementById('planner-output');
      var plannerWebhookOutputEl = document.getElementById('planner-webhook-output');
      var plannerLogEl = document.getElementById('planner-log');
      var uspsLoading = false;
      var uspsDownloadUrl = null;
      var uspsPendingDownloadUrl = null;
      var cache = [];
      var refreshing = false;

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
      function appendWebhookLine(entry){
        if(!plannerWebhookOutputEl) return;
        var line = '';
        try { line = JSON.stringify(entry); } catch(e){ line = String(entry); }
        var existing = plannerWebhookOutputEl.textContent || '';
        plannerWebhookOutputEl.textContent = (existing ? existing + '\n' : '') + line;
        plannerWebhookOutputEl.style.display = 'block';
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
          plannerRunPollBtn,
          plannerDebugTaskBtn,
          plannerWebhookLogBtn,
          plannerWebhookStreamBtn,
          plannerWebhookStopBtn,
          plannerCreateSubsBtn,
          plannerRenewSubsBtn,
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
            throw new Error(msg);
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
      if(plannerRunBcBtn){
        plannerRunBcBtn.addEventListener('click', function(){
          var projectNo = plannerProjectInput && plannerProjectInput.value ? plannerProjectInput.value.trim() : '';
          var body = projectNo ? { projectNo: projectNo } : undefined;
          runPlannerRequest({ label: 'BC → Planner sync', url: '/api/sync/run-bc-to-planner', body: body });
        });
      }
      if(plannerRunBcPrBtn){
        plannerRunBcPrBtn.addEventListener('click', function(){
          runPlannerRequest({ label: 'BC → Planner sync (PR00001)', url: '/api/sync/run-bc-to-planner', body: { projectNo: 'PR00001' } });
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
          renderWebhookOutput(null);
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
      if(plannerRunPollBtn){
        plannerRunPollBtn.addEventListener('click', function(){
          runPlannerRequest({ label: 'Polling sync', url: '/api/sync/run-poll' });
        });
      }
      if(plannerCreateSubsBtn){
        plannerCreateSubsBtn.addEventListener('click', function(){
          var notificationUrl = plannerNotifyInput && plannerNotifyInput.value ? plannerNotifyInput.value.trim() : '';
          var planIdsRaw = plannerPlanIdsInput && plannerPlanIdsInput.value ? plannerPlanIdsInput.value : '';
          var planIds = planIdsRaw.split(',').map(function(id){ return id.trim(); }).filter(Boolean);
          var body = {};
          if(notificationUrl) body.notificationUrl = notificationUrl;
          if(planIds.length) body.planIds = planIds;
          runPlannerRequest({ label: 'Create subscriptions', url: '/api/sync/subscriptions/create', body: Object.keys(body).length ? body : undefined });
        });
      }
      if(plannerRenewSubsBtn){
        plannerRenewSubsBtn.addEventListener('click', function(){
          runPlannerRequest({ label: 'Renew subscriptions', url: '/api/sync/subscriptions/renew' });
        });
      }
      if(plannerTestWebhookBtn){
        plannerTestWebhookBtn.addEventListener('click', function(){
          var token = 'test_' + Math.random().toString(36).slice(2, 10);
          runPlannerRequest({
            label: 'Webhook validation',
            url: '/api/webhooks/graph/planner?validationToken=' + encodeURIComponent(token),
            method: 'POST'
          });
        });
      }
      window.__plannerAction = function(action){
        if(action === 'bc' && plannerRunBcBtn){ plannerRunBcBtn.click(); return; }
        if(action === 'bc-pr00001' && plannerRunBcPrBtn){ plannerRunBcPrBtn.click(); return; }
        if(action === 'poll' && plannerRunPollBtn){ plannerRunPollBtn.click(); return; }
        if(action === 'create-subs' && plannerCreateSubsBtn){ plannerCreateSubsBtn.click(); return; }
        if(action === 'renew-subs' && plannerRenewSubsBtn){ plannerRenewSubsBtn.click(); return; }
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
    })();
  
