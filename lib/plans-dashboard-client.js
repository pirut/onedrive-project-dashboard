(function(){
  if(window.__plansDashboardInit) return;
  window.__plansDashboardInit = true;

  var refreshBtn = document.getElementById('plans-refresh');
  var filterInput = document.getElementById('plans-filter');
  var graphTbody = document.getElementById('graph-tbody');
  var premiumTbody = document.getElementById('premium-tbody');
  var graphSelectAllBtn = document.getElementById('graph-select-all');
  var graphClearBtn = document.getElementById('graph-clear');
  var graphDeleteBtn = document.getElementById('graph-delete');
  var graphDeleteAllBtn = document.getElementById('graph-delete-all');
  var premiumSelectAllBtn = document.getElementById('premium-select-all');
  var premiumClearBtn = document.getElementById('premium-clear');
  var premiumDeleteBtn = document.getElementById('premium-delete');
  var premiumDeleteAllBtn = document.getElementById('premium-delete-all');
  var graphCountEl = document.getElementById('graph-count');
  var graphMetaEl = document.getElementById('graph-meta');
  var premiumCountEl = document.getElementById('premium-count');
  var premiumMetaEl = document.getElementById('premium-meta');
  var autoCreateEl = document.getElementById('auto-create-status');
  var groupStatusEl = document.getElementById('group-status');
  var groupMetaEl = document.getElementById('group-meta');
  var graphSelectedCountEl = document.getElementById('graph-count-selected');
  var premiumSelectedCountEl = document.getElementById('premium-count-selected');
  var recreateBtn = document.getElementById('recreate-all');
  var resetBtn = document.getElementById('reset-all');
  var actionStatusEl = document.getElementById('action-status');
  var actionOutputEl = document.getElementById('action-output');
  var bcFilterInput = document.getElementById('bc-projects-filter');
  var bcTbody = document.getElementById('bc-projects-tbody');
  var bcSelectAllBtn = document.getElementById('bc-projects-select-all');
  var bcClearBtn = document.getElementById('bc-projects-clear');
  var bcSyncBtn = document.getElementById('bc-projects-sync');
  var bcCountEl = document.getElementById('bc-projects-count');
  var bcStatusEl = document.getElementById('bc-projects-status');
  var bcOutputEl = document.getElementById('bc-projects-output');

  var graphCache = [];
  var premiumCache = [];
  var bcProjects = [];
  var mapping = {};
  var projectUrlTemplate = '';
  var graphEnabled = false;

  var graphSelected = new Set();
  var premiumSelected = new Set();
  var bcSelected = new Set();
  var graphFiltered = [];
  var premiumFiltered = [];
  var bcFiltered = [];

  function htmlEscape(s){
    return String(s)
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#39;');
  }

  function setCardValue(el, text, tone){
    if(!el) return;
    el.textContent = text;
    if(!tone) return;
    var cls = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : '';
    el.classList.remove('ok','bad','warn');
    if(cls){ el.classList.add(cls); }
  }

  function setActionStatus(text, tone){
    if(!actionStatusEl) return;
    var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
    actionStatusEl.textContent = text;
    actionStatusEl.className = 'small ' + toneClass;
  }

  function renderOutput(payload){
    if(!actionOutputEl) return;
    if(payload == null){
      actionOutputEl.style.display = 'none';
      actionOutputEl.textContent = '';
      return;
    }
    var text = '';
    try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
    catch(e){ text = String(payload); }
    actionOutputEl.textContent = text;
    actionOutputEl.style.display = 'block';
  }

  function setBcStatus(text, tone){
    if(!bcStatusEl) return;
    var toneClass = tone === 'ok' ? 'ok' : tone === 'bad' ? 'bad' : tone === 'warn' ? 'warn' : 'muted';
    bcStatusEl.textContent = text;
    bcStatusEl.className = 'small ' + toneClass;
  }

  function renderBcOutput(payload){
    if(!bcOutputEl) return;
    if(payload == null){
      bcOutputEl.style.display = 'none';
      bcOutputEl.textContent = '';
      return;
    }
    var text = '';
    try { text = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2); }
    catch(e){ text = String(payload); }
    bcOutputEl.textContent = text;
    bcOutputEl.style.display = 'block';
  }

  function updateSelectedCounts(){
    if(graphSelectedCountEl){
      graphSelectedCountEl.textContent = graphSelected.size + ' selected';
    }
    if(premiumSelectedCountEl){
      premiumSelectedCountEl.textContent = premiumSelected.size + ' selected';
    }
    if(graphDeleteBtn) graphDeleteBtn.disabled = graphSelected.size === 0;
    if(premiumDeleteBtn) premiumDeleteBtn.disabled = premiumSelected.size === 0;
  }

  function updateBcSelectedCounts(){
    if(bcCountEl){
      bcCountEl.textContent = bcSelected.size + ' selected';
    }
    if(bcSyncBtn) bcSyncBtn.disabled = bcSelected.size === 0;
  }

  function renderTables(){
    var q = filterInput && filterInput.value ? filterInput.value.trim().toLowerCase() : '';
    graphFiltered = !q ? graphCache : graphCache.filter(function(item){
      var hay = [item.title, item.id, item.owner].join(' ').toLowerCase();
      return hay.indexOf(q) !== -1;
    });
    premiumFiltered = !q ? premiumCache : premiumCache.filter(function(item){
      var hay = [item.title, item.id, item.bcNo].join(' ').toLowerCase();
      return hay.indexOf(q) !== -1;
    });

    if(graphTbody){
      var graphRows = graphFiltered.map(function(item){
        var isSelected = graphSelected.has(item.id);
        var created = item.createdDateTime ? ('<span class="small mono" title="' + htmlEscape(item.createdDateTime) + '">' + htmlEscape(item.createdDateTime.split('T')[0]) + '</span>') : '<span class="muted">—</span>';
        return '<tr>'
          + '<td><input type="checkbox" data-graph-id="' + htmlEscape(item.id) + '"' + (isSelected ? ' checked' : '') + ' /></td>'
          + '<td>' + htmlEscape(item.title || '') + '</td>'
          + '<td class="small mono">' + htmlEscape(item.id || '') + '</td>'
          + '<td>' + created + '</td>'
          + '</tr>';
      }).join('');
      graphTbody.innerHTML = graphRows || '<tr><td colspan="4" class="muted">No Graph plans found.</td></tr>';
    }

    if(premiumTbody){
      var premiumRows = premiumFiltered.map(function(item){
        var isSelected = premiumSelected.has(item.id);
        var linkCell = item.url ? ('<a href="' + htmlEscape(item.url) + '" target="_blank" rel="noreferrer" class="pill">Open</a>') : '<span class="muted">—</span>';
        var modified = item.modifiedOn ? ('<span class="small mono" title="' + htmlEscape(item.modifiedOn) + '">' + htmlEscape(item.modifiedOn.split('T')[0]) + '</span>') : '<span class="muted">—</span>';
        return '<tr>'
          + '<td><input type="checkbox" data-premium-id="' + htmlEscape(item.id) + '"' + (isSelected ? ' checked' : '') + ' /></td>'
          + '<td>' + htmlEscape(item.title || '') + '</td>'
          + '<td class="small mono">' + htmlEscape(item.bcNo || '—') + '</td>'
          + '<td>' + linkCell + '</td>'
          + '<td class="small mono">' + htmlEscape(item.id || '') + '</td>'
          + '<td>' + modified + '</td>'
          + '</tr>';
      }).join('');
      premiumTbody.innerHTML = premiumRows || '<tr><td colspan="6" class="muted">No Premium projects found.</td></tr>';
    }

    updateSelectedCounts();
  }

  function renderBcProjects(){
    var q = bcFilterInput && bcFilterInput.value ? bcFilterInput.value.trim().toLowerCase() : '';
    bcFiltered = !q ? bcProjects : bcProjects.filter(function(item){
      var hay = [item.projectNo, item.description, item.status].join(' ').toLowerCase();
      return hay.indexOf(q) !== -1;
    });
    if(bcTbody){
      var rows = bcFiltered.map(function(item){
        var key = item.projectNo || '';
        var isSelected = bcSelected.has(key);
        var total = Number(item.tasksTotal || 0);
        var linked = Number(item.tasksLinked || 0);
        var tasksText = linked + '/' + total;
        var syncEnabled = item.syncEnabled !== false;
        var syncState = item.syncState || 'unknown';
        var syncText = syncEnabled ? syncState : 'disabled';
        var syncClass = !syncEnabled
          ? 'warn'
          : syncState === 'linked'
          ? 'ok'
          : syncState === 'partial' || syncState === 'empty'
          ? 'warn'
          : 'muted';
        var lastSync = item.lastSyncAt ? ('<span class="small mono" title="' + htmlEscape(item.lastSyncAt) + '">' + htmlEscape(item.lastSyncAt.split('T')[0]) + '</span>') : '<span class="muted">—</span>';
        var premiumText = item.hasPremiumProject ? 'Yes' : 'No';
        var premiumClass = item.hasPremiumProject ? 'ok' : 'muted';
        return '<tr>'
          + '<td><input type="checkbox" data-bc-project="' + htmlEscape(key) + '"' + (isSelected ? ' checked' : '') + ' /></td>'
          + '<td class="mono small">' + htmlEscape(key) + '</td>'
          + '<td>' + htmlEscape(item.description || '') + '</td>'
          + '<td class="small">' + htmlEscape(item.status || '—') + '</td>'
          + '<td><span class="pill ' + syncClass + '">' + htmlEscape(syncText) + '</span></td>'
          + '<td class="small mono">' + htmlEscape(tasksText) + '</td>'
          + '<td>' + lastSync + '</td>'
          + '<td><span class="' + premiumClass + '">' + premiumText + '</span></td>'
          + '</tr>';
      }).join('');
      bcTbody.innerHTML = rows || '<tr><td colspan="8" class="muted">No BC projects found.</td></tr>';
    }
    updateBcSelectedCounts();
  }

  async function runPlannerAssetsAction(payload){
    var res = await fetch('/api/admin-planner-assets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      credentials: 'same-origin'
    });
    var data;
    try { data = await res.json(); }
    catch(parseErr){
      var text = await res.text();
      throw new Error(text || (parseErr && parseErr.message ? parseErr.message : 'Invalid JSON'));
    }
    if(!res.ok || (data && data.ok === false)){
      var msg = data && data.error ? data.error : ('HTTP ' + res.status);
      throw new Error(msg);
    }
    return data;
  }

  async function fetchAssets(){
    setActionStatus('Loading plans…', 'muted');
    try{
      var res = await fetch('/api/admin-planner-assets', { headers: { 'cache-control': 'no-cache' }, credentials: 'same-origin' });
      var payload;
      try{ payload = await res.json(); }
      catch(parseErr){
        var text = await res.text();
        throw new Error(text || (parseErr && parseErr.message ? parseErr.message : 'Invalid JSON'));
      }
      if(!res.ok || (payload && payload.ok === false)){
        var msg = payload && payload.error ? payload.error : ('HTTP ' + res.status);
        throw new Error(msg);
      }
      graphEnabled = Boolean(payload.graphEnabled);
      graphCache = Array.isArray(payload.graphPlans) ? payload.graphPlans : [];
      mapping = payload.mapping || {};
      projectUrlTemplate = payload.projectUrlTemplate || '';
      bcProjects = Array.isArray(payload.bcProjects) ? payload.bcProjects : [];
      premiumCache = Array.isArray(payload.dataverseProjects) ? payload.dataverseProjects.map(function(item){
        var id = item[mapping.projectIdField];
        return {
          id: id,
          title: item[mapping.projectTitleField],
          bcNo: mapping.projectBcNoField ? item[mapping.projectBcNoField] : '',
          modifiedOn: item.modifiedon || item.modifiedOn || item.modified || '',
          url: projectUrlTemplate && id ? projectUrlTemplate.replace('{projectId}', id) : ''
        };
      }) : [];

      graphSelected.clear();
      premiumSelected.clear();
      bcSelected.clear();

      if(graphCountEl) graphCountEl.textContent = String(graphCache.length || 0);
      if(graphMetaEl) graphMetaEl.textContent = graphEnabled ? 'Graph configured' : 'Graph not configured';
      if(premiumCountEl) premiumCountEl.textContent = String(premiumCache.length || 0);
      if(premiumMetaEl) premiumMetaEl.textContent = mapping.projectEntitySet ? ('Entity: ' + mapping.projectEntitySet) : 'Dataverse projects';
      if(autoCreateEl){
        setCardValue(autoCreateEl, 'Enabled', 'ok');
      }
      if(groupStatusEl){
        setCardValue(groupStatusEl, graphEnabled ? 'Configured' : 'Missing', graphEnabled ? 'ok' : 'warn');
      }
      if(groupMetaEl){
        groupMetaEl.textContent = graphEnabled ? 'PLANNER_GROUP_ID present' : 'Set PLANNER_GROUP_ID';
      }

      renderTables();
      renderBcProjects();
      setActionStatus('Loaded ' + graphCache.length + ' standard plans and ' + premiumCache.length + ' premium projects.', 'ok');
      setBcStatus('Loaded ' + bcProjects.length + ' BC projects.', 'ok');
      renderOutput(null);
      renderBcOutput(null);
    }catch(err){
      graphCache = [];
      premiumCache = [];
      graphSelected.clear();
      premiumSelected.clear();
      bcProjects = [];
      bcSelected.clear();
      renderTables();
      renderBcProjects();
      renderOutput(err && err.message ? err.message : err);
      setActionStatus('Failed to load plans: ' + (err && err.message ? err.message : 'error'), 'bad');
      setBcStatus('Failed to load BC projects: ' + (err && err.message ? err.message : 'error'), 'bad');
      renderBcOutput(null);
    }
  }

  async function deleteGraphPlans(ids, opts){
    opts = opts || {};
    if(!ids.length){
      return { ok: true, results: [] };
    }
    var data = await runPlannerAssetsAction({ action: 'delete-graph', ids: ids });
    if(!opts.skipRefresh){
      await fetchAssets();
    }
    return data;
  }

  async function deletePremiumProjects(ids, opts){
    opts = opts || {};
    if(!ids.length){
      return { ok: true, results: [] };
    }
    var data = await runPlannerAssetsAction({ action: 'delete-dataverse', ids: ids });
    if(!opts.skipRefresh){
      await fetchAssets();
    }
    return data;
  }

  async function recreateAll(){
    var data = await runPlannerAssetsAction({ action: 'recreate-all', skipProjectAccess: true });
    return data;
  }

  async function syncBcProjects(projectNos){
    var data = await runPlannerAssetsAction({
      action: 'sync-bc-projects',
      projectNos: projectNos,
      skipProjectAccess: true,
      forceProjectCreate: true
    });
    return data;
  }

  if(filterInput){
    filterInput.addEventListener('input', function(){
      renderTables();
    });
  }

  if(bcFilterInput){
    bcFilterInput.addEventListener('input', function(){
      renderBcProjects();
    });
  }

  if(refreshBtn){
    refreshBtn.addEventListener('click', function(){
      fetchAssets();
    });
  }

  if(graphSelectAllBtn){
    graphSelectAllBtn.addEventListener('click', function(){
      graphFiltered.forEach(function(item){
        graphSelected.add(item.id);
      });
      renderTables();
    });
  }

  if(graphClearBtn){
    graphClearBtn.addEventListener('click', function(){
      graphSelected.clear();
      renderTables();
    });
  }

  if(premiumSelectAllBtn){
    premiumSelectAllBtn.addEventListener('click', function(){
      premiumFiltered.forEach(function(item){
        premiumSelected.add(item.id);
      });
      renderTables();
    });
  }

  if(premiumClearBtn){
    premiumClearBtn.addEventListener('click', function(){
      premiumSelected.clear();
      renderTables();
    });
  }

  if(bcSelectAllBtn){
    bcSelectAllBtn.addEventListener('click', function(){
      bcFiltered.forEach(function(item){
        if(item.projectNo){
          bcSelected.add(item.projectNo);
        }
      });
      renderBcProjects();
    });
  }

  if(bcClearBtn){
    bcClearBtn.addEventListener('click', function(){
      bcSelected.clear();
      renderBcProjects();
    });
  }

  if(graphTbody){
    graphTbody.addEventListener('change', function(ev){
      var target = ev.target;
      if(!target || target.tagName !== 'INPUT') return;
      if(target.type !== 'checkbox') return;
      var id = target.getAttribute('data-graph-id') || '';
      if(!id) return;
      if(target.checked){
        graphSelected.add(id);
      } else {
        graphSelected.delete(id);
      }
      updateSelectedCounts();
    });
  }

  if(premiumTbody){
    premiumTbody.addEventListener('change', function(ev){
      var target = ev.target;
      if(!target || target.tagName !== 'INPUT') return;
      if(target.type !== 'checkbox') return;
      var id = target.getAttribute('data-premium-id') || '';
      if(!id) return;
      if(target.checked){
        premiumSelected.add(id);
      } else {
        premiumSelected.delete(id);
      }
      updateSelectedCounts();
    });
  }

  if(bcTbody){
    bcTbody.addEventListener('change', function(ev){
      var target = ev.target;
      if(!target || target.tagName !== 'INPUT') return;
      if(target.type !== 'checkbox') return;
      var key = target.getAttribute('data-bc-project') || '';
      if(!key) return;
      if(target.checked){
        bcSelected.add(key);
      } else {
        bcSelected.delete(key);
      }
      updateBcSelectedCounts();
    });
  }

  if(bcSyncBtn){
    bcSyncBtn.addEventListener('click', async function(){
      if(!bcSelected.size){
        setBcStatus('Select at least one BC project to sync.', 'warn');
        return;
      }
      setBcStatus('Syncing selected BC projects…', 'warn');
      try{
        var projectNos = Array.from(bcSelected);
        var data = await syncBcProjects(projectNos);
        renderBcOutput(data);
        await fetchAssets();
        setBcStatus('Sync complete. Review results for failures.', 'ok');
      }catch(err){
        renderBcOutput(err && err.message ? err.message : err);
        setBcStatus('Sync failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  if(graphDeleteBtn){
    graphDeleteBtn.addEventListener('click', async function(){
      if(!graphSelected.size){
        setActionStatus('Select at least one standard plan to delete.', 'warn');
        return;
      }
      var ok = window.confirm('Delete ' + graphSelected.size + ' standard plans? This cannot be undone.');
      if(!ok) return;
      setActionStatus('Deleting standard plans…', 'warn');
      try{
        var ids = Array.from(graphSelected);
        var data = await deleteGraphPlans(ids);
        renderOutput(data);
        var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
        if(failures.length){
          setActionStatus('Delete completed with ' + failures.length + ' failures.', 'warn');
        } else {
          setActionStatus('Standard plans deleted.', 'ok');
        }
      }catch(err){
        renderOutput(err && err.message ? err.message : err);
        setActionStatus('Delete failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  if(graphDeleteAllBtn){
    graphDeleteAllBtn.addEventListener('click', async function(){
      if(!graphCache.length){
        setActionStatus('No standard plans to delete.', 'warn');
        return;
      }
      var ok = window.confirm('Delete ALL ' + graphCache.length + ' standard plans? This cannot be undone.');
      if(!ok) return;
      setActionStatus('Deleting all standard plans…', 'warn');
      try{
        var ids = graphCache.map(function(item){ return item.id; });
        var data = await deleteGraphPlans(ids);
        renderOutput(data);
        var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
        if(failures.length){
          setActionStatus('Delete completed with ' + failures.length + ' failures.', 'warn');
        } else {
          setActionStatus('All standard plans deleted.', 'ok');
        }
      }catch(err){
        renderOutput(err && err.message ? err.message : err);
        setActionStatus('Delete failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  if(premiumDeleteBtn){
    premiumDeleteBtn.addEventListener('click', async function(){
      if(!premiumSelected.size){
        setActionStatus('Select at least one premium project to delete.', 'warn');
        return;
      }
      var ok = window.confirm('Delete ' + premiumSelected.size + ' premium projects? This cannot be undone.');
      if(!ok) return;
      setActionStatus('Deleting premium projects…', 'warn');
      try{
        var ids = Array.from(premiumSelected);
        var data = await deletePremiumProjects(ids);
        renderOutput(data);
        var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
        if(failures.length){
          setActionStatus('Delete completed with ' + failures.length + ' failures.', 'warn');
        } else {
          setActionStatus('Premium projects deleted.', 'ok');
        }
      }catch(err){
        renderOutput(err && err.message ? err.message : err);
        setActionStatus('Delete failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  if(premiumDeleteAllBtn){
    premiumDeleteAllBtn.addEventListener('click', async function(){
      if(!premiumCache.length){
        setActionStatus('No premium projects to delete.', 'warn');
        return;
      }
      var ok = window.confirm('Delete ALL ' + premiumCache.length + ' premium projects? This cannot be undone.');
      if(!ok) return;
      setActionStatus('Deleting all premium projects…', 'warn');
      try{
        var ids = premiumCache.map(function(item){ return item.id; });
        var data = await deletePremiumProjects(ids);
        renderOutput(data);
        var failures = data && data.results ? data.results.filter(function(item){ return !item.ok; }) : [];
        if(failures.length){
          setActionStatus('Delete completed with ' + failures.length + ' failures.', 'warn');
        } else {
          setActionStatus('All premium projects deleted.', 'ok');
        }
      }catch(err){
        renderOutput(err && err.message ? err.message : err);
        setActionStatus('Delete failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  if(recreateBtn){
    recreateBtn.addEventListener('click', async function(){
      var ok = window.confirm('Recreate all premium projects from BC? This will run a full BC → Premium sync.');
      if(!ok) return;
      setActionStatus('Recreating premium projects…', 'warn');
      try{
        var data = await recreateAll();
        renderOutput(data);
        await fetchAssets();
        setActionStatus('Recreate complete. Check sync results for errors.', 'ok');
      }catch(err){
        renderOutput(err && err.message ? err.message : err);
        setActionStatus('Recreate failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  if(resetBtn){
    resetBtn.addEventListener('click', async function(){
      var ok = window.confirm('Delete ALL standard + premium plans and recreate from BC? This cannot be undone.');
      if(!ok) return;
      setActionStatus('Resetting plans…', 'warn');
      try{
        var graphIds = graphCache.map(function(item){ return item.id; });
        var premiumIds = premiumCache.map(function(item){ return item.id; });
        var graphResult = graphIds.length ? await deleteGraphPlans(graphIds, { skipRefresh: true }) : { ok: true, results: [] };
        var premiumResult = premiumIds.length ? await deletePremiumProjects(premiumIds, { skipRefresh: true }) : { ok: true, results: [] };
        var recreateResult = await recreateAll();
        await fetchAssets();
        var summary = {
          deleteGraph: graphResult,
          deletePremium: premiumResult,
          recreate: recreateResult
        };
        renderOutput(summary);
        setActionStatus('Reset complete. Review results for any failures.', 'ok');
      }catch(err){
        renderOutput(err && err.message ? err.message : err);
        setActionStatus('Reset failed: ' + (err && err.message ? err.message : 'error'), 'bad');
      }
    });
  }

  fetchAssets();
})();
