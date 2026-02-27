(() => {
  const state = {
    generatedAt: null,
    items: [],
  };

  const listEl = document.getElementById('validation-list');
  const countEl = document.getElementById('validation-count');
  const generatedAtEl = document.getElementById('generated-at');
  const flashEl = document.getElementById('flash');
  const timezoneLabelEl = document.getElementById('timezone-label');
  const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
  const localDateTimeFormatter = new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    timeZone: browserTimeZone,
  });

  function flash(message, isError = false) {
    flashEl.textContent = message;
    flashEl.style.color = isError ? 'var(--err)' : 'var(--warn)';
    flashEl.classList.add('show');
    window.setTimeout(() => flashEl.classList.remove('show'), 2600);
  }

  async function api(path, options = {}) {
    const resp = await fetch(path, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    if (!resp.ok) {
      const detail = await resp.text();
      throw new Error(`${resp.status}: ${detail}`);
    }
    return resp.json();
  }

  function parseBackendDate(value) {
    if (!value) return null;
    if (value instanceof Date) return value;
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const hasOffset = /([zZ]|[+\-]\d{2}:\d{2})$/.test(trimmed);
    const normalized = hasOffset ? trimmed : `${trimmed}Z`;
    const parsed = new Date(normalized);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed;
  }

  function fmtDate(value) {
    if (!value) return '—';
    const dt = parseBackendDate(value);
    if (!dt) return String(value);
    return localDateTimeFormatter.format(dt);
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function render() {
    if (timezoneLabelEl) {
      timezoneLabelEl.textContent = browserTimeZone;
    }
    generatedAtEl.textContent = fmtDate(state.generatedAt);
    countEl.textContent = String(state.items.length);

    if (!state.items.length) {
      listEl.innerHTML = '<div class="muted">Невалидных последних запусков по задачам не найдено.</div>';
      return;
    }

    listEl.innerHTML = state.items.map((item) => {
      const runId = String(item.run_id || '');
      const runShort = runId ? runId.slice(0, 12) : '—';
      const parserName = escapeHtml(item.parser_name || 'unknown');
      const activeChipClass = item.is_active ? 'success' : 'assigned';
      const activeText = item.is_active ? 'активна' : 'неактивна';
      const errorText = escapeHtml(item.dataclass_validation_error || 'Пустая ошибка валидации');
      const remoteStatusRaw = String(item.remote_status || 'unknown').trim().toLowerCase() || 'unknown';
      const remoteChipClass = ['success', 'error', 'running', 'queued', 'waiting'].includes(remoteStatusRaw)
        ? remoteStatusRaw
        : 'unknown';
      const remoteStatus = escapeHtml(remoteStatusRaw);
      return `
        <article class="validation-item">
          <div class="row-top">
            <strong class="mono">task #${item.task_id} • run ${runShort}</strong>
            <div class="chip-set">
              <span class="chip warning">validation_failed</span>
              <span class="chip ${activeChipClass}">${activeText}</span>
              <span class="chip running">parser:${parserName}</span>
            </div>
          </div>
          <div class="muted">store: ${escapeHtml(item.store || '—')} • city: ${escapeHtml(item.city || '—')}</div>
          <div class="muted">assigned: ${fmtDate(item.assigned_at)} • finished: ${fmtDate(item.finished_at)}</div>
          <div class="muted">remote: <span class="chip remote-chip ${remoteChipClass}">${remoteStatus}</span></div>
          <pre class="validation-error-text">${errorText}</pre>
        </article>
      `;
    }).join('');
  }

  async function refreshAll() {
    try {
      const payload = await api('/api/validation-errors');
      state.generatedAt = payload.generated_at || null;
      state.items = Array.isArray(payload.items) ? payload.items : [];
      render();
    } catch (err) {
      flash(`Ошибка обновления: ${err.message}`, true);
    }
  }

  document.getElementById('refresh').addEventListener('click', refreshAll);
  refreshAll();
  window.setInterval(refreshAll, 15000);
})();
