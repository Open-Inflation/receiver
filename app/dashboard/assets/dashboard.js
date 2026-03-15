    const state = {
      overview: null,
      tasks: [],
      logViewer: {
        runId: null,
        socket: null,
        compactEnabled: true,
        rawLines: [],
      },
    };

    const metricsEl = document.getElementById('metrics');
    const taskBodyEl = document.getElementById('task-body');
    const orchListEl = document.getElementById('orch-list');
    const runListEl = document.getElementById('run-list');
    const generatedAtEl = document.getElementById('generated-at');
    const flashEl = document.getElementById('flash');
    const runLogModalEl = document.getElementById('run-log-modal');
    const runLogTitleEl = document.getElementById('run-log-title');
    const runLogStatusEl = document.getElementById('run-log-status');
    const runLogOutputEl = document.getElementById('run-log-output');
    const runLogTailEl = document.getElementById('run-log-tail');
    const runLogCompactEl = document.getElementById('run-log-compact');
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
      return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function wsUrl(path) {
      const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${scheme}//${window.location.host}${path}`;
    }

    function closeLogSocket() {
      const socket = state.logViewer.socket;
      state.logViewer.socket = null;
      if (!socket) return;
      if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
        socket.close();
      }
    }

    function closeLogModal() {
      closeLogSocket();
      runLogModalEl.hidden = true;
      state.logViewer.runId = null;
      state.logViewer.rawLines = [];
      document.body.style.overflow = '';
    }

    function shouldStickToBottom(element) {
      return (element.scrollHeight - element.scrollTop - element.clientHeight) < 24;
    }

    function normalizeRawLine(value) {
      if (typeof value === 'string') return value;
      if (value == null) return '';
      return String(value);
    }

    function parseStructuredLogLine(line) {
      if (typeof line !== 'string') return null;
      const parts = line.split(' | ');
      if (parts.length < 4) return null;

      const timestamp = parts[0].trim();
      const level = parts[1].trim().toUpperCase();
      const logger = parts[parts.length - 2].trim();
      const message = parts[parts.length - 1].trim();
      const metadata = [];

      for (const segment of parts.slice(2, -2)) {
        const value = segment.trim();
        if (!value) continue;
        const eqIndex = value.indexOf('=');
        if (eqIndex > 0) {
          metadata.push({
            key: value.slice(0, eqIndex).trim(),
            value: value.slice(eqIndex + 1).trim(),
          });
          continue;
        }
        metadata.push({ key: 'meta', value });
      }

      return { timestamp, level, logger, message, metadata, raw: line };
    }

    function levelTone(level) {
      const normalized = String(level || '').trim().toLowerCase();
      if (normalized === 'debug') return 'debug';
      if (normalized === 'warning' || normalized === 'warn') return 'warning';
      if (normalized === 'error' || normalized === 'critical' || normalized === 'fatal') return 'error';
      return 'info';
    }

    function displayLogLine(parsed, rawLine) {
      if (!parsed) return rawLine;
      if (!state.logViewer.compactEnabled) return rawLine;
      return `${parsed.timestamp} | ${parsed.message}`;
    }

    function renderMetaPopover(parsed, rawLine) {
      const entries = parsed
        ? [
            { key: 'timestamp', value: parsed.timestamp },
            { key: 'level', value: parsed.level },
            { key: 'logger', value: parsed.logger },
            ...parsed.metadata,
            { key: 'message', value: parsed.message },
          ]
        : [{ key: 'raw', value: rawLine }];

      const rowsHtml = entries
        .map(
          (entry) => `
            <div class="log-meta-item">
              <span class="log-meta-key">${escapeHtml(entry.key)}</span>
              <span class="log-meta-value">${escapeHtml(entry.value)}</span>
            </div>
          `
        )
        .join('');

      const title = parsed ? 'Распарсенные поля' : 'Сырая строка';
      return `
        <div class="log-meta-popover">
          <div class="log-meta-title">${title}</div>
          ${rowsHtml}
        </div>
      `;
    }

    function renderLogRowsHtml(lines) {
      return lines
        .map((sourceLine, index) => {
          const rawLine = normalizeRawLine(sourceLine);
          const parsed = parseStructuredLogLine(rawLine);
          const tone = levelTone(parsed?.level);
          const text = displayLogLine(parsed, rawLine);
          const popover = renderMetaPopover(parsed, rawLine);
          const ariaLabel = parsed
            ? `Показать мета-данные строки ${index + 1}`
            : `Показать сырую строку ${index + 1}`;

          return `
            <div class="log-row log-tone-${tone}">
              <span class="log-meta-wrap">
                <span class="log-meta-dot" tabindex="0" role="button" aria-label="${escapeHtml(ariaLabel)}"></span>
                ${popover}
              </span>
              <span class="log-line-text">${escapeHtml(text)}</span>
            </div>
          `;
        })
        .join('');
    }

    function renderLogOutput() {
      const stickToBottom = shouldStickToBottom(runLogOutputEl);
      const sourceLines = state.logViewer.rawLines;
      runLogOutputEl.innerHTML = sourceLines.length ? renderLogRowsHtml(sourceLines) : '';
      if (stickToBottom) {
        runLogOutputEl.scrollTop = runLogOutputEl.scrollHeight;
      }
    }

    function replaceLogLines(lines) {
      state.logViewer.rawLines = Array.isArray(lines) ? lines.map(normalizeRawLine) : [];
      renderLogOutput();
    }

    function pushLogLines(lines) {
      if (!Array.isArray(lines) || !lines.length) return;
      state.logViewer.rawLines.push(...lines.map(normalizeRawLine));
      if (state.logViewer.rawLines.length > 10000) {
        state.logViewer.rawLines = state.logViewer.rawLines.slice(-10000);
      }
      renderLogOutput();
    }

    function connectRunLog(runId) {
      closeLogSocket();
      state.logViewer.runId = runId;
      replaceLogLines([]);
      const tailValue = Number(runLogTailEl.value);
      const tailLines = Number.isFinite(tailValue) ? Math.max(0, Math.min(5000, Math.floor(tailValue))) : 200;
      runLogTailEl.value = String(tailLines);
      runLogStatusEl.textContent = `Подключение к live-логу run ${runId.slice(0, 12)}...`;
      runLogTitleEl.textContent = `Worker log • run ${runId.slice(0, 12)}`;

      const url = wsUrl(`/ws/runs/${encodeURIComponent(runId)}/log?tail=${tailLines}`);
      const socket = new WebSocket(url);
      state.logViewer.socket = socket;

      socket.onopen = () => {
        runLogStatusEl.textContent = `Подключено. Tail: ${tailLines} строк.`;
      };

      socket.onmessage = (event) => {
        let payload;
        try {
          payload = JSON.parse(event.data);
        } catch (_) {
          runLogStatusEl.textContent = 'Ошибка: backend вернул невалидный JSON лога.';
          return;
        }
        if (!payload || typeof payload !== 'object') {
          runLogStatusEl.textContent = 'Ошибка: backend вернул неожиданный payload.';
          return;
        }

        if (payload.event === 'snapshot') {
          replaceLogLines(payload.lines);
          runLogStatusEl.textContent = `Snapshot получен (${Array.isArray(payload.lines) ? payload.lines.length : 0} строк).`;
          return;
        }

        if (payload.event === 'append') {
          pushLogLines(payload.lines);
          return;
        }

        if (payload.event === 'waiting') {
          runLogStatusEl.textContent = payload.message || 'Ожидание появления файла лога...';
          return;
        }

        if (payload.event === 'end') {
          runLogStatusEl.textContent = `Поток завершен: статус ${payload.status || 'unknown'}.`;
          closeLogSocket();
          return;
        }

        if (payload.event === 'error' || payload.ok === false) {
          runLogStatusEl.textContent = payload.error || 'Ошибка потока лога.';
          closeLogSocket();
        }
      };

      socket.onerror = () => {
        runLogStatusEl.textContent = 'Ошибка websocket-подключения к backend.';
      };

      socket.onclose = () => {
        if (state.logViewer.socket === socket) {
          state.logViewer.socket = null;
        }
      };
    }

    function openRunLog(runId) {
      runLogModalEl.hidden = false;
      document.body.style.overflow = 'hidden';
      connectRunLog(runId);
    }

    function isDue(task) {
      if (typeof task?.is_due === 'boolean') return task.is_due;
      if (!task.is_active) return false;
      if (!task.last_crawl_at) return true;
      const parsedLast = parseBackendDate(task.last_crawl_at);
      if (!parsedLast) return false;
      const last = parsedLast.getTime();
      const dueTs = last + Number(task.frequency_hours || 0) * 3600 * 1000;
      return dueTs <= Date.now();
    }

    function renderMetrics() {
      const o = state.overview;
      if (!o) return;
      generatedAtEl.textContent = fmtDate(o.generated_at);
      if (timezoneLabelEl) {
        timezoneLabelEl.textContent = browserTimeZone;
      }

      const entries = [
        ['Задачи всего', o.tasks_total],
        ['Активные', o.tasks_active],
        ['Просроченные', o.tasks_due],
        ['В lease', o.tasks_leased],
        ['Оркестраторы', o.orchestrators_total],
        ['Runs всего', o.runs_total],
        ['Runs success', o.runs_success],
        ['Runs warning', o.runs_warning ?? 0],
        ['Runs error', o.runs_error],
        ['Runs assigned', o.runs_assigned],
      ];
      metricsEl.innerHTML = entries.map(([label, value]) => `
        <article class="metric">
          <div class="label">${label}</div>
          <div class="value">${value}</div>
        </article>
      `).join('');
    }

    function renderTasks() {
      taskBodyEl.innerHTML = state.tasks.map((task) => {
        const due = isDue(task);
        const statusClass = task.is_active ? (due ? 'off' : 'on') : 'off';
        const statusLabel = task.is_active ? (due ? 'Просрочена' : 'В графике') : 'Отключена';
        return `
          <tr data-id="${task.id}">
            <td class="mono">${task.id}</td>
            <td><input data-field="city" value="${task.city}" /></td>
            <td><input data-field="store" value="${task.store}" /></td>
            <td><input data-field="parser_name" value="${task.parser_name}" /></td>
            <td>
              <select data-field="include_images">
                <option value="true" ${(task.include_images ?? true) ? 'selected' : ''}>true</option>
                <option value="false" ${!(task.include_images ?? true) ? 'selected' : ''}>false</option>
              </select>
            </td>
            <td><input data-field="frequency_hours" type="number" min="1" value="${task.frequency_hours}" /></td>
            <td>${fmtDate(task.last_crawl_at)}</td>
            <td>
              <div class="status ${statusClass}">${statusLabel}</div>
              <label style="margin-top:8px;">active
                <select data-field="is_active"><option value="true" ${task.is_active ? 'selected' : ''}>true</option><option value="false" ${!task.is_active ? 'selected' : ''}>false</option></select>
              </label>
            </td>
            <td>
              <div class="toolbar">
                <button data-action="save" data-id="${task.id}">Сохранить</button>
                <button data-action="delete" data-id="${task.id}" class="danger">Удалить</button>
              </div>
            </td>
          </tr>
        `;
      }).join('');
    }

    function renderOrchestrators() {
      const list = state.overview?.orchestrators || [];
      if (!list.length) {
        orchListEl.innerHTML = '<div class="muted">Оркестраторы не зарегистрированы</div>';
        return;
      }
      orchListEl.innerHTML = list.map((item) => `
        <article class="orch">
          <div class="row-top"><strong>${item.name}</strong><span class="mono">${item.id.slice(0, 8)}</span></div>
          <div class="muted">heartbeat: ${fmtDate(item.last_heartbeat_at)}</div>
        </article>
      `).join('');
    }

    function renderRuns() {
      const runs = state.overview?.recent_runs || [];
      if (!runs.length) {
        runListEl.innerHTML = '<div class="muted">История запусков пуста</div>';
        return;
      }
      runListEl.innerHTML = runs.map((run) => {
        const localStatus = (run.display_status || run.status || 'unknown').toString();
        const localChipClass = localStatus === 'validation_failed' ? 'warning' : localStatus;
        const isDone = run.status === 'success' || run.status === 'error';
        const canOpenLiveLog = !!run.can_open_live_log;
        const remoteStatus = typeof run.remote_status === 'string' && run.remote_status
          ? run.remote_status
          : 'unknown';
        const attrs = canOpenLiveLog
          ? ` data-run-id="${run.id}" tabindex="0" role="button" aria-label="Открыть live-лог run ${run.id.slice(0, 12)}"`
          : '';
        const liveLogText = canOpenLiveLog
          ? 'клик: live worker log'
          : (isDone
            ? 'лог недоступен: run завершен'
            : (run.remote_terminal
              ? `лог недоступен: remote ${remoteStatus}`
              : 'лог недоступен: нет remote job'));
        const validationText = run.validation_failed
          ? '<div class="muted">данные сохранены, dataclass validation failed</div>'
          : '';
        const errorMessage = run.status === 'error' && typeof run.error_message === 'string'
          ? run.error_message.trim()
          : '';
        const errorText = errorMessage
          ? `<div class="run-error">${escapeHtml(errorMessage)}</div>`
          : '';
        return `
        <article class="run ${canOpenLiveLog ? '' : 'done'}"${attrs}>
          <div class="row-top">
            <strong class="mono">${run.id.slice(0, 12)}</strong>
            <div class="chip-set">
              <span class="chip ${localChipClass}">run:${localStatus}</span>
              <span class="chip remote-chip ${remoteStatus}">remote:${remoteStatus}</span>
            </div>
          </div>
          <div class="muted">task #${run.task_id} • ${run.city || '—'} / ${run.store || '—'}</div>
          <div class="muted">orch: ${run.orchestrator_name || '—'}</div>
          <div class="muted">images: ${run.processed_images} • start: ${fmtDate(run.assigned_at)}</div>
          <div class="muted">converter: ${Number(run.converter_elapsed_sec || 0)}s • finish: ${fmtDate(run.finish)}</div>
          ${validationText}
          ${errorText}
          <div class="muted">${liveLogText}</div>
        </article>
      `;
      }).join('');
    }

    async function refreshAll() {
      try {
        const [overview, tasks] = await Promise.all([api('/api/overview'), api('/api/tasks')]);
        state.overview = overview;
        state.tasks = tasks;
        renderMetrics();
        renderTasks();
        renderOrchestrators();
        renderRuns();
      } catch (err) {
        flash(`Ошибка обновления: ${err.message}`, true);
      }
    }

    async function createTask(event) {
      event.preventDefault();
      const form = event.currentTarget;
      const payload = {
        city: form.city.value.trim(),
        store: form.store.value.trim(),
        frequency_hours: Number(form.frequency_hours.value),
        parser_name: form.parser_name.value.trim() || 'fixprice',
        include_images: form.include_images.value === 'true',
        is_active: form.is_active.value === 'true',
      };
      if (!payload.city || !payload.store || !payload.frequency_hours) {
        flash('Заполните обязательные поля', true);
        return;
      }

      try {
        await api('/api/tasks', { method: 'POST', body: JSON.stringify(payload) });
        form.reset();
        form.frequency_hours.value = 24;
        form.parser_name.value = 'fixprice';
        form.include_images.value = 'true';
        form.is_active.value = 'true';
        flash('Задача создана');
        await refreshAll();
      } catch (err) {
        flash(`Ошибка создания: ${err.message}`, true);
      }
    }

    async function onTaskAction(event) {
      const button = event.target.closest('button[data-action]');
      if (!button) return;

      const row = button.closest('tr[data-id]');
      if (!row) return;
      const taskId = row.dataset.id;

      if (button.dataset.action === 'save') {
        const getValue = (field) => row.querySelector(`[data-field="${field}"]`);
        const payload = {
          city: getValue('city').value.trim(),
          store: getValue('store').value.trim(),
          parser_name: getValue('parser_name').value.trim() || 'fixprice',
          include_images: getValue('include_images').value === 'true',
          frequency_hours: Number(getValue('frequency_hours').value),
          is_active: getValue('is_active').value === 'true',
        };

        try {
          await api(`/api/tasks/${taskId}`, {
            method: 'PATCH',
            body: JSON.stringify(payload),
          });
          flash(`Задача #${taskId} обновлена`);
          await refreshAll();
        } catch (err) {
          flash(`Ошибка сохранения: ${err.message}`, true);
        }
        return;
      }

      if (button.dataset.action === 'delete') {
        if (!window.confirm(`Удалить задачу #${taskId}?`)) return;
        try {
          await api(`/api/tasks/${taskId}`, { method: 'DELETE' });
          flash(`Задача #${taskId} удалена`);
          await refreshAll();
        } catch (err) {
          flash(`Ошибка удаления: ${err.message}`, true);
        }
      }
    }

    function onRunAction(event) {
      const card = event.target.closest('article.run[data-run-id]');
      if (!card) return;
      openRunLog(card.dataset.runId);
    }

    function onRunKeydown(event) {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      const card = event.target.closest('article.run[data-run-id]');
      if (!card) return;
      event.preventDefault();
      openRunLog(card.dataset.runId);
    }

    document.getElementById('refresh').addEventListener('click', refreshAll);
    document.getElementById('create-form').addEventListener('submit', createTask);
    taskBodyEl.addEventListener('click', onTaskAction);
    runListEl.addEventListener('click', onRunAction);
    runListEl.addEventListener('keydown', onRunKeydown);
    document.getElementById('run-log-close').addEventListener('click', closeLogModal);
    document.getElementById('run-log-backdrop').addEventListener('click', closeLogModal);
    document.getElementById('run-log-reconnect').addEventListener('click', () => {
      if (!state.logViewer.runId) return;
      connectRunLog(state.logViewer.runId);
    });
    if (runLogCompactEl) {
      runLogCompactEl.checked = state.logViewer.compactEnabled;
      runLogCompactEl.addEventListener('change', () => {
        state.logViewer.compactEnabled = !!runLogCompactEl.checked;
        renderLogOutput();
      });
    }
    document.addEventListener('keydown', (event) => {
      if (event.key !== 'Escape') return;
      if (runLogModalEl.hidden) return;
      closeLogModal();
    });

    refreshAll();
    window.setInterval(refreshAll, 15000);
