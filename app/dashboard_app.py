from __future__ import annotations

import argparse
import contextlib
from contextlib import asynccontextmanager
import json
import logging
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.config import Settings, load_settings
from app.database import create_session_factory, create_sqlalchemy_engine
from app.models import Base, CrawlTask, Orchestrator, TaskRun
from app.services.scheduler import as_utc, is_task_due, utcnow


LOGGER = logging.getLogger(__name__)


async def _connect_orchestrator_ws(ws_url: str) -> Any:
    try:
        import websockets
    except ModuleNotFoundError as exc:
        raise RuntimeError("Package 'websockets' is required for dashboard log proxy") from exc
    return await websockets.connect(ws_url)


def _dispatch_meta(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    meta = payload.get("_receiver_dispatch")
    if not isinstance(meta, dict):
        return {}
    return meta


DASHBOARD_HTML = """<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Receiver Control Room</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Sans:wght@400;600&display=swap');

    :root {
      --bg: #0d1220;
      --bg-soft: #151d33;
      --panel: rgba(20, 28, 48, 0.78);
      --panel-border: rgba(149, 176, 255, 0.22);
      --text: #eaf0ff;
      --muted: #9eb0d2;
      --accent: #3bd5ff;
      --accent-warm: #ffac5e;
      --ok: #3ddf9a;
      --warn: #ffcc4d;
      --err: #ff6f84;
      --radius: 18px;
      --shadow: 0 22px 60px rgba(4, 8, 20, 0.42);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      background:
        radial-gradient(1300px 600px at 12% -12%, rgba(59, 213, 255, 0.22), transparent),
        radial-gradient(900px 500px at 86% 0%, rgba(255, 172, 94, 0.18), transparent),
        linear-gradient(170deg, var(--bg) 0%, #090d17 58%, #0d1426 100%);
      font-family: "IBM Plex Sans", "Noto Sans", sans-serif;
    }

    .layout {
      max-width: 1260px;
      margin: 0 auto;
      padding: 30px 18px 56px;
      display: grid;
      gap: 18px;
    }

    .hero {
      position: relative;
      overflow: hidden;
      border-radius: calc(var(--radius) + 8px);
      border: 1px solid rgba(132, 176, 255, 0.27);
      background: linear-gradient(132deg, rgba(21, 31, 54, 0.92), rgba(13, 21, 39, 0.9));
      box-shadow: var(--shadow);
      padding: 26px;
    }

    .hero::before,
    .hero::after {
      content: "";
      position: absolute;
      border-radius: 50%;
      filter: blur(0px);
      pointer-events: none;
    }

    .hero::before {
      width: 180px;
      height: 180px;
      right: -42px;
      top: -52px;
      background: radial-gradient(circle at 30% 30%, rgba(59, 213, 255, 0.45), transparent 72%);
    }

    .hero::after {
      width: 170px;
      height: 170px;
      left: -58px;
      bottom: -80px;
      background: radial-gradient(circle at 70% 70%, rgba(255, 172, 94, 0.34), transparent 72%);
    }

    .hero h1 {
      margin: 0;
      font: 700 2rem/1.1 "Space Grotesk", "IBM Plex Sans", sans-serif;
      letter-spacing: 0.02em;
    }

    .hero p {
      margin: 8px 0 0;
      color: var(--muted);
      max-width: 760px;
    }

    .hero-toolbar {
      margin-top: 16px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }

    .grid-metrics {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }

    .metric {
      border: 1px solid var(--panel-border);
      background: var(--panel);
      border-radius: var(--radius);
      padding: 14px;
      box-shadow: var(--shadow);
    }

    .metric .label {
      color: var(--muted);
      font-size: 0.9rem;
    }

    .metric .value {
      margin-top: 6px;
      font: 700 1.66rem/1 "Space Grotesk", "IBM Plex Sans", sans-serif;
    }

    .panel-grid {
      display: grid;
      gap: 14px;
      grid-template-columns: 1.15fr 1fr;
    }

    .panel {
      border: 1px solid var(--panel-border);
      border-radius: var(--radius);
      background: var(--panel);
      box-shadow: var(--shadow);
      padding: 14px;
    }

    .panel h2 {
      margin: 0 0 12px;
      font: 600 1.05rem/1.2 "Space Grotesk", "IBM Plex Sans", sans-serif;
      letter-spacing: 0.02em;
    }

    .toolbar {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }

    .task-create {
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      align-items: end;
    }

    label { display: grid; gap: 4px; font-size: 0.78rem; color: var(--muted); }

    input, select, button {
      font: 500 0.92rem/1.1 "IBM Plex Sans", "Noto Sans", sans-serif;
      border-radius: 12px;
      border: 1px solid rgba(146, 173, 247, 0.4);
      background: rgba(11, 16, 30, 0.88);
      color: var(--text);
      padding: 10px 12px;
      transition: border-color .2s ease, transform .2s ease, box-shadow .2s ease;
    }

    input:focus, select:focus {
      outline: none;
      border-color: rgba(59, 213, 255, 0.85);
      box-shadow: 0 0 0 3px rgba(59, 213, 255, 0.18);
    }

    button {
      border-color: rgba(59, 213, 255, 0.52);
      background: linear-gradient(135deg, rgba(59, 213, 255, 0.23), rgba(59, 213, 255, 0.06));
      cursor: pointer;
      font-weight: 600;
    }

    button:hover {
      transform: translateY(-1px);
      border-color: rgba(59, 213, 255, 0.9);
    }

    button.soft {
      border-color: rgba(255, 172, 94, 0.46);
      background: linear-gradient(135deg, rgba(255, 172, 94, 0.2), rgba(255, 172, 94, 0.07));
    }

    button.ghost {
      border-color: rgba(163, 182, 225, 0.4);
      background: rgba(13, 20, 36, 0.7);
    }

    .tasks-wrap {
      overflow: auto;
      max-height: 62vh;
      border-radius: 12px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 960px;
    }

    th, td {
      border-bottom: 1px solid rgba(134, 159, 224, 0.16);
      padding: 9px 8px;
      text-align: left;
      vertical-align: top;
    }

    th {
      font: 600 0.78rem/1.1 "Space Grotesk", "IBM Plex Sans", sans-serif;
      color: var(--muted);
      position: sticky;
      top: 0;
      background: rgba(14, 22, 40, 0.98);
      z-index: 1;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }

    .status {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.78rem;
      color: var(--muted);
    }

    .status::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 50%;
      display: inline-block;
      background: var(--warn);
    }

    .status.on::before { background: var(--ok); }
    .status.off::before { background: var(--err); }

    .runs, .orch-list {
      display: grid;
      gap: 8px;
      max-height: 62vh;
      overflow: auto;
      padding-right: 4px;
    }

    .run, .orch {
      border: 1px solid rgba(130, 158, 228, 0.22);
      border-radius: 12px;
      padding: 10px;
      background: rgba(10, 16, 31, 0.74);
    }

    .run {
      cursor: pointer;
      transition: border-color .2s ease, transform .2s ease;
    }

    .run:hover {
      border-color: rgba(59, 213, 255, 0.65);
      transform: translateY(-1px);
    }

    .run:focus-visible {
      outline: 2px solid rgba(59, 213, 255, 0.8);
      outline-offset: 2px;
    }

    .row-top {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      align-items: baseline;
    }

    .chip {
      border-radius: 999px;
      padding: 2px 10px;
      font-size: 0.73rem;
      border: 1px solid rgba(255, 255, 255, 0.22);
      color: var(--text);
    }

    .chip.success { border-color: rgba(61, 223, 154, 0.62); color: var(--ok); }
    .chip.error { border-color: rgba(255, 111, 132, 0.62); color: var(--err); }
    .chip.assigned { border-color: rgba(255, 204, 77, 0.6); color: var(--warn); }

    .muted { color: var(--muted); }
    .mono { font-family: ui-monospace, "SFMono-Regular", "JetBrains Mono", monospace; }

    .flash {
      min-height: 1.2rem;
      font-size: 0.9rem;
      color: var(--warn);
      opacity: 0;
      transform: translateY(-4px);
      transition: all .25s ease;
    }

    .flash.show {
      opacity: 1;
      transform: translateY(0);
    }

    .log-modal[hidden] { display: none; }

    .log-modal {
      position: fixed;
      inset: 0;
      z-index: 50;
      display: grid;
      place-items: center;
    }

    .log-backdrop {
      position: absolute;
      inset: 0;
      background: rgba(3, 6, 12, 0.72);
      backdrop-filter: blur(3px);
    }

    .log-shell {
      position: relative;
      width: min(1020px, calc(100vw - 20px));
      max-height: calc(100vh - 28px);
      border: 1px solid rgba(147, 176, 247, 0.45);
      border-radius: 14px;
      background: rgba(9, 13, 25, 0.96);
      box-shadow: var(--shadow);
      padding: 12px;
      display: grid;
      gap: 10px;
      grid-template-rows: auto auto minmax(220px, 1fr);
    }

    .log-head {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
    }

    .log-head h3 {
      margin: 0;
      font: 600 1rem/1.2 "Space Grotesk", "IBM Plex Sans", sans-serif;
    }

    .log-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: end;
    }

    .log-tail {
      max-width: 140px;
    }

    .log-status {
      color: var(--muted);
      font-size: 0.83rem;
      min-height: 1.2rem;
    }

    .log-output {
      margin: 0;
      overflow: auto;
      border: 1px solid rgba(129, 157, 225, 0.26);
      border-radius: 10px;
      padding: 12px;
      background: rgba(3, 6, 13, 0.95);
      color: #d9e8ff;
      font: 500 0.82rem/1.38 ui-monospace, "SFMono-Regular", "JetBrains Mono", monospace;
      white-space: pre-wrap;
      word-break: break-word;
    }

    @media (max-width: 1080px) {
      .grid-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .panel-grid { grid-template-columns: 1fr; }
      .task-create { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }

    @media (max-width: 640px) {
      .hero h1 { font-size: 1.5rem; }
      .grid-metrics { grid-template-columns: 1fr; }
      .task-create { grid-template-columns: 1fr; }
      .layout { padding: 20px 12px 40px; }
      .log-shell { width: calc(100vw - 14px); max-height: calc(100vh - 14px); }
    }
  </style>
</head>
<body>
  <main class=\"layout\">
    <section class=\"hero\">
      <h1>Receiver Control Room</h1>
      <p>Управление задачами обхода, быстрый контроль оркестраторов и мониторинг последних запусков в одном отдельном приложении.</p>
      <div class=\"hero-toolbar\">
        <button id=\"refresh\">Обновить данные</button>
        <span class=\"muted\">Обновлено: <span id=\"generated-at\">—</span></span>
      </div>
      <div class=\"flash\" id=\"flash\"></div>
    </section>

    <section class=\"grid-metrics\" id=\"metrics\"></section>

    <section class=\"panel\">
      <h2>Новая задача</h2>
      <form class=\"task-create\" id=\"create-form\">
        <label>Город<input required name=\"city\" placeholder=\"Moscow\" /></label>
        <label>Магазин<input required name=\"store\" placeholder=\"C001\" /></label>
        <label>Частота (ч)<input required min=\"1\" type=\"number\" name=\"frequency_hours\" value=\"24\" /></label>
        <label>Парсер<input name=\"parser_name\" value=\"fixprice\" /></label>
        <label>Активна
          <select name=\"is_active\"><option value=\"true\" selected>Да</option><option value=\"false\">Нет</option></select>
        </label>
        <button type=\"submit\">Создать</button>
      </form>
    </section>

    <section class=\"panel-grid\">
      <section class=\"panel\">
        <h2>Задачи</h2>
        <div class=\"tasks-wrap\">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Город</th>
                <th>Магазин</th>
                <th>Парсер</th>
                <th>Частота</th>
                <th>Последний обход</th>
                <th>Статус</th>
                <th>Действия</th>
              </tr>
            </thead>
            <tbody id=\"task-body\"></tbody>
          </table>
        </div>
      </section>

      <section class=\"panel\">
        <h2>Оркестраторы</h2>
        <div class=\"orch-list\" id=\"orch-list\"></div>
        <h2 style=\"margin-top:14px;\">Последние run</h2>
        <div class=\"runs\" id=\"run-list\"></div>
      </section>
    </section>
  </main>

  <section class=\"log-modal\" id=\"run-log-modal\" hidden>
    <div class=\"log-backdrop\" id=\"run-log-backdrop\"></div>
    <section class=\"log-shell\">
      <div class=\"log-head\">
        <h3 id=\"run-log-title\">Worker log</h3>
        <button type=\"button\" id=\"run-log-close\" class=\"ghost\">Закрыть</button>
      </div>
      <div class=\"log-toolbar\">
        <label>Tail lines
          <input id=\"run-log-tail\" class=\"log-tail\" type=\"number\" min=\"0\" max=\"5000\" value=\"200\" />
        </label>
        <button type=\"button\" id=\"run-log-reconnect\">Переподключить</button>
      </div>
      <div class=\"log-status\" id=\"run-log-status\">Выберите run для просмотра live-лога.</div>
      <pre class=\"log-output\" id=\"run-log-output\"></pre>
    </section>
  </section>

  <script>
    const state = {
      overview: null,
      tasks: [],
      logViewer: {
        runId: null,
        socket: null,
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

    function fmtDate(value) {
      if (!value) return '—';
      const dt = new Date(value);
      if (Number.isNaN(dt.getTime())) return value;
      return dt.toLocaleString();
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
    }

    function appendLogLines(lines) {
      if (!Array.isArray(lines) || !lines.length) return;
      const chunk = `${lines.join('\\n')}\\n`;
      runLogOutputEl.textContent += chunk;
      runLogOutputEl.scrollTop = runLogOutputEl.scrollHeight;
    }

    function connectRunLog(runId) {
      closeLogSocket();
      state.logViewer.runId = runId;
      runLogOutputEl.textContent = '';
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
          runLogOutputEl.textContent = '';
          appendLogLines(payload.lines);
          runLogStatusEl.textContent = `Snapshot получен (${Array.isArray(payload.lines) ? payload.lines.length : 0} строк).`;
          return;
        }

        if (payload.event === 'append') {
          appendLogLines(payload.lines);
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
      connectRunLog(runId);
    }

    function isDue(task) {
      if (!task.is_active) return false;
      if (!task.last_crawl_at) return true;
      const last = new Date(task.last_crawl_at).getTime();
      if (Number.isNaN(last)) return false;
      const dueTs = last + Number(task.frequency_hours || 0) * 3600 * 1000;
      return dueTs <= Date.now();
    }

    function renderMetrics() {
      const o = state.overview;
      if (!o) return;
      generatedAtEl.textContent = fmtDate(o.generated_at);

      const entries = [
        ['Задачи всего', o.tasks_total],
        ['Активные', o.tasks_active],
        ['Просроченные', o.tasks_due],
        ['В lease', o.tasks_leased],
        ['Оркестраторы', o.orchestrators_total],
        ['Runs всего', o.runs_total],
        ['Runs success', o.runs_success],
        ['Runs error', o.runs_error],
        ['Runs assigned', o.runs_assigned],
      ];
      metricsEl.innerHTML = entries.map(([label, value]) => `
        <article class=\"metric\">
          <div class=\"label\">${label}</div>
          <div class=\"value\">${value}</div>
        </article>
      `).join('');
    }

    function renderTasks() {
      taskBodyEl.innerHTML = state.tasks.map((task) => {
        const due = isDue(task);
        const statusClass = task.is_active ? (due ? 'off' : 'on') : 'off';
        const statusLabel = task.is_active ? (due ? 'Просрочена' : 'В графике') : 'Отключена';
        return `
          <tr data-id=\"${task.id}\">
            <td class=\"mono\">${task.id}</td>
            <td><input data-field=\"city\" value=\"${task.city}\" /></td>
            <td><input data-field=\"store\" value=\"${task.store}\" /></td>
            <td><input data-field=\"parser_name\" value=\"${task.parser_name}\" /></td>
            <td><input data-field=\"frequency_hours\" type=\"number\" min=\"1\" value=\"${task.frequency_hours}\" /></td>
            <td>${fmtDate(task.last_crawl_at)}</td>
            <td>
              <div class=\"status ${statusClass}\">${statusLabel}</div>
              <label style=\"margin-top:8px;\">active
                <select data-field=\"is_active\"><option value=\"true\" ${task.is_active ? 'selected' : ''}>true</option><option value=\"false\" ${!task.is_active ? 'selected' : ''}>false</option></select>
              </label>
            </td>
            <td>
              <div class=\"toolbar\">
                <button data-action=\"save\" data-id=\"${task.id}\">Сохранить</button>
              </div>
            </td>
          </tr>
        `;
      }).join('');
    }

    function renderOrchestrators() {
      const list = state.overview?.orchestrators || [];
      if (!list.length) {
        orchListEl.innerHTML = '<div class=\"muted\">Оркестраторы не зарегистрированы</div>';
        return;
      }
      orchListEl.innerHTML = list.map((item) => `
        <article class=\"orch\">
          <div class=\"row-top\"><strong>${item.name}</strong><span class=\"mono\">${item.id.slice(0, 8)}</span></div>
          <div class=\"muted\">heartbeat: ${fmtDate(item.last_heartbeat_at)}</div>
        </article>
      `).join('');
    }

    function renderRuns() {
      const runs = state.overview?.recent_runs || [];
      if (!runs.length) {
        runListEl.innerHTML = '<div class=\"muted\">История запусков пуста</div>';
        return;
      }
      runListEl.innerHTML = runs.map((run) => `
        <article class=\"run\" data-run-id=\"${run.id}\" tabindex=\"0\" role=\"button\" aria-label=\"Открыть live-лог run ${run.id.slice(0, 12)}\">
          <div class=\"row-top\">
            <strong class=\"mono\">${run.id.slice(0, 12)}</strong>
            <span class=\"chip ${run.status}\">${run.status}</span>
          </div>
          <div class=\"muted\">task #${run.task_id} • ${run.city || '—'} / ${run.store || '—'}</div>
          <div class=\"muted\">orch: ${run.orchestrator_name || '—'}</div>
          <div class=\"muted\">images: ${run.processed_images} • start: ${fmtDate(run.assigned_at)}</div>
          <div class=\"muted\">клик: live worker log</div>
        </article>
      `).join('');
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

    refreshAll();
    window.setInterval(refreshAll, 15000);
  </script>
</body>
</html>
"""


class TaskCreateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    city: str = Field(min_length=1, max_length=120)
    store: str = Field(min_length=1, max_length=120)
    frequency_hours: int = Field(ge=1, le=24 * 365)
    parser_name: str = Field(default="fixprice", min_length=1, max_length=64)
    is_active: bool = True


class TaskUpdateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    city: str | None = Field(default=None, min_length=1, max_length=120)
    store: str | None = Field(default=None, min_length=1, max_length=120)
    frequency_hours: int | None = Field(default=None, ge=1, le=24 * 365)
    parser_name: str | None = Field(default=None, min_length=1, max_length=64)
    is_active: bool | None = None


def _ensure_sqlite_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("sqlite"):
        return

    if not url.database or url.database == ":memory:":
        return

    db_path = Path(url.database).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _task_to_dict(task: CrawlTask) -> dict[str, object]:
    return {
        "id": task.id,
        "city": task.city,
        "store": task.store,
        "frequency_hours": task.frequency_hours,
        "last_crawl_at": task.last_crawl_at.isoformat() if task.last_crawl_at else None,
        "parser_name": task.parser_name,
        "is_active": task.is_active,
        "lease_owner_id": task.lease_owner_id,
        "lease_until": task.lease_until.isoformat() if task.lease_until else None,
        "created_at": task.created_at.isoformat(),
        "updated_at": task.updated_at.isoformat(),
    }


def create_dashboard_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or load_settings()

    _ensure_sqlite_parent_dir(app_settings.database_url)
    engine = create_sqlalchemy_engine(app_settings.database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            engine.dispose()

    app = FastAPI(title="Receiver Dashboard", version="0.1.0", lifespan=lifespan)
    app.state.engine = engine
    app.state.session_factory = session_factory

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard_page() -> str:
        return DASHBOARD_HTML

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/tasks")
    def list_tasks() -> list[dict[str, object]]:
        session = session_factory()
        try:
            tasks = session.scalars(select(CrawlTask).order_by(CrawlTask.id.asc())).all()
            return [_task_to_dict(task) for task in tasks]
        finally:
            session.close()

    @app.post("/api/tasks", status_code=status.HTTP_201_CREATED)
    def create_task(payload: TaskCreateIn) -> dict[str, object]:
        session = session_factory()
        try:
            now = utcnow()
            task = CrawlTask(
                city=payload.city.strip(),
                store=payload.store.strip(),
                frequency_hours=payload.frequency_hours,
                parser_name=payload.parser_name.strip(),
                is_active=payload.is_active,
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            session.commit()
            session.refresh(task)
            return _task_to_dict(task)
        finally:
            session.close()

    @app.patch("/api/tasks/{task_id}")
    def update_task(task_id: int, payload: TaskUpdateIn) -> dict[str, object]:
        session = session_factory()
        try:
            task = session.get(CrawlTask, task_id)
            if task is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

            changes = payload.model_dump(exclude_unset=True)
            for field_name, value in changes.items():
                setattr(task, field_name, value)

            if task.is_active is False:
                task.lease_owner_id = None
                task.lease_until = None

            task.updated_at = utcnow()
            session.commit()
            session.refresh(task)
            return _task_to_dict(task)
        finally:
            session.close()

    @app.websocket("/ws/runs/{run_id}/log")
    async def stream_run_log(
        websocket: WebSocket,
        run_id: str,
        tail: int = Query(default=200, ge=0, le=5000),
    ) -> None:
        await websocket.accept()
        session = session_factory()
        parser_socket: Any | None = None

        async def _send_error(message: str) -> None:
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {
                        "ok": False,
                        "action": "stream_job_log",
                        "event": "error",
                        "run_id": run_id,
                        "error": message,
                    }
                )

        try:
            run = session.get(TaskRun, run_id)
            if run is None:
                await _send_error("Run not found.")
                return

            dispatch_meta = _dispatch_meta(run.payload_json)
            remote_job_id = dispatch_meta.get("remote_job_id")
            if not isinstance(remote_job_id, str) or not remote_job_id.strip():
                await _send_error("Run is not linked to orchestrator WS job.")
                return

            parser_socket = await _connect_orchestrator_ws(app_settings.orchestrator_ws_url)
            request_payload: dict[str, Any] = {
                "action": "stream_job_log",
                "job_id": remote_job_id.strip(),
                "tail_lines": int(tail),
            }
            if app_settings.orchestrator_ws_password is not None:
                request_payload["password"] = app_settings.orchestrator_ws_password
            await parser_socket.send(json.dumps(request_payload, ensure_ascii=False))

            while True:
                raw_payload = await parser_socket.recv()
                raw_text = (
                    raw_payload.decode("utf-8", errors="replace")
                    if isinstance(raw_payload, (bytes, bytearray))
                    else str(raw_payload)
                )
                try:
                    parsed_payload = json.loads(raw_text)
                except json.JSONDecodeError:
                    await _send_error("Orchestrator returned invalid JSON.")
                    return

                if not isinstance(parsed_payload, dict):
                    await _send_error("Orchestrator returned unexpected payload format.")
                    return

                await websocket.send_json(parsed_payload)
                event_name = str(parsed_payload.get("event", "")).strip().lower()
                if parsed_payload.get("ok") is False or event_name in {"end", "error"}:
                    return
        except WebSocketDisconnect:
            LOGGER.debug("Dashboard client disconnected from run-log stream: run_id=%s", run_id)
        except Exception as exc:
            LOGGER.exception("Run log proxy failed: run_id=%s error=%s", run_id, exc)
            await _send_error(str(exc))
        finally:
            session.close()
            if parser_socket is not None:
                with contextlib.suppress(Exception):
                    await parser_socket.close()
            with contextlib.suppress(Exception):
                await websocket.close()

    @app.get("/api/overview")
    def overview() -> dict[str, object]:
        session = session_factory()
        try:
            now = utcnow()
            tasks = session.scalars(select(CrawlTask)).all()
            orchestrators = session.scalars(
                select(Orchestrator).order_by(Orchestrator.updated_at.desc())
            ).all()

            due_count = sum(1 for task in tasks if task.is_active and is_task_due(task, now=now))
            leased_count = sum(
                1
                for task in tasks
                if (
                    task.lease_owner_id is not None
                    and (lease_until := as_utc(task.lease_until)) is not None
                    and lease_until > now
                )
            )

            run_counts = dict(
                session.execute(select(TaskRun.status, func.count(TaskRun.id)).group_by(TaskRun.status)).all()
            )

            recent_rows = session.execute(
                select(
                    TaskRun.id,
                    TaskRun.task_id,
                    TaskRun.status,
                    TaskRun.assigned_at,
                    TaskRun.finished_at,
                    Orchestrator.name,
                    CrawlTask.city,
                    CrawlTask.store,
                    TaskRun.image_results_json,
                )
                .join(Orchestrator, Orchestrator.id == TaskRun.orchestrator_id)
                .join(CrawlTask, CrawlTask.id == TaskRun.task_id)
                .order_by(TaskRun.assigned_at.desc())
                .limit(12)
            ).all()

            recent_runs = []
            for (
                run_id,
                task_id,
                run_status,
                assigned_at,
                finished_at,
                orchestrator_name,
                city,
                store,
                image_results,
            ) in recent_rows:
                processed_images = 0
                if isinstance(image_results, list):
                    processed_images = sum(
                        1 for item in image_results if isinstance(item, dict) and item.get("uploaded_url")
                    )
                recent_runs.append(
                    {
                        "id": run_id,
                        "task_id": task_id,
                        "status": run_status,
                        "assigned_at": assigned_at.isoformat(),
                        "finished_at": finished_at.isoformat() if finished_at else None,
                        "orchestrator_name": orchestrator_name,
                        "city": city,
                        "store": store,
                        "processed_images": processed_images,
                    }
                )

            return {
                "generated_at": now.isoformat(),
                "tasks_total": len(tasks),
                "tasks_active": sum(1 for task in tasks if task.is_active),
                "tasks_due": due_count,
                "tasks_leased": leased_count,
                "orchestrators_total": len(orchestrators),
                "runs_total": sum(run_counts.values()),
                "runs_assigned": int(run_counts.get("assigned", 0)),
                "runs_success": int(run_counts.get("success", 0)),
                "runs_error": int(run_counts.get("error", 0)),
                "orchestrators": [
                    {
                        "id": item.id,
                        "name": item.name,
                        "created_at": item.created_at.isoformat(),
                        "updated_at": item.updated_at.isoformat(),
                        "last_heartbeat_at": item.last_heartbeat_at.isoformat(),
                    }
                    for item in orchestrators
                ],
                "recent_runs": recent_runs,
            }
        finally:
            session.close()

    return app


app = create_dashboard_app()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Receiver dashboard app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--reload", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    uvicorn.run("app.dashboard_app:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
