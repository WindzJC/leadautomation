const state = {
  runs: [],
  selectedRunId: "",
  selectedManifest: {},
  leadRows: [],
  leadSource: "final",
  leadCopyText: "",
  selectedLeadIndex: -1,
  currentConfig: {},
  activeRun: {},
  liveStatus: {},
  runLogStatus: {},
  diagnosticTab: "overview",
};

let runConfigSaveTimer = null;
const RUN_CONFIG_STORAGE_KEY = "lead-finder.run-config";
const DIAGNOSTIC_TABS = ["overview", "history", "rejects", "logs", "artifacts", "validated"];

const overviewCards = [
  ["Total Runs", "total_runs"],
  ["Harvested", "harvested_count"],
  ["Validated", "validated_count"],
  ["Rejected", "rejected_count"],
  ["Duplicates", "duplicate_count"],
];

const RUN_FORM_FIELDS = [
  "validation_profile",
  "goal_final",
  "max_runs",
  "max_stale_runs",
  "target",
  "min_candidates",
  "max_candidates",
  "batch_min",
  "batch_max",
  "queries_file",
  "run_folder_name",
  "listing_strict",
];

const MODE_PRESETS = {
  fast_pull: {
    label: "Fast Lead Pull",
    profile: "email_only",
    defaults: {
      validation_profile: "email_only",
      goal_final: 100,
      max_runs: 5,
      max_stale_runs: 5,
      target: 80,
      min_candidates: 80,
      max_candidates: 80,
      batch_min: 10,
      batch_max: 20,
      listing_strict: false,
    },
  },
  strict_us: {
    label: "Strict U.S. Indie Authors",
    profile: "strict_full",
    defaults: {
      validation_profile: "strict_full",
      goal_final: 20,
      max_runs: 1,
      max_stale_runs: 1,
      target: 80,
      min_candidates: 80,
      max_candidates: 80,
      batch_min: 20,
      batch_max: 20,
      listing_strict: true,
    },
  },
  scout_mode: {
    label: "Scout Mode",
    profile: "agent_hunt",
    defaults: {
      validation_profile: "agent_hunt",
      goal_final: 200,
      max_runs: 12,
      max_stale_runs: 4,
      target: 80,
      min_candidates: 50,
      max_candidates: 50,
      batch_min: 20,
      batch_max: 20,
      listing_strict: false,
    },
  },
};

const PROFILE_TO_MODE = {
  email_only: "fast_pull",
  agent_hunt: "scout_mode",
  strict_full: "strict_us",
  strict_interactive: "strict_us",
  fully_verified: "strict_us",
  verified_no_us: "strict_us",
  astra_outbound: "strict_us",
};

function readRunConfigDraft() {
  try {
    const raw = window.localStorage.getItem(RUN_CONFIG_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const payload = JSON.parse(raw);
    return payload && typeof payload === "object" ? payload : {};
  } catch (_error) {
    return {};
  }
}

function writeRunConfigDraft(payload) {
  try {
    window.localStorage.setItem(RUN_CONFIG_STORAGE_KEY, JSON.stringify(payload || {}));
  } catch (_error) {
    // Ignore local persistence failures.
  }
}

function basename(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const parts = raw.replace(/\\/g, "/").split("/").filter(Boolean);
  return parts.length ? parts[parts.length - 1] : raw;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeCsvValue(value) {
  const text = String(value ?? "");
  if (!/[",\n]/.test(text)) {
    return text;
  }
  return `"${text.replace(/"/g, '""')}"`;
}

function setError(message) {
  const banner = document.getElementById("error-banner");
  if (!message) {
    banner.textContent = "";
    banner.classList.add("hidden");
    return;
  }
  banner.textContent = message;
  banner.classList.remove("hidden");
}

async function fetchJson(url, fallback, options) {
  try {
    const response = await fetch(url, options);
    if (!response.ok) {
      if (response.status === 404) {
        return fallback;
      }
      let detail = `${response.status} ${response.statusText}`;
      try {
        const payload = await response.json();
        if (payload && payload.detail) {
          detail = payload.detail;
        }
      } catch (_error) {
        // Keep default detail.
      }
      throw new Error(detail);
    }
    return await response.json();
  } catch (error) {
    setError(`Request failed for ${url}: ${error.message}`);
    return fallback;
  }
}

async function postJson(url, payload, fallback) {
  return fetchJson(url, fallback, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

function formatTimestamp(value) {
  if (!value) {
    return "n/a";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

function prettifyLabel(value) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function modeKeyFromProfile(profile) {
  return PROFILE_TO_MODE[String(profile || "").trim()] || "scout_mode";
}

function modePresetForKey(modeKey) {
  return MODE_PRESETS[modeKey] || MODE_PRESETS.scout_mode;
}

function modeLabelForProfile(profile) {
  return modePresetForKey(modeKeyFromProfile(profile)).label;
}

function buildModeConfig(modeKey, currentPayload) {
  const preset = modePresetForKey(modeKey);
  const current = { ...(currentPayload || {}) };
  const next = {
    ...current,
    ...preset.defaults,
    validation_profile: preset.profile,
  };
  if (Number(current.goal_final || 0) > 0) {
    next.goal_final = Number(current.goal_final || 0);
  }
  if (String(current.queries_file || "").trim()) {
    next.queries_file = current.queries_file;
  }
  if (String(current.run_folder_name || "").trim()) {
    next.run_folder_name = current.run_folder_name;
  }
  return next;
}

function buildProgressSnippet(counts) {
  const snippets = [];
  if (typeof counts.harvested_candidates !== "undefined") {
    snippets.push(`harvested ${formatNumber(counts.harvested_candidates)}`);
  }
  if (typeof counts.validated !== "undefined") {
    snippets.push(`validated ${formatNumber(counts.validated)}`);
  }
  return snippets.join(" • ");
}

function getEffectiveRunStatus() {
  const activeRun = state.activeRun || {};
  const live = state.liveStatus || {};
  const logStatus = state.runLogStatus || {};
  const active = Boolean(live.active || activeRun.active || logStatus.active);
  const statusValue = active
    ? live.status || live.status_label || logStatus.status_label || activeRun.status_label || activeRun.status || "running"
    : activeRun.status_label || live.status || logStatus.status_label || activeRun.status || "idle";
  const status = String(statusValue || "idle").toLowerCase();
  const runRoot = activeRun.run_root || logStatus.run_root || "";
  const profile = (activeRun.config || {}).validation_profile || state.currentConfig.validation_profile || "agent_hunt";
  const folder = basename(runRoot) || String(state.currentConfig.run_folder_name || "").trim() || "dashboard folder";
  const runTag = live.run_tag || activeRun.run_tag || "";
  const stage = active
    ? live.stage || live.message || prettifyLabel(status)
    : activeRun.status_label
      ? `Last run ${prettifyLabel(activeRun.status_label)}`
      : "No active run";
  const updatedAt = live.updated_at_utc || logStatus.log_updated_at || activeRun.log_updated_at || activeRun.finished_at || "";
  const counts = live.counts || {};
  const config = activeRun.config || state.currentConfig || {};
  const targetCount = Number(config.goal_final || config.target_final || 0) || null;
  const acceptedCount = typeof counts.goal_qualified_total !== "undefined"
    ? Number(counts.goal_qualified_total || 0)
    : null;
  return {
    active,
    status,
    statusLabel: prettifyLabel(status),
    stage,
    runTag,
    folder,
    runRoot,
    profile,
    modeLabel: modeLabelForProfile(profile),
    startedAt: activeRun.started_at || "",
    updatedAt,
    progress: buildProgressSnippet(counts),
    targetCount,
    acceptedCount,
    stopReason: live.stop_reason || activeRun.stop_reason || "",
  };
}

function renderLivePanel() {
  const badge = document.getElementById("live-status-badge");
  const stage = document.getElementById("live-status-stage");
  const meta = document.getElementById("live-status-meta");
  const effective = getEffectiveRunStatus();

  badge.className = `status-badge ${effective.status}`;
  badge.textContent = effective.statusLabel;
  stage.textContent = effective.stage || "No active run";

  const parts = [];
  if (effective.runTag) {
    parts.push(`Run ${effective.runTag}`);
  }
  if (effective.modeLabel) {
    parts.push(effective.modeLabel);
  }
  if (effective.updatedAt) {
    parts.push(`Updated ${formatTimestamp(effective.updatedAt)}`);
  }
  if (effective.targetCount !== null) {
    parts.push(`Target Count: ${formatNumber(effective.targetCount)}`);
  }
  if (effective.acceptedCount !== null) {
    parts.push(`Accepted So Far: ${formatNumber(effective.acceptedCount)}`);
  }
  if (effective.progress) {
    parts.push(effective.progress);
  }
  meta.textContent = parts.join(" • ") || "Waiting for launch.";
}

function renderActiveRunStrip() {
  const effective = getEffectiveRunStatus();
  const strip = document.getElementById("active-run-strip");
  const badge = document.getElementById("active-strip-badge");
  const statusText = document.getElementById("active-strip-status-text");
  const stage = document.getElementById("active-strip-stage");
  const note = document.getElementById("active-strip-note");
  const profile = document.getElementById("active-strip-profile");
  const runTag = document.getElementById("active-strip-run-tag");
  const folder = document.getElementById("active-strip-folder");
  const started = document.getElementById("active-strip-started");
  const updated = document.getElementById("active-strip-updated");
  const targetCount = document.getElementById("active-strip-target-count");
  const acceptedCount = document.getElementById("active-strip-accepted-count");
  const stopButton = document.getElementById("active-strip-stop-button");

  strip.className = `active-strip ${effective.status}`;
  badge.className = `status-badge ${effective.status}`;
  badge.textContent = effective.statusLabel;
  statusText.textContent = effective.active
    ? "Run in progress"
    : effective.status === "completed"
      ? "Last run finished"
      : "Ready";
  stage.textContent = effective.active
    ? effective.stage
    : effective.runTag
      ? `Run ${effective.runTag}`
      : "No active run";
  note.textContent = effective.active
    ? (effective.stopReason ? `Stop reason: ${prettifyLabel(effective.stopReason)}` : effective.progress || "Polling live status.")
    : `Next run mode is ${effective.modeLabel}. Older runs stay in Diagnostics.`;
  profile.textContent = effective.modeLabel;
  runTag.textContent = effective.runTag ? `Run ${effective.runTag}` : "No active run";
  folder.textContent = effective.folder || "Dashboard folder";
  started.textContent = effective.startedAt ? formatTimestamp(effective.startedAt) : "Not running";
  updated.textContent = effective.updatedAt ? formatTimestamp(effective.updatedAt) : "Waiting for launch";
  targetCount.textContent = effective.targetCount !== null ? formatNumber(effective.targetCount) : "—";
  acceptedCount.textContent = effective.acceptedCount !== null ? formatNumber(effective.acceptedCount) : "—";
  stopButton.disabled = !effective.active;
  stopButton.classList.toggle("hidden", !effective.active);
}

function renderRunActionState() {
  const label = document.getElementById("run-control-status");
  const startButton = document.getElementById("run-start-button");
  const effective = getEffectiveRunStatus();

  label.textContent = effective.active
    ? `Running • ${effective.folder}`
    : `Ready • ${modeLabelForProfile(state.currentConfig.validation_profile || "agent_hunt")}`;
  startButton.disabled = effective.active;
}

function setFormFieldValue(field, value) {
  const form = document.getElementById("run-control-form");
  const element = form.elements.namedItem(field);
  if (!element) {
    return;
  }
  if (element.type === "checkbox") {
    element.checked = Boolean(value);
  } else {
    element.value = typeof value === "undefined" || value === null ? "" : value;
  }
}

function applyPayloadToForm(config) {
  const normalized = { ...(config || {}) };
  const modeSelect = document.getElementById("run-mode-select");
  const hiddenProfile = document.getElementById("run-validation-profile");
  const profile = String(normalized.validation_profile || "agent_hunt");

  hiddenProfile.value = profile;
  modeSelect.value = modeKeyFromProfile(profile);

  RUN_FORM_FIELDS.forEach((field) => {
    if (field === "validation_profile") {
      return;
    }
    setFormFieldValue(field, normalized[field]);
  });
}

function getRunFormPayload() {
  const form = document.getElementById("run-control-form");
  const payload = {};
  RUN_FORM_FIELDS.forEach((field) => {
    const element = form.elements.namedItem(field);
    if (!element) {
      return;
    }
    if (element.type === "checkbox") {
      payload[field] = element.checked;
    } else if (element.type === "number") {
      payload[field] = Number(element.value || 0);
    } else {
      payload[field] = element.value || "";
    }
  });
  return payload;
}

function renderRunPlanner(config) {
  state.currentConfig = { ...config };
  renderLivePanel();
  renderActiveRunStrip();
  renderRunActionState();
}

function applyRunConfig(configPayload) {
  const merged = {
    ...(configPayload.config || {}),
    ...readRunConfigDraft(),
  };
  state.activeRun = configPayload.active_run || {};
  applyPayloadToForm(merged);
  renderRunPlanner(merged);
  writeRunConfigDraft(getRunFormPayload());
}

function applyModePreset(modeKey) {
  const next = buildModeConfig(modeKey, getRunFormPayload());
  applyPayloadToForm(next);
  scheduleRunConfigSave();
}

function renderOverview(overview) {
  const container = document.getElementById("overview-cards");
  container.innerHTML = overviewCards
    .map(
      ([label, key]) => `
        <article class="overview-card">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${formatNumber(overview[key])}</div>
        </article>
      `,
    )
    .join("");
  renderReasonList(document.getElementById("top-reject-reasons"), overview.top_reject_reasons || []);
}

function renderReasonList(container, reasons) {
  if (!container) {
    return;
  }
  if (!reasons.length) {
    container.innerHTML = '<div class="empty-state">No reject reasons available.</div>';
    return;
  }
  container.innerHTML = reasons
    .map(
      (reason) => `
        <div class="reason-row">
          <span class="reason-label">${escapeHtml(prettifyLabel(reason.label))}</span>
          <span class="reason-count">${formatNumber(reason.count)}</span>
        </div>
      `,
    )
    .join("");
}

function renderRuns(runs) {
  const tbody = document.getElementById("runs-table-body");
  const effective = getEffectiveRunStatus();
  const activeTag = effective.active ? effective.runTag : "";

  document.getElementById("run-count").textContent = `${runs.length} runs`;
  if (!runs.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="7">No run manifests found.</td></tr>';
    return;
  }

  tbody.innerHTML = runs
    .map((run) => {
      const counts = run.counts || {};
      const selected = run.api_run_id === state.selectedRunId ? "selected" : "";
      const activeClass = activeTag && run.run_id === activeTag ? "active-run" : "";
      const classes = [selected, activeClass].filter(Boolean).join(" ");
      return `
        <tr data-run-id="${escapeHtml(run.api_run_id || "")}" class="${classes}">
          <td class="cell-primary">${escapeHtml(run.run_id || "run")}</td>
          <td>${escapeHtml(run.context_name || "")}</td>
          <td><span class="status-badge ${escapeHtml(run.status || "unknown")}">${escapeHtml(prettifyLabel(run.status || "unknown"))}</span></td>
          <td>${formatNumber(counts.validated)}</td>
          <td>${formatNumber(counts.rejected)}</td>
          <td>${formatNumber(counts.duplicates)}</td>
          <td>${escapeHtml(formatTimestamp(run.generated_at_utc))}</td>
        </tr>
      `;
    })
    .join("");

  tbody.querySelectorAll("tr[data-run-id]").forEach((row) => {
    row.addEventListener("click", () => selectRun(row.dataset.runId));
  });
}

function renderSelectedManifest(manifest) {
  const title = document.getElementById("selected-run-title");
  const status = document.getElementById("selected-run-status");
  const summary = document.getElementById("selected-run-summary");
  const funnel = document.getElementById("selected-run-funnel");
  const counts = (manifest && manifest.counts) || {};
  const effective = getEffectiveRunStatus();
  const activeFlag = effective.active && manifest && manifest.run_id && manifest.run_id === effective.runTag;

  state.selectedManifest = manifest || {};

  if (!manifest || !Object.keys(manifest).length) {
    title.textContent = "No run selected";
    status.textContent = "Choose a run to inspect details.";
    summary.innerHTML = "Select a run to load details.";
    summary.classList.add("empty-state");
    funnel.innerHTML = "";
    return;
  }

  title.textContent = manifest.run_id || "Run details";
  status.textContent = `${activeFlag ? "Active Run" : "Historical Run"} • ${prettifyLabel(manifest.status || "unknown")} • ${modeLabelForProfile(manifest.validation_profile || "agent_hunt")}`;
  summary.classList.remove("empty-state");
  summary.innerHTML = `
    <div class="summary-row"><strong>Generated</strong><span>${escapeHtml(formatTimestamp(manifest.generated_at_utc))}</span></div>
    <div class="summary-row"><strong>Target Final</strong><span>${formatNumber(manifest.target_final)}</span></div>
    <div class="summary-row"><strong>Pipeline Exit Code</strong><span>${formatNumber(manifest.pipeline_exit_code)}</span></div>
    <div class="summary-row"><strong>Stop Reason</strong><span>${escapeHtml(prettifyLabel((manifest.loop_control || {}).stop_reason || "none"))}</span></div>
  `;

  const stages = [
    ["Harvested", counts.harvested_candidates],
    ["Filtered", counts.filtered_candidates],
    ["Validated", counts.validated],
    ["Final", counts.final],
    ["Rejected", counts.rejected],
    ["Added To Master", counts.added_to_master],
  ];

  funnel.innerHTML = stages
    .map(
      ([label, value]) => `
        <div class="funnel-card">
          <div class="meta">${escapeHtml(label)}</div>
          <div class="value">${formatNumber(value)}</div>
        </div>
      `,
    )
    .join("");
}

function renderArtifactLinks(artifacts) {
  const container = document.getElementById("artifact-links");
  const entries = Object.entries(artifacts || {}).filter(([, payload]) => payload && payload.available && payload.download_url);
  if (!entries.length) {
    container.textContent = "Select a run to see downloadable artifacts.";
    container.classList.add("empty-state");
    return;
  }
  container.classList.remove("empty-state");
  container.innerHTML = entries
    .map(
      ([artifactName, payload]) => `
        <a class="artifact-link" href="${escapeHtml(payload.download_url)}" data-artifact="${escapeHtml(artifactName)}" target="_blank" rel="noreferrer">
          ${escapeHtml(payload.label)}
        </a>
      `,
    )
    .join("");
}

function renderTableBody(tbodyId, rows, columns, emptyMessage) {
  const tbody = document.getElementById(tbodyId);
  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="${columns.length}">${escapeHtml(emptyMessage)}</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((row) => {
      const cells = columns
        .map((column, index) => {
          const value = row[column] || "";
          const klass = index === 0 ? "cell-primary" : "";
          return `<td class="${klass}">${escapeHtml(value)}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
}

function renderSelectedRows(rejectedRows, validatedRows) {
  document.getElementById("rejected-count").textContent = `${rejectedRows.length} rows`;
  document.getElementById("validated-count").textContent = `${validatedRows.length} rows`;
  renderTableBody(
    "rejected-table-body",
    rejectedRows,
    ["AuthorName", "BookTitle", "CandidateDomain", "PrimaryFailReason", "FailReasons"],
    "No rejected rows for this run.",
  );
  renderTableBody(
    "validated-table-body",
    validatedRows,
    ["AuthorName", "BookTitle", "AuthorEmail", "ListingStatus", "RecencyStatus"],
    "No validated rows for this run.",
  );
}

function renderLeadSelection() {
  const label = document.getElementById("lead-output-selection");
  if (!state.leadRows.length || state.selectedLeadIndex < 0) {
    label.textContent = "Select a run to review accepted leads.";
    return;
  }
  const row = state.leadRows[state.selectedLeadIndex] || {};
  label.textContent = `Selected: ${row.AuthorName || "Unknown author"} • ${row.AuthorEmail || "No email"}`;
}

function renderLeadTableRows() {
  const tbody = document.getElementById("lead-output-table-body");
  const emptyState = document.getElementById("lead-output-empty");
  const tableWrap = document.getElementById("lead-output-table-wrap");

  if (!state.leadRows.length) {
    tbody.innerHTML = "";
    emptyState.classList.remove("hidden");
    tableWrap.classList.add("hidden");
    renderLeadSelection();
    return;
  }

  emptyState.classList.add("hidden");
  tableWrap.classList.remove("hidden");
  tbody.innerHTML = state.leadRows
    .map((row, index) => {
      const selected = index === state.selectedLeadIndex ? "selected" : "";
      return `
        <tr data-lead-index="${index}" class="${selected}">
          <td class="cell-primary">${escapeHtml(row.AuthorName || "")}</td>
          <td>${escapeHtml(row.AuthorEmail || "")}</td>
        </tr>
      `;
    })
    .join("");

  tbody.querySelectorAll("tr[data-lead-index]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedLeadIndex = Number(row.dataset.leadIndex);
      renderLeadTableRows();
    });
  });

  renderLeadSelection();
}

function updateLeadCopyUi() {
  const copyButton = document.getElementById("lead-output-copy-button");
  const exportButton = document.getElementById("lead-output-export-button");
  const enabled = state.leadRows.length > 0;
  copyButton.disabled = !enabled;
  exportButton.disabled = !enabled;
  if (!enabled) {
    setLeadCopyStatus("");
  }
}

function renderLeadOutput(payload) {
  const rows = payload.rows || [];
  const source = payload.source || "final";
  const selectedRun = state.selectedManifest || {};

  state.leadRows = rows;
  state.leadSource = source;
  state.leadCopyText = payload.copy_text || buildLeadCopyText(rows);
  if (!rows.length) {
    state.selectedLeadIndex = -1;
  } else if (state.selectedLeadIndex < 0 || state.selectedLeadIndex >= rows.length) {
    state.selectedLeadIndex = 0;
  }

  document.getElementById("lead-output-count").textContent = `${rows.length} ${rows.length === 1 ? "lead" : "leads"}`;
  document.getElementById("lead-output-source").textContent = rows.length
    ? `${selectedRun.run_id || "Selected run"} • ${prettifyLabel(source)} output • full SourceURL audit remains in run artifacts.`
    : "No accepted leads are available for the selected run.";

  renderLeadTableRows();
  updateLeadCopyUi();
}

function buildLeadCopyText(rows) {
  const csvLines = rows.map((row) => `${escapeCsvValue(row.AuthorName || "")},${escapeCsvValue(row.AuthorEmail || "")}`);
  const sourceLines = rows.map((row) => `${row.AuthorName || ""} — ${row.SourceURL || ""}`);
  const fencedBlock = ["```", ...csvLines, "```"].join("\n");
  if (!sourceLines.length) {
    return fencedBlock;
  }
  return `${fencedBlock}\n\n${sourceLines.join("\n")}`;
}

function buildLeadCsvText(rows) {
  const header = "AuthorName,AuthorEmail";
  const lines = rows.map((row) => `${escapeCsvValue(row.AuthorName || "")},${escapeCsvValue(row.AuthorEmail || "")}`);
  return [header, ...lines].join("\n");
}

function setLeadCopyStatus(message) {
  document.getElementById("lead-output-copy-status").textContent = message || "";
}

async function copyLeadRows() {
  if (!state.leadRows.length) {
    return;
  }
  const text = state.leadCopyText || buildLeadCopyText(state.leadRows);
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const helper = document.createElement("textarea");
      helper.value = text;
      document.body.appendChild(helper);
      helper.select();
      document.execCommand("copy");
      helper.remove();
    }
    setLeadCopyStatus(`Copied ${state.leadRows.length} rows`);
  } catch (_error) {
    setLeadCopyStatus("Copy failed");
  }
}

function exportLeadRows() {
  if (!state.leadRows.length) {
    return;
  }
  const blob = new Blob([buildLeadCsvText(state.leadRows)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  const runId = (state.selectedManifest || {}).run_id || "lead-output";
  anchor.href = url;
  anchor.download = `${runId}-${state.leadSource || "leads"}.csv`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
  setLeadCopyStatus(`Exported ${state.leadRows.length} rows`);
}

function setDiagnosticTab(tabName) {
  const next = DIAGNOSTIC_TABS.includes(tabName) ? tabName : "overview";
  state.diagnosticTab = next;
  document.querySelectorAll("[data-diagnostic-tab]").forEach((button) => {
    const active = button.dataset.diagnosticTab === next;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });
  document.querySelectorAll("[data-diagnostic-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.diagnosticPanel !== next;
  });
}

async function selectRun(apiRunId) {
  state.selectedRunId = apiRunId;
  renderRuns(state.runs);
  const encoded = encodeURIComponent(apiRunId);
  const [manifest, rejectedRows, validatedRows, leadOutput, artifacts] = await Promise.all([
    fetchJson(`/api/runs/${encoded}/manifest`, {}),
    fetchJson(`/api/runs/${encoded}/rejected`, []),
    fetchJson(`/api/runs/${encoded}/validated`, []),
    fetchJson(`/api/runs/${encoded}/leads`, { source: "final", rows: [] }),
    fetchJson(`/api/runs/${encoded}/artifacts`, {}),
  ]);
  renderSelectedManifest(manifest);
  renderArtifactLinks(artifacts);
  renderSelectedRows(rejectedRows, validatedRows);
  renderLeadOutput(leadOutput);
}

async function loadOverview() {
  const overview = await fetchJson("/api/overview", {
    total_runs: 0,
    harvested_count: 0,
    validated_count: 0,
    rejected_count: 0,
    duplicate_count: 0,
    latest_run_summary: {},
    top_reject_reasons: [],
  });
  renderOverview(overview);
}

async function loadRuns() {
  const runs = await fetchJson("/api/runs", []);
  state.runs = runs;
  const effective = getEffectiveRunStatus();
  const activeTag = effective.runTag;

  if (!state.selectedRunId && runs.length) {
    const activeMatch = activeTag
      ? runs.find((run) => run.run_id === activeTag || run.api_run_id === activeTag)
      : null;
    state.selectedRunId = activeMatch ? activeMatch.api_run_id : runs[0].api_run_id;
  }

  renderRuns(runs);
  if (state.selectedRunId) {
    await selectRun(state.selectedRunId);
  } else {
    renderSelectedManifest({});
    renderArtifactLinks({});
    renderSelectedRows([], []);
    renderLeadOutput({ source: "final", rows: [] });
  }
}

function renderLiveStatus(payload) {
  state.liveStatus = payload || {};
  renderLivePanel();
  renderActiveRunStrip();
  renderRunActionState();
  if (state.runs.length) {
    renderRuns(state.runs);
  }
}

async function refreshLiveStatus() {
  const payload = await fetchJson("/api/live-status", {
    active: false,
    status: "idle",
    stage: "idle",
    updated_at_utc: "",
    message: "No active run",
  });
  renderLiveStatus(payload);
}

function renderRunLog(payload) {
  const output = document.getElementById("run-log-output");
  const meta = document.getElementById("run-log-meta");
  const status = payload.status || {};
  const parts = [];

  state.runLogStatus = status;
  output.textContent = payload.text || "No log output yet.";
  if (status.run_root) {
    parts.push(basename(status.run_root));
  }
  if (status.log_updated_at) {
    parts.push(`Updated ${formatTimestamp(status.log_updated_at)}`);
  }
  meta.textContent = parts.join(" • ");

  renderLivePanel();
  renderActiveRunStrip();
  renderRunActionState();
}

async function loadRunConfig() {
  const payload = await fetchJson("/api/run-config", {
    config: {},
    validation_profiles: [],
    active_run: { active: false, status_label: "idle" },
  });
  applyRunConfig(payload);
}

async function refreshRunLog() {
  const payload = await fetchJson("/api/run/log", { status: {}, text: "" });
  renderRunLog(payload);
}

async function handleRunStart(event) {
  event.preventDefault();
  setError("");
  const response = await postJson("/api/run/start", getRunFormPayload(), { ok: false });
  if (!response.ok) {
    return;
  }
  await Promise.all([loadRunConfig(), refreshLiveStatus(), refreshRunLog(), loadOverview(), loadRuns()]);
}

async function handleRunStop() {
  setError("");
  const response = await postJson("/api/run/stop", {}, { ok: false });
  if (!response.ok) {
    return;
  }
  await Promise.all([loadRunConfig(), refreshLiveStatus(), refreshRunLog()]);
}

function scheduleRunConfigSave() {
  const payload = getRunFormPayload();
  renderRunPlanner(payload);
  writeRunConfigDraft(payload);
  if (runConfigSaveTimer !== null) {
    window.clearTimeout(runConfigSaveTimer);
  }
  runConfigSaveTimer = window.setTimeout(async () => {
    runConfigSaveTimer = null;
    await postJson("/api/run-config", payload, { ok: false });
  }, 250);
}

function bindDiagnosticTabs() {
  document.querySelectorAll("[data-diagnostic-tab]").forEach((button) => {
    button.addEventListener("click", () => setDiagnosticTab(button.dataset.diagnosticTab));
  });
}

function bindRunControls() {
  const form = document.getElementById("run-control-form");
  form.addEventListener("submit", handleRunStart);
  document.getElementById("run-mode-select").addEventListener("change", (event) => {
    applyModePreset(event.target.value || "scout_mode");
  });
  document.getElementById("active-strip-stop-button").addEventListener("click", handleRunStop);
  document.getElementById("lead-output-copy-button").addEventListener("click", copyLeadRows);
  document.getElementById("lead-output-export-button").addEventListener("click", exportLeadRows);

  form.querySelectorAll("input, select").forEach((element) => {
    if (element.id === "run-mode-select") {
      return;
    }
    element.addEventListener("change", scheduleRunConfigSave);
    if (element.type === "text" || element.type === "number") {
      element.addEventListener("input", scheduleRunConfigSave);
    }
  });
}

async function initialize() {
  setError("");
  bindRunControls();
  bindDiagnosticTabs();
  setDiagnosticTab(state.diagnosticTab);
  await Promise.all([loadRunConfig(), loadOverview(), loadRuns(), refreshLiveStatus(), refreshRunLog()]);
  window.setInterval(() => {
    refreshLiveStatus();
    refreshRunLog();
    loadOverview();
    loadRuns();
  }, 5000);
}

window.addEventListener("DOMContentLoaded", initialize);
