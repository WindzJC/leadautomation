const state = {
  runs: [],
  selectedRunId: "",
  leadRows: [],
};
let runConfigSaveTimer = null;
const RUN_CONFIG_STORAGE_KEY = "lead-finder.run-config";

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

const RUN_PRESETS = {
  safe_day: {
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
  aggressive_day: {
    validation_profile: "agent_hunt",
    goal_final: 250,
    max_runs: 15,
    max_stale_runs: 5,
    target: 100,
    min_candidates: 60,
    max_candidates: 60,
    batch_min: 20,
    batch_max: 20,
    listing_strict: false,
  },
};

const PROFILE_COPY = {
  agent_hunt: {
    title: "Agent Hunt",
    summary: "Volume-first scouting with public-email quality scoring. Best when you want broader outreach coverage without calling the lead strictly verified.",
    pills: ["Public email preferred", "Plausible author match", "Daily scout default"],
  },
  email_only: {
    title: "Email Only",
    summary: "Collect plausible author names with visible public emails and a preserved SourceURL. Higher yield than strict verification, but still rejects obvious junk.",
    pills: ["Name + email + source", "Lighter proof gates", "Contact collection mode"],
  },
  strict_full: {
    title: "Strict Full",
    summary: "Hard verified outbound mode. Keeps only rows that satisfy the full proof stack for quality-controlled outreach.",
    pills: ["Strict listing proof", "Strict recency", "Outbound-ready only"],
  },
  strict_interactive: {
    title: "Strict Interactive",
    summary: "Same hard acceptance rules as strict verification, but with a tighter runtime posture for interactive sessions.",
    pills: ["Smaller runtime budget", "Strict proof stack", "Interactive debugging"],
  },
  fully_verified: {
    title: "Fully Verified",
    summary: "Strict verified acceptance without the extra product presets layered on top.",
    pills: ["Full proof gates", "Verified export path", "Strict quality floor"],
  },
  verified_no_us: {
    title: "Verified No US",
    summary: "Keeps the strict proof gates but removes the U.S.-location requirement when geography is not the constraint.",
    pills: ["Strict proof gates", "No US requirement", "Verified export path"],
  },
  astra_outbound: {
    title: "Astra Outbound",
    summary: "Strict outbound preset for Astra-style runs. Higher-volume strict mode with listing and U.S. requirements preserved.",
    pills: ["Astra preset", "US required", "Strict listing gate"],
  },
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
    // Ignore local persistence failures and keep the server-backed draft path.
  }
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
        // Keep the default detail text.
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
  return fetchJson(
    url,
    fallback,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {}),
    },
  );
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

function profileCopyFor(value) {
  return PROFILE_COPY[value] || {
    title: prettifyLabel(value),
    summary: "Run mode summary is not defined for this profile yet.",
    pills: ["Operator mode"],
  };
}

function renderRunModeCard(config) {
  const profile = config.validation_profile || "agent_hunt";
  const copy = profileCopyFor(profile);
  document.getElementById("run-mode-title").textContent = copy.title;
  document.getElementById("run-mode-summary").textContent = copy.summary;
  document.getElementById("run-mode-pills").innerHTML = (copy.pills || [])
    .map((pill) => `<span class="pill">${pill}</span>`)
    .join("");
}

function renderRunSnapshot(config) {
  const goalFinal = Number(config.goal_final || 0);
  const target = Number(config.target || 0);
  const maxCandidates = Number(config.max_candidates || 0);
  const batchMin = Number(config.batch_min || 0);
  const batchMax = Number(config.batch_max || 0);
  const maxRuns = Number(config.max_runs || 0);
  const maxStaleRuns = Number(config.max_stale_runs || 0);
  const folderName = String(config.run_folder_name || "").trim();

  document.getElementById("run-session-title").textContent = goalFinal
    ? `${formatNumber(goalFinal)} row session target`
    : "Session target not set";
  document.getElementById("run-snapshot-goal").textContent = formatNumber(goalFinal);
  document.getElementById("run-snapshot-target").textContent = formatNumber(target);
  document.getElementById("run-snapshot-max-candidates").textContent = formatNumber(maxCandidates);
  document.getElementById("run-snapshot-batch").textContent = `${formatNumber(batchMin)}-${formatNumber(batchMax)}`;
  document.getElementById("run-snapshot-runs").textContent = `${formatNumber(maxRuns)} / ${formatNumber(maxStaleRuns)} stale`;
  document.getElementById("run-folder-preview").textContent = folderName
    ? `Artifacts will land in ${folderName}/outputs/csv, logs, and runs.`
    : "Artifacts will use the suggested dashboard run folder.";
}

function renderRunPlanner(config) {
  renderRunModeCard(config);
  renderRunSnapshot(config);
}

function renderOverview(overview) {
  const container = document.getElementById("overview-cards");
  container.innerHTML = overviewCards
    .map(
      ([label, key]) => `
        <article class="overview-card">
          <div class="label">${label}</div>
          <div class="value">${formatNumber(overview[key])}</div>
        </article>
      `,
    )
    .join("");
  const latest = overview.latest_run_summary || {};
  document.getElementById("latest-run-label").textContent = latest.run_id
    ? `Latest: ${latest.run_id} (${latest.context_name || "project-root"})`
    : "No runs found";
  renderReasonList(document.getElementById("top-reject-reasons"), overview.top_reject_reasons || []);
}

function renderReasonList(container, reasons) {
  if (!reasons.length) {
    container.innerHTML = '<div class="empty-state">No reject reasons available.</div>';
    return;
  }
  container.innerHTML = reasons
    .map(
      (reason) => `
        <div class="reason-row">
          <span class="reason-label">${prettifyLabel(reason.label)}</span>
          <span class="reason-count">${formatNumber(reason.count)}</span>
        </div>
      `,
    )
    .join("");
}

function renderRuns(runs) {
  const tbody = document.getElementById("runs-table-body");
  document.getElementById("run-count").textContent = `${runs.length} runs`;
  if (!runs.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="7">No run manifests found.</td></tr>';
    return;
  }
  tbody.innerHTML = runs
    .map((run) => {
      const counts = run.counts || {};
      const selected = run.api_run_id === state.selectedRunId ? "selected" : "";
      return `
        <tr data-run-id="${run.api_run_id}" class="${selected}">
          <td class="cell-primary">${run.run_id || "run"}</td>
          <td>${run.context_name || ""}</td>
          <td><span class="status-badge ${run.status || "unknown"}">${prettifyLabel(run.status || "unknown")}</span></td>
          <td>${formatNumber(counts.validated)}</td>
          <td>${formatNumber(counts.rejected)}</td>
          <td>${formatNumber(counts.duplicates)}</td>
          <td>${formatTimestamp(run.generated_at_utc)}</td>
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

  if (!manifest || !Object.keys(manifest).length) {
    title.textContent = "No run selected";
    status.textContent = "";
    summary.innerHTML = "Select a run to load details.";
    summary.classList.add("empty-state");
    funnel.innerHTML = "";
    return;
  }

  title.textContent = manifest.run_id || "Run details";
  status.textContent = `${prettifyLabel(manifest.status || "unknown")} • ${manifest.validation_profile || "default"}`;
  summary.classList.remove("empty-state");
  summary.innerHTML = `
    <div class="summary-row"><strong>Generated</strong><span>${formatTimestamp(manifest.generated_at_utc)}</span></div>
    <div class="summary-row"><strong>Target Final</strong><span>${formatNumber(manifest.target_final)}</span></div>
    <div class="summary-row"><strong>Pipeline Exit Code</strong><span>${formatNumber(manifest.pipeline_exit_code)}</span></div>
    <div class="summary-row"><strong>Stop Reason</strong><span>${prettifyLabel((manifest.loop_control || {}).stop_reason || "none")}</span></div>
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
          <div class="meta">${label}</div>
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
    container.textContent = "No downloadable artifacts available for this run.";
    container.classList.add("empty-state");
    return;
  }
  container.classList.remove("empty-state");
  container.innerHTML = entries
    .map(
      ([artifactName, payload]) => `
        <a class="artifact-link" href="${payload.download_url}" data-artifact="${artifactName}" target="_blank" rel="noreferrer">
          ${payload.label}
        </a>
      `,
    )
    .join("");
}

function renderTableBody(tbodyId, rows, columns, emptyMessage) {
  const tbody = document.getElementById(tbodyId);
  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="${columns.length}">${emptyMessage}</td></tr>`;
    return;
  }
  tbody.innerHTML = rows
    .map((row) => {
      const cells = columns
        .map((column, index) => {
          const value = row[column] || "";
          const klass = index === 0 ? "cell-primary" : "";
          return `<td class="${klass}">${value}</td>`;
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

function renderLeadOutput(payload) {
  const rows = payload.rows || [];
  const source = payload.source || "final";
  state.leadRows = rows;
  document.getElementById("lead-output-count").textContent = `${rows.length} rows`;
  document.getElementById("lead-output-source").textContent = rows.length
    ? `Showing ${prettifyLabel(source)} lead output. SourceURL is preserved in the run CSVs for auditability.`
    : "No accepted leads available for this run.";
  renderTableBody(
    "lead-output-table-body",
    rows,
    ["AuthorName", "AuthorEmail"],
    "No accepted leads for this run.",
  );
  updateLeadCopyUi();
}

function buildLeadCopyText(rows) {
  return rows
    .map((row) => `${row.AuthorName || ""},${row.AuthorEmail || ""}`)
    .join("\n");
}

function setLeadCopyStatus(message) {
  document.getElementById("lead-output-copy-status").textContent = message || "";
}

function updateLeadCopyUi() {
  const button = document.getElementById("lead-output-copy-button");
  button.disabled = !state.leadRows.length;
  if (!state.leadRows.length) {
    setLeadCopyStatus("");
  }
}

async function copyLeadRows() {
  if (!state.leadRows.length) {
    return;
  }
  const text = buildLeadCopyText(state.leadRows);
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

function applyRunConfig(configPayload) {
  const profiles = configPayload.validation_profiles || [];
  const config = {
    ...(configPayload.config || {}),
    ...readRunConfigDraft(),
  };
  const hiddenInput = document.getElementById("run-validation-profile");
  const overrideSelect = document.getElementById("run-validation-profile-override");
  const profileLabel = document.getElementById("run-validation-profile-label");
  if (!overrideSelect.options.length) {
    overrideSelect.innerHTML = profiles.map((profile) => `<option value="${profile}">${prettifyLabel(profile)}</option>`).join("");
  }
  const validationProfile = config.validation_profile || "agent_hunt";
  hiddenInput.value = validationProfile;
  overrideSelect.value = validationProfile;
  profileLabel.textContent = prettifyLabel(validationProfile);
  RUN_FORM_FIELDS.forEach((field) => {
    if (field === "validation_profile") {
      return;
    }
    const element = document.querySelector(`[name="${field}"]`);
    if (!element) {
      return;
    }
    if (element.type === "checkbox") {
      element.checked = Boolean(config[field]);
    } else if (typeof config[field] !== "undefined") {
      element.value = config[field];
    }
  });
  renderRunPlanner(config);
  writeRunConfigDraft(getRunFormPayload());
  renderRunControlStatus(configPayload.active_run || {});
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

function applyRunPreset(presetName) {
  const preset = RUN_PRESETS[presetName];
  if (!preset) {
    return;
  }
  const form = document.getElementById("run-control-form");
  Object.entries(preset).forEach(([field, value]) => {
    if (field === "validation_profile") {
      document.getElementById("run-validation-profile").value = value;
      document.getElementById("run-validation-profile-override").value = value;
      document.getElementById("run-validation-profile-label").textContent = prettifyLabel(value);
      return;
    }
    const element = form.elements.namedItem(field);
    if (!element) {
      return;
    }
    if (element.type === "checkbox") {
      element.checked = Boolean(value);
    } else {
      element.value = value;
    }
  });
  scheduleRunConfigSave();
}

function renderRunControlStatus(status) {
  const label = document.getElementById("run-control-status");
  const startButton = document.getElementById("run-start-button");
  const stopButton = document.getElementById("run-stop-button");
  const active = Boolean(status.active);
  const statusLabel = prettifyLabel(status.status_label || status.status || "idle");
  label.textContent = status.run_root ? `${statusLabel} • ${status.run_root.split("/").pop()}` : statusLabel;
  startButton.disabled = active;
  stopButton.disabled = !active;
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
  if (!state.selectedRunId && runs.length) {
    state.selectedRunId = runs[0].api_run_id;
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
  const badge = document.getElementById("live-status-badge");
  const stage = document.getElementById("live-status-stage");
  const meta = document.getElementById("live-status-meta");
  const status = String(payload.status || "idle").toLowerCase();
  badge.className = `status-badge ${status}`;
  badge.textContent = prettifyLabel(status);
  stage.textContent = prettifyLabel(payload.stage || payload.message || "idle");

  const parts = [];
  if (payload.run_tag) {
    parts.push(`Run ${payload.run_tag}`);
  }
  if (payload.batch_index) {
    parts.push(`Batch ${payload.batch_index}${payload.max_runs ? `/${payload.max_runs}` : ""}`);
  }
  if (payload.updated_at_utc) {
    parts.push(`Updated ${formatTimestamp(payload.updated_at_utc)}`);
  }
  const counts = payload.counts || {};
  if (typeof counts.validated !== "undefined" || typeof counts.harvested_candidates !== "undefined") {
    const snippets = [];
    if (typeof counts.harvested_candidates !== "undefined") {
      snippets.push(`harvested ${formatNumber(counts.harvested_candidates)}`);
    }
    if (typeof counts.validated !== "undefined") {
      snippets.push(`validated ${formatNumber(counts.validated)}`);
    }
    if (typeof counts.goal_qualified_total !== "undefined") {
      snippets.push(`goal total ${formatNumber(counts.goal_qualified_total)}`);
    }
    if (snippets.length) {
      parts.push(snippets.join(" • "));
    }
  }
  if (payload.stop_reason) {
    parts.push(`stop ${prettifyLabel(payload.stop_reason)}`);
  }
  meta.textContent = parts.join(" • ") || "No active run";
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
  output.textContent = payload.text || "No log output yet.";
  const parts = [];
  if (status.run_root) {
    parts.push(status.run_root.split("/").pop());
  }
  if (status.log_updated_at) {
    parts.push(`Updated ${formatTimestamp(status.log_updated_at)}`);
  }
  meta.textContent = parts.join(" • ");
  renderRunControlStatus(status);
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

function bindRunControls() {
  const form = document.getElementById("run-control-form");
  form.addEventListener("submit", handleRunStart);
  document.getElementById("run-stop-button").addEventListener("click", handleRunStop);
  document.getElementById("lead-output-copy-button").addEventListener("click", copyLeadRows);
  document.getElementById("preset-safe-day").addEventListener("click", () => applyRunPreset("safe_day"));
  document.getElementById("preset-aggressive-day").addEventListener("click", () => applyRunPreset("aggressive_day"));
  document.getElementById("run-validation-profile-override").addEventListener("change", (event) => {
    const value = event.target.value || "agent_hunt";
    document.getElementById("run-validation-profile").value = value;
    document.getElementById("run-validation-profile-label").textContent = prettifyLabel(value);
    scheduleRunConfigSave();
  });
  form.querySelectorAll("input, select").forEach((element) => {
    element.addEventListener("change", scheduleRunConfigSave);
    if (element.type === "text" || element.type === "number") {
      element.addEventListener("input", scheduleRunConfigSave);
    }
  });
}

async function initialize() {
  setError("");
  bindRunControls();
  await Promise.all([loadRunConfig(), loadOverview(), loadRuns(), refreshLiveStatus(), refreshRunLog()]);
  window.setInterval(() => {
    refreshLiveStatus();
    refreshRunLog();
    loadOverview();
    loadRuns();
  }, 5000);
}

window.addEventListener("DOMContentLoaded", initialize);
