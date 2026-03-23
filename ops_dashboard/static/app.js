const state = {
  runs: [],
  selectedRunId: "",
};
let runConfigSaveTimer = null;

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
  document.getElementById("lead-output-count").textContent = `${rows.length} rows`;
  document.getElementById("lead-output-source").textContent = rows.length
    ? `Showing ${prettifyLabel(source)} lead output`
    : "No accepted leads available for this run.";
  renderTableBody(
    "lead-output-table-body",
    rows,
    ["AuthorName", "AuthorEmail", "SourceURL"],
    "No accepted leads for this run.",
  );
}

function applyRunConfig(configPayload) {
  const profiles = configPayload.validation_profiles || [];
  const config = configPayload.config || {};
  const select = document.getElementById("run-validation-profile");
  if (!select.options.length) {
    select.innerHTML = profiles.map((profile) => `<option value="${profile}">${prettifyLabel(profile)}</option>`).join("");
  }
  RUN_FORM_FIELDS.forEach((field) => {
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
  if (runConfigSaveTimer !== null) {
    window.clearTimeout(runConfigSaveTimer);
  }
  runConfigSaveTimer = window.setTimeout(async () => {
    runConfigSaveTimer = null;
    await postJson("/api/run-config", getRunFormPayload(), { ok: false });
  }, 250);
}

function bindRunControls() {
  const form = document.getElementById("run-control-form");
  form.addEventListener("submit", handleRunStart);
  document.getElementById("run-stop-button").addEventListener("click", handleRunStop);
  form.querySelectorAll("input, select").forEach((element) => {
    const eventName = element.type === "text" || element.type === "number" ? "change" : "change";
    element.addEventListener(eventName, scheduleRunConfigSave);
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
