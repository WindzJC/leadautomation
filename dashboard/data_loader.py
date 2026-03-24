from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_STATS_RE = re.compile(r"^run_(\d+)_stats\.json$")

VERIFIED_EXPORT_COLUMNS = ["AuthorName", "AuthorEmail", "SourceURL"]
SCOUTED_EXPORT_COLUMNS = ["AuthorName", "AuthorEmail", "SourceURL"]
EMAIL_ONLY_EXPORT_COLUMNS = ["AuthorName", "BookTitle", "AuthorEmail", "EmailSourceURL"]


@dataclass(frozen=True)
class RunContext:
    name: str
    root_path: Path
    runs_path: Path
    modified_ts: float

    @property
    def label(self) -> str:
        return self.name


def resolve_runs_path(root: Path) -> Path | None:
    candidates = [
        root / "runs",
        root / "outputs" / "runs",
        root / "outputs" / "logs" / "runs",
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return None


def _latest_mtime(path: Path) -> float:
    latest = path.stat().st_mtime
    for child in path.rglob("*"):
        try:
            latest = max(latest, child.stat().st_mtime)
        except OSError:
            continue
    return latest


def discover_run_contexts(base_dir: Path | None = None) -> list[RunContext]:
    root = (base_dir or PROJECT_ROOT).resolve()
    candidates: list[RunContext] = []
    search_roots = [root]
    search_roots.extend(
        child.resolve()
        for child in root.iterdir()
        if child.is_dir() and not child.name.startswith(".")
    )
    seen: set[Path] = set()
    for context_root in search_roots:
        runs_dir = resolve_runs_path(context_root)
        if context_root in seen or runs_dir is None:
            continue
        seen.add(context_root)
        name = "project-root" if context_root == root else context_root.name
        candidates.append(
            RunContext(
                name=name,
                root_path=context_root,
                runs_path=runs_dir,
                modified_ts=_latest_mtime(runs_dir),
            )
        )
    candidates.sort(key=lambda item: (-item.modified_ts, item.name.lower()))
    return candidates


def resolve_context(path_value: str | None = None) -> RunContext | None:
    if not path_value:
        contexts = discover_run_contexts(PROJECT_ROOT)
        return contexts[0] if contexts else None
    root_path = Path(path_value).expanduser().resolve()
    runs_path = resolve_runs_path(root_path)
    if runs_path is None:
        return None
    return RunContext(
        name=root_path.name or "project-root",
        root_path=root_path,
        runs_path=runs_path,
        modified_ts=_latest_mtime(runs_path),
    )


def list_run_tags(runs_path: Path) -> list[str]:
    tags: list[str] = []
    for child in runs_path.iterdir():
        match = RUN_STATS_RE.match(child.name)
        if match:
            tags.append(match.group(1))
    return sorted(tags, key=int)


def latest_run_tag(runs_path: Path) -> str | None:
    tags = list_run_tags(runs_path)
    return tags[-1] if tags else None


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def read_jsonl_file(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    except OSError:
        return []
    return rows


def read_csv_with_header(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]
    except OSError:
        return []


def read_csv_without_header(path: Path, columns: Iterable[str]) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    fieldnames = list(columns)
    rows: list[dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            for raw_row in reader:
                if not raw_row:
                    continue
                padded = list(raw_row[: len(fieldnames)])
                while len(padded) < len(fieldnames):
                    padded.append("")
                rows.append(dict(zip(fieldnames, padded)))
    except OSError:
        return []
    return rows


def read_lead_export(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    rows: list[dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            for raw_row in reader:
                if not raw_row:
                    continue
                if len(raw_row) >= 4:
                    author, _book, email, source_url = raw_row[:4]
                else:
                    padded = list(raw_row[:3])
                    while len(padded) < 3:
                        padded.append("")
                    author, email, source_url = padded
                rows.append(
                    {
                        "AuthorName": author,
                        "AuthorEmail": email,
                        "SourceURL": source_url,
                    }
                )
    except OSError:
        return []
    return rows


def count_nonempty_lines(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    except OSError:
        return 0


def resolve_csv_path(root: Path, name: str) -> Path:
    preferred = root / "outputs" / "csv" / name
    legacy = root / name
    return preferred if preferred.exists() or not legacy.exists() else legacy


def resolve_json_path(root: Path, name: str) -> Path:
    preferred = root / "outputs" / "json" / name
    legacy = root / name
    return preferred if preferred.exists() or not legacy.exists() else legacy


def context_output_paths(context: RunContext) -> dict[str, Path]:
    lead_export_path = resolve_csv_path(context.root_path, "author_email_source.csv")
    if not lead_export_path.is_file():
        lead_export_path = resolve_csv_path(context.root_path, "author_email_book.csv")
    return {
        "fully_verified_leads": resolve_csv_path(context.root_path, "fully_verified_leads.csv"),
        "scouted_leads": resolve_csv_path(context.root_path, "scouted_leads.csv"),
        "contact_queue": resolve_csv_path(context.root_path, "contact_queue.csv"),
        "near_miss_location": resolve_csv_path(context.root_path, "near_miss_location.csv"),
        "lead_export": lead_export_path,
    }


def load_context_outputs(context: RunContext) -> dict[str, Any]:
    paths = context_output_paths(context)
    return {
        "paths": paths,
        "fully_verified_leads": read_lead_export(paths["fully_verified_leads"]),
        "scouted_leads": read_lead_export(paths["scouted_leads"]),
        "contact_queue": read_csv_with_header(paths["contact_queue"]),
        "near_miss_location": read_csv_with_header(paths["near_miss_location"]),
        "lead_export_rows": count_nonempty_lines(paths["lead_export"]),
    }


def run_file_map(context: RunContext, run_tag: str) -> dict[str, Path]:
    runs = context.runs_path
    return {
        "stats": runs / f"run_{run_tag}_stats.json",
        "validate_stats": runs / f"run_{run_tag}_validate_stats.json",
        "run_manifest": resolve_json_path(context.root_path, f"run_manifest_run_{run_tag}.json"),
        "listing_debug": runs / f"run_{run_tag}_validate_stats_listing_debug.jsonl",
        "location_debug": runs / f"run_{run_tag}_validate_stats_location_debug.jsonl",
        "validated": runs / f"run_{run_tag}_validated.csv",
        "validated_export": resolve_csv_path(context.root_path, f"validated_run_{run_tag}.csv"),
        "rejected_export": resolve_csv_path(context.root_path, f"rejected_run_{run_tag}.csv"),
        "final": runs / f"run_{run_tag}_final.csv",
        "new_leads": runs / f"run_{run_tag}_new_leads.csv",
        "pipeline_stdout": runs / f"run_{run_tag}_pipeline.stdout.log",
        "pipeline_stderr": runs / f"run_{run_tag}_pipeline.stderr.log",
    }


def load_run_bundle(context: RunContext, run_tag: str) -> dict[str, Any]:
    paths = run_file_map(context, run_tag)
    return {
        "paths": paths,
        "stats": read_json_file(paths["stats"]),
        "validate_stats": read_json_file(paths["validate_stats"]),
        "run_manifest": read_json_file(paths["run_manifest"]),
        "listing_debug": read_jsonl_file(paths["listing_debug"]),
        "location_debug": read_jsonl_file(paths["location_debug"]),
        "validated_rows": read_csv_with_header(paths["validated"]),
        "validated_export_rows": read_csv_with_header(paths["validated_export"]),
        "rejected_rows": read_csv_with_header(paths["rejected_export"]),
        "final_rows": read_csv_with_header(paths["final"]),
        "new_leads_rows": read_lead_export(paths["new_leads"]),
    }


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def build_funnel_rows(run_bundle: dict[str, Any]) -> list[dict[str, Any]]:
    manifest_counts = dict((run_bundle.get("run_manifest", {}) or {}).get("counts", {}) or {})
    run_counts = dict((run_bundle.get("stats", {}) or {}).get("counts", {}) or {})
    validate_stats = dict(run_bundle.get("validate_stats", {}) or {})

    stage_counts = [
        ("Harvested", _coerce_int(manifest_counts.get("harvested_candidates", run_counts.get("harvested_candidates", 0)))),
        ("Filtered", _coerce_int(manifest_counts.get("filtered_candidates", run_counts.get("candidates", 0)))),
        ("Validated", _coerce_int(manifest_counts.get("validated", run_counts.get("validated", validate_stats.get("kept_rows", 0))))),
        ("Final", _coerce_int(manifest_counts.get("final", run_counts.get("final", len(run_bundle.get("final_rows", [])))))),
        ("Kept After Gate", _coerce_int(manifest_counts.get("kept_rows_after_gate", run_counts.get("kept_rows_after_gate", 0)))),
        ("Added To Master", _coerce_int(manifest_counts.get("added_to_master", run_counts.get("added_to_master", 0)))),
    ]

    rows: list[dict[str, Any]] = []
    previous = 0
    for label, count in stage_counts:
        if count <= 0 and rows:
            continue
        conversion = round((count / previous) * 100.0, 1) if previous > 0 else 100.0
        rows.append({"stage": label, "count": count, "conversion_pct": conversion})
        previous = count if count > 0 else previous
    return rows


def summarize_counter(counter: dict[str, Any] | None, *, limit: int = 10) -> list[dict[str, Any]]:
    if not counter:
        return []
    items = []
    for key, value in counter.items():
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            continue
        items.append({"label": str(key), "count": numeric})
    items.sort(key=lambda item: (-item["count"], item["label"]))
    return items[:limit]


def build_candidate_index(run_bundle: dict[str, Any]) -> list[dict[str, Any]]:
    validated_by_url = {
        str(row.get("SourceURL") or row.get("AuthorWebsite") or row.get("ContactURL") or row.get("ListingURL") or ""): row
        for row in run_bundle.get("validated_rows", [])
    }
    listing_by_candidate = {
        str(row.get("candidate_url") or ""): row for row in run_bundle.get("listing_debug", []) if row.get("candidate_url")
    }
    location_by_candidate = {
        str(row.get("candidate_url") or ""): row for row in run_bundle.get("location_debug", []) if row.get("candidate_url")
    }
    all_keys = set(validated_by_url) | set(listing_by_candidate) | set(location_by_candidate)
    combined: list[dict[str, Any]] = []
    for candidate_url in sorted(key for key in all_keys if key):
        validated_row = validated_by_url.get(candidate_url, {})
        listing_row = listing_by_candidate.get(candidate_url, {})
        location_row = location_by_candidate.get(candidate_url, {})
        combined.append(
            {
                "candidate_url": candidate_url,
                "candidate_domain": (
                    listing_row.get("candidate_domain")
                    or location_row.get("candidate_domain")
                    or ""
                ),
                "validated_row": validated_row,
                "listing_debug": listing_row,
                "location_debug": location_row,
            }
        )
    return combined


def filter_rows(rows: list[dict[str, Any]], query: str, *, fields: Iterable[str] | None = None) -> list[dict[str, Any]]:
    if not query.strip():
        return rows
    lowered = query.strip().lower()
    selected_fields = list(fields or [])
    filtered: list[dict[str, Any]] = []
    for row in rows:
        haystacks = []
        if selected_fields:
            haystacks.extend(str(row.get(field, "") or "") for field in selected_fields)
        else:
            haystacks.extend(str(value or "") for value in row.values())
        if lowered in " ".join(haystacks).lower():
            filtered.append(row)
    return filtered
