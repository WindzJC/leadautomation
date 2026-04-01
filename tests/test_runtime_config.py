from __future__ import annotations

import sys
from pathlib import Path

import run_lead_finder
from run_lead_finder import parse_args as parse_single_run_args
from run_lead_finder_loop import parse_args as parse_loop_args
from runtime_config import DEFAULT_CONFIG_PATH, allowed_year_bounds, load_runtime_config


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_load_runtime_config_and_allowed_year_bounds(tmp_path: Path) -> None:
    config_path = tmp_path / "default.yaml"
    _write(
        config_path,
        "\n".join(
            [
                "validation_profile: strict_full",
                "goal_final: 25",
                "allowed_years: 2",
                "listing_strict: true",
            ]
        ),
    )

    config = load_runtime_config(config_path)

    assert config["goal_final"] == 25
    assert config["listing_strict"] is True
    assert allowed_year_bounds(config, now_year=2026) == (2024, 2026)


def test_default_config_path_is_repo_relative() -> None:
    assert DEFAULT_CONFIG_PATH.is_absolute()
    assert DEFAULT_CONFIG_PATH.is_file()


def test_loop_args_use_runtime_config_defaults_and_allow_cli_override(tmp_path: Path) -> None:
    config_path = tmp_path / "default.yaml"
    _write(
        config_path,
        "\n".join(
            [
                "validation_profile: strict_full",
                "goal_final: 25",
                "max_runs: 3",
                "max_stale_runs: 2",
                "target: 90",
                "min_candidates: 70",
                "max_candidates: 60",
                "batch_min: 18",
                "batch_max: 22",
                "merge_policy: balanced",
                "listing_strict: true",
                "allowed_years: 2",
            ]
        ),
    )

    args = parse_loop_args(["--config", str(config_path)])

    assert args.validation_profile == "strict_full"
    assert args.goal_final == 25
    assert args.max_runs == 3
    assert args.max_stale_runs == 2
    assert args.target == 90
    assert args.min_candidates == 70
    assert args.max_candidates == 60
    assert args.batch_min == 18
    assert args.batch_max == 22
    assert args.merge_policy == "balanced"
    assert args.listing_strict is True
    assert (args.min_year, args.max_year) == (2024, 2026)

    overridden = parse_loop_args(["--config", str(config_path), "--goal-final", "8", "--merge-policy", "strict"])
    assert overridden.goal_final == 8
    assert overridden.merge_policy == "strict"


def test_single_run_args_use_runtime_config_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "default.yaml"
    _write(
        config_path,
        "\n".join(
            [
                "validation_profile: strict_full",
                "target: 75",
                "min_candidates: 65",
                "max_candidates: 55",
                "batch_min: 15",
                "batch_max: 19",
                "listing_strict: true",
                "allowed_years: 3",
            ]
        ),
    )

    args = parse_single_run_args(["--config", str(config_path)])

    assert args.validation_profile == "strict_full"
    assert args.target == 75
    assert args.min_candidates == 65
    assert args.max_candidates == 55
    assert args.min_final == 15
    assert args.max_final == 19
    assert args.listing_strict is True
    assert (args.min_year, args.max_year) == (2023, 2026)


def test_single_run_main_uses_repo_relative_stage_scripts(tmp_path: Path, monkeypatch) -> None:
    commands: list[list[str]] = []
    candidates_path = tmp_path / "candidates.csv"
    validated_path = tmp_path / "validated.csv"
    final_path = tmp_path / "final.csv"
    final_path.write_text("", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_lead_finder.py",
            "--candidates",
            str(candidates_path),
            "--validated",
            str(validated_path),
            "--final",
            str(final_path),
        ],
    )
    monkeypatch.setattr(run_lead_finder, "run_command", lambda cmd: commands.append(cmd))

    assert run_lead_finder.main() == 0
    assert [Path(cmd[1]).name for cmd in commands] == [
        "prospect_harvest.py",
        "prospect_validate.py",
        "prospect_dedupe.py",
    ]
    assert all(Path(cmd[1]).is_absolute() for cmd in commands)
