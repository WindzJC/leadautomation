from __future__ import annotations

import json
import sys
from pathlib import Path

from dashboard import runner
from dashboard.views.run_pipeline import (
    FORM_SESSION_KEYS,
    apply_profile_preset_to_session,
    merged_form_defaults,
    status_snapshot_changed,
    sync_run_form_state,
)


def test_build_run_paths_uses_isolated_run_root(tmp_path: Path) -> None:
    paths = runner.build_run_paths(tmp_path / "browser_run_20260312_120000")

    assert paths["master_output"].name == "leads_full.csv"
    assert paths["scouted_output"].name == "scouted_leads.csv"
    assert paths["child_pid_path"].name == "dashboard_run.child.pid"
    assert paths["runs_dir"].name == "runs"
    assert paths["log_path"].name == "dashboard_run.log"


def test_build_run_command_includes_selected_profile_and_output_paths(tmp_path: Path, monkeypatch) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)

    run_root = fake_root / "browser_run_test"
    config = {
        "validation_profile": "strict_full",
        "listing_strict": True,
        "goal_final": 20,
        "max_runs": 1,
        "max_stale_runs": 1,
        "target": 80,
        "min_candidates": 80,
        "max_candidates": 80,
        "batch_min": 20,
        "batch_max": 20,
        "queries_file": "",
    }

    command = runner.build_run_command(config, run_root)

    assert command[0] == sys.executable
    assert str(fake_root / "run_lead_finder_loop.py") in command
    assert "--validation-profile" in command
    assert "strict_full" in command
    assert "--listing-strict" in command
    assert str(run_root / "outputs" / "csv" / "scouted_leads.csv") in command
    assert str(run_root / "outputs" / "csv" / "fully_verified_leads.csv") in command
    assert str(run_root / "outputs" / "runs") in command


def test_tail_log_returns_last_lines(tmp_path: Path) -> None:
    log_path = tmp_path / "dashboard_run.log"
    log_path.write_text("1\n2\n3\n4\n", encoding="utf-8")

    assert runner.tail_log(log_path, max_lines=2) == "3\n4"


def test_save_and_load_run_form_state_round_trip(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    state_dir = fake_root / ".dashboard_state"
    state_dir.mkdir()
    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(runner, "STATE_DIR", state_dir)
    monkeypatch.setattr(runner, "RUN_FORM_STATE_PATH", state_dir / "run_form_state.json")

    payload = {"validation_profile": "agent_hunt", "target": 60, "run_folder_name": "leads 1"}
    runner.save_run_form_state(payload)

    assert runner.load_run_form_state() == payload


def test_get_run_status_reads_finished_exit_code(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    state_dir = fake_root / ".dashboard_state"
    state_dir.mkdir()
    state_path = state_dir / "runner_state.json"
    run_root = fake_root / "browser_run_test"
    paths = runner.build_run_paths(run_root)
    run_root.mkdir()
    paths["log_path"].parent.mkdir(parents=True, exist_ok=True)
    paths["exit_code_path"].write_text("0", encoding="utf-8")
    paths["finished_at_path"].write_text("2026-03-12T00:00:00+00:00", encoding="utf-8")
    paths["log_path"].write_text("done\n", encoding="utf-8")
    state_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "run_root": str(run_root),
                "log_path": str(paths["log_path"]),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(runner, "STATE_PATH", state_path)

    status = runner.get_run_status()

    assert status["active"] is False
    assert status["status_label"] == "finished"
    assert status["exit_code"] == 0
    assert status["finished_at"] == "2026-03-12T00:00:00+00:00"


def test_get_run_status_marks_failure_from_exit_code(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    state_dir = fake_root / ".dashboard_state"
    state_dir.mkdir()
    state_path = state_dir / "runner_state.json"
    run_root = fake_root / "browser_run_test"
    paths = runner.build_run_paths(run_root)
    run_root.mkdir()
    paths["log_path"].parent.mkdir(parents=True, exist_ok=True)
    paths["exit_code_path"].write_text("2", encoding="utf-8")
    state_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "run_root": str(run_root),
                "log_path": str(paths["log_path"]),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(runner, "STATE_PATH", state_path)

    status = runner.get_run_status()

    assert status["active"] is False
    assert status["status_label"] == "failed"


def test_merged_form_defaults_applies_agent_hunt_preset() -> None:
    defaults = merged_form_defaults({"validation_profile": "agent_hunt"})

    assert defaults["validation_profile"] == "agent_hunt"
    assert defaults["listing_strict"] is False
    assert defaults["target"] == 60
    assert defaults["min_candidates"] == 40
    assert defaults["max_candidates"] == 40


def test_sync_run_form_state_keeps_existing_draft_values() -> None:
    session_state = {
        FORM_SESSION_KEYS["validation_profile"]: "agent_hunt",
        FORM_SESSION_KEYS["target"]: 40,
    }

    sync_run_form_state(
        session_state,
        status_config={"validation_profile": "strict_full", "target": 80},
        run_folder_name="browser_run_test",
    )

    assert session_state[FORM_SESSION_KEYS["validation_profile"]] == "agent_hunt"
    assert session_state[FORM_SESSION_KEYS["target"]] == 40
    assert session_state[FORM_SESSION_KEYS["run_folder_name"]] == "browser_run_test"


def test_apply_profile_preset_to_session_updates_run_fields() -> None:
    session_state = {}

    apply_profile_preset_to_session(session_state, "agent_hunt")

    assert session_state[FORM_SESSION_KEYS["validation_profile"]] == "agent_hunt"
    assert session_state[FORM_SESSION_KEYS["listing_strict"]] is False
    assert session_state[FORM_SESSION_KEYS["target"]] == 60
    assert session_state[FORM_SESSION_KEYS["min_candidates"]] == 40


def test_apply_profile_preset_to_session_preserves_current_profile_widget_value() -> None:
    session_state = {
        FORM_SESSION_KEYS["validation_profile"]: "agent_hunt",
        FORM_SESSION_KEYS["target"]: 80,
        FORM_SESSION_KEYS["min_candidates"]: 80,
        FORM_SESSION_KEYS["max_candidates"]: 80,
        FORM_SESSION_KEYS["listing_strict"]: True,
    }

    apply_profile_preset_to_session(session_state, "agent_hunt")

    assert session_state[FORM_SESSION_KEYS["validation_profile"]] == "agent_hunt"
    assert session_state[FORM_SESSION_KEYS["target"]] == 60
    assert session_state[FORM_SESSION_KEYS["min_candidates"]] == 40
    assert session_state[FORM_SESSION_KEYS["max_candidates"]] == 40
    assert session_state[FORM_SESSION_KEYS["listing_strict"]] is False


def test_status_snapshot_changed_only_for_status_fields() -> None:
    previous = {
        "status_label": "running",
        "active": True,
        "pid": 100,
        "exit_code": None,
        "finished_at": "",
        "log_updated_at": "t1",
    }
    current = {
        "status_label": "finished",
        "active": False,
        "pid": 100,
        "exit_code": 0,
        "finished_at": "2026-03-12T10:00:00+00:00",
        "log_updated_at": "t2",
    }

    assert status_snapshot_changed(previous, current) is True
    assert status_snapshot_changed(previous, {**previous, "log_updated_at": "t3"}) is False


def test_get_run_status_prefers_child_pid(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    state_dir = fake_root / ".dashboard_state"
    state_dir.mkdir()
    state_path = state_dir / "runner_state.json"
    run_root = fake_root / "browser_run_test"
    paths = runner.build_run_paths(run_root)
    run_root.mkdir()
    paths["log_path"].parent.mkdir(parents=True, exist_ok=True)
    paths["child_pid_path"].write_text("12345", encoding="utf-8")
    state_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "run_root": str(run_root),
                "log_path": str(paths["log_path"]),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(runner, "STATE_PATH", state_path)
    monkeypatch.setattr(runner, "process_is_running", lambda pid: pid == 12345)

    status = runner.get_run_status()

    assert status["active"] is True
    assert status["pid"] == 12345
    assert status["child_pid"] == 12345
    assert status["launcher_pid"] == 999999


def test_stop_run_kills_child_process_group_before_launcher(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    state_dir = fake_root / ".dashboard_state"
    state_dir.mkdir()
    state_path = state_dir / "runner_state.json"
    run_root = fake_root / "browser_run_test"
    paths = runner.build_run_paths(run_root)
    run_root.mkdir()
    paths["log_path"].parent.mkdir(parents=True, exist_ok=True)
    paths["child_pid_path"].write_text("12345", encoding="utf-8")
    state_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "run_root": str(run_root),
                "log_path": str(paths["log_path"]),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(runner, "STATE_PATH", state_path)
    monkeypatch.setattr(runner, "process_is_running", lambda pid: pid in {12345, 999999})
    killed: list[tuple[str, int]] = []
    monkeypatch.setattr(runner, "_kill_process_group", lambda pid: killed.append(("group", pid)))
    monkeypatch.setattr(runner, "_kill_pid", lambda pid: killed.append(("pid", pid)))

    state = runner.stop_run()

    assert ("group", 12345) in killed
    assert ("pid", 999999) in killed
    assert "stopped_at" in state


def test_process_is_running_treats_zombie_as_inactive(monkeypatch, tmp_path: Path) -> None:
    proc_root = tmp_path / "proc" / "2222"
    proc_root.mkdir(parents=True)
    stat_path = proc_root / "stat"
    stat_path.write_text("2222 (python) Z 1 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 1 1 0", encoding="utf-8")

    original_path = runner.Path

    class FakePath(type(original_path())):
        pass

    def fake_path(value):
        if value == "/proc/2222/stat":
            return original_path(stat_path)
        return original_path(value)

    monkeypatch.setattr(runner, "Path", fake_path)
    monkeypatch.setattr(runner.os, "kill", lambda pid, sig: None)

    assert runner.process_is_running(2222) is False


def test_start_run_uses_resolved_status_not_stale_launcher_pid(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_root.mkdir()
    state_dir = fake_root / "state"
    state_dir.mkdir()
    monkeypatch.setattr(runner, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(runner, "STATE_DIR", state_dir)
    monkeypatch.setattr(runner, "STATE_PATH", state_dir / "runner_state.json")
    monkeypatch.setattr(
        runner,
        "get_run_status",
        lambda: {
            "pid": 999999,
            "launcher_pid": 999999,
            "child_pid": 12345,
            "active": False,
            "run_root": "",
        },
    )

    class DummyProcess:
        pid = 5555

    monkeypatch.setattr(runner.subprocess, "Popen", lambda *args, **kwargs: DummyProcess())

    state = runner.start_run(
        {
            "validation_profile": "strict_full",
            "listing_strict": True,
            "goal_final": 20,
            "max_runs": 1,
            "max_stale_runs": 1,
            "target": 80,
            "min_candidates": 80,
            "max_candidates": 80,
            "batch_min": 20,
            "batch_max": 20,
            "queries_file": "",
            "run_folder_name": "browser_run_test",
        }
    )

    assert state["pid"] == 5555
