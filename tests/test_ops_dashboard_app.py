from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from ops_dashboard.app import app


def test_run_config_endpoint_returns_defaults_and_profiles(monkeypatch) -> None:
    monkeypatch.setattr("ops_dashboard.app.runner.get_run_status", lambda: {"active": False, "status_label": "idle"})
    monkeypatch.setattr(
        "ops_dashboard.app.runner.load_run_form_state",
        lambda: {"validation_profile": "agent_hunt", "goal_final": 15, "run_folder_name": "browser_run_custom"},
    )
    monkeypatch.setattr("ops_dashboard.app.runner.suggested_run_folder_name", lambda: "browser_run_default")

    client = TestClient(app)
    response = client.get("/api/run-config")

    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["validation_profile"] == "agent_hunt"
    assert payload["config"]["goal_final"] == 15
    assert payload["config"]["run_folder_name"] == "browser_run_custom"
    assert "strict_full" in payload["validation_profiles"]


def test_run_config_post_persists_draft(monkeypatch) -> None:
    saved: dict[str, object] = {}

    def fake_save(payload):
        saved.update(payload)

    monkeypatch.setattr("ops_dashboard.app.runner.save_run_form_state", fake_save)
    monkeypatch.setattr("ops_dashboard.app.runner.suggested_run_folder_name", lambda: "browser_run_default")

    client = TestClient(app)
    response = client.post(
        "/api/run-config",
        json={
            "validation_profile": "strict_full",
            "goal_final": "18",
            "max_runs": "2",
            "max_stale_runs": "1",
            "target": "50",
            "min_candidates": "30",
            "max_candidates": "40",
            "batch_min": "10",
            "batch_max": "20",
            "listing_strict": False,
            "queries_file": "queries.txt",
            "run_folder_name": "browser_run_saved",
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert saved["goal_final"] == 18
    assert saved["run_folder_name"] == "browser_run_saved"


def test_run_start_endpoint_normalizes_and_starts(monkeypatch) -> None:
    saved: dict[str, object] = {}
    started: dict[str, object] = {}

    def fake_save(payload):
        saved.update(payload)

    def fake_start(payload):
        started.update(payload)
        return {"pid": 123}

    monkeypatch.setattr("ops_dashboard.app.runner.save_run_form_state", fake_save)
    monkeypatch.setattr("ops_dashboard.app.runner.start_run", fake_start)
    monkeypatch.setattr(
        "ops_dashboard.app.runner.get_run_status",
        lambda: {"active": True, "status_label": "running", "pid": 123, "run_root": "/tmp/browser_run_demo"},
    )
    monkeypatch.setattr("ops_dashboard.app.runner.suggested_run_folder_name", lambda: "browser_run_default")

    client = TestClient(app)
    response = client.post(
        "/api/run/start",
        json={
            "validation_profile": "strict_full",
            "goal_final": "12",
            "max_runs": "2",
            "max_stale_runs": "1",
            "target": "40",
            "min_candidates": "20",
            "max_candidates": "30",
            "batch_min": "10",
            "batch_max": "15",
            "listing_strict": "true",
        },
    )

    assert response.status_code == 200
    assert saved["goal_final"] == 12
    assert started["max_runs"] == 2
    assert started["listing_strict"] is True
    assert started["run_folder_name"] == "browser_run_default"
    assert response.json()["status"]["status_label"] == "running"


def test_run_start_endpoint_returns_conflict_for_active_run(monkeypatch) -> None:
    monkeypatch.setattr("ops_dashboard.app.runner.save_run_form_state", lambda payload: None)
    monkeypatch.setattr(
        "ops_dashboard.app.runner.start_run",
        lambda payload: (_ for _ in ()).throw(RuntimeError("A dashboard-started run is already active.")),
    )

    client = TestClient(app)
    response = client.post("/api/run/start", json={"validation_profile": "strict_full"})

    assert response.status_code == 409
    assert response.json()["detail"] == "A dashboard-started run is already active."


def test_run_stop_and_log_endpoints(monkeypatch) -> None:
    monkeypatch.setattr(
        "ops_dashboard.app.runner.stop_run",
        lambda: {"active": False, "status_label": "stopped", "run_root": "/tmp/browser_run_demo"},
    )
    monkeypatch.setattr(
        "ops_dashboard.app.runner.get_run_status",
        lambda: {"active": False, "status_label": "stopped", "log_path": "/tmp/demo.log", "run_root": "/tmp/browser_run_demo"},
    )
    monkeypatch.setattr("ops_dashboard.app.runner.tail_log", lambda path: "line 1\nline 2")

    client = TestClient(app)

    stop_response = client.post("/api/run/stop")
    assert stop_response.status_code == 200
    assert stop_response.json()["status"]["status_label"] == "stopped"

    log_response = client.get("/api/run/log")
    assert log_response.status_code == 200
    assert log_response.json()["text"] == "line 1\nline 2"


def test_run_config_ignores_stale_external_runner_state(monkeypatch, tmp_path) -> None:
    external_root = tmp_path / "external_run"
    external_root.mkdir()
    monkeypatch.setattr(
        "ops_dashboard.app.runner.get_run_status",
        lambda: {
            "active": False,
            "status_label": "idle",
            "exit_code": None,
            "finished_at": "",
            "run_root": str(external_root),
            "log_path": str(external_root / "outputs" / "logs" / "dashboard_run.log"),
        },
    )
    monkeypatch.setattr("ops_dashboard.app.runner.load_run_form_state", lambda: {})

    client = TestClient(app)
    response = client.get("/api/run-config")

    assert response.status_code == 200
    assert response.json()["active_run"] == {"active": False, "status_label": "idle"}


def test_run_leads_and_artifact_endpoints(monkeypatch, tmp_path: Path) -> None:
    artifact_path = tmp_path / "run_001_final.csv"
    artifact_path.write_text("AuthorName\nJane Doe\n", encoding="utf-8")
    monkeypatch.setattr(
        "ops_dashboard.app.load_lead_output",
        lambda api_run_id: {"source": "final", "rows": [{"AuthorName": "Jane Doe", "BookTitle": "Skyfall"}]},
    )
    monkeypatch.setattr(
        "ops_dashboard.app.load_run_artifact_catalog",
        lambda api_run_id: {
            "manifest": {"label": "Manifest JSON", "filename": "run_manifest_run_001.json", "available": True},
            "final": {"label": "Final Leads CSV", "filename": "run_001_final.csv", "available": True},
        },
    )
    monkeypatch.setattr("ops_dashboard.app.load_run_artifact_path", lambda api_run_id, artifact_name: artifact_path)

    client = TestClient(app)

    leads_response = client.get("/api/runs/project-root~001/leads")
    assert leads_response.status_code == 200
    assert leads_response.json()["rows"][0]["AuthorName"] == "Jane Doe"

    artifacts_response = client.get("/api/runs/project-root~001/artifacts")
    assert artifacts_response.status_code == 200
    artifacts_payload = artifacts_response.json()
    assert artifacts_payload["final"]["download_url"].endswith("/api/runs/project-root~001/download/final")

    download_response = client.get("/api/runs/project-root~001/download/final")
    assert download_response.status_code == 200
    assert download_response.headers["content-type"].startswith("text/csv")
    assert "Jane Doe" in download_response.text


def test_run_artifact_download_returns_404_for_missing_artifact(monkeypatch) -> None:
    monkeypatch.setattr(
        "ops_dashboard.app.load_run_artifact_path",
        lambda api_run_id, artifact_name: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )

    client = TestClient(app)
    response = client.get("/api/runs/project-root~001/download/final")

    assert response.status_code == 404
    assert response.json()["detail"] == "artifact missing"
