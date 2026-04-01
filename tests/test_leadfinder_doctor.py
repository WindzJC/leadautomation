from __future__ import annotations

import subprocess
from pathlib import Path

import leadfinder_doctor


def test_collect_status_reports_git_venv_and_env_presence(tmp_path: Path, monkeypatch) -> None:
    fake_root = tmp_path / "repo"
    (fake_root / ".venv" / "bin").mkdir(parents=True)
    (fake_root / ".venv" / "bin" / "python").write_text("", encoding="utf-8")
    env_path = fake_root / ".env.local"
    env_path.write_text("GOOGLE_API_KEY=test-google-key\n", encoding="utf-8")

    def fake_repo_path(*parts: str) -> Path:
        return fake_root.joinpath(*parts)

    def fake_run(cmd, cwd, check, capture_output, text):  # noqa: ANN001
        joined = " ".join(cmd)
        if joined == "git rev-parse --abbrev-ref HEAD":
            return subprocess.CompletedProcess(cmd, 0, stdout="main\n", stderr="")
        if joined == "git log -1 --oneline":
            return subprocess.CompletedProcess(cmd, 0, stdout="abc123 Add doctor\n", stderr="")
        if joined == "git status --porcelain":
            return subprocess.CompletedProcess(cmd, 0, stdout=" M README.md\n", stderr="")
        raise AssertionError(joined)

    monkeypatch.setattr(leadfinder_doctor, "repo_path", fake_repo_path)
    monkeypatch.setattr(leadfinder_doctor, "LOCAL_ENV_PATH", env_path)
    monkeypatch.setattr(leadfinder_doctor, "load_local_env", lambda: monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key"))
    monkeypatch.setattr(leadfinder_doctor.subprocess, "run", fake_run)
    monkeypatch.delenv("GOOGLE_CSE_CX", raising=False)

    status = leadfinder_doctor.collect_status()

    assert status["branch"] == "main"
    assert status["latest_commit"] == "abc123 Add doctor"
    assert status["repo_dirty"] is True
    assert status["venv_exists"] is True
    assert status["local_env_exists"] is True
    assert status["env_present"] == {
        "GOOGLE_API_KEY": True,
        "GOOGLE_CSE_CX": False,
    }


def test_format_status_redacts_secret_values() -> None:
    rendered = leadfinder_doctor.format_status(
        {
            "repo_root": "/tmp/repo",
            "branch": "main",
            "latest_commit": "abc123 Add doctor",
            "repo_dirty": False,
            "venv_exists": True,
            "local_env_exists": True,
            "local_env_path": "/tmp/repo/.env.local",
            "env_present": {
                "GOOGLE_API_KEY": True,
                "GOOGLE_CSE_CX": False,
            },
        }
    )

    assert "GOOGLE_API_KEY: present" in rendered
    assert "GOOGLE_CSE_CX: missing" in rendered
    assert "test-google-key" not in rendered
