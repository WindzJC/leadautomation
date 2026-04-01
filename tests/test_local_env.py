from __future__ import annotations

from pathlib import Path

from local_env import load_local_env, parse_local_env_text


def test_parse_local_env_text_reads_supported_google_keys_only() -> None:
    parsed = parse_local_env_text(
        "\n".join(
            [
                "# comment",
                "export GOOGLE_API_KEY='google-key'",
                'GOOGLE_CSE_CX="engine-id"',
                "BRAVE_SEARCH_API_KEY=ignored",
                "UNRELATED=value",
            ]
        )
    )

    assert parsed == {
        "GOOGLE_API_KEY": "google-key",
        "GOOGLE_CSE_CX": "engine-id",
    }


def test_load_local_env_sets_missing_values_without_overwriting_existing(tmp_path: Path, monkeypatch) -> None:
    env_path = tmp_path / ".env.local"
    env_path.write_text(
        "\n".join(
            [
                "GOOGLE_API_KEY=file-google-key",
                "GOOGLE_CSE_CX=file-engine-id",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_CSE_CX", "existing-engine-id")

    active = load_local_env(env_path)

    assert active == {
        "GOOGLE_API_KEY": "file-google-key",
        "GOOGLE_CSE_CX": "existing-engine-id",
    }
