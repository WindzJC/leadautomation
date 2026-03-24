from __future__ import annotations

import os

import uvicorn


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "")
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    host = os.getenv("OPS_DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("OPS_DASHBOARD_PORT", "8000"))
    reload = _env_flag("OPS_DASHBOARD_RELOAD", default=False)
    uvicorn.run("ops_dashboard.app:app", host=host, port=port, reload=reload)
