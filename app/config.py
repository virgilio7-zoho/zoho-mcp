"""
Simple configuration helper for the Zoho Analytics MCP server.

This module defines a ``Settings`` class which reads required and optional
environment variables at import time. It exposes a global ``settings``
instance for convenience. Note that this configuration is not used
directly by the current ``zoho_client.py``, which reads environment
variables on demand. However, this module remains here for backward
compatibility and potential future use.

If any mandatory environment variable is missing, accessing the
corresponding attribute will raise a ``RuntimeError``.
"""

from __future__ import annotations

import os
from typing import Optional


def env(name: str, default: Optional[str] = None) -> str:
    """Retrieve an environment variable or raise if not present and no default."""
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return val


class Settings:
    """Configuration settings pulled from environment variables."""

    ANALYTICS_CLIENT_ID: str = env("ANALYTICS_CLIENT_ID")
    ANALYTICS_CLIENT_SECRET: str = env("ANALYTICS_CLIENT_SECRET")
    ANALYTICS_REFRESH_TOKEN: str = env("ANALYTICS_REFRESH_TOKEN")
    ANALYTICS_ORG_ID: str = env("ANALYTICS_ORG_ID")
    ANALYTICS_SERVER_URL: str = env("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com").rstrip("/")
    ACCOUNTS_SERVER_URL: str = env("ACCOUNTS_SERVER_URL", "https://accounts.zoho.com").rstrip("/")

    # Optional limits for listing workspaces and views
    WORKSPACE_RESULT_LIMIT: int = int(os.getenv("WORKSPACE_RESULT_LIMIT", "20"))
    VIEW_RESULT_LIMIT: int = int(os.getenv("VIEW_RESULT_LIMIT", "20"))


settings = Settings()


__all__ = ["settings"]
