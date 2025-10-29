import os
from typing import Optional

def env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return val

class Settings:
    ANALYTICS_CLIENT_ID = env("ANALYTICS_CLIENT_ID")
    ANALYTICS_CLIENT_SECRET = env("ANALYTICS_CLIENT_SECRET")
    ANALYTICS_REFRESH_TOKEN = env("ANALYTICS_REFRESH_TOKEN")
    ANALYTICS_ORG_ID = env("ANALYTICS_ORG_ID")
    ANALYTICS_SERVER_URL = env("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com").rstrip("/")
    ACCOUNTS_SERVER_URL = env("ACCOUNTS_SERVER_URL", "https://accounts.zoho.com").rstrip("/")

    # Opcionales
    WORKSPACE_RESULT_LIMIT = int(os.getenv("WORKSPACE_RESULT_LIMIT", "20"))
    VIEW_RESULT_LIMIT = int(os.getenv("VIEW_RESULT_LIMIT", "20"))

settings = Settings()
