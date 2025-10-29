import os

def getenv_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Falta {name} en variables de entorno.")
    return v

class Settings:
    # Bases (permite cambiar a EU u otros)
    ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
    ZOHO_ANALYTICS_API_BASE = os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com")

    # OAuth
    ZOHO_CLIENT_ID = getenv_str("ZOHO_CLIENT_ID")
    ZOHO_CLIENT_SECRET = getenv_str("ZOHO_CLIENT_SECRET")
    ZOHO_REFRESH_TOKEN = getenv_str("ZOHO_REFRESH_TOKEN")

    # Contexto de Analytics (owner/org + workspace)
    ZOHO_OWNER_ORG = getenv_str("ZOHO_OWNER_ORG")
    ZOHO_WORKSPACE = getenv_str("ZOHO_WORKSPACE")

    # Límite por defecto para evitar OOM en Render (512MB)
    DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))

settings = Settings()
