from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    ZOHO_ANALYTICS_API_BASE: str = "https://analyticsapi.zoho.com"
    ZOHO_ACCOUNTS_BASE: str = "https://accounts.zoho.com"

    # Oauth
    ZOHO_CLIENT_ID: str = ""
    ZOHO_CLIENT_SECRET: str = ""
    ZOHO_REFRESH_TOKEN: str = ""

    # Identificadores
    ZOHO_OWNER_ORG: str = ""       # p.ej. 697009942 (no estrictamente usado en v2)
    ZOHO_WORKSPACE: str = ""       # nombre (informativo)
    ZOHO_WORKSPACE_ID: str = ""    # *** Requerido para v2 ***

    DEFAULT_LIMIT: int = 100

settings = Settings()
