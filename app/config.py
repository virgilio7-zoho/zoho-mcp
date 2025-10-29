# app/config.py
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Región/host (ajústalo si usas .eu / .in)
    ZOHO_ACCOUNTS_BASE: str = Field(default="https://accounts.zoho.com")
    ZOHO_ANALYTICS_API_BASE: str = Field(default="https://analyticsapi.zoho.com")

    # Identidad y workspace por defecto
    ZOHO_OWNER_ORG: str = Field(..., description="Email u OrgID dueño del workspace, ej: 697009942 o vacevedo@...")
    ZOHO_WORKSPACE: str = Field(..., description='Nombre del workspace, ej: "MARKEM"')

    # OAuth
    ZOHO_ACCESS_TOKEN: str | None = None
    ZOHO_REFRESH_TOKEN: str | None = None
    ZOHO_CLIENT_ID: str | None = None
    ZOHO_CLIENT_SECRET: str | None = None

    # Límites por defecto
    DEFAULT_LIMIT: int = 100

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
DEFAULT_LIMIT = settings.DEFAULT_LIMIT
