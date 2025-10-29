import os
import requests
from typing import Dict, Any, List, Optional

# ==========================================================
# CONFIGURACIÓN: Variables del entorno (formato MCP oficial)
# ==========================================================

def env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if val is None:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return val

ANALYTICS_CLIENT_ID = env("ANALYTICS_CLIENT_ID")
ANALYTICS_CLIENT_SECRET = env("ANALYTICS_CLIENT_SECRET")
ANALYTICS_REFRESH_TOKEN = env("ANALYTICS_REFRESH_TOKEN")
ANALYTICS_ORG_ID = env("ANALYTICS_ORG_ID")
ANALYTICS_SERVER_URL = env("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com").rstrip("/")
ACCOUNTS_SERVER_URL = env("ACCOUNTS_SERVER_URL", "https://accounts.zoho.com").rstrip("/")
ANALYTICS_MCP_DATA_DIR = env("ANALYTICS_MCP_DATA_DIR", "/tmp")

# Parámetros opcionales
WORKSPACE_RESULT_LIMIT = int(os.getenv("WORKSPACE_RESULT_LIMIT", "20"))
VIEW_RESULT_LIMIT = int(os.getenv("VIEW_RESULT_LIMIT", "20"))

# Cache local del token
_ACCESS_TOKEN_CACHE: Optional[str] = None


# ==========================================================
# FUNCIONES DE AUTENTICACIÓN
# ==========================================================

def get_access_token(force_refresh: bool = False) -> str:
    """Obtiene (o renueva) el access token de Zoho Analytics."""
    global _ACCESS_TOKEN_CACHE

    if _ACCESS_TOKEN_CACHE and not force_refresh:
        return _ACCESS_TOKEN_CACHE

    url = f"{ACCOUNTS_SERVER_URL}/oauth/v2/token"
    data = {
        "refresh_token": ANALYTICS_REFRESH_TOKEN,
        "client_id": ANALYTICS_CLIENT_ID,
        "client_secret": ANALYTICS_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }

    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Error al refrescar token: {r.status_code} {r.text}")

    _ACCESS_TOKEN_CACHE = r.json().get("access_token")
    if not _ACCESS_TOKEN_CACHE:
        raise RuntimeError(f"Respuesta inválida al refrescar token: {r.text}")

    return _ACCESS_TOKEN_CACHE


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Zoho-oauthtoken {token}",
        "ZANALYTICS-ORGID": ANALYTICS_ORG_ID,
        "Accept": "application/json",
    }


# ==========================================================
# FUNCIONES DE LA API v2 (Workspaces, Views)
# ==========================================================

def list_workspaces(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Lista los workspaces disponibles en la organización."""
    token = get_access_token()
    url = f"{ANALYTICS_SERVER_URL}/restapi/v2/workspaces"

    r = requests.get(url, headers=_headers(token), timeout=30)
    if r.status_code == 401:
        token = get_access_token(force_refresh=True)
        r = requests.get(url, headers=_headers(token), timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"Error list_workspaces: {r.status_code} {r.text}")

    data = r.json()
    items = data.get("data") or data.get("workspaces") or []
    if limit:
        items = items[:limit]
    return items


def list_views(workspace_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Lista las vistas/tablas dentro de un workspace."""
    token = get_access_token()
    url = f"{ANALYTICS_SERVER_URL}/restapi/v2/workspaces/{workspace_id}/views"

    r = requests.get(url, headers=_headers(token), timeout=30)
    if r.status_code == 401:
        token = get_access_token(force_refresh=True)
        r = requests.get(url, headers=_headers(token), timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"Error list_views: {r.status_code} {r.text}")

    data = r.json()
    items = data.get("data") or data.get("views") or []
    if limit:
        items = items[:limit]
    return items


# ==========================================================
# SALUD / VALIDACIÓN
# ==========================================================

def health_info() -> Dict[str, Any]:
    """Devuelve información de salud del servicio."""
    token = get_access_token()
    return {
        "status": "UP",
        "mode": "v2",
        "org_id": ANALYTICS_ORG_ID,
        "server": ANALYTICS_SERVER_URL,
        "data_dir": ANALYTICS_MCP_DATA_DIR,
        "token_len": len(token) if token else 0,
    }
