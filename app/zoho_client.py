# app/zoho_client.py
"""
Cliente Zoho Analytics v2 para MCP – Nombres alineados con los tools oficiales.
"""

import os
from typing import Optional, Dict, Any, List
import requests
from urllib.parse import urlencode

# ====== ENV ======
ACCOUNTS_SERVER_URL = os.getenv("ACCOUNTS_SERVER_URL", "https://accounts.zoho.com").rstrip("/")
ANALYTICS_SERVER_URL = os.getenv("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com").rstrip("/")
ANALYTICS_CLIENT_ID = os.getenv("ANALYTICS_CLIENT_ID")
ANALYTICS_CLIENT_SECRET = os.getenv("ANALYTICS_CLIENT_SECRET")
ANALYTICS_REFRESH_TOKEN = os.getenv("ANALYTICS_REFRESH_TOKEN")
ANALYTICS_ORG_ID = os.getenv("ANALYTICS_ORG_ID")
ANALYTICS_MCP_DATA_DIR = os.getenv("ANALYTICS_MCP_DATA_DIR", "/tmp")

# ====== TOKEN ======
def get_access_token(force_refresh: bool = False) -> str:
    token = os.getenv("ZOHO_ACCESS_TOKEN")
    has_oauth = all([ANALYTICS_CLIENT_ID, ANALYTICS_CLIENT_SECRET, ANALYTICS_REFRESH_TOKEN])

    if not token or force_refresh:
        if not has_oauth:
            raise RuntimeError("Faltan credenciales OAuth (client_id/secret/refresh_token).")
        url = f"{ACCOUNTS_SERVER_URL}/oauth/v2/token"
        data = {
            "refresh_token": ANALYTICS_REFRESH_TOKEN,
            "client_id": ANALYTICS_CLIENT_ID,
            "client_secret": ANALYTICS_CLIENT_SECRET,
            "grant_type": "refresh_token",
        }
        r = requests.post(url, data=data, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Error refrescando token: {r.status_code} {r.text}")
        token = r.json().get("access_token")
        if not token:
            raise RuntimeError(f"Respuesta sin access_token: {r.text}")
        os.environ["ZOHO_ACCESS_TOKEN"] = token
        print("🔁 Nuevo access token obtenido.")
    return token


def _auth_headers(token: Optional[str] = None) -> Dict[str, str]:
    t = token or get_access_token()
    return {
        "Authorization": f"Zoho-oauthtoken {t}",
        "Accept": "application/json",
        "ZANALYTICS-ORGID": ANALYTICS_ORG_ID or "",
    }


# ====== HELPERS HTTP ======
def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{ANALYTICS_SERVER_URL}{path}"
    r = requests.get(url, headers=_auth_headers(), params=params or {}, timeout=60)
    if r.status_code == 401:
        # token vencido → refrescar y reintentar
        r = requests.get(url, headers=_auth_headers(get_access_token(True)), params=params or {}, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()


def _post(path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{ANALYTICS_SERVER_URL}{path}"
    r = requests.post(url, headers=_auth_headers(), json=json_body, timeout=120)
    if r.status_code == 401:
        r = requests.post(url, headers=_auth_headers(get_access_token(True)), json=json_body, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")
    return r.json()


# ====== TOOLS (V2) ======

# Tool: get_workspaces_list
def get_workspaces_list() -> Dict[str, Any]:
    """
    GET /restapi/v2/workspaces
    """
    path = "/restapi/v2/workspaces"
    return _get(path)


# Tool: search_views
def search_views(workspace_id: str, q: Optional[str] = None, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
    """
    GET /restapi/v2/workspaces/{workspace_id}/views?search=<q>&limit=&offset=
    """
    if not workspace_id:
        raise ValueError("workspace_id es obligatorio")
    path = f"/restapi/v2/workspaces/{workspace_id}/views"
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if q:
        params["search"] = q
    return _get(path, params)


# Tool: get_view_details
def get_view_details(workspace_id: str, view_id_or_name: str) -> Dict[str, Any]:
    """
    GET /restapi/v2/workspaces/{workspace_id}/views/{view_id_or_name}
    """
    if not workspace_id or not view_id_or_name:
        raise ValueError("workspace_id y view_id_or_name son obligatorios")
    path = f"/restapi/v2/workspaces/{workspace_id}/views/{view_id_or_name}"
    return _get(path)


# Tool: export_view (formato JSON; se puede ampliar a CSV/XLS)
def export_view(workspace_id: str, view: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """
    GET /restapi/v2/workspaces/{workspace_id}/views/{view}/data?format=json&limit=&offset=
    """
    if not workspace_id or not view:
        raise ValueError("workspace_id y view son obligatorios")

    path = f"/restapi/v2/workspaces/{workspace_id}/views/{view}/data"
    params = {"format": "json", "limit": limit, "offset": offset}
    # hacemos GET manual para pasar querystring
    url = f"{ANALYTICS_SERVER_URL}{path}?{urlencode(params)}"
    r = requests.get(url, headers=_auth_headers(), timeout=120)

    if r.status_code == 401:
        r = requests.get(url, headers=_auth_headers(get_access_token(True)), timeout=120)

    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()


# Tool: query_data (SQL)
def query_data(workspace_id: str, sql: str) -> Dict[str, Any]:
    """
    POST /restapi/v2/workspaces/{workspace_id}/sql
    body: { "query": "<SQL>" }
    """
    if not workspace_id or not sql:
        raise ValueError("workspace_id y sql son obligatorios")

    path = f"/restapi/v2/workspaces/{workspace_id}/sql"
    body = {"query": sql}
    return _post(path, body)


# ====== HEALTH ======
def health_info() -> Dict[str, Any]:
    token = os.getenv("ZOHO_ACCESS_TOKEN", "")
    return {
        "status": "up",
        "mode": "v2",
        "org_id": ANALYTICS_ORG_ID,
        "server": ANALYTICS_SERVER_URL,
        "data_dir": ANALYTICS_MCP_DATA_DIR,
        "token_len": len(token),
    }
