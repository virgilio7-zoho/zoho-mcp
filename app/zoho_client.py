"""
zoho_client.py — Cliente MCP para Zoho Analytics
Estable para Render. C1 (Export API) usa GET; SQL usa POST.
"""

import os
from typing import Optional

import requests
from urllib.parse import quote

# ====== Settings por variables de entorno ======
ZOHO_ANALYTICS_API_BASE = os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com").rstrip("/")
ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com").rstrip("/")
ZOHO_OWNER_ORG = os.getenv("ZOHO_OWNER_ORG", "")  # p. ej. "697009942" o "correo@dominio.com"
ZOHO_WORKSPACE = os.getenv("ZOHO_WORKSPACE", "")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))

# ==============================================================
# 🔐 Tokens
# ==============================================================

def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> str:
    url = f"{ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Error al refrescar token Zoho: {r.status_code} {r.text}")
    new_token = r.json().get("access_token")
    if not new_token:
        raise RuntimeError(f"No se obtuvo access_token: {r.text}")
    os.environ["ZOHO_ACCESS_TOKEN"] = new_token
    print("🔁 Nuevo access token obtenido.")
    return new_token


def get_access_token(force_refresh: bool = False) -> str:
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")

    if force_refresh and all([refresh_token, client_id, client_secret]):
        return refresh_access_token(refresh_token, client_id, client_secret)

    token = os.getenv("ZOHO_ACCESS_TOKEN")
    if not token and all([refresh_token, client_id, client_secret]):
        token = refresh_access_token(refresh_token, client_id, client_secret)

    if not token:
        raise RuntimeError("Falta ZOHO_ACCESS_TOKEN o configuración OAuth (refresh/client_id/client_secret).")

    return token


# ==============================================================
# 🚀 Export API (C1) — SIEMPRE GET
# ==============================================================

def _api_base() -> str:
    return f"{ZOHO_ANALYTICS_API_BASE}/api"

def smart_view_export(
    owner_email_or_org: str,
    workspace: str,
    view_or_table: str,
    access_token: str,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> dict:
    """
    Exporta un view/tabla usando Export API clásico (C1).
    - Método: GET
    - Obligatorio: ZOHO_API_VERSION=1.0
    """
    base = _api_base()
    owner_enc = quote(owner_email_or_org, safe="")
    ws_enc = quote(workspace, safe="")
    view_enc = quote(view_or_table, safe="")

    url = f"{base}/{owner_enc}/{ws_enc}/{view_enc}"
    params = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_ESCAPE": "true",
        "ZOHO_STARTROW": str(offset),
        "ZOHO_BULK_SIZE": str(limit),
    }
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Accept": "application/json",
    }

    print(f"[SMART][C1] GET {url}")
    resp = requests.get(url, headers=headers, params=params, timeout=60)

    # Token expirado → refrescar y reintentar
    if resp.status_code == 401 and "invalid_token" in resp.text:
        print("🔑 Token expirado. Refrescando…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.get(url, headers=headers, params=params, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(
            f"smart_view_export failed.\nURL: {resp.url}\nstatus: {resp.status_code}\nbody: {resp.text}"
        )

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"No se pudo parsear JSON.\nBody: {resp.text[:500]}")


# ==============================================================
# 🧠 Helpers invocados por FastAPI
# ==============================================================

def export_view_or_table(
    view_or_table: str,
    workspace: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> dict:
    if not view_or_table:
        raise ValueError("view no puede estar vacío")
    if offset < 0:
        raise ValueError("offset no puede ser negativo")
    lim = limit if (limit and limit > 0) else DEFAULT_LIMIT
    ws = workspace or ZOHO_WORKSPACE
    owner = ZOHO_OWNER_ORG or ""
    if not owner or not ws:
        raise RuntimeError("Faltan ZOHO_OWNER_ORG o ZOHO_WORKSPACE en variables de entorno.")
    token = get_access_token()
    return smart_view_export(owner, ws, view_or_table, token, lim, offset)


def export_sql(sql: str, workspace: Optional[str] = None, limit: Optional[int] = None, offset: int = 0) -> dict:
    """
    Ejecuta SQL con el endpoint /sql del Export API (método POST).
    """
    if not sql or not sql.strip():
        raise ValueError("sql no puede estar vacío")
    ws = workspace or ZOHO_WORKSPACE
    owner = ZOHO_OWNER_ORG or ""
    token = get_access_token()

    base = _api_base()
    owner_enc = quote(owner, safe="")
    ws_enc = quote(ws, safe="")
    url = f"{base}/{owner_enc}/{ws_enc}/sql"

    data = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQL_QUERY": sql,
        "ZOHO_STARTROW": str(offset),
        "ZOHO_BULK_SIZE": str(limit if (limit and limit > 0) else DEFAULT_LIMIT),
    }
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Accept": "application/json",
    }

    print(f"[SQL][C1] POST {url}")
    resp = requests.post(url, headers=headers, data=data, timeout=60)

    if resp.status_code == 401 and "invalid_token" in resp.text:
        print("🔑 Token expirado (SQL). Refrescando…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.post(url, headers=headers, data=data, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(f"sql export failed.\nURL: {resp.url}\nstatus: {resp.status_code}\nbody: {resp.text}")

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"No se pudo parsear JSON.\nBody: {resp.text[:500]}")

# ==============================================================
# 🩺 Health
# ==============================================================

def health_status() -> dict:
    return {"status": "UP", "workspace": ZOHO_WORKSPACE or "UNKNOWN"}
