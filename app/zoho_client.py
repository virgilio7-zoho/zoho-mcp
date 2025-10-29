# app/zoho_client.py
"""
Cliente Zoho Analytics para MCP (Render)
- Export API (C1) con ZOHO_API_VERSION=1.0
- Fallback automático a SQL si C1 devuelve error 7005 (internal Zoho)
- Manejo de refresh token OAuth
"""

import os
from typing import Optional

import requests
from urllib.parse import quote

from .config import settings, DEFAULT_LIMIT


# ================================================================
# 🔐 Tokens
# ================================================================
def get_access_token(force_refresh: bool = False) -> str:
    """
    Devuelve el access token; si force_refresh=True o no existe,
    intenta refrescar usando el refresh_token.
    """
    refresh_token = settings.ZOHO_REFRESH_TOKEN or os.getenv("ZOHO_REFRESH_TOKEN")
    client_id = settings.ZOHO_CLIENT_ID or os.getenv("ZOHO_CLIENT_ID")
    client_secret = settings.ZOHO_CLIENT_SECRET or os.getenv("ZOHO_CLIENT_SECRET")

    if force_refresh and all([refresh_token, client_id, client_secret]):
        return refresh_access_token(refresh_token, client_id, client_secret)

    token = settings.ZOHO_ACCESS_TOKEN or os.getenv("ZOHO_ACCESS_TOKEN")
    if not token and all([refresh_token, client_id, client_secret]):
        token = refresh_access_token(refresh_token, client_id, client_secret)

    if not token:
        raise RuntimeError("Falta ZOHO_ACCESS_TOKEN o credenciales para refrescar (REFRESH/CLIENT_ID/CLIENT_SECRET).")

    # guarda también en env para este proceso
    os.environ["ZOHO_ACCESS_TOKEN"] = token
    return token


def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> str:
    accounts_base = settings.ZOHO_ACCOUNTS_BASE.rstrip("/")
    url = f"{accounts_base}/oauth/v2/token"
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
        raise RuntimeError(f"No se recibió access_token al refrescar: {r.text}")
    os.environ["ZOHO_ACCESS_TOKEN"] = new_token
    print("🟦 Nuevo access token obtenido.")
    return new_token


# ================================================================
# 🧩 Helpers internos
# ================================================================
def _api_base() -> str:
    base = settings.ZOHO_ANALYTICS_API_BASE.rstrip("/")
    return f"{base}/api"


def _resolve_workspace(workspace: Optional[str]) -> str:
    return workspace or settings.ZOHO_WORKSPACE


def _resolve_limit(limit: Optional[int]) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    if limit <= 0:
        raise ValueError("limit debe ser > 0")
    return limit


# ================================================================
# 🚀 Export API (C1) con fallback a SQL
# ================================================================
def smart_view_export(
    owner_email_or_org: str,
    workspace: str,
    view_or_table: str,
    access_token: str,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
) -> dict:
    """
    Exporta una vista/tabla usando Export API clásico (C1).
    Si Zoho responde 7005 (internal error), hace fallback por SQL:
      SELECT * FROM "view_or_table" LIMIT {limit} OFFSET {offset}
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

    # Reintento por token
    if resp.status_code == 401 and "invalid_token" in resp.text:
        print("🔑 Token expirado. Refrescando…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.get(url, headers=headers, params=params, timeout=60)

    # Éxito C1
    if resp.status_code == 200:
        try:
            return resp.json()
        except Exception:
            raise RuntimeError(f"No se pudo parsear JSON.\nBody: {resp.text[:500]}")

    # Posible 7005 → fallback
    code = None
    try:
        j = resp.json()
        code = (j or {}).get("response", {}).get("error", {}).get("code")
    except Exception:
        pass

    if code == 7005:
        print("⚠️ Zoho 7005 en Export API. Intentando fallback por SQL…")
        sql = f'SELECT * FROM "{view_or_table}" LIMIT {int(limit)} OFFSET {int(offset)}'
        return export_sql(sql, workspace=workspace)

    # Otros errores
    raise RuntimeError(
        f"smart_view_export failed.\nURL: {resp.url}\nstatus: {resp.status_code}\nbody: {resp.text}"
    )


# ================================================================
# 🧠 Helpers públicos usados por FastAPI
# ================================================================
def export_view_or_table(
    view_or_table: str,
    workspace: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> dict:
    lim = _resolve_limit(limit)
    if offset < 0:
        raise ValueError("offset no puede ser negativo")
    ws = _resolve_workspace(workspace)
    owner = settings.ZOHO_OWNER_ORG
    token = get_access_token()
    return smart_view_export(owner, ws, view_or_table, token, lim, offset)


def export_sql(sql: str, workspace: Optional[str] = None) -> dict:
    if not sql or not sql.strip():
        raise ValueError("sql no puede estar vacío")
    ws = _resolve_workspace(workspace)
    owner = settings.ZOHO_OWNER_ORG
    token = get_access_token()
    return _sql_export(owner, ws, sql, token)


def _sql_export(owner_email_or_org: str, workspace: str, sql: str, access_token: str) -> dict:
    base = _api_base()
    owner_enc = quote(owner_email_or_org, safe="")
    ws_enc = quote(workspace, safe="")
    url = f"{base}/{owner_enc}/{ws_enc}/sql"

    data = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQL_QUERY": sql,
    }
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Accept": "application/json",
    }

    print(f"[SQL][C1] POST {url}")
    resp = requests.post(url, headers=headers, data=data, timeout=60)

    # Reintento por token
    if resp.status_code == 401 and "invalid_token" in resp.text:
        print("🔑 Token expirado (SQL). Refrescando…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.post(url, headers=headers, data=data, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(
            f"sql_export failed.\nURL: {resp.url}\nstatus: {resp.status_code}\nbody: {resp.text}"
        )
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"No se pudo parsear JSON (SQL).\nBody: {resp.text[:500]}")


# ================================================================
# 🩺 Health
# ================================================================
def health_status() -> dict:
    return {"status": "UP", "workspace": settings.ZOHO_WORKSPACE}
