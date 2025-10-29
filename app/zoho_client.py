"""
zoho_client.py — Cliente MCP para Zoho Analytics
Versión estable para Render (Export C1 por POST form-encoded + refresh token).
"""

import os
from typing import Optional
from urllib.parse import quote

import requests

# ------- Config mínima segura (evita reventar si falta alguna var) -------
ZOHO_ANALYTICS_API_BASE = os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com").rstrip("/")
ZOHO_ACCOUNTS_BASE = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com").rstrip("/")
ZOHO_OWNER_ORG = os.getenv("ZOHO_OWNER_ORG") or os.getenv("ZOHO_OWNER_EMAIL", "")
ZOHO_WORKSPACE = os.getenv("ZOHO_WORKSPACE", "")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))


# ================================================================
# 🔐 Tokens
# ================================================================
def get_access_token(force_refresh: bool = False) -> str:
    rt = os.getenv("ZOHO_REFRESH_TOKEN")
    cid = os.getenv("ZOHO_CLIENT_ID")
    csec = os.getenv("ZOHO_CLIENT_SECRET")

    if force_refresh and all([rt, cid, csec]):
        return refresh_access_token(rt, cid, csec)

    token = os.getenv("ZOHO_ACCESS_TOKEN")
    if not token and all([rt, cid, csec]):
        token = refresh_access_token(rt, cid, csec)

    if not token:
        raise RuntimeError("Falta ZOHO_ACCESS_TOKEN o configuración OAuth (REFRESH/ID/SECRET).")
    return token


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
        raise RuntimeError(f"No se obtuvo access_token en respuesta: {r.text}")
    os.environ["ZOHO_ACCESS_TOKEN"] = new_token
    print("🔁 Nuevo access token obtenido.")
    return new_token


# ================================================================
# 🚚 Export (C1) — SIEMPRE por POST form-encoded
# ================================================================
def _api_base() -> str:
    return f"{ZOHO_ANALYTICS_API_BASE}/api"


def smart_view_export(
    owner_email: str,
    workspace: str,
    view: str,
    access_token: str,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """
    Exporta un view/table usando Export API clásico (C1).
    IMPORTANTE: POST + application/x-www-form-urlencoded
    """

    base = _api_base()
    owner_enc = quote(owner_email, safe="")
    workspace_enc = quote(workspace, safe="")
    view_enc = quote(view, safe="")

    url = f"{base}/{owner_enc}/{workspace_enc}/{view_enc}"

    form = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_ESCAPE": "true",
        "ZOHO_STARTROW": str(offset),
        "ZOHO_BULK_SIZE": str(limit),
    }

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Accept": "application/json",
        # ¡Clave! form-encoded (no JSON)
        "Content-Type": "application/x-www-form-urlencoded",
    }

    print(f"[SMART][C1] POST {url}")
    resp = requests.post(url, headers=headers, data=form, timeout=90)

    # Manejo de token expirado
    if resp.status_code == 401 and "invalid_token" in resp.text:
        print("🔑 Token expirado. Refrescando…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.post(url, headers=headers, data=form, timeout=90)

    if resp.status_code != 200:
        raise RuntimeError(
            f"smart_view_export failed.\nURL: {resp.url}\nStatus: {resp.status_code}\nBody: {resp.text}"
        )

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"No se pudo parsear JSON.\nBody: {resp.text[:500]}")


# ================================================================
# 🧩 Wrapper usado por el endpoint /view_smart
# ================================================================
def view_smart(owner: str, workspace: str, view: str, limit: int = 100, offset: int = 0):
    token = get_access_token()
    return smart_view_export(owner, workspace, view, token, limit or DEFAULT_LIMIT, offset)


# ================================================================
# 🩺 Health
# ================================================================
def health_status():
    return {"status": "UP", "workspace": ZOHO_WORKSPACE or "UNKNOWN"}


# ================================================================
# Helpers públicos opcionales (por si los usas en main.py)
# ================================================================
def export_view_or_table(
    view_or_table: str,
    workspace: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> dict:
    owner = ZOHO_OWNER_ORG
    ws = workspace or ZOHO_WORKSPACE
    if not owner or not ws:
        raise RuntimeError("Faltan ZOHO_OWNER_ORG/ZOHO_WORKSPACE.")
    token = get_access_token()
    return smart_view_export(owner, ws, view_or_table, token, limit or DEFAULT_LIMIT, offset)
    # ================================================================
# 🔎 SQL Export (C1) — POST form-encoded
# ================================================================
def export_sql(sql: str, workspace: Optional[str] = None) -> dict:
    """
    Ejecuta una consulta SQL en Zoho Analytics usando el API clásico (C1).
    Requiere permisos de lectura sobre las tablas referenciadas.
    """
    if not sql or not sql.strip():
        raise ValueError("sql no puede estar vacío")

    owner = ZOHO_OWNER_ORG
    ws = workspace or ZOHO_WORKSPACE
    if not owner or not ws:
        raise RuntimeError("Faltan ZOHO_OWNER_ORG/ZOHO_WORKSPACE.")

    token = get_access_token()
    return _sql_export(owner, ws, sql, token)


def _sql_export(owner_email: str, workspace: str, sql: str, access_token: str) -> dict:
    """
    Implementación de SQL EXPORT por POST (form-encoded).
    Endpoint: {base}/{owner}/{workspace}/sql
    """
    base = _api_base()
    owner_enc = quote(owner_email, safe="")
    workspace_enc = quote(workspace, safe="")
    url = f"{base}/{owner_enc}/{workspace_enc}/sql"

    form = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_SQL_QUERY": sql,
    }

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    print(f"[SQL][C1] POST {url}")
    resp = requests.post(url, headers=headers, data=form, timeout=90)

    if resp.status_code == 401 and "invalid_token" in resp.text:
        print("🔑 Token expirado (SQL). Refrescando…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.post(url, headers=headers, data=form, timeout=90)

    if resp.status_code != 200:
        raise RuntimeError(
            f"sql_export failed.\nURL: {resp.url}\nStatus: {resp.status_code}\nBody: {resp.text}"
        )

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"No se pudo parsear JSON (SQL).\nBody: {resp.text[:500]}")

