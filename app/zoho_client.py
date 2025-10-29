"""
zoho_client.py — Cliente MCP para Zoho Analytics
Versión estable para Render con:
- Export API clásico (C1) vía POST (form data)  ✅
- ZOHO_API_VERSION=1.0 siempre enviado         ✅
- Refresh automático de OAuth token             ✅
- Helpers para /view_smart y /health            ✅

Variables de entorno requeridas en Render:
- ZOHO_OWNER            (email del owner u org id, p.ej. vacevedo@markem.com.co)
- ZOHO_WORKSPACE        (p. ej. MARKEM)
- ZOHO_ACCESS_TOKEN     (opcional si tienes refresh)
- ZOHO_REFRESH_TOKEN    (recomendado)
- ZOHO_CLIENT_ID        (recomendado)
- ZOHO_CLIENT_SECRET    (recomendado)
- ZOHO_ANALYTICS_API_BASE (opcional, default: https://analyticsapi.zoho.com)
- ZOHO_ACCOUNTS_BASE      (opcional, default: https://accounts.zoho.com)
"""

import os
from typing import Optional
from urllib.parse import quote

import requests


# ------------------------------------------------------------------
# Config básicos (con defaults seguros)
# ------------------------------------------------------------------
def _analytics_base() -> str:
    return os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com").rstrip("/")


def _accounts_base() -> str:
    return os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com").rstrip("/")


# ------------------------------------------------------------------
# Tokens
# ------------------------------------------------------------------
def get_access_token(force_refresh: bool = False) -> str:
    """Devuelve el access token activo. Si force_refresh=True, refresca con OAuth."""
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")

    if force_refresh and all([refresh_token, client_id, client_secret]):
        return refresh_access_token(refresh_token, client_id, client_secret)

    token = os.getenv("ZOHO_ACCESS_TOKEN")
    if not token and all([refresh_token, client_id, client_secret]):
        token = refresh_access_token(refresh_token, client_id, client_secret)

    if not token:
        raise RuntimeError(
            "❌ Falta ZOHO_ACCESS_TOKEN o configuración de OAuth (ZOHO_REFRESH_TOKEN, ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET)."
        )
    return token


def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> str:
    """Usa refresh token para generar un access token nuevo."""
    url = f"{_accounts_base()}/oauth/v2/token"
    data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"❌ Error al refrescar token Zoho: {r.status_code} {r.text}")

    access_token = r.json().get("access_token")
    if not access_token:
        raise RuntimeError(f"❌ No se devolvió access_token en el response: {r.text}")

    os.environ["ZOHO_ACCESS_TOKEN"] = access_token
    print("🔁 Access token actualizado correctamente.")
    return access_token


# ------------------------------------------------------------------
# Export API (C1) — POST form-data (evita error ZOHO_API_VERSION)
# ------------------------------------------------------------------
def smart_view_export(
    owner_email: str,
    workspace: str,
    view: str,
    access_token: str,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """
    Exporta una vista/tabla usando Export API clásico (C1).

    IMPORTANTE:
    - Zoho requiere POST (form-data) para que reconozca correctamente ZOHO_API_VERSION.
    - Enviar por GET puede provocar: "The parameter ZOHO_API_VERSION is not proper".
    """

    base = _analytics_base()  # p.ej. https://analyticsapi.zoho.com
    owner_enc = quote(owner_email, safe="")
    workspace_enc = quote(workspace, safe="")
    view_enc = quote(view, safe="")

    # Endpoint C1
    url = f"{base}/api/{owner_enc}/{workspace_enc}/{view_enc}"

    # Parámetros obligatorios del Export API
    form = {
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

    print(f"[SMART][C1][POST] → {url}")
    resp = requests.post(url, headers=headers, data=form, timeout=90)

    # Token expirado → refrescar 1 vez y reintentar
    if resp.status_code in (401, 403) and "invalid_token" in resp.text.lower():
        print("🔑 Token expirado. Intentando refrescar…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.post(url, headers=headers, data=form, timeout=90)

    if resp.status_code != 200:
        raise RuntimeError(
            f"❌ smart_view_export failed.\nURL: {resp.url}\nStatus: {resp.status_code}\nBody: {resp.text}"
        )

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"❌ Respuesta no es JSON.\nBody (primeros 600 chars): {resp.text[:600]}")


# ------------------------------------------------------------------
# SQL Export (opcional) — también C1, por POST
# ------------------------------------------------------------------
def sql_export(owner_email: str, workspace: str, sql: str, access_token: str) -> dict:
    if not sql or not sql.strip():
        raise ValueError("sql no puede estar vacío")

    base = _analytics_base()
    owner_enc = quote(owner_email, safe="")
    workspace_enc = quote(workspace, safe="")
    url = f"{base}/api/{owner_enc}/{workspace_enc}/sql"

    form = {
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

    print(f"[SQL][C1][POST] → {url}")
    resp = requests.post(url, headers=headers, data=form, timeout=120)

    if resp.status_code in (401, 403) and "invalid_token" in resp.text.lower():
        print("🔑 Token expirado en SQL. Intentando refrescar…")
        new_token = get_access_token(force_refresh=True)
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.post(url, headers=headers, data=form, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"❌ sql_export failed.\nURL: {resp.url}\nStatus: {resp.status_code}\nBody: {resp.text}")

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"❌ Respuesta no es JSON.\nBody (primeros 600 chars): {resp.text[:600]}")


# ------------------------------------------------------------------
# Helpers para FastAPI (/view_smart y /health)
# ------------------------------------------------------------------
def view_smart(owner: Optional[str], workspace: Optional[str], view: str, limit: int = 100, offset: int = 0):
    """Usado por el endpoint /view_smart."""
    if not view:
        raise ValueError("view es obligatorio")

    owner_final = owner or os.getenv("ZOHO_OWNER")
    workspace_final = workspace or os.getenv("ZOHO_WORKSPACE")

    if not owner_final:
        raise RuntimeError("❌ Falta ZOHO_OWNER (email del owner u org id) en variables de entorno o en la petición.")
    if not workspace_final:
        raise RuntimeError("❌ Falta ZOHO_WORKSPACE en variables de entorno o en la petición.")

    token = get_access_token()
    return smart_view_export(owner_final, workspace_final, view, token, limit, offset)


def health_status():
    """Para GET /health."""
    return {
        "status": "UP",
        "workspace": os.getenv("ZOHO_WORKSPACE", "UNKNOWN"),
    }
