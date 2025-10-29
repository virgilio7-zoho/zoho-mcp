"""
zoho_client.py — Cliente MCP para Zoho Analytics
Versión estable para Render (incluye refresh token y soporte Export API clásico).
"""

import os
import requests
from urllib.parse import quote


# ================================================================
# 🔐 Manejo de tokens
# ================================================================

def get_access_token() -> str:
    """
    Devuelve el token de acceso activo.
    Si expira, usa el refresh token para obtener uno nuevo.
    """
    token = os.getenv("ZOHO_ACCESS_TOKEN")
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")

    if not token and all([refresh_token, client_id, client_secret]):
        token = refresh_access_token(refresh_token, client_id, client_secret)

    if not token:
        raise RuntimeError("❌ Falta ZOHO_ACCESS_TOKEN o configuración de OAuth en variables de entorno.")

    return token


def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> str:
    """
    Usa el refresh token de Zoho para generar un nuevo access token.
    """
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=data, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"❌ Error al refrescar token Zoho: {r.status_code} {r.text}")

    new_token = r.json().get("access_token")
    if not new_token:
        raise RuntimeError(f"❌ No se pudo obtener access_token del response: {r.text}")

    os.environ["ZOHO_ACCESS_TOKEN"] = new_token
    print("🔁 Nuevo token de acceso generado correctamente.")
    return new_token


# ================================================================
# 🧠 Función principal — Export (C1)
# ================================================================

def smart_view_export(
    owner_email: str,
    workspace: str,
    view: str,
    access_token: str,
    limit: int = 1000,
    offset: int = 0
) -> dict:
    """
    Exporta un view/table usando el Export API clásico (C1).
    Incluye ZOHO_API_VERSION=1.0 obligatorio.
    """

    base = "https://analyticsapi.zoho.com/api"

    # Aseguramos codificación correcta de caracteres
    owner_enc = quote(owner_email, safe="")
    workspace_enc = quote(workspace, safe="")
    view_enc = quote(view, safe="")

    url = f"{base}/{owner_enc}/{workspace_enc}/{view_enc}"

    # Parámetros requeridos por el API
    params = {
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
    }

    print(f"[SMART][C1] → Requesting: {url}")
    resp = requests.get(url, headers=headers, params=params, timeout=60)

    if resp.status_code == 401 and "invalid_token" in resp.text:
        # Token expirado → refrescar automáticamente
        print("🔑 Token expirado. Intentando refrescar...")
        new_token = get_access_token()
        headers["Authorization"] = f"Zoho-oauthtoken {new_token}"
        resp = requests.get(url, headers=headers, params=params, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(
            f"❌ smart_view_export failed.\nURL: {resp.url}\nStatus: {resp.status_code}\nBody: {resp.text}"
        )

    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"❌ No se pudo convertir respuesta JSON.\nBody: {resp.text[:500]}")


# ================================================================
# 🧩 Función intermedia — usada por el endpoint /view_smart
# ================================================================

def view_smart(owner: str, workspace: str, view: str, limit: int = 100, offset: int = 0):
    """
    Función pública que usa smart_view_export.
    Se conecta automáticamente con las variables de entorno.
    """
    token = get_access_token()
    result = smart_view_export(owner, workspace, view, token, limit, offset)
    return result


# ================================================================
# 🩺 Healthcheck para /health
# ================================================================

def health_status():
    """
    Verifica que el servidor MCP esté vivo y configurado correctamente.
    """
    workspace = os.getenv("ZOHO_WORKSPACE", "UNKNOWN")
    return {"status": "UP", "workspace": workspace}
