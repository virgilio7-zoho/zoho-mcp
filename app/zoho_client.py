from __future__ import annotations
import time
from typing import Any, Dict, Tuple
import requests
from urllib.parse import quote
from .config import settings

# Cache simple en memoria para el access_token
_ACCESS_TOKEN: str | None = None
_ACCESS_TOKEN_EXP: float | None = None  # epoch seconds

def _now() -> float:
    return time.time()

def get_access_token() -> str:
    """
    Obtiene (o renueva) el access token usando el refresh token.
    Guarda en cache hasta ~55 minutos.
    """
    global _ACCESS_TOKEN, _ACCESS_TOKEN_EXP
    if _ACCESS_TOKEN and _ACCESS_TOKEN_EXP and _now() < _ACCESS_TOKEN_EXP:
        return _ACCESS_TOKEN

    token_url = f"{settings.ZOHO_ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": settings.ZOHO_REFRESH_TOKEN,
        "client_id": settings.ZOHO_CLIENT_ID,
        "client_secret": settings.ZOHO_CLIENT_SECRET,
    }
    resp = requests.post(token_url, data=data, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Error renovando access_token: {resp.status_code} {resp.text}"
        )
    payload = resp.json()
    access = payload.get("access_token")
    if not access:
        raise RuntimeError(f"Respuesta sin access_token: {payload}")

    # Zoho típicamente expira en 3600s; usamos 3300 para margen
    _ACCESS_TOKEN = access
    _ACCESS_TOKEN_EXP = _now() + float(payload.get("expires_in", 3600)) - 300
    return _ACCESS_TOKEN

def _auth_headers() -> Dict[str, str]:
    return {"Authorization": f"Zoho-oauthtoken {get_access_token()}"}

def _api(path: str) -> str:
    return f"{settings.ZOHO_ANALYTICS_API_BASE}{path}"

def export_sql(sql: str, workspace: str | None = None) -> Dict[str, Any]:
    """
    Exporta resultado de una consulta SQL en JSON.
    Usa el endpoint 'simple' que ha mostrado mejor compatibilidad:
    /api/{owner}/{workspace}/sql  (EXPORT -> JSON)
    """
    owner = settings.ZOHO_OWNER_ORG
    ws = workspace or settings.ZOHO_WORKSPACE
    # Cuidado con caracteres: owner puede ser numérico u org key; no escapamos el slash.
    path = f"/api/{quote(owner, safe='')}/{quote(ws, safe='')}/sql"
    url = _api(path)

    # Parámetros estilo Zoho Analytics EXPORT
    form = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "SQLQUERY": sql,
    }
    r = requests.post(url, headers=_auth_headers(), data=form, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"SQL export error {r.status_code}: {r.text}")
    return r.json()

def export_view_or_table(
    view_or_table: str,
    workspace: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> Dict[str, Any]:
    """
    Exporta una vista/tabla en JSON usando el 'simple path' que te funcionó:
    /api/{owner}/{workspace}/{view_or_table}?EXPORT JSON

    Se agregan LIMIT y OFFSET para paginar y evitar OOM.
    """
    owner = settings.ZOHO_OWNER_ORG
    ws = workspace or settings.ZOHO_WORKSPACE
    lim = limit or settings.DEFAULT_LIMIT
    off = offset or 0

    path = f"/api/{quote(owner, safe='')}/{quote(ws, safe='')}/{quote(view_or_table, safe='')}"
    url = _api(path)

    form = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "LIMIT": str(lim),
        "OFFSET": str(off),
    }
    r = requests.post(url, headers=_auth_headers(), data=form, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(
            f"Export view error {r.status_code} url={url} body={r.text}"
        )
    return r.json()
