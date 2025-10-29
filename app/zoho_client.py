import os
import time
import requests
from typing import Dict, Any, List, Optional
from .config import settings

_ACCESS_TOKEN_CACHE: Optional[str] = os.getenv("ANALYTICS_ACCESS_TOKEN") or None

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Zoho-oauthtoken {token}",
        "ZANALYTICS-ORGID": settings.ANALYTICS_ORG_ID,
        "Accept": "application/json",
    }

def get_access_token(force_refresh: bool = False) -> str:
    global _ACCESS_TOKEN_CACHE
    if _ACCESS_TOKEN_CACHE and not force_refresh:
        return _ACCESS_TOKEN_CACHE

    # Refresh token
    url = f"{settings.ACCOUNTS_SERVER_URL}/oauth/v2/token"
    data = {
        "refresh_token": settings.ANALYTICS_REFRESH_TOKEN,
        "client_id": settings.ANALYTICS_CLIENT_ID,
        "client_secret": settings.ANALYTICS_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Error al refrescar token: {r.status_code} {r.text}")
    _ACCESS_TOKEN_CACHE = r.json().get("access_token")
    if not _ACCESS_TOKEN_CACHE:
        raise RuntimeError(f"Respuesta inválida al refrescar token: {r.text}")
    return _ACCESS_TOKEN_CACHE

def list_workspaces(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    token = get_access_token()
    url = f"{settings.ANALYTICS_SERVER_URL}/restapi/v2/workspaces"
    hdr = _headers(token)
    params = {}
    r = requests.get(url, headers=hdr, params=params, timeout=30)
    if r.status_code == 401:
        token = get_access_token(force_refresh=True)
        hdr = _headers(token)
        r = requests.get(url, headers=hdr, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"workspaces failed: {r.status_code} {r.text}")
    data = r.json()
    items = data.get("data") or data.get("workspaces") or []
    if limit:
        items = items[:limit]
    return items

def list_views(workspace_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    token = get_access_token()
    url = f"{settings.ANALYTICS_SERVER_URL}/restapi/v2/workspaces/{workspace_id}/views"
    hdr = _headers(token)
    r = requests.get(url, headers=hdr, timeout=30)
    if r.status_code == 401:
        token = get_access_token(force_refresh=True)
        hdr = _headers(token)
        r = requests.get(url, headers=hdr, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"views failed: {r.status_code} {r.text}")
    data = r.json()
    items = data.get("data") or data.get("views") or []
    if limit:
        items = items[:limit]
    return items
