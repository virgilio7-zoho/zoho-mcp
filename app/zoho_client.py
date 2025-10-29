# app/zoho_client.py
"""
Cliente Zoho Analytics API v2 (estable para Render).
- Refresco automático de access_token con refresh_token
- Endpoints v2: export view y SQL
- Usa variables de entorno via pydantic-settings (app.config)
"""

import os
import requests
from urllib.parse import quote

# Si tu config está en app/config.py (recomendado):
from app.config import settings  # pydantic-settings

# =========================================================
# 🔐 Tokens
# =========================================================

def _refresh_access_token() -> str:
    url = f"{settings.ZOHO_ACCOUNTS_BASE.rstrip('/')}/oauth/v2/token"
    data = {
        "refresh_token": settings.ZOHO_REFRESH_TOKEN,
        "client_id": settings.ZOHO_CLIENT_ID,
        "client_secret": settings.ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token",
    }
    r = requests.post(url, data=data, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Error refresh token: {r.status_code} {r.text}")
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {r.text}")
    os.environ["ZOHO_ACCESS_TOKEN"] = token
    print("🔄 Nuevo access token obtenido.")
    return token


def _get_access_token(force: bool = False) -> str:
    tok = os.getenv("ZOHO_ACCESS_TOKEN")
    if force or not tok:
        return _refresh_access_token()
    return tok


# =========================================================
# 🔗 Helpers base v2
# =========================================================

def _rest_v2_base() -> str:
    # https://analyticsapi.zoho.com/restapi/v2
    return settings.ZOHO_ANALYTICS_API_BASE.rstrip("/") + "/restapi/v2"


# =========================================================
# 📤 Exportar vista/tabla (API v2)
# =========================================================

def v2_export_view(view: str, *, workspace_id: str | None = None, limit: int = 100, offset: int = 0) -> dict:
    """
    Exporta datos de un view/table por API v2.
    GET /restapi/v2/workspaces/{workspace_id}/views/{view}/data?limit=&offset=
    """
    wsid = workspace_id or settings.ZOHO_WORKSPACE_ID
    if not wsid:
        raise ValueError("Falta ZOHO_WORKSPACE_ID")

    base = _rest_v2_base()
    view_enc = quote(view, safe="")
    url = f"{base}/workspaces/{wsid}/views/{view_enc}/data"

    params = {"limit": str(limit), "offset": str(offset)}
    headers = {
        "Authorization": f"Zoho-oauthtoken {_get_access_token()}",
        "Accept": "application/json",
    }

    print(f"[V2][GET] {url}")
    r = requests.get(url, headers=headers, params=params, timeout=60)

    if r.status_code == 401:
        headers["Authorization"] = f"Zoho-oauthtoken {_get_access_token(force=True)}"
        r = requests.get(url, headers=headers, params=params, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(f"v2_export_view failed.\nURL: {r.url}\nstatus: {r.status_code}\nbody:\n{r.text}")

    return r.json()


# =========================================================
# 🧪 SQL (API v2)
# =========================================================

def v2_sql_query(sql: str, *, workspace_id: str | None = None) -> dict:
    """
    Ejecuta SQL por API v2 (si tu tenant lo tiene habilitado).
    POST /restapi/v2/workspaces/{workspace_id}/sql
    Body: { "sql": "SELECT ..." }
    """
    wsid = workspace_id or settings.ZOHO_WORKSPACE_ID
    if not wsid:
        raise ValueError("Falta ZOHO_WORKSPACE_ID")
    if not sql or not sql.strip():
        raise ValueError("sql vacío")

    base = _rest_v2_base()
    url = f"{base}/workspaces/{wsid}/sql"
    data = {"sql": sql}

    headers = {
        "Authorization": f"Zoho-oauthtoken {_get_access_token()}",
        "Accept": "application/json",
    }

    print(f"[V2][POST] {url}")
    r = requests.post(url, headers=headers, json=data, timeout=60)

    if r.status_code == 401:
        headers["Authorization"] = f"Zoho-oauthtoken {_get_access_token(force=True)}"
        r = requests.post(url, headers=headers, json=data, timeout=60)

    if r.status_code != 200:
        raise RuntimeError(f"v2_sql_query failed.\nURL: {url}\nstatus: {r.status_code}\nbody:\n{r.text}")

    return r.json()


# =========================================================
# 🩺 Health
# =========================================================

def health_info() -> dict:
    return {
        "status": "UP",
        "mode": "v2",
        "workspace": settings.ZOHO_WORKSPACE,
        "workspace_id": settings.ZOHO_WORKSPACE_ID,
    }
