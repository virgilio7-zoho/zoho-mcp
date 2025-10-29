import os
from typing import Optional, Dict, Tuple
from urllib.parse import quote

import requests


# =========================
# Bases de Zoho (con defaults)
# =========================
def _analytics_base() -> str:
    return os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com").rstrip("/")


def _accounts_base() -> str:
    return os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com").rstrip("/")


# =========================
# OAuth tokens
# =========================
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
        raise RuntimeError(
            "❌ Falta ZOHO_ACCESS_TOKEN o configuración de OAuth (ZOHO_REFRESH_TOKEN, ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET)."
        )
    return token


def refresh_access_token(refresh_token: str, client_id: str, client_secret: str) -> str:
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


# =========================
# Export helpers (parametrización)
# =========================
def _owner_candidates() -> Tuple[str, Optional[str]]:
    """
    Devuelve (owner_email, owner_orgid?) para probar primero por email y luego por org id.
    """
    owner_email = os.getenv("ZOHO_OWNER") or ""
    owner_orgid = os.getenv("ZOHO_OWNER_ORGID")  # opcional (numérico)
    if not owner_email and not owner_orgid:
        raise RuntimeError("❌ Define ZOHO_OWNER (email) o ZOHO_OWNER_ORGID (numérico) en variables de entorno.")
    return owner_email, owner_orgid


def _export_form(limit: int, offset: int) -> Dict[str, str]:
    return {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_ESCAPE": "true",
        "ZOHO_STARTROW": str(offset),
        "ZOHO_BULK_SIZE": str(limit),
    }


def _build_urls(owner: str, workspace: str, view: str) -> str:
    base = _analytics_base()
    owner_enc = quote(owner, safe="")
    workspace_enc = quote(workspace, safe="")
    view_enc = quote(view, safe="")
    return f"{base}/api/{owner_enc}/{workspace_enc}/{view_enc}"


def _post_export(url: str, token: str, form: Dict[str, str]) -> requests.Response:
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Accept": "application/json",
        # 👇 Esto es LO CRÍTICO para que Zoho reconozca ZOHO_API_VERSION
        "Content-Type": "application/x-www-form-urlencoded",
    }
    return requests.post(url, headers=headers, data=form, timeout=90)


def _get_export(url: str, token: str, form: Dict[str, str]) -> requests.Response:
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Accept": "application/json",
    }
    return requests.get(url, headers=headers, params=form, timeout=90)


def _is_invalid_token(resp: requests.Response) -> bool:
    text = resp.text.lower()
    return resp.status_code in (401, 403) and ("invalid_token" in text or "invalid oauth token" in text)


def _is_8504(resp: requests.Response) -> bool:
    # 8504 = "The parameter ZOHO_API_VERSION is not proper ..."
    return resp.status_code == 400 and "8504" in resp.text


# =========================
# Export principal (C1) con reintentos inteligentes
# =========================
def smart_view_export(
    owner_email: str,
    workspace: str,
    view: str,
    access_token: str,
    limit: int = 1000,
    offset: int = 0,
) -> dict:
    """
    Exporta una vista/tabla usando el Export API clásico (C1), con estas estrategias:
      1) POST form-data con owner por EMAIL (recomendado)
      2) Si código 8504 → reintento con owner por ORGID (si está configurado)
      3) Si aún 8504 → último recurso GET con params
    """

    form = _export_form(limit, offset)

    # 1) EMAIL
    url_email = _build_urls(owner_email, workspace, view)
    print(f"[C1][POST][EMAIL] {url_email}")
    resp = _post_export(url_email, access_token, form)

    # Refresh si hace falta
    if _is_invalid_token(resp):
        print("🔑 Token expirado (EMAIL). Refrescando…")
        token = get_access_token(force_refresh=True)
        resp = _post_export(url_email, token, form)
    else:
        token = access_token

    if resp.status_code == 200:
        return _as_json(resp)

    # ¿Error 8504? → probar con ORGID si existe
    owner_email_env, owner_orgid_env = _owner_candidates()
    if _is_8504(resp) and owner_orgid_env:
        url_org = _build_urls(owner_orgid_env, workspace, view)
        print(f"[C1][POST][ORGID] {url_org}")
        resp2 = _post_export(url_org, token, form)

        if _is_invalid_token(resp2):
            print("🔑 Token expirado (ORGID). Refrescando…")
            token = get_access_token(force_refresh=True)
            resp2 = _post_export(url_org, token, form)

        if resp2.status_code == 200:
            return _as_json(resp2)

        # ¿sigue 8504?
        if _is_8504(resp2):
            # Último recurso GET
            print(f"[C1][GET][ORGID-FALLBACK] {url_org}")
            resp3 = _get_export(url_org, token, form)
            if resp3.status_code == 200:
                return _as_json(resp3)
            _raise_export_error(resp3)

        _raise_export_error(resp2)

    # Si llegamos aquí y no era 8504 (u ORGID no existe), intentamos GET con EMAIL
    if _is_8504(resp):
        print(f"[C1][GET][EMAIL-FALLBACK] {url_email}")
        resp4 = _get_export(url_email, token, form)
        if resp4.status_code == 200:
            return _as_json(resp4)
        _raise_export_error(resp4)

    _raise_export_error(resp)


def _raise_export_error(resp: requests.Response) -> None:
    raise RuntimeError(
        f"❌ smart_view_export failed.\nStatus: {resp.status_code}\nURL: {resp.url}\nBody: {resp.text}"
    )


def _as_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        raise RuntimeError(f"❌ Respuesta no es JSON.\nBody (primeros 600 chars): {resp.text[:600]}")


# =========================
# SQL export (opcional)
# =========================
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
        "Content-Type": "application/x-www-form-urlencoded",
    }

    print(f"[SQL][C1][POST] {url}")
    resp = requests.post(url, headers=headers, data=form, timeout=120)

    if _is_invalid_token(resp):
        print("🔑 Token expirado en SQL. Refrescando…")
        token = get_access_token(force_refresh=True)
        resp = requests.post(url, headers={**headers, "Authorization": f"Zoho-oauthtoken {token}"}, data=form, timeout=120)

    if resp.status_code != 200:
        raise RuntimeError(f"❌ sql_export failed.\nURL: {resp.url}\nStatus: {resp.status_code}\nBody: {resp.text}")

    return _as_json(resp)


# =========================
# Helpers para FastAPI
# =========================
def view_smart(owner: Optional[str], workspace: Optional[str], view: str, limit: int = 100, offset: int = 0):
    if not view:
        raise ValueError("view es obligatorio")

    owner_final = owner or os.getenv("ZOHO_OWNER") or os.getenv("ZOHO_OWNER_ORGID", "")
    workspace_final = workspace or os.getenv("ZOHO_WORKSPACE", "")

    if not owner_final:
        raise RuntimeError("❌ Falta ZOHO_OWNER (email) o ZOHO_OWNER_ORGID en variables de entorno/petición.")
    if not workspace_final:
        raise RuntimeError("❌ Falta ZOHO_WORKSPACE en variables de entorno/petición.")

    token = get_access_token()
    return smart_view_export(owner_final, workspace_final, view, token, limit, offset)


def health_status():
    return {"status": "UP", "workspace": os.getenv("ZOHO_WORKSPACE", "UNKNOWN")}
