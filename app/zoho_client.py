import os
import requests
from urllib.parse import quote
from .zoho_oauth import ZohoOAuth


# ============================================================
# 🔐 FUNCIONES BASE DE AUTENTICACIÓN Y UTILIDADES
# ============================================================

def _org_id():
    org = os.getenv("ANALYTICS_ORG_ID") or os.getenv("ZOHO_OWNER_ORG")
    if not org:
        raise RuntimeError("Falta ANALYTICS_ORG_ID o ZOHO_OWNER_ORG")
    return str(org)

def _base():
    return (
        os.getenv("ANALYTICS_SERVER_URL")
        or os.getenv("ZOHO_ANALYTICS_API_BASE")
        or "https://analyticsapi.zoho.com"
    ).rstrip("/")

def _auth_headers(include_org=True):
    token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if include_org:
        headers["ZANALYTICS-ORGID"] = _org_id()
    return headers

def _retry_once(func):
    """Ejecuta la función (petición) una vez y reintenta si da 401/403 (token expirado)."""
    resp = func()
    if resp.status_code in (401, 403):
        ZohoOAuth.clear()
        hdrs = _auth_headers()
        resp = func(hdrs)
    return resp


# ============================================================
# 🚀 FUNCIÓN PRINCIPAL: SMART EXPORT
# ============================================================

def smart_view_export(
    workspace: str,
    view: str,
    limit: int = 100,
    offset: int = 0,
    columns: str | None = None,
    criteria: str | None = None,
    workspace_id: str | None = None,
) -> dict:
    """
    Intenta automáticamente todas las rutas conocidas hasta encontrar la que funcione:
      A) REST v2 (bases /restapi/v2 o /api/v2) usando workspaceName
      B) REST v2 usando workspaceId
      C) Legacy API (/api/{ORG}/{workspace}/tables|views/{view}) con EXPORT JSON
    """
    base = _base()
    org = _org_id()
    ws_name_enc = quote(str(workspace), safe="")
    ws_id_enc = quote(str(workspace_id), safe="") if workspace_id else None
    view_enc = quote(str(view), safe="")

    def _params():
        p = {"limit": int(limit), "offset": int(offset)}
        if columns:
            p["columns"] = columns
        if criteria:
            p["criteria"] = criteria
        return p

    # Bases posibles (algunos tenants usan /api/v2, otros /restapi/v2)
    v2_bases = [f"{base}/restapi/v2", f"{base}/api/v2"]

    last_err = None

    # ---------- A) REST v2 usando nombre ----------
    for v2 in v2_bases:
        for kind in ("views", "tables"):
            url = f"{v2}/workspaces/{ws_name_enc}/{kind}/{view_enc}/data"
            print("[SMART] Try A:", url)
            def do(h=_auth_headers()):
                return requests.get(url, headers=h, params=_params(), timeout=60)
            resp = _retry_once(do)
            if resp.status_code < 400:
                print("[SMART][A] ✅ OK:", url)
                return resp.json()
            last_err = (url, resp.status_code, resp.text[:600])
            print("[SMART][A] ❌ ERR", last_err)

    # ---------- B) REST v2 usando workspace ID ----------
    if ws_id_enc:
        for v2 in v2_bases:
            for kind in ("views", "tables"):
                url = f"{v2}/workspaces/{ws_id_enc}/{kind}/{view_enc}/data"
                print("[SMART] Try B:", url)
                def do(h=_auth_headers()):
                    return requests.get(url, headers=h, params=_params(), timeout=60)
                resp = _retry_once(do)
                if resp.status_code < 400:
                    print("[SMART][B] ✅ OK:", url)
                    return resp.json()
                last_err = (url, resp.status_code, resp.text[:600])
                print("[SMART][B] ❌ ERR", last_err)

    # ---------- C) Legacy API /api/{ORG}/{workspace}/tables|views/{view} ----------
    form = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_START_INDEX": int(offset),
        "ZOHO_END_INDEX": int(offset) + int(limit),
    }
    if columns:
        form["ZOHO_COLUMNS"] = columns
    if criteria:
        form["ZOHO_CRITERIA"] = criteria

    for kind in ("tables", "views"):
        url = f"{base}/api/{org}/{ws_name_enc}/{kind}/{view_enc}"
        print("[SMART] Try C:", url, "(EXPORT JSON)")
        def do(h=_auth_headers()):
            return requests.post(url, headers=h, data=form, timeout=60)
        resp = _retry_once(do)
        if resp.status_code < 400:
            print("[SMART][C] ✅ OK:", url)
            return resp.json()
        last_err = (url, resp.status_code, resp.text[:600])
        print("[SMART][C] ❌ ERR", last_err)

    # Si nada funcionó, devuelve error
    url, status, body = last_err if last_err else ("", "", "")
    raise RuntimeError(f"smart_view_export failed. Last tried: {url} status={status} body={body}")


# ============================================================
# 🧠 OPCIONAL: SQL EXPORT
# ============================================================

def sql_export(workspace: str, sql: str) -> dict:
    """
    Ejecuta un SQL en Zoho Analytics vía API legacy /api/{ORG}/{workspace}/sql con SQLEXPORT.
    """
    base = _base()
    org = _org_id()
    ws_enc = quote(str(workspace), safe="")
    url = f"{base}/api/{org}/{ws_enc}/sql"

    form = {
        "ZOHO_ACTION": "SQLEXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQLQUERY": sql,
        "ZOHO_ERROR_FORMAT": "JSON",
    }

    print("[SMART] Try SQL:", url)
    def do(h=_auth_headers()):
        return requests.post(url, headers=h, data=form, timeout=60)
    resp = _retry_once(do)
    if resp.status_code >= 400:
        print("[SMART][SQL] ❌ ERR", resp.status_code, resp.text[:600])
        resp.raise_for_status()
    print("[SMART][SQL] ✅ OK:", url)
    return resp.json()
