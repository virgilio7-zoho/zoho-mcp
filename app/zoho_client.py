import os
import requests
from urllib.parse import quote
from .zoho_oauth import ZohoOAuth


# ============================================================
# 🔐 Autenticación & utilidades
# ============================================================

def _org_id() -> str:
    org = os.getenv("ANALYTICS_ORG_ID") or os.getenv("ZOHO_OWNER_ORG")
    if not org:
        raise RuntimeError("Falta ANALYTICS_ORG_ID o ZOHO_OWNER_ORG en variables de entorno.")
    return str(org).strip()

def _owner_name() -> str | None:
    owner = os.getenv("ANALYTICS_OWNER_NAME")
    return owner.strip() if owner else None

def _base() -> str:
    return (
        os.getenv("ANALYTICS_SERVER_URL")
        or os.getenv("ZOHO_ANALYTICS_API_BASE")
        or "https://analyticsapi.zoho.com"
    ).rstrip("/")

def _auth_headers(include_org: bool = True) -> dict:
    token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if include_org:
        headers["ZANALYTICS-ORGID"] = _org_id()
    return headers

def _retry_once(do_request):
    """
    Ejecuta la petición y, si recibe 401/403 (token expirado), refresca y reintenta 1 vez.
    `do_request` debe aceptar opcionalmente un dict de headers: do_request(new_headers)
    """
    resp = do_request(_auth_headers())
    if resp.status_code in (401, 403):
        ZohoOAuth.clear()
        resp = do_request(_auth_headers())
    return resp


# ============================================================
# 🚀 Exportación de datos por vista (auto-detector de API)
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
    Intenta automáticamente todas las variantes conocidas de Zoho Analytics hasta que una funcione:

      A) REST v2 con nombre de workspace:
         {base}/restapi/v2/workspaces/{workspace}/views|tables/{view}/data
         {base}/api/v2/workspaces/{workspace}/views|tables/{view}/data

      B) REST v2 con ID de workspace:
         {base}/restapi/v2/workspaces/{workspaceId}/views|tables/{view}/data
         {base}/api/v2/workspaces/{workspaceId}/views|tables/{view}/data

      C) Legacy API por ORG_ID y por OWNER_NAME:
         POST {base}/api/{ORG_ID}/{workspace}/tables|views/{view}
         POST {base}/api/{OWNER_NAME}/{workspace}/tables|views/{view}
         (con ZOHO_ACTION=EXPORT → JSON)
    """
    base = _base()
    org = _org_id()

    ws_name_enc = quote(str(workspace), safe="")
    ws_id_enc   = quote(str(workspace_id), safe="") if workspace_id else None
    view_enc    = quote(str(view), safe="")

    def _params() -> dict:
        p = {"limit": int(limit), "offset": int(offset)}
        if columns:
            p["columns"] = columns
        if criteria:
            p["criteria"] = criteria
        return p

    # Algunos tenants exponen /restapi/v2, otros /api/v2
    v2_bases = [f"{base}/restapi/v2", f"{base}/api/v2"]

    last_err = None

    # ---------- A) REST v2 con nombre ----------
    for v2 in v2_bases:
        for kind in ("views", "tables"):
            url = f"{v2}/workspaces/{ws_name_enc}/{kind}/{view_enc}/data"
            print("[SMART] Try A:", url)
            def _do(h):
                return requests.get(url, headers=h, params=_params(), timeout=60)
            resp = _retry_once(_do)
            if resp.status_code < 400:
                print("[SMART][A] ✅ OK:", url)
                return resp.json()
            last_err = (url, resp.status_code, resp.text[:600])
            print("[SMART][A] ❌ ERR", last_err)

    # ---------- B) REST v2 con workspace ID ----------
    if ws_id_enc:
        for v2 in v2_bases:
            for kind in ("views", "tables"):
                url = f"{v2}/workspaces/{ws_id_enc}/{kind}/{view_enc}/data"
                print("[SMART] Try B:", url)
                def _do(h):
                    return requests.get(url, headers=h, params=_params(), timeout=60)
                resp = _retry_once(_do)
                if resp.status_code < 400:
                    print("[SMART][B] ✅ OK:", url)
                    return resp.json()
                last_err = (url, resp.status_code, resp.text[:600])
                print("[SMART][B] ❌ ERR", last_err)

        # ---------- C) Legacy API (ORG_ID y OWNER_NAME) ----------
    # En legacy v1 la URL correcta para EXPORT es:
    #   POST {base}/api/{OWNER_o_ORG}/{workspace}/{view_o_table}
    #  (sin 'views/' ni 'tables/' en el path)
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

    owner = _owner_name()
    ws_name_enc = quote(str(workspace), safe="")
    view_enc = quote(str(view), safe="")

    # Construimos bases legacy por ORG y por OWNER (si hay owner)
    legacy_bases = [f"{base}/api/{org}/{ws_name_enc}"]
    if owner:
        owner_enc = quote(owner, safe="")
        legacy_bases = [f"{base}/api/{owner_enc}/{ws_name_enc}"] + legacy_bases  # probamos OWNER primero

    # 1) PRUEBA SIMPLE (sin 'views/' ni 'tables/') -> LA CORRECTA EN LEGACY
    for legacy_base in legacy_bases:
        url = f"{legacy_base}/{view_enc}"
        print("[SMART] Try C1:", url, "(EXPORT JSON, simple path)")
        def _do(h):
            return requests.post(url, headers=h, data=form, timeout=60)
        resp = _retry_once(_do)
        if resp.status_code < 400:
            print("[SMART][C1] ✅ OK:", url)
            return resp.json()
        last_err = (url, resp.status_code, resp.text[:600])
        print("[SMART][C1] ❌ ERR", last_err)

    # 2) FALLBACK (con 'tables/' y 'views/') -> por si el tenant tuviera una variante
    for legacy_base in legacy_bases:
        for kind in ("tables", "views"):
            url = f"{legacy_base}/{kind}/{view_enc}"
            print("[SMART] Try C2:", url, "(EXPORT JSON, with segment)")
            def _do(h):
                return requests.post(url, headers=h, data=form, timeout=60)
            resp = _retry_once(_do)
            if resp.status_code < 400:
                print("[SMART][C2] ✅ OK:", url)
                return resp.json()
            last_err = (url, resp.status_code, resp.text[:600])
            print("[SMART][C2] ❌ ERR", last_err)

    # Si nada funcionó:
    url, status, body = last_err if last_err else ("", "", "")
    raise RuntimeError(f"smart_view_export failed. Last tried: {url} status={status} body={body}")

# ============================================================
# 🧠 SQL export (SQLEXPORT) con fallback por ORG_ID y OWNER_NAME
# ============================================================

def sql_export(workspace: str, sql: str) -> dict:
    """
    Ejecuta SQL usando la API legacy:
      POST {base}/api/{ORG_ID}/{workspace}/sql
      POST {base}/api/{OWNER_NAME}/{workspace}/sql
    con ZOHO_ACTION=SQLEXPORT → JSON
    """
    base = _base()
    org = _org_id()
    ws_enc = quote(str(workspace), safe="")

    form = {
        "ZOHO_ACTION": "SQLEXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQLQUERY": sql,
        "ZOHO_ERROR_FORMAT": "JSON",
    }

    urls = [f"{base}/api/{org}/{ws_enc}/sql"]  # por ORG_ID
    owner = _owner_name()
    if owner:
        owner_enc = quote(owner, safe="")
        urls.append(f"{base}/api/{owner_enc}/{ws_enc}/sql")  # por OWNER_NAME

    last = None
    for url in urls:
        print("[SMART] Try SQL:", url)
        def _do(h):
            return requests.post(url, headers=h, data=form, timeout=60)
        resp = _retry_once(_do)
        if resp.status_code < 400:
            print("[SMART][SQL] ✅ OK:", url)
            return resp.json()
        print("[SMART][SQL] ❌ ERR", resp.status_code, resp.text[:600])
        last = (url, resp.status_code, resp.text[:600])

    raise requests.HTTPError(f"SQL failed. Last: {last}")


# ============================================================
# 🔁 Aliases de compatibilidad para main.py existente
# ============================================================

def get_view_data(
    workspace: str,
    view: str,
    limit: int = 100,
    offset: int = 0,
    columns: str | None = None,
    criteria: str | None = None,
    workspace_id: str | None = None,
) -> dict:
    """
    Alias de compatibilidad: conserva la firma que usa main.py y delega en smart_view_export().
    """
    return smart_view_export(
        workspace=workspace,
        view=view,
        limit=limit,
        offset=offset,
        columns=columns,
        criteria=criteria,
        workspace_id=workspace_id,
    )

def run_sql(workspace: str, view: str, sql: str) -> dict:
    """
    Alias de compatibilidad: main.py aún importa run_sql.
    Ignora 'view' (no es requerido por Zoho para SQLEXPORT) y usa sql_export().
    """
    return sql_export(workspace, sql)
