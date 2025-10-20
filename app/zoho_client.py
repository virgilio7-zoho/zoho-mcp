# app/zoho_client.py
import os
import json
import time
import urllib.parse
import requests

ZOHO_DC = os.getenv("ZOHO_DC", "com").strip() or "com"
BASE_REST = f"https://analyticsapi.zoho.{ZOHO_DC}"
USER_AGENT = "zoho-mcp/1.0 (+render)"

class ZohoAuthError(RuntimeError): ...
class ZohoApiError(RuntimeError): ...

def _access_token() -> str:
    token = os.getenv("ZOHO_ACCESS_TOKEN")
    if not token:
        raise ZohoAuthError("Falta ZOHO_ACCESS_TOKEN en variables de entorno.")
    return token.strip()

def _auth_headers():
    return {
        "Authorization": f"Zoho-oauthtoken {_access_token()}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }

def _get(url: str, params=None):
    r = requests.get(url, headers=_auth_headers(), params=params, timeout=60)
    return r.status_code, r.text

def _post(url: str, data=None, form=False):
    headers = _auth_headers()
    if form:
        r = requests.post(url, headers=headers, data=data, timeout=120)
    else:
        headers["Content-Type"] = "application/json"
        r = requests.post(url, headers=headers, json=data, timeout=120)
    return r.status_code, r.text

def smart_view_export(owner: str, orgowner: str, workspace: str, view: str,
                      limit: int = 100, offset: int = 0) -> dict:
    """
    Estrategia:
      1) Rutas REST v2 (views/tables)  -> suelen fallar con 404/400
      2) Ruta EXPORT JSON 'simple path' -> /api/{owner}/{ws}/{view}
    Devuelve dict con: columns, rows, source_url
    """
    view_enc = urllib.parse.quote(view, safe="")
    ws_enc = urllib.parse.quote(workspace, safe="")

    trials = []

    # A) REST v2 (views)
    url = f"{BASE_REST}/restapi/v2/workspaces/{ws_enc}/views/{view_enc}/data"
    status, body = _get(url, params={"limit": limit, "offset": offset})
    if status == 200:
        payload = json.loads(body)
        cols = payload.get("columns", [])
        rows = payload.get("rows", [])
        return {"columns": cols, "rows": rows, "source_url": url}
    trials.append(("A-views", url, status, body[:400]))

    # A2) REST v2 (tables)
    url = f"{BASE_REST}/restapi/v2/workspaces/{ws_enc}/tables/{view_enc}/data"
    status, body = _get(url, params={"limit": limit, "offset": offset})
    if status == 200:
        payload = json.loads(body)
        cols = payload.get("columns", [])
        rows = payload.get("rows", [])
        return {"columns": cols, "rows": rows, "source_url": url}
    trials.append(("A-tables", url, status, body[:400]))

    # C1) EXPORT JSON simple path (este es el que te funcionó)
    # Nota: aquí NO va /data ni /export, es el recurso directo.
    owner_enc = urllib.parse.quote(owner, safe="")
    url = f"{BASE_REST}/api/{owner_enc}/{ws_enc}/{view_enc}"
    status, body = _get(url, params={"ZOHO_OUTPUT_FORMAT": "json"})
    if status == 200:
        payload = json.loads(body)
        # El formato de EXPORT trae 'response/results/columns/rows' típico
        resp = payload.get("response", {})
        results = resp.get("result", resp.get("results", {}))
        columns = results.get("column_order") or results.get("columns") or []
        rows = results.get("rows") or results.get("data") or []
        return {"columns": columns, "rows": rows, "source_url": url}
    trials.append(("C1-export", url, status, body[:400]))

    # Si nada funcionó, lanza error con diagnóstico corto
    diag = "\n".join([f"[{k}] {u} -> {s} {b}" for (k, u, s, b) in trials])
    raise ZohoApiError(f"smart_view_export failed.\n{diag}")

# Atajo que usa el endpoint /view_smart
def view_smart(owner: str, orgowner: str, workspace: str, workspace_id: str,
               view: str, limit: int = 100, offset: int = 0) -> dict:
    # workspace_id hoy no es estrictamente necesario para la ruta C1.
    return smart_view_export(owner, orgowner, workspace, view, limit, offset)
