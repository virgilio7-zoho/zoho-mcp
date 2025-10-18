import requests
from .config import ANALYTICS_BASE, OWNER_ORG
from .zoho_oauth import ZohoOAuth

def run_sql(workspace: str, view: str, sql: str) -> dict:
    """
    Versión alineada con la documentación oficial de Zoho Analytics MCP Server:
      - Usa SQLEXPORT como acción.
      - Llama al endpoint /api/{OWNER_ORG}/{WORKSPACE}.
    """
    access_token = ZohoOAuth.get_access_token()
    url = f"{ANALYTICS_BASE}/api/{OWNER_ORG}/{workspace}"

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    form = {
        "ZOHO_ACTION": "SQLEXPORT",        # <-- Clave: acción correcta para SQL
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQLQUERY": sql,
        "ZOHO_ERROR_FORMAT": "JSON"
    }

    try:
        resp = requests.post(url, headers=headers, data=form, timeout=60)
        if resp.status_code in (401, 403):
            ZohoOAuth.clear()
            headers["Authorization"] = f"Zoho-oauthtoken {ZohoOAuth.get_access_token()}"
            resp = requests.post(url, headers=headers, data=form, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        text = resp.text if resp is not None else ""
        raise RuntimeError(f"Zoho API {resp.status_code}: {text[:400]}") from e
