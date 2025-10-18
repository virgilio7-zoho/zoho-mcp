import requests
from .config import ANALYTICS_BASE, OWNER_ORG
from .zoho_oauth import ZohoOAuth

def run_sql(workspace: str, view: str, sql: str) -> dict:
    access_token = ZohoOAuth.get_access_token()
    url = f"{ANALYTICS_BASE}/api/{OWNER_ORG}/{workspace}/{view}"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    form = {
    "ZOHO_ACTION": "EXPORT",
    "ZOHO_OUTPUT_FORMAT": "JSON",
    "ZOHO_API_VERSION": "1.0",     # <-- requerido por Zoho
    "ZOHO_SQLQUERY": sql,
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
