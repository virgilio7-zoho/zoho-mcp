import requests
from .config import ANALYTICS_BASE, OWNER_ORG, WORKSPACE
from .zoho_oauth import ZohoOAuth

def run_sql(view: str, sql: str) -> dict:
    access_token = ZohoOAuth.get_access_token()
    url = f"{ANALYTICS_BASE}/api/{OWNER_ORG}/{WORKSPACE}/{view}"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    form = {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_SQLQUERY": sql,
    }
    resp = requests.post(url, headers=headers, data=form, timeout=60)
    if resp.status_code in (401, 403):
        ZohoOAuth.clear()
        headers["Authorization"] = f"Zoho-oauthtoken {ZohoOAuth.get_access_token()}"
        resp = requests.post(url, headers=headers, data=form, timeout=60)
    resp.raise_for_status()
    return resp.json()
