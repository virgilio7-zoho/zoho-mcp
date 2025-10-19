import os
import requests
from .zoho_oauth import ZohoOAuth

def run_sql(workspace: str, view: str, sql: str) -> dict:
    """
    Usa el endpoint universal de SQL:
      POST {base}/api/sql
    Enviando en el form:
      - ZOHO_ORG_ID
      - ZOHO_WORKSPACE_NAME
      - ZOHO_ACTION=SQLEXPORT
      - ZOHO_API_VERSION=1.0
      - ZOHO_SQLQUERY=...
    """

    base = os.getenv("ANALYTICS_SERVER_URL") or os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com")
    org  = os.getenv("ANALYTICS_ORG_ID") or os.getenv("ZOHO_OWNER_ORG")
    if not org:
        raise RuntimeError("Falta ORG ID (ANALYTICS_ORG_ID o ZOHO_OWNER_ORG).")

    url = f"{base}/api/sql"  # <--- endpoint universal

    access_token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "ZANALYTICS-ORGID": str(org),  # ayuda en algunos DC
    }

    form = {
        "ZOHO_ACTION": "SQLEXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQLQUERY": sql,
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_ORG_ID": str(org),
        "ZOHO_WORKSPACE_NAME": workspace,
    }

    # Logs útiles en Render → Logs
    print("[DEBUG] Zoho URL:", url)
    print("[DEBUG] Org ID:", org)
    print("[DEBUG] Workspace (name):", workspace)

    try:
        resp = requests.post(url, headers=headers, data=form, timeout=60)
        if resp.status_code in (401, 403):
            # refresca token e intenta de nuevo
            ZohoOAuth.clear()
            headers["Authorization"] = f"Zoho-oauthtoken {ZohoOAuth.get_access_token()}"
            resp = requests.post(url, headers=headers, data=form, timeout=60)

        if resp.status_code >= 400:
            print("[ERROR] HTTP:", resp.status_code)
            print("[ERROR] Body:", resp.text[:600])

        resp.raise_for_status()
        return resp.json()

    except requests.HTTPError as e:
        text = resp.text if resp is not None else ""
        raise RuntimeError(f"Zoho API {resp.status_code}: {text[:400]}") from e
