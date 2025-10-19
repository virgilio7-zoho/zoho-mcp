import os
import requests
from .zoho_oauth import ZohoOAuth


def run_sql(workspace: str, view: str, sql: str) -> dict:
    """
    Ejecuta consultas SQL en Zoho Analytics usando el endpoint oficial:
    POST https://analyticsapi.zoho.com/api/{ORG_ID}/{WORKSPACE_NAME}/sql
    """
    base = os.getenv("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com")
    org = os.getenv("ANALYTICS_ORG_ID")
    if not org:
        raise RuntimeError("Falta ANALYTICS_ORG_ID en las variables de entorno.")

    # Construir la URL correcta
    url = f"{base}/api/{org}/{workspace}/sql"

    access_token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    form = {
        "ZOHO_ACTION": "SQLEXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQLQUERY": sql,
        "ZOHO_ERROR_FORMAT": "JSON"
    }

    print("[DEBUG] Zoho URL:", url)
    print("[DEBUG] Org ID:", org)
    print("[DEBUG] Workspace (name):", workspace)

    try:
        resp = requests.post(url, headers=headers, data=form, timeout=60)

        if resp.status_code in (401, 403):
            ZohoOAuth.clear()
            headers["Authorization"] = f"Zoho-oauthtoken {ZohoOAuth.get_access_token()}"
            resp = requests.post(url, headers=headers, data=form, timeout=60)

        if resp.status_code >= 400:
            print("[ERROR] HTTP:", resp.status_code)
            print("[ERROR] Body:", resp.text[:600])

        resp.raise_for_status()
        return resp.json()

    except requests.HTTPError as e:
        raise RuntimeError(f"Zoho API {resp.status_code}: {resp.text[:400]}") from e
