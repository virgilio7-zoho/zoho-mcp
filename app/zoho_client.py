import os
import requests
from .zoho_oauth import ZohoOAuth

def run_sql(workspace: str, view: str, sql: str) -> dict:
    """
    Llama a la SQL Export API de Zoho usando la forma recomendada:
    - URL:  https://analyticsapi.zoho.com/api/{ORG_ID}/{WORKSPACE_NAME}
    - Body: ZOHO_ACTION=SQLEXPORT, ZOHO_API_VERSION=1.0, ZOHO_SQLQUERY=...
    """

    base = os.getenv("ANALYTICS_SERVER_URL") or os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com")
    org  = os.getenv("ANALYTICS_ORG_ID") or os.getenv("ZOHO_OWNER_ORG")
    if not org:
        raise RuntimeError("Falta ORG ID (ANALYTICS_ORG_ID o ZOHO_OWNER_ORG).")

    # Endpoint: orgId + workspaceName (no uses el workspaceId largo)
    url = f"{base}/api/{org}/{workspace}/sql"

    access_token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        # Suele ayudar en algunos centros de datos:
        "ZANALYTICS-ORGID": str(org),
    }

    form = {
        "ZOHO_ACTION": "SQLEXPORT",     # <-- CLAVE para SQL
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_SQLQUERY": sql,
        "ZOHO_ERROR_FORMAT": "JSON",
    }

    # --------- LOGS DE DEPURACIÓN (se ven en Render → Logs) ---------
    print("[DEBUG] Zoho URL:", url)
    print("[DEBUG] Org ID:", org)
    print("[DEBUG] Workspace (name):", workspace)
    # No imprimimos el token ni el SQL completo por seguridad
    # ---------------------------------------------------------------

    try:
        resp = requests.post(url, headers=headers, data=form, timeout=60)
        if resp.status_code in (401, 403):
            # refresca token e intenta de nuevo
            ZohoOAuth.clear()
            headers["Authorization"] = f"Zoho-oauthtoken {ZohoOAuth.get_access_token()}"
            resp = requests.post(url, headers=headers, data=form, timeout=60)

        # Si falla, imprimimos la respuesta para entender por qué
        if resp.status_code >= 400:
            print("[ERROR] HTTP:", resp.status_code)
            print("[ERROR] Body:", resp.text[:400])

        resp.raise_for_status()
        return resp.json()

    except requests.HTTPError as e:
        text = resp.text if resp is not None else ""
        raise RuntimeError(f"Zoho API {resp.status_code}: {text[:400]}") from e
