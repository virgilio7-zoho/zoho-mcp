import os
import requests
from .zoho_oauth import ZohoOAuth

def run_sql(workspace: str, view: str, sql: str) -> dict:
    """
    Intenta las 4 combinaciones soportadas por Zoho Analytics para SQL:
      1) /api/{org}/{ws}/sql + SQLEXPORT
      2) /api/{org}/{ws}     + SQLEXPORT
      3) /api/{org}/{ws}/sql + EXPORT
      4) /api/{org}/{ws}     + EXPORT

    Así salimos de dudas de si tu tenant expone /sql y/o acepta SQLEXPORT o EXPORT para SQL.
    Deja logs claros para ver qué combinación fue la válida.
    """

    base = os.getenv("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com").rstrip("/")
    org  = os.getenv("ANALYTICS_ORG_ID") or os.getenv("ZOHO_OWNER_ORG")
    if not org:
        raise RuntimeError("Falta ANALYTICS_ORG_ID o ZOHO_OWNER_ORG en variables de entorno.")

    access_token = ZohoOAuth.get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
        # A algunos DC les gusta este header:
        "ZANALYTICS-ORGID": str(org),
    }

    # Combinaciones a probar (en orden)
    endpoints = [
        f"{base}/api/{org}/{workspace}/sql",
        f"{base}/api/{org}/{workspace}",
    ]
    actions = ["SQLEXPORT", "EXPORT"]

    last_error = None

    for url in endpoints:
        for action in actions:
            form = {
                "ZOHO_ACTION": action,
                "ZOHO_OUTPUT_FORMAT": "JSON",
                "ZOHO_API_VERSION": "1.0",
                "ZOHO_SQLQUERY": sql,
                "ZOHO_ERROR_FORMAT": "JSON",
            }

            print("────────────────────────────────────────")
            print("[DEBUG] Trying:", url, "action=", action)
            try:
                resp = requests.post(url, headers=headers, data=form, timeout=60)

                # Si token caducó, refrescamos una vez
                if resp.status_code in (401, 403):
                    print("[DEBUG] Got", resp.status_code, "→ refreshing token and retrying…")
                    ZohoOAuth.clear()
                    headers["Authorization"] = f"Zoho-oauthtoken {ZohoOAuth.get_access_token()}"
                    resp = requests.post(url, headers=headers, data=form, timeout=60)

                if resp.status_code >= 400:
                    print("[ERROR] HTTP:", resp.status_code)
                    print("[ERROR] Body:", resp.text[:600])

                resp.raise_for_status()
                # Si llegamos aquí, ¡funcionó!
                print("[OK] Worked with:", url, "action=", action)
                return resp.json()

            except requests.HTTPError as e:
                last_error = (url, action, resp.status_code if 'resp' in locals() else None, resp.text[:600] if 'resp' in locals() else str(e))
                continue
            except Exception as e:
                last_error = (url, action, None, str(e))
                continue

    # Si ninguna combinación funcionó, devolvemos el último error con detalle
    url, action, status, body = last_error if last_error else ("", "", "", "")
    raise RuntimeError(f"No combination worked. Last error → url={url} action={action} status={status} body={body}")
