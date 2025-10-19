import os
import logging
import requests
import json
from typing import Dict, Any, Iterable, Tuple

LOGGER = logging.getLogger("zoho")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

ANALYTICS_BASE = os.getenv("ZOHO_ANALYTICS_BASE", "https://analyticsapi.zoho.com")
ACCESS_TOKEN = os.getenv("ZOHO_ACCESS_TOKEN")  # “Zoho-oauthtoken xxx”
TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))  # s
MAX_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))

def _auth_header() -> Dict[str, str]:
    if not ACCESS_TOKEN:
        raise RuntimeError("Falta ZOHO_ACCESS_TOKEN en variables de entorno.")
    return {"Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}"}

def _export_sql_payload(view_name: str, limit: int, offset: int) -> Dict[str, str]:
    safe_view = view_name.replace('"', '\\"')
    sql = f'SELECT * FROM "{safe_view}" LIMIT {limit} OFFSET {offset}'
    return {
        "ZOHO_ACTION": "EXPORT",
        "ZOHO_OUTPUT_FORMAT": "JSON",
        "ZOHO_ERROR_FORMAT": "JSON",
        "ZOHO_API_VERSION": "1.0",
        "ZOHO_AUTO_IDENTIFY": "true",
        "ZOHO_COLUMN_FORMAT": "JSON",
        "ZOHO_SQL": sql,
    }

def smart_view_export(owner: str, workspace: str, view: str, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    """
    Exporta con API clásico + SQL paginado. Devuelve solo la página pedida.
    """
    url = f"{ANALYTICS_BASE}/api/{owner}/{workspace}/{requests.utils.quote(view, safe='')}"
    payload = _export_sql_payload(view, limit, offset)

    last_err: Tuple[int, str] | None = None
    for attempt in range(MAX_RETRIES + 1):
        LOGGER.info("[SMART] Export SQL: %s  (limit=%s offset=%s) try=%s",
                    url, limit, offset, attempt + 1)
        r = requests.post(
            url,
            headers={**_auth_header(), "Accept": "application/json"},
            data=payload,  # API clásico usa form-encoded
            timeout=TIMEOUT,
        )
        if r.status_code == 200:
            return r.json()
        last_err = (r.status_code, r.text[:600])
        if r.status_code >= 500:
            LOGGER.warning("[SMART] 5xx: %s  body=%s", r.status_code, r.text[:300])
            continue
        break

    code, body = last_err if last_err else (0, "no response")
    raise RuntimeError(f"smart_view_export failed: {url} status={code} body={body}")

# --------- STREAMING GRANDES: NDJSON ---------

def iter_rows_ndjson(owner: str, workspace: str, view: str, page_size: int = 1000) -> Iterable[str]:
    """
    Itera todo el dataset en páginas y emite NDJSON (una línea por fila).
    No guarda todas las filas en memoria: transforma y YIELD línea por línea.
    """
    if page_size <= 0:
        page_size = 1000

    offset = 0
    total = 0
    while True:
        js = smart_view_export(owner, workspace, view, limit=page_size, offset=offset)
        # Estructura típica del API clásico (EXPORT JSON)
        # {"response":{"result":{"column_order":[...], "rows":[ [...], [...], ... ]}}}
        try:
            result = js["response"]["result"]
            cols = result["column_order"]
            rows = result.get("rows", [])
        except Exception as e:
            raise RuntimeError(f"Estructura inesperada de respuesta: {e}; sample={str(js)[:400]}")

        if not rows:
            break

        for row in rows:
            # row puede venir como lista alineada a column_order
            if isinstance(row, dict):
                obj = row
            else:
                obj = {cols[i]: row[i] if i < len(row) else None for i in range(len(cols))}
            yield json.dumps(obj, ensure_ascii=False) + "\n"

        got = len(rows)
        total += got
        offset += got
        LOGGER.info("[STREAM] view=%s page_size=%s got=%s total=%s", view, page_size, got, total)
