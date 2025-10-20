# app/main.py
from __future__ import annotations

import os
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Importa las funciones/errores del cliente Zoho
from .zoho_client import (
    view_smart as z_view_smart,
    ZohoAuthError,
    ZohoApiError,
)

# -----------------------------------------------------------------------------
# Configuración básica
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger("zoho-mcp")

app = FastAPI(
    title="Zoho MCP",
    version="1.0.0",
    description="Servicio mínimo para exportar vistas de Zoho Analytics por API.",
)

# CORS abierto (útil para probar desde Postman/Swagger o páginas internas)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Modelos de entrada/salida
# -----------------------------------------------------------------------------
class ViewSmartIn(BaseModel):
    owner: str = Field(..., description="Cuenta propietaria (email codificable).")
    orgowner: str = Field(..., description="Org owner (no se usa en C1, se acepta para compat).")
    workspace: str = Field(..., description="Nombre del Workspace.")
    workspace_id: Optional[str] = Field(None, description="ID del Workspace (opcional).")
    view: str = Field(..., description="Nombre de la vista/tabla en Zoho.")
    limit: int = Field(100, ge=1, le=10000, description="Límite de filas a traer.")
    offset: int = Field(0, ge=0, description="Desplazamiento para paginación.")

class ViewSmartOut(BaseModel):
    source_url: str
    columns: list
    rows: list

class HealthOut(BaseModel):
    ok: bool
    message: str
    dc: str
    has_token: bool

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/", response_model=dict)
def root():
    """
    Ping de bienvenida.
    """
    return {"ok": True, "service": "zoho-mcp", "endpoints": ["/docs", "/health", "/debug/has_token", "/view_smart"]}


@app.get("/health", response_model=HealthOut)
def health():
    """
    Salud básica del servicio:
    - Detecta presencia de ZOHO_ACCESS_TOKEN (sin validarlo contra Zoho).
    - Muestra el DC configurado.
    """
    dc = os.getenv("ZOHO_DC", "com").strip() or "com"
    has_token = bool(os.getenv("ZOHO_ACCESS_TOKEN"))
    msg = "OK" if has_token else "Falta ZOHO_ACCESS_TOKEN"
    return HealthOut(ok=has_token, message=msg, dc=dc, has_token=has_token)


@app.get("/debug/has_token", response_model=dict)
def debug_has_token():
    """
    Endpoint de diagnóstico: muestra si el token está presente.
    *No* devuelve el token por seguridad.
    """
    return {"has_token": bool(os.getenv("ZOHO_ACCESS_TOKEN", ""))}


@app.post("/view_smart", response_model=ViewSmartOut)
def view_smart(inb: ViewSmartIn):
    """
    Exporta datos de una vista/tabla de Zoho Analytics.
    Implementa la estrategia validada:
      1) Intenta REST v2 (views/tables).
      2) Fallback al EXPORT JSON 'simple path' /api/{owner}/{workspace}/{view}.
    """
    try:
        data = z_view_smart(
            owner=inb.owner,
            orgowner=inb.orgowner,
            workspace=inb.workspace,
            workspace_id=inb.workspace_id or "",
            view=inb.view,
            limit=inb.limit,
            offset=inb.offset,
        )
        # Normaliza salida al esquema ViewSmartOut
        return ViewSmartOut(
            source_url=data.get("source_url", ""),
            columns=data.get("columns", []),
            rows=data.get("rows", []),
        )
    except ZohoAuthError as e:
        # Token faltante o inválido en entorno
        log.error("Auth error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    except ZohoApiError as e:
        # No se pudo exportar por ninguna ruta
        log.error("Zoho API error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("Unexpected error in /view_smart")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

# -----------------------------------------------------------------------------
# Nota de ejecución (Render)
# -----------------------------------------------------------------------------
# En Render, asegúrate de:
# - Environment Variables:
#     ZOHO_ACCESS_TOKEN = <tu_token_oauth_vigente>
#     ZOHO_DC           = com   (o eu/in segun tu data center)
# - Start command: uvicorn app.main:app --host 0.0.0.0 --port 8000
# - Probar en /docs -> POST /view_smart
#
# Campos típicos para /view_smart:
# {
#   "owner": "vacevedo@markem.com.co",
#   "orgowner": "697009942",
#   "workspace": "MARKEM",
#   "workspace_id": "2086177000000002085",
#   "view": "VENDEDORES_DIFERENTES_COMPLETO",
#   "limit": 50,
#   "offset": 0
# }
#
# Si una vista con espacios falla, prueba con guion o underscore según naming real,
# y recuerda que el cliente internamente codifica el nombre para URL.
