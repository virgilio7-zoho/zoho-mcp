import os
import logging
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from app.zoho_client import smart_view_export, iter_rows_ndjson

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("api")

app = FastAPI(title="Zoho MCP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ViewSmartReq(BaseModel):
    owner: str = Field(..., description="Ej: vacevedo@markem.com.co")
    workspace: str = Field(..., description="Workspace (tal cual en Zoho)")
    view: str = Field(..., description="Vista/tabla exacta (con espacios si aplica)")
    limit: int = Field(100, ge=1, le=1000)
    offset: int = Field(0, ge=0)

class StreamReq(BaseModel):
    owner: str
    workspace: str
    view: str
    page_size: int = Field(1000, ge=10, le=5000)
    filename: str | None = Field(default=None, description="Nombre del archivo a descargar")

@app.get("/")
def root():
    return {"status": "ok"}

# Página única (pequeña) — útil para pruebas/UI
@app.post("/view_smart")
def view_smart(req: ViewSmartReq = Body(...)):
    try:
        data = smart_view_export(
            owner=req.owner,
            workspace=req.workspace,
            view=req.view,
            limit=req.limit,
            offset=req.offset,
        )
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# STREAMING TOTAL como NDJSON — cero picos de memoria
@app.post("/view_stream")
def view_stream(req: StreamReq = Body(...)):
    try:
        generator = iter_rows_ndjson(
            owner=req.owner,
            workspace=req.workspace,
            view=req.view,
            page_size=req.page_size,
        )
        filename = req.filename or f"{req.view.replace(' ', '_')}.ndjson"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(generator, media_type="application/x-ndjson", headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
