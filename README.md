# Zoho Analytics MCP (Docker + FastAPI)

Servidor HTTP sencillo para consultar Zoho Analytics vía SQL (Export API) y exponer endpoints para usarlos como MCP remoto.

## Endpoints
- `GET /health` -> estado
- `POST /query` -> ejecuta SQL en Zoho Analytics
- `POST /helpers/group_sum` -> helper para agrupar y sumar

## Variables de entorno requeridas
- `ZOHO_ACCOUNTS_BASE` (p.ej. https://accounts.zoho.com)
- `ZOHO_ANALYTICS_API_BASE` (p.ej. https://analyticsapi.zoho.com)
- `ZOHO_CLIENT_ID`
- `ZOHO_CLIENT_SECRET`
- `ZOHO_REFRESH_TOKEN`
- `ZOHO_OWNER_ORG` (owner email u org name)
- `ZOHO_WORKSPACE`
- `ZOHO_VIEW` (vista/tabla por defecto)

## Ejecutar local con Docker
```bash
cp .env.example .env  # edita valores
docker compose up --build
curl http://localhost:8000/health
```

## Despliegue en Render
Crea un nuevo Web Service desde este repo (Render detecta el Dockerfile). Configura las variables de entorno anteriores. Puerto: 8000.
