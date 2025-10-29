# Zoho Analytics MCP (Render-ready)

Servidor FastAPI para consultar Zoho Analytics con OAuth (refresh token) y endpoints:
- `POST /query` -> SQL EXPORT JSON
- `POST /view_smart` -> EXPORT JSON de vista/tabla con LIMIT/OFFSET
- `GET /health`, `GET /token-check`

## Variables de entorno requeridas
- `ZOHO_CLIENT_ID`
- `ZOHO_CLIENT_SECRET`
- `ZOHO_REFRESH_TOKEN`
- `ZOHO_OWNER_ORG`
- `ZOHO_WORKSPACE`
- `ZOHO_ACCOUNTS_BASE` (opcional, default `https://accounts.zoho.com`)
- `ZOHO_ANALYTICS_API_BASE` (opcional, default `https://analyticsapi.zoho.com`)
- `DEFAULT_LIMIT` (opcional, default `1000`)

## Desarrollo local
