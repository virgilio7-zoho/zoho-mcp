import os

ACCOUNTS_BASE   = os.getenv("ZOHO_ACCOUNTS_BASE", "https://accounts.zoho.com")
ANALYTICS_BASE  = os.getenv("ZOHO_ANALYTICS_API_BASE", "https://analyticsapi.zoho.com")

CLIENT_ID       = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET   = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN   = os.getenv("ZOHO_REFRESH_TOKEN")

OWNER_ORG       = os.getenv("ZOHO_OWNER_ORG")  # owner email u org name
WORKSPACE       = os.getenv("ZOHO_WORKSPACE")
VIEW            = os.getenv("ZOHO_VIEW")       # vista/tabla por defecto
