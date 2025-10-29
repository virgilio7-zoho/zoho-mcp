"""
Zoho Analytics client module (v2) for the MCP server.

This module provides a handful of helper functions that wrap the
Zoho Analytics REST API v2. They handle OAuth token management,
HTTP request execution, and simple parameter validation. The function
names mirror the tool names advertised in the official Zoho
Analytics MCP documentation【658604353678378†L430-L449】 for ease of
mapping and integration.

Environment Variables
---------------------
The following environment variables control the client behaviour:

```
ANALYTICS_CLIENT_ID      – OAuth client ID for your Zoho Analytics app.
ANALYTICS_CLIENT_SECRET  – OAuth client secret.
ANALYTICS_REFRESH_TOKEN  – OAuth refresh token used to obtain access tokens.
ANALYTICS_ORG_ID         – ID of your Zoho Analytics organisation (required
                           in headers for most API calls).
ANALYTICS_SERVER_URL     – Base URL of the Analytics API (defaults to
                           "https://analyticsapi.zoho.com").
ACCOUNTS_SERVER_URL      – Base URL of the Accounts API (defaults to
                           "https://accounts.zoho.com").
ANALYTICS_MCP_DATA_DIR   – Directory where exported files may be stored
                           (defaults to "/tmp").
ZOHO_ACCESS_TOKEN        – Cached OAuth access token; this module will
                           refresh it as needed.
```

If any of the mandatory variables (client ID, secret, refresh token) are
missing, calls that require authentication will raise a `RuntimeError`.

References
----------
* Zoho Analytics REST API v2 documentation
  - Get Views: lists views in a workspace and supports keyword filtering【985669613019000†L53-L56】.
  - Get View Details: retrieves a view's metadata using only its ID【357598884937503†L126-L134】.
  - Export Data: exports data from a view using the bulk API【215211381353514†L1337-L1343】.
* Zoho Analytics MCP server description: lists available tools and their
  high-level purpose【658604353678378†L430-L449】.
"""

from __future__ import annotations

import os
from typing import Optional, Dict, Any
import requests
from urllib.parse import urlencode

# -----------------------------------------------------------------------------
# Environment configuration
# -----------------------------------------------------------------------------

ACCOUNTS_SERVER_URL = os.getenv("ACCOUNTS_SERVER_URL", "https://accounts.zoho.com").rstrip("/")
ANALYTICS_SERVER_URL = os.getenv("ANALYTICS_SERVER_URL", "https://analyticsapi.zoho.com").rstrip("/")
ANALYTICS_CLIENT_ID = os.getenv("ANALYTICS_CLIENT_ID")
ANALYTICS_CLIENT_SECRET = os.getenv("ANALYTICS_CLIENT_SECRET")
ANALYTICS_REFRESH_TOKEN = os.getenv("ANALYTICS_REFRESH_TOKEN")
ANALYTICS_ORG_ID = os.getenv("ANALYTICS_ORG_ID")
ANALYTICS_MCP_DATA_DIR = os.getenv("ANALYTICS_MCP_DATA_DIR", "/tmp")


def get_access_token(force_refresh: bool = False) -> str:
    """Return a valid OAuth access token.

    This helper caches the token in the ``ZOHO_ACCESS_TOKEN`` environment
    variable to avoid unnecessary refreshes. When called with ``force_refresh``
    set to ``True``, a new token is fetched regardless of the cached value.

    Raises
    ------
    RuntimeError
        If OAuth credentials are missing or the token refresh request fails.
    """
    token = os.getenv("ZOHO_ACCESS_TOKEN")
    has_oauth = all([ANALYTICS_CLIENT_ID, ANALYTICS_CLIENT_SECRET, ANALYTICS_REFRESH_TOKEN])

    if not token or force_refresh:
        if not has_oauth:
            raise RuntimeError(
                "Faltan credenciales OAuth (client_id/secret/refresh_token)."
            )
        url = f"{ACCOUNTS_SERVER_URL}/oauth/v2/token"
        data = {
            "refresh_token": ANALYTICS_REFRESH_TOKEN,
            "client_id": ANALYTICS_CLIENT_ID,
            "client_secret": ANALYTICS_CLIENT_SECRET,
            "grant_type": "refresh_token",
        }
        r = requests.post(url, data=data, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(
                f"Error refrescando token: {r.status_code} {r.text}"
            )
        token = r.json().get("access_token")
        if not token:
            raise RuntimeError(f"Respuesta sin access_token: {r.text}")
        os.environ["ZOHO_ACCESS_TOKEN"] = token
        print("🔁 Nuevo access token obtenido.")
    return token


def _auth_headers(token: Optional[str] = None) -> Dict[str, str]:
    """Construct HTTP headers with OAuth and organisation ID."""
    t = token or get_access_token()
    return {
        "Authorization": f"Zoho-oauthtoken {t}",
        "Accept": "application/json",
        # Some endpoints require ZANALYTICS-ORGID; send it even if empty
        "ZANALYTICS-ORGID": ANALYTICS_ORG_ID or "",
    }


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Internal helper to perform a GET request and return JSON."""
    url = f"{ANALYTICS_SERVER_URL}{path}"
    r = requests.get(url, headers=_auth_headers(), params=params or {}, timeout=60)
    if r.status_code == 401:
        # token expired → refresh and retry once
        r = requests.get(
            url,
            headers=_auth_headers(get_access_token(True)),
            params=params or {},
            timeout=60,
        )
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()


def _post(path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
    """Internal helper to perform a POST request and return JSON."""
    url = f"{ANALYTICS_SERVER_URL}{path}"
    r = requests.post(url, headers=_auth_headers(), json=json_body, timeout=120)
    if r.status_code == 401:
        r = requests.post(
            url,
            headers=_auth_headers(get_access_token(True)),
            json=json_body,
            timeout=120,
        )
    if r.status_code != 200:
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")
    return r.json()


# -----------------------------------------------------------------------------
# Public tool functions
# -----------------------------------------------------------------------------

def get_workspaces_list() -> Dict[str, Any]:
    """List all workspaces in the organisation.

    Implements GET ``/restapi/v2/workspaces``. See the official
    documentation for details【658604353678378†L430-L449】.
    """
    path = "/restapi/v2/workspaces"
    return _get(path)


def search_views(
    workspace_id: str,
    q: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> Dict[str, Any]:
    """Fetch views in a workspace filtered by an optional keyword.

    Parameters
    ----------
    workspace_id : str
        Identifier of the workspace. Must not be empty.
    q : str | None
        Optional search keyword. According to the metadata API, the parameter
        ``keyword`` filters views by name【985669613019000†L53-L56】. The existing
        implementation uses the ``search`` query parameter for backward
        compatibility with earlier clients; both keywords are accepted by the
        Zoho API as of 2025. If provided, the API returns only views whose
        metadata matches the keyword. If omitted, all views are returned.
    limit : int
        Maximum number of results to return. Defaults to 200.
    offset : int
        Index of the first result to return (pagination). Defaults to 0.

    Returns
    -------
    dict
        JSON object containing the list of matching views.
    """
    if not workspace_id:
        raise ValueError("workspace_id es obligatorio")
    path = f"/restapi/v2/workspaces/{workspace_id}/views"
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if q:
        # Use the "search" parameter for now; Zoho also supports "keyword" in the CONFIG
        params["search"] = q
    return _get(path, params)


def get_view_details(workspace_id: str, view_id_or_name: str) -> Dict[str, Any]:
    """Fetch details of a specific view by its ID or name.

    According to the Zoho Analytics v2 REST API documentation, view details are
    retrieved via the endpoint ``/restapi/v2/views/<view-id>`` and do not
    require the workspace identifier in the URL【357598884937503†L126-L134】. Passing
    the workspace ID as part of the path results in a 400 error with an
    ``INVALID_METHOD`` summary. For compatibility with earlier versions of
    this client (and the public MCP specification), the ``workspace_id``
    parameter remains in the signature but is ignored.

    Parameters
    ----------
    workspace_id : str
        Identifier of the workspace. Unused but retained for compatibility.
    view_id_or_name : str
        Identifier or exact name of the view whose metadata is to be
        fetched. Must not be empty.

    Returns
    -------
    dict
        A dictionary containing the view details returned by the API.

    Raises
    ------
    ValueError
        If ``view_id_or_name`` is empty.
    RuntimeError
        If the underlying HTTP request fails (non‑200 status code).
    """
    if not view_id_or_name:
        raise ValueError("view_id_or_name es obligatorio")
    # Build the correct path without the workspace ID
    path = f"/restapi/v2/views/{view_id_or_name}"
    return _get(path)


def export_view(
    workspace_id: str,
    view: str,
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    """Export data from a view as JSON.

    Implements GET ``/restapi/v2/workspaces/{workspace_id}/views/{view}/data``
    with query parameters ``format=json``, ``limit`` and ``offset``. The
    official API documentation illustrates bulk exports using a ``CONFIG``
    parameter【215211381353514†L1337-L1343】. However, the simpler synchronous
    variant used here is sufficient for small data sets and supports
    pagination via ``limit`` and ``offset``. The returned JSON object
    contains the exported rows.

    Parameters
    ----------
    workspace_id : str
        Workspace identifier. Must not be empty.
    view : str
        Identifier or name of the view. Must not be empty.
    limit : int
        Number of rows to return. Defaults to 100. Maximum allowed by the
        API is 10,000.
    offset : int
        Starting index for pagination. Defaults to 0.

    Returns
    -------
    dict
        JSON object containing the exported rows.
    """
    if not workspace_id or not view:
        raise ValueError("workspace_id y view son obligatorios")
    path = f"/restapi/v2/workspaces/{workspace_id}/views/{view}/data"
    params = {"format": "json", "limit": limit, "offset": offset}
    # Construct full URL manually to include querystring; using params in
    # requests.get would encode them again when path already includes '?' in some
    # clients. This approach ensures the query parameters are appended exactly
    # once.
    url = f"{ANALYTICS_SERVER_URL}{path}?{urlencode(params)}"
    r = requests.get(url, headers=_auth_headers(), timeout=120)
    if r.status_code == 401:
        r = requests.get(
            url,
            headers=_auth_headers(get_access_token(True)),
            timeout=120,
        )
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()


def query_data(workspace_id: str, sql: str) -> Dict[str, Any]:
    """Execute a SQL query against the specified workspace.

    Implements POST ``/restapi/v2/workspaces/{workspace_id}/sql`` with a
    JSON body ``{"query": sql}``. Returns the query results as a JSON
    object. Any errors from the server result in a ``RuntimeError``.

    Parameters
    ----------
    workspace_id : str
        Identifier of the workspace. Must not be empty.
    sql : str
        The SQL query to execute. Must not be empty.

    Returns
    -------
    dict
        JSON object containing the query results.
    """
    if not workspace_id or not sql:
        raise ValueError("workspace_id y sql son obligatorios")
    path = f"/restapi/v2/workspaces/{workspace_id}/sql"
    body = {"query": sql}
    return _post(path, body)


def health_info() -> Dict[str, Any]:
    """Return basic health and configuration information."""
    token = os.getenv("ZOHO_ACCESS_TOKEN", "")
    return {
        "status": "up",
        "mode": "v2",
        "org_id": ANALYTICS_ORG_ID,
        "server": ANALYTICS_SERVER_URL,
        "data_dir": ANALYTICS_MCP_DATA_DIR,
        "token_len": len(token),
    }


__all__ = [
    "get_workspaces_list",
    "search_views",
    "get_view_details",
    "export_view",
    "query_data",
    "health_info",
    ]
