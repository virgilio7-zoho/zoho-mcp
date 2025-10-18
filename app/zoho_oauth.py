import requests
from .config import ACCOUNTS_BASE, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN

class ZohoOAuth:
    _access_token = None

    @classmethod
    def get_access_token(cls) -> str:
        if cls._access_token:
            return cls._access_token
        token_url = f"{ACCOUNTS_BASE}/oauth/v2/token"
        data = {
            "refresh_token": REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
        }
        resp = requests.post(token_url, data=data, timeout=30)
        resp.raise_for_status()
        cls._access_token = resp.json()["access_token"]
        return cls._access_token

    @classmethod
    def clear(cls):
        cls._access_token = None
