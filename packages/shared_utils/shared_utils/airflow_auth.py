"""Airflow 3.0 REST API authentication utilities."""

import requests


def get_airflow_auth_headers(api_url: str, username: str, password: str) -> dict:
    """
    Get JWT auth headers for Airflow 3.0 REST API.

    Airflow 3.0 replaced basic auth with JWT token authentication.
    This function obtains a token from /auth/token and returns headers
    ready to use with requests.

    Args:
        api_url: Airflow API base URL (e.g. "http://localhost:8080/api/v2")
        username: Airflow username
        password: Airflow password

    Returns:
        Dict with Authorization and Content-Type headers
    """
    auth_url = api_url.rsplit("/api/", 1)[0] + "/auth/token"
    response = requests.post(
        auth_url,
        json={"username": username, "password": password},
        timeout=10,
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
