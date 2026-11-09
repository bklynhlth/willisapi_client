import base64
import json
import time

import requests

from willisapi_client.logging_setup import logger as logger
from willisapi_client.willisapi_client import WillisapiClient

_SUCCESS_MESSAGE = "API key refreshed successfully."
_MISSING_FIELDS_MESSAGE = "Required fields are missing from the request."
_EXPIRED_MESSAGE = "Access token has expired."
#: Message per HTTP status of a failed refresh.
_FAILURE_MESSAGES = {
    400: "Invalid request. Please check the request payload.",
    401: "Invalid or missing access token.",
    403: "You do not have permission to perform this operation.",
    404: "Resource not found.",
    500: "Internal server error.",
    503: "The service is temporarily unavailable. Please try again later.",
}


def _failed(code: int, message: str = None) -> dict:
    return {
        "status": "FAILED",
        "code": str(code),
        "message": message or _FAILURE_MESSAGES[code],
    }


def _is_expired(token: str) -> bool:
    """Whether the token's ``exp`` claim has passed.

    Read without verifying the signature: it only picks the message to show,
    and the API checks the token itself.
    """
    try:
        payload = token.split(".")[1]
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
        return float(claims["exp"]) <= time.time()
    except (IndexError, KeyError, TypeError, ValueError):
        return False


def refresh_api_key(refresh_token: str, env: str = None) -> dict:
    """Swap a refresh token for a new API key and refresh token.

    Returns ``status`` (``SUCCESS`` or ``FAILED``), ``code`` and ``message``,
    plus the new ``api_key`` and ``refresh_token`` on success. Store both new
    tokens: the previous API key and refresh token stop working at once.
    """
    if not refresh_token:
        return _failed(400, _MISSING_FIELDS_MESSAGE)
    if _is_expired(refresh_token):
        return _failed(401, _EXPIRED_MESSAGE)

    wc = WillisapiClient(env=env)
    try:
        response = requests.post(
            wc.get_api_key_refresh_url(),
            headers=wc.get_headers(),
            json={"refresh_token": refresh_token},
            timeout=30,
        )
    except requests.RequestException as error:
        logger.error(f"API key refresh failed: {error}")
        return _failed(503)

    if response.status_code == 200:
        try:
            body = response.json()
        except ValueError:
            body = {}
        return {
            "status": "SUCCESS",
            "code": "200",
            "message": _SUCCESS_MESSAGE,
            "api_key": body.get("api_key"),
            "refresh_token": body.get("refresh_token"),
        }

    logger.error(f"API key refresh failed with HTTP {response.status_code}")
    if response.status_code == 401 and _is_expired(refresh_token):
        return _failed(401, _EXPIRED_MESSAGE)
    if response.status_code in _FAILURE_MESSAGES:
        return _failed(response.status_code)
    return _failed(500 if response.status_code >= 500 else 400)
