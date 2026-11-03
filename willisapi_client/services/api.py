# website:   https://www.brooklyn.health

# import the required packages
import requests

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.diarize import (
    willis_diarize_call_remaining,
    willis_diarize,
)
from willisapi_client.services.metadata import upload, processed_upload


def refresh_api_key(refresh_token: str, **kwargs) -> dict:
    """Swap a refresh token for a new API key and refresh token.

    The old API key and refresh token stop working as soon as the server
    answers. Not retried: a refresh token works once, so a retry after a lost
    response would fail anyway.
    """
    wc = WillisapiClient(env=kwargs.get("env"))
    try:
        res = requests.post(
            wc.get_refresh_api_key_url(),
            headers=wc.get_headers(),
            json={"refresh_token": refresh_token},
            timeout=(10, 60),
        )
    except requests.RequestException as ex:
        return {"status": "FAILED", "message": str(ex)}

    if res.status_code == 200:
        return res.json()
    return {"status": "FAILED", "code": str(res.status_code), "message": res.text}
