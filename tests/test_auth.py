from unittest.mock import MagicMock, patch

import base64
import json
import time

import requests

from willisapi_client import refresh_api_key
from willisapi_client.willisapi_client import authorization_header

API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJ0eXAiOiJhcGlfa2V5In0.c2lnbmF0dXJl"
REFRESH_URL = "https://api.willis.health/api/v2/auth/api-keys/refresh"
POST = "willisapi_client.services.auth.api_keys.requests.post"


def _jwt(exp):
    def part(data):
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")

    return f"{part({'alg': 'HS256'})}.{part({'typ': 'refresh', 'exp': exp})}.sig"


def _response(status, body):
    response = MagicMock(status_code=status)
    response.json.return_value = body
    return response


def test_api_keys_use_the_apikey_scheme():
    assert authorization_header(API_KEY) == f"ApiKey {API_KEY}"


def test_personal_access_tokens_keep_the_token_scheme():
    assert authorization_header("abc123def456") == "token abc123def456"
    assert authorization_header("eyJ.only-two") == "token eyJ.only-two"


@patch(POST)
def test_refresh_returns_the_new_pair(post):
    post.return_value = _response(
        200,
        {
            "api_key": "new-key",
            "refresh_token": "new-refresh",
            "api_key_expires_at": "2027-05-08T08:08:00Z",
            "refresh_token_expires_at": "2027-11-09T08:08:00Z",
        },
    )

    assert refresh_api_key("old-refresh") == {
        "status": "SUCCESS",
        "code": "200",
        "message": "API key refreshed successfully.",
        "api_key": "new-key",
        "refresh_token": "new-refresh",
    }
    assert post.call_args.args[0] == REFRESH_URL
    assert post.call_args.kwargs["json"] == {"refresh_token": "old-refresh"}
    assert "Authorization" not in post.call_args.kwargs["headers"]


@patch(POST)
def test_refresh_uses_the_environment_host(post):
    post.return_value = _response(200, {})

    refresh_api_key("old-refresh", env="dev")

    assert post.call_args.args[0] == (
        "https://api.dev.willis.health/api/v2/auth/api-keys/refresh"
    )


@patch(POST)
def test_refresh_reports_a_rejected_token(post):
    post.return_value = _response(
        401, {"errors": [{"message": "Invalid or expired refresh token."}]}
    )

    assert refresh_api_key("bad") == {
        "status": "FAILED",
        "code": "401",
        "message": "Invalid or missing access token.",
    }


@patch(POST)
def test_refresh_maps_each_status_to_its_message(post):
    for code, message in [
        (400, "Invalid request. Please check the request payload."),
        (403, "You do not have permission to perform this operation."),
        (404, "Resource not found."),
        (500, "Internal server error."),
        (503, "The service is temporarily unavailable. Please try again later."),
    ]:
        post.return_value = _response(code, {})
        assert refresh_api_key("token") == {
            "status": "FAILED",
            "code": str(code),
            "message": message,
        }


@patch(POST)
def test_refresh_without_a_token_is_not_sent(post):
    assert refresh_api_key("") == {
        "status": "FAILED",
        "code": "400",
        "message": "Required fields are missing from the request.",
    }
    post.assert_not_called()


@patch(POST, side_effect=requests.ConnectionError("down"))
def test_refresh_reports_a_network_failure(_post):
    assert refresh_api_key("old-refresh") == {
        "status": "FAILED",
        "code": "503",
        "message": "The service is temporarily unavailable. Please try again later.",
    }


@patch(POST)
def test_an_expired_refresh_token_is_not_sent(post):
    assert refresh_api_key(_jwt(time.time() - 60)) == {
        "status": "FAILED",
        "code": "401",
        "message": "Access token has expired.",
    }
    post.assert_not_called()


@patch(POST)
def test_a_token_expiring_during_the_call_reports_expired(post):
    token = _jwt(time.time() + 3600)
    post.return_value = _response(401, {})

    with patch(
        "willisapi_client.services.auth.api_keys.time.time",
        return_value=time.time() + 7200,
    ):
        result = refresh_api_key(token)

    assert result["message"] == "Access token has expired."


@patch(POST)
def test_a_live_token_rejected_by_the_server_is_invalid(post):
    post.return_value = _response(401, {})

    assert refresh_api_key(_jwt(time.time() + 3600))["message"] == (
        "Invalid or missing access token."
    )
