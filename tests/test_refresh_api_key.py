from unittest.mock import MagicMock, patch

import requests

import willisapi_client as willisapi


class TestRefreshApiKey:
    @patch("willisapi_client.services.api.requests.post")
    def test_refresh_api_key_success(self, mocked_post):
        body = {
            "status": "SUCCESS",
            "code": "200",
            "message": "API key refreshed successfully.",
            "api_key": "new_key",
            "refresh_token": "new_refresh",
        }
        mocked_post.return_value = MagicMock(status_code=200, json=lambda: body)

        assert willisapi.refresh_api_key(refresh_token="old", env="dev") == body
        args, kwargs = mocked_post.call_args
        assert args[0] == "https://api.dev.willis.health/api/v2/auth/api-key/refresh"
        assert kwargs["json"] == {"refresh_token": "old"}
        assert "Authorization" not in kwargs["headers"]

    @patch("willisapi_client.services.api.requests.post")
    def test_refresh_api_key_invalid_token(self, mocked_post):
        text = '{"error_type":"ValidationError","errors":[{"field":"refresh_token","message":"invalid or expired"}]}'
        mocked_post.return_value = MagicMock(status_code=400, text=text)

        result = willisapi.refresh_api_key(refresh_token="used")
        assert result == {"status": "FAILED", "code": "400", "message": text}

    @patch("willisapi_client.services.api.requests.post")
    def test_refresh_api_key_network_error(self, mocked_post):
        mocked_post.side_effect = requests.ConnectionError("down")

        result = willisapi.refresh_api_key(refresh_token="old")
        assert result == {"status": "FAILED", "message": "down"}
