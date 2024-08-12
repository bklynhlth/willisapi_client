from unittest.mock import patch

from willisapi_client.services.diarize.willisdiarize_call_remaining import (
    willis_diarize_call_remaining,
)
from willisapi_client.logging_setup import logger as logger
import logging


class TestDiarizeCallsFunction:
    def setup(self):
        self.key = "dummy"

    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request_call_remaining"
    )
    def test_willisdiarize_remaining_calls_failed(self, mocked_data, caplog):
        mocked_data.return_value = {}
        with caplog.at_level(logging.INFO):
            willis_diarize_call_remaining(self.key)
        assert "" in caplog.text

    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request_call_remaining"
    )
    def test_willisdiarize_remaining_calls_missing_auth(self, mocked_data, caplog):
        mocked_data.return_value = {"status_code": 401, "message": "message"}
        with caplog.at_level(logging.INFO):
            willis_diarize_call_remaining(self.key)
        assert "message" in caplog.text

    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request_call_remaining"
    )
    def test_willisdiarize_remaining_calls_success(self, mocked_data, caplog):
        mocked_data.return_value = {
            "status_code": 401,
            "message": "Your account has 10 WillisDiarize API calls remaining.",
        }
        with caplog.at_level(logging.INFO):
            willis_diarize_call_remaining(self.key)
        assert "Your account has 10 WillisDiarize API calls remaining." in caplog.text
